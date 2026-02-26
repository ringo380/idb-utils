use std::io::Write;
use std::path::Path;

use colored::Colorize;
use rayon::prelude::*;

use crate::cli::{create_progress_bar, wprintln};
use crate::innodb::compat::{
    build_compat_report, check_compatibility, extract_tablespace_info, MysqlVersion,
    ScanCompatReport, ScanFileResult, Severity,
};
use crate::util::fs::find_tablespace_files;
use crate::IdbError;

/// Options for the `inno compat` subcommand.
pub struct CompatOptions {
    /// Path to a single InnoDB data file (.ibd).
    pub file: Option<String>,
    /// Path to a data directory to scan.
    pub scan: Option<String>,
    /// Target MySQL version (e.g., "8.4.0", "9.0.0").
    pub target: String,
    /// Show detailed check information.
    pub verbose: bool,
    /// Output in JSON format.
    pub json: bool,
    /// Override page size (default: auto-detect).
    pub page_size: Option<u32>,
    /// Path to MySQL keyring file for decrypting encrypted tablespaces.
    pub keyring: Option<String>,
    /// Use memory-mapped I/O.
    pub mmap: bool,
    /// Maximum directory recursion depth (None = default 2, Some(0) = unlimited).
    pub depth: Option<u32>,
}

/// Execute the compat subcommand.
pub fn execute(opts: &CompatOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    if opts.file.is_some() && opts.scan.is_some() {
        return Err(IdbError::Argument(
            "--file and --scan are mutually exclusive".to_string(),
        ));
    }

    if opts.file.is_none() && opts.scan.is_none() {
        return Err(IdbError::Argument(
            "Either --file or --scan must be provided".to_string(),
        ));
    }

    let target = MysqlVersion::parse(&opts.target)?;

    if opts.scan.is_some() {
        execute_scan(opts, &target, writer)
    } else {
        execute_single(opts, &target, writer)
    }
}

fn execute_single(
    opts: &CompatOptions,
    target: &MysqlVersion,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    let file = opts.file.as_ref().unwrap();

    let mut ts = crate::cli::open_tablespace(file, opts.page_size, opts.mmap)?;

    if let Some(ref keyring_path) = opts.keyring {
        crate::cli::setup_decryption(&mut ts, keyring_path)?;
    }

    let info = extract_tablespace_info(&mut ts)?;
    let report = build_compat_report(&info, target, file);

    if opts.json {
        let json = serde_json::to_string_pretty(&report)
            .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
        wprintln!(writer, "{}", json)?;
    } else {
        wprintln!(writer, "Compatibility Check: {}", file)?;
        wprintln!(writer, "  Target version: MySQL {}", report.target_version)?;
        if let Some(ref sv) = report.source_version {
            wprintln!(writer, "  Source version: MySQL {}", sv)?;
        }
        wprintln!(writer)?;

        if report.checks.is_empty() {
            wprintln!(writer, "  No compatibility issues found.")?;
        } else {
            for check in &report.checks {
                let severity_str = match check.severity {
                    Severity::Error => "ERROR".red().to_string(),
                    Severity::Warning => "WARN".yellow().to_string(),
                    Severity::Info => "INFO".blue().to_string(),
                };
                wprintln!(
                    writer,
                    "  [{}] {}: {}",
                    severity_str,
                    check.check,
                    check.message
                )?;
                if opts.verbose {
                    if let Some(ref cv) = check.current_value {
                        wprintln!(writer, "         Current: {}", cv)?;
                    }
                    if let Some(ref exp) = check.expected {
                        wprintln!(writer, "         Expected: {}", exp)?;
                    }
                }
            }
        }

        wprintln!(writer)?;
        let overall = if report.compatible {
            "COMPATIBLE".green().to_string()
        } else {
            "INCOMPATIBLE".red().to_string()
        };
        wprintln!(
            writer,
            "  Result: {} ({} errors, {} warnings, {} info)",
            overall,
            report.summary.errors,
            report.summary.warnings,
            report.summary.info
        )?;
    }

    Ok(())
}

fn check_file_compat(
    path: &Path,
    datadir: &Path,
    target: &MysqlVersion,
    page_size_override: Option<u32>,
    keyring: &Option<String>,
    use_mmap: bool,
) -> ScanFileResult {
    let display = path.strip_prefix(datadir).unwrap_or(path);
    let display_str = display.display().to_string();
    let path_str = path.to_string_lossy();

    let mut ts = match crate::cli::open_tablespace(&path_str, page_size_override, use_mmap) {
        Ok(t) => t,
        Err(e) => {
            return ScanFileResult {
                file: display_str,
                compatible: false,
                error: Some(e.to_string()),
                checks: Vec::new(),
            };
        }
    };

    if let Some(ref kp) = keyring {
        let _ = crate::cli::setup_decryption(&mut ts, kp);
    }

    let info = match extract_tablespace_info(&mut ts) {
        Ok(i) => i,
        Err(e) => {
            return ScanFileResult {
                file: display_str,
                compatible: false,
                error: Some(e.to_string()),
                checks: Vec::new(),
            };
        }
    };

    let checks = check_compatibility(&info, target);
    let compatible = !checks.iter().any(|c| c.severity == Severity::Error);

    ScanFileResult {
        file: display_str,
        compatible,
        error: None,
        checks,
    }
}

fn execute_scan(
    opts: &CompatOptions,
    target: &MysqlVersion,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    let scan_dir = opts.scan.as_ref().unwrap();
    let datadir = Path::new(scan_dir);

    if !datadir.is_dir() {
        return Err(IdbError::Argument(format!(
            "Data directory does not exist: {}",
            scan_dir
        )));
    }

    let ibd_files = find_tablespace_files(datadir, &["ibd"], opts.depth)?;

    if ibd_files.is_empty() {
        if opts.json {
            let report = ScanCompatReport {
                target_version: target.to_string(),
                files_scanned: 0,
                files_compatible: 0,
                files_incompatible: 0,
                files_error: 0,
                results: Vec::new(),
            };
            let json = serde_json::to_string_pretty(&report)
                .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
            wprintln!(writer, "{}", json)?;
        } else {
            wprintln!(writer, "No .ibd files found in {}", scan_dir)?;
        }
        return Ok(());
    }

    let pb = if !opts.json {
        Some(create_progress_bar(ibd_files.len() as u64, "files"))
    } else {
        None
    };

    let page_size = opts.page_size;
    let keyring = opts.keyring.clone();
    let use_mmap = opts.mmap;

    let mut results: Vec<ScanFileResult> = ibd_files
        .par_iter()
        .map(|path| {
            let r = check_file_compat(path, datadir, target, page_size, &keyring, use_mmap);
            if let Some(ref pb) = pb {
                pb.inc(1);
            }
            r
        })
        .collect();

    if let Some(ref pb) = pb {
        pb.finish_and_clear();
    }

    // Sort by file path for deterministic output
    results.sort_by(|a, b| a.file.cmp(&b.file));

    let files_scanned = results.len();
    let files_compatible = results
        .iter()
        .filter(|r| r.compatible && r.error.is_none())
        .count();
    let files_incompatible = results
        .iter()
        .filter(|r| !r.compatible && r.error.is_none())
        .count();
    let files_error = results.iter().filter(|r| r.error.is_some()).count();

    if opts.json {
        let report = ScanCompatReport {
            target_version: target.to_string(),
            files_scanned,
            files_compatible,
            files_incompatible,
            files_error,
            results,
        };
        let json = serde_json::to_string_pretty(&report)
            .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
        wprintln!(writer, "{}", json)?;
    } else {
        wprintln!(
            writer,
            "Scanning {} for MySQL {} compatibility ({} files)...\n",
            scan_dir,
            target,
            files_scanned
        )?;

        for r in &results {
            if let Some(ref err) = r.error {
                wprintln!(writer, "  {:<50} {}   {}", r.file, "ERROR".yellow(), err)?;
            } else if r.compatible {
                wprintln!(writer, "  {:<50} {}", r.file, "OK".green())?;
            } else {
                wprintln!(writer, "  {:<50} {}", r.file, "INCOMPATIBLE".red())?;
            }

            if opts.verbose {
                for check in &r.checks {
                    if check.severity != Severity::Info {
                        let severity_str = match check.severity {
                            Severity::Info => "INFO".green().to_string(),
                            Severity::Warning => "WARN".yellow().to_string(),
                            Severity::Error => "ERROR".red().to_string(),
                        };
                        wprintln!(
                            writer,
                            "    [{}] {}: {}",
                            severity_str,
                            check.check,
                            check.message
                        )?;
                    }
                }
            }
        }

        wprintln!(writer)?;
        wprintln!(writer, "Summary:")?;
        wprintln!(
            writer,
            "  Files: {} ({} compatible, {} incompatible{})",
            files_scanned,
            files_compatible,
            files_incompatible,
            if files_error > 0 {
                format!(", {} error", files_error)
            } else {
                String::new()
            }
        )?;
    }

    Ok(())
}
