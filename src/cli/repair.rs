use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use colored::Colorize;
use rayon::prelude::*;
use serde::Serialize;

use crate::cli::{create_progress_bar, wprintln};
use crate::innodb::checksum::{validate_checksum, validate_lsn, ChecksumAlgorithm};
use crate::innodb::write;
use crate::util::audit::AuditLogger;
use crate::util::fs::find_tablespace_files;
use crate::IdbError;

/// Options for the `inno repair` subcommand.
pub struct RepairOptions {
    /// Path to the InnoDB tablespace file (.ibd).
    pub file: Option<String>,
    /// Repair all .ibd files under a data directory.
    pub batch: Option<String>,
    /// Repair only a specific page number.
    pub page: Option<u64>,
    /// Checksum algorithm to use: "auto", "crc32c", "innodb", "full_crc32".
    pub algorithm: String,
    /// Skip creating a backup before repair.
    pub no_backup: bool,
    /// Show what would be repaired without modifying the file.
    pub dry_run: bool,
    /// Show per-page details.
    pub verbose: bool,
    /// Emit output as JSON.
    pub json: bool,
    /// Override the auto-detected page size.
    pub page_size: Option<u32>,
    /// Path to MySQL keyring file for decrypting encrypted tablespaces.
    pub keyring: Option<String>,
    /// Use memory-mapped I/O for file access.
    pub mmap: bool,
    /// Audit logger for recording write operations.
    pub audit_logger: Option<Arc<AuditLogger>>,
}

#[derive(Serialize)]
struct RepairReport {
    file: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    backup_path: Option<String>,
    algorithm: String,
    dry_run: bool,
    total_pages: u64,
    already_valid: u64,
    repaired: u64,
    empty: u64,
    pages: Vec<PageRepairInfo>,
}

#[derive(Serialize)]
struct PageRepairInfo {
    page_number: u64,
    action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    old_checksum: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    new_checksum: Option<u32>,
}

/// Parse an algorithm string into a ChecksumAlgorithm.
fn parse_algorithm(s: &str) -> Result<Option<ChecksumAlgorithm>, IdbError> {
    match s.to_lowercase().as_str() {
        "auto" => Ok(None),
        "crc32c" | "crc32" => Ok(Some(ChecksumAlgorithm::Crc32c)),
        "innodb" | "legacy" => Ok(Some(ChecksumAlgorithm::InnoDB)),
        "full_crc32" | "mariadb" => Ok(Some(ChecksumAlgorithm::MariaDbFullCrc32)),
        _ => Err(IdbError::Argument(format!(
            "Unknown checksum algorithm '{}'. Use: auto, crc32c, innodb, full_crc32",
            s
        ))),
    }
}

fn algo_name(algorithm: ChecksumAlgorithm) -> &'static str {
    match algorithm {
        ChecksumAlgorithm::Crc32c => "crc32c",
        ChecksumAlgorithm::InnoDB => "innodb",
        ChecksumAlgorithm::MariaDbFullCrc32 => "full_crc32",
        ChecksumAlgorithm::None => "none",
    }
}

/// Recalculate and write correct checksums for corrupt pages.
pub fn execute(opts: &RepairOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    match (&opts.file, &opts.batch) {
        (Some(_), Some(_)) => {
            return Err(IdbError::Argument(
                "--file and --batch are mutually exclusive".to_string(),
            ));
        }
        (None, None) => {
            return Err(IdbError::Argument(
                "Either --file or --batch is required".to_string(),
            ));
        }
        (_, Some(_)) if opts.page.is_some() => {
            return Err(IdbError::Argument(
                "--page cannot be used with --batch".to_string(),
            ));
        }
        _ => {}
    }

    if opts.batch.is_some() {
        execute_batch(opts, writer)
    } else {
        execute_single(opts, writer)
    }
}

/// Repair a single tablespace file.
fn execute_single(opts: &RepairOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    let file = opts.file.as_ref().unwrap();

    // Open tablespace read-only to get metadata
    let mut ts = crate::cli::open_tablespace(file, opts.page_size, opts.mmap)?;

    if let Some(ref keyring_path) = opts.keyring {
        crate::cli::setup_decryption(&mut ts, keyring_path)?;
    }

    let page_size = ts.page_size();
    let page_count = ts.page_count();
    let vendor_info = ts.vendor_info().clone();

    // Determine algorithm
    let explicit_algo = parse_algorithm(&opts.algorithm)?;
    let algorithm = match explicit_algo {
        Some(algo) => algo,
        None => {
            // Auto-detect from page 0
            let page0 = ts.read_page(0)?;
            write::detect_algorithm(&page0, page_size, Some(&vendor_info))
        }
    };

    // Create backup unless --no-backup or --dry-run
    let backup_path = if !opts.no_backup && !opts.dry_run {
        let path = write::create_backup(file)?;
        if !opts.json {
            wprintln!(writer, "Backup created: {}", path.display())?;
        }
        if let Some(ref logger) = opts.audit_logger {
            let _ = logger.log_backup(file, &path.display().to_string());
        }
        Some(path)
    } else {
        None
    };

    // Determine page range
    let (start_page, end_page) = match opts.page {
        Some(p) => {
            if p >= page_count {
                return Err(IdbError::Argument(format!(
                    "Page {} out of range (tablespace has {} pages)",
                    p, page_count
                )));
            }
            (p, p + 1)
        }
        None => (0, page_count),
    };
    let scan_count = end_page - start_page;

    let pb = if !opts.json && scan_count > 1 && !opts.dry_run {
        Some(create_progress_bar(scan_count, "pages"))
    } else {
        None
    };

    let aname = algo_name(algorithm);

    if !opts.json && !opts.dry_run {
        wprintln!(writer, "Repairing {} using {} algorithm...", file, aname)?;
    } else if !opts.json && opts.dry_run {
        wprintln!(
            writer,
            "Dry run: scanning {} using {} algorithm...",
            file,
            aname
        )?;
    }

    let mut already_valid = 0u64;
    let mut repaired = 0u64;
    let mut empty = 0u64;
    let mut page_details: Vec<PageRepairInfo> = Vec::new();

    for page_num in start_page..end_page {
        let mut page_data = write::read_page_raw(file, page_num, page_size)?;

        // Check if page is empty (all zeros)
        if page_data.iter().all(|&b| b == 0) {
            empty += 1;
            if let Some(ref pb) = pb {
                pb.inc(1);
            }
            continue;
        }

        // Validate current checksum
        let result = validate_checksum(&page_data, page_size, Some(&vendor_info));
        let lsn_ok = validate_lsn(&page_data, page_size);

        if result.valid && lsn_ok {
            already_valid += 1;
            if let Some(ref pb) = pb {
                pb.inc(1);
            }
            continue;
        }

        // Page needs repair
        let (old_checksum, new_checksum) =
            write::fix_page_checksum(&mut page_data, page_size, algorithm);

        if !opts.dry_run {
            write::write_page(file, page_num, page_size, &page_data)?;
            if let Some(ref logger) = opts.audit_logger {
                let _ = logger.log_page_write(
                    file,
                    page_num,
                    "repair",
                    Some(old_checksum),
                    Some(new_checksum),
                );
            }
        }

        repaired += 1;

        if opts.verbose && !opts.json {
            let action = if opts.dry_run {
                "would repair"
            } else {
                "repaired"
            };
            let mut detail = format!(
                "Page {:>4}: {} (0x{:08X} -> 0x{:08X})",
                page_num, action, old_checksum, new_checksum
            );
            if !result.valid {
                detail.push_str(" [checksum mismatch]");
            }
            if !lsn_ok {
                detail.push_str(" [LSN mismatch]");
            }
            wprintln!(writer, "{}", detail)?;
        }

        page_details.push(PageRepairInfo {
            page_number: page_num,
            action: if opts.dry_run {
                "would_repair".to_string()
            } else {
                "repaired".to_string()
            },
            old_checksum: Some(old_checksum),
            new_checksum: Some(new_checksum),
        });

        if let Some(ref pb) = pb {
            pb.inc(1);
        }
    }

    if let Some(pb) = pb {
        pb.finish_and_clear();
    }

    if opts.json {
        let report = RepairReport {
            file: file.clone(),
            backup_path: backup_path.map(|p| p.display().to_string()),
            algorithm: aname.to_string(),
            dry_run: opts.dry_run,
            total_pages: scan_count,
            already_valid,
            repaired,
            empty,
            pages: page_details,
        };
        let json = serde_json::to_string_pretty(&report)
            .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
        wprintln!(writer, "{}", json)?;
    } else {
        wprintln!(writer)?;
        wprintln!(writer, "Repair Summary:")?;
        wprintln!(writer, "  Algorithm:     {}", aname)?;
        wprintln!(writer, "  Total pages:   {:>4}", scan_count)?;
        wprintln!(writer, "  Already valid: {:>4}", already_valid)?;
        if repaired > 0 {
            let label = if opts.dry_run {
                "Would repair"
            } else {
                "Repaired"
            };
            wprintln!(
                writer,
                "  {}:     {:>4}",
                label,
                format!("{}", repaired).green()
            )?;
        } else {
            wprintln!(writer, "  Repaired:      {:>4}", repaired)?;
        }
        wprintln!(writer, "  Empty:         {:>4}", empty)?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Batch repair mode (#89)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct BatchRepairReport {
    datadir: String,
    dry_run: bool,
    algorithm: String,
    files: Vec<FileRepairResult>,
    summary: BatchRepairSummary,
}

#[derive(Serialize, Clone)]
struct FileRepairResult {
    file: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    backup_path: Option<String>,
    total_pages: u64,
    already_valid: u64,
    repaired: u64,
    empty: u64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    errors: Vec<String>,
}

#[derive(Serialize)]
struct BatchRepairSummary {
    total_files: usize,
    files_repaired: usize,
    files_already_valid: usize,
    files_error: usize,
    total_pages_scanned: u64,
    total_pages_repaired: u64,
}

/// Repair a single file for batch mode. Returns the result for aggregation.
#[allow(clippy::too_many_arguments)]
fn repair_file(
    path: &Path,
    datadir: &Path,
    explicit_algo: Option<ChecksumAlgorithm>,
    no_backup: bool,
    dry_run: bool,
    page_size_override: Option<u32>,
    keyring: &Option<String>,
    use_mmap: bool,
    audit_logger: &Option<Arc<AuditLogger>>,
) -> FileRepairResult {
    let display = path.strip_prefix(datadir).unwrap_or(path);
    let display_str = display.display().to_string();
    let path_str = path.to_string_lossy().to_string();

    let mut ts = match crate::cli::open_tablespace(&path_str, page_size_override, use_mmap) {
        Ok(t) => t,
        Err(e) => {
            return FileRepairResult {
                file: display_str,
                backup_path: None,
                total_pages: 0,
                already_valid: 0,
                repaired: 0,
                empty: 0,
                errors: vec![e.to_string()],
            };
        }
    };

    if let Some(ref kp) = keyring {
        let _ = crate::cli::setup_decryption(&mut ts, kp);
    }

    let page_size = ts.page_size();
    let page_count = ts.page_count();
    let vendor_info = ts.vendor_info().clone();

    // Determine algorithm
    let algorithm = match explicit_algo {
        Some(algo) => algo,
        None => {
            let page0 = match ts.read_page(0) {
                Ok(p) => p,
                Err(e) => {
                    return FileRepairResult {
                        file: display_str,
                        backup_path: None,
                        total_pages: page_count,
                        already_valid: 0,
                        repaired: 0,
                        empty: 0,
                        errors: vec![e.to_string()],
                    };
                }
            };
            write::detect_algorithm(&page0, page_size, Some(&vendor_info))
        }
    };

    // Create backup unless --no-backup or --dry-run
    let backup_path = if !no_backup && !dry_run {
        match write::create_backup(&path_str) {
            Ok(bp) => {
                if let Some(ref logger) = audit_logger {
                    let _ = logger.log_backup(&path_str, &bp.display().to_string());
                }
                Some(bp.display().to_string())
            }
            Err(e) => {
                return FileRepairResult {
                    file: display_str,
                    backup_path: None,
                    total_pages: page_count,
                    already_valid: 0,
                    repaired: 0,
                    empty: 0,
                    errors: vec![format!("Backup failed: {}", e)],
                };
            }
        }
    } else {
        None
    };

    let mut already_valid = 0u64;
    let mut repaired = 0u64;
    let mut empty = 0u64;
    let mut errors = Vec::new();

    for page_num in 0..page_count {
        let mut page_data = match write::read_page_raw(&path_str, page_num, page_size) {
            Ok(d) => d,
            Err(e) => {
                errors.push(format!("Page {}: {}", page_num, e));
                continue;
            }
        };

        if page_data.iter().all(|&b| b == 0) {
            empty += 1;
            continue;
        }

        let result = validate_checksum(&page_data, page_size, Some(&vendor_info));
        let lsn_ok = validate_lsn(&page_data, page_size);

        if result.valid && lsn_ok {
            already_valid += 1;
            continue;
        }

        let (old_checksum, new_checksum) =
            write::fix_page_checksum(&mut page_data, page_size, algorithm);

        if !dry_run {
            if let Err(e) = write::write_page(&path_str, page_num, page_size, &page_data) {
                errors.push(format!("Page {}: write failed: {}", page_num, e));
                continue;
            }
            if let Some(ref logger) = audit_logger {
                let _ = logger.log_page_write(
                    &path_str,
                    page_num,
                    "batch_repair",
                    Some(old_checksum),
                    Some(new_checksum),
                );
            }
        }

        repaired += 1;
    }

    FileRepairResult {
        file: display_str,
        backup_path,
        total_pages: page_count,
        already_valid,
        repaired,
        empty,
        errors,
    }
}

/// Repair all .ibd files under a data directory.
fn execute_batch(opts: &RepairOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    let datadir_str = opts.batch.as_ref().unwrap();
    let datadir = Path::new(datadir_str);

    if !datadir.is_dir() {
        return Err(IdbError::Argument(format!(
            "Data directory does not exist: {}",
            datadir_str
        )));
    }

    let ibd_files = find_tablespace_files(datadir, &["ibd"])?;

    if ibd_files.is_empty() {
        if opts.json {
            let report = BatchRepairReport {
                datadir: datadir_str.clone(),
                dry_run: opts.dry_run,
                algorithm: opts.algorithm.clone(),
                files: Vec::new(),
                summary: BatchRepairSummary {
                    total_files: 0,
                    files_repaired: 0,
                    files_already_valid: 0,
                    files_error: 0,
                    total_pages_scanned: 0,
                    total_pages_repaired: 0,
                },
            };
            let json = serde_json::to_string_pretty(&report)
                .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
            wprintln!(writer, "{}", json)?;
        } else {
            wprintln!(writer, "No .ibd files found in {}", datadir_str)?;
        }
        return Ok(());
    }

    let explicit_algo = parse_algorithm(&opts.algorithm)?;

    let pb = if !opts.json {
        Some(create_progress_bar(ibd_files.len() as u64, "files"))
    } else {
        None
    };

    let page_size = opts.page_size;
    let keyring = opts.keyring.clone();
    let use_mmap = opts.mmap;
    let no_backup = opts.no_backup;
    let dry_run = opts.dry_run;
    let audit_logger = opts.audit_logger.clone();

    let mut results: Vec<FileRepairResult> = ibd_files
        .par_iter()
        .map(|path| {
            let r = repair_file(
                path,
                datadir,
                explicit_algo,
                no_backup,
                dry_run,
                page_size,
                &keyring,
                use_mmap,
                &audit_logger,
            );
            if let Some(ref pb) = pb {
                pb.inc(1);
            }
            r
        })
        .collect();

    if let Some(ref pb) = pb {
        pb.finish_and_clear();
    }

    results.sort_by(|a, b| a.file.cmp(&b.file));

    // Compute summary
    let total_files = results.len();
    let files_repaired = results.iter().filter(|r| r.repaired > 0).count();
    let files_already_valid = results
        .iter()
        .filter(|r| r.repaired == 0 && r.errors.is_empty())
        .count();
    let files_error = results.iter().filter(|r| !r.errors.is_empty()).count();
    let total_pages_scanned: u64 = results.iter().map(|r| r.total_pages).sum();
    let total_pages_repaired: u64 = results.iter().map(|r| r.repaired).sum();

    let aname = match explicit_algo {
        Some(a) => algo_name(a).to_string(),
        None => "auto".to_string(),
    };

    if opts.json {
        let report = BatchRepairReport {
            datadir: datadir_str.clone(),
            dry_run: opts.dry_run,
            algorithm: aname,
            files: results,
            summary: BatchRepairSummary {
                total_files,
                files_repaired,
                files_already_valid,
                files_error,
                total_pages_scanned,
                total_pages_repaired,
            },
        };
        let json = serde_json::to_string_pretty(&report)
            .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
        wprintln!(writer, "{}", json)?;
    } else {
        let mode = if opts.dry_run {
            "Dry run"
        } else {
            "Batch repair"
        };
        wprintln!(
            writer,
            "{}: {} ({} files)\n",
            mode,
            datadir_str,
            total_files
        )?;

        for r in &results {
            if !r.errors.is_empty() {
                wprintln!(
                    writer,
                    "  {:<40} {}   {}",
                    r.file,
                    "ERROR".yellow(),
                    r.errors[0]
                )?;
            } else if r.repaired > 0 {
                let label = if opts.dry_run {
                    format!("would repair {} pages", r.repaired)
                } else {
                    format!("repaired {} pages", r.repaired)
                };
                wprintln!(writer, "  {:<40} {}", r.file, label.green())?;
            } else {
                wprintln!(writer, "  {:<40} OK", r.file)?;
            }
        }

        wprintln!(writer)?;
        wprintln!(writer, "Summary:")?;
        wprintln!(
            writer,
            "  Files: {} ({} repaired, {} already valid{})",
            total_files,
            files_repaired,
            files_already_valid,
            if files_error > 0 {
                format!(", {} error", files_error)
            } else {
                String::new()
            }
        )?;
        wprintln!(
            writer,
            "  Pages: {} scanned, {} repaired",
            total_pages_scanned,
            total_pages_repaired
        )?;
    }

    Ok(())
}
