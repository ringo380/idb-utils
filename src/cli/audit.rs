use std::io::Write;
use std::path::Path;
use std::time::Instant;

use colored::Colorize;
use rayon::prelude::*;
use serde::Serialize;

use crate::cli::{create_progress_bar, csv_escape, wprintln};
use crate::innodb::checksum::{validate_checksum, validate_lsn};
use crate::innodb::health;
use crate::util::fs::find_tablespace_files;
use crate::util::prometheus as prom;
use crate::IdbError;

/// Options for the `inno audit` subcommand.
pub struct AuditOptions {
    /// MySQL data directory path to scan.
    pub datadir: String,
    /// Show per-tablespace health metrics instead of integrity validation.
    pub health: bool,
    /// List only pages with checksum mismatches.
    pub checksum_mismatch: bool,
    /// Show additional details.
    pub verbose: bool,
    /// Emit output as JSON.
    pub json: bool,
    /// Output as CSV.
    pub csv: bool,
    /// Output in Prometheus exposition format.
    pub prometheus: bool,
    /// Override the auto-detected page size.
    pub page_size: Option<u32>,
    /// Path to MySQL keyring file for decrypting encrypted tablespaces.
    pub keyring: Option<String>,
    /// Use memory-mapped I/O for file access.
    pub mmap: bool,
    /// Show tables with fill factor below this threshold (0-100).
    pub min_fill_factor: Option<f64>,
    /// Show tables with fragmentation above this threshold (0-100).
    pub max_fragmentation: Option<f64>,
    /// Maximum directory recursion depth (None = default 2, Some(0) = unlimited).
    pub depth: Option<u32>,
}

// ---------------------------------------------------------------------------
// JSON output structs — default integrity mode
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct AuditReport {
    datadir: String,
    files: Vec<FileIntegrityResult>,
    summary: AuditSummary,
}

#[derive(Serialize, Clone)]
struct FileIntegrityResult {
    file: String,
    status: String,
    page_size: u32,
    total_pages: u64,
    empty_pages: u64,
    valid_pages: u64,
    invalid_pages: u64,
    lsn_mismatches: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    corrupt_pages: Vec<u64>,
}

#[derive(Serialize)]
struct AuditSummary {
    total_files: usize,
    files_passed: usize,
    files_failed: usize,
    files_error: usize,
    total_pages: u64,
    corrupt_pages: u64,
    integrity_pct: f64,
}

// ---------------------------------------------------------------------------
// JSON output structs — health mode
// ---------------------------------------------------------------------------

#[derive(Serialize, Clone)]
struct FileHealthResult {
    file: String,
    avg_fill_factor: f64,
    avg_fragmentation: f64,
    avg_garbage_ratio: f64,
    index_count: u64,
    total_index_pages: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct HealthAuditReport {
    datadir: String,
    tablespaces: Vec<FileHealthResult>,
    summary: DirectoryHealthSummary,
}

#[derive(Serialize)]
struct DirectoryHealthSummary {
    total_files: usize,
    total_index_pages: u64,
    avg_fill_factor: f64,
    avg_fragmentation: f64,
    avg_garbage_ratio: f64,
}

// ---------------------------------------------------------------------------
// JSON output structs — mismatch mode
// ---------------------------------------------------------------------------

#[derive(Serialize, Clone)]
struct MismatchEntry {
    file: String,
    page_number: u64,
    stored_checksum: u32,
    calculated_checksum: u32,
    algorithm: String,
}

#[derive(Serialize)]
struct MismatchReport {
    datadir: String,
    mismatches: Vec<MismatchEntry>,
    total_files_scanned: usize,
    total_pages_scanned: u64,
}

// ---------------------------------------------------------------------------
// Per-file worker functions
// ---------------------------------------------------------------------------

/// Audit a single file for integrity. Returns `None` if the file cannot be opened.
fn audit_file(
    path: &Path,
    datadir: &Path,
    page_size_override: Option<u32>,
    keyring: &Option<String>,
    use_mmap: bool,
) -> FileIntegrityResult {
    let display = path.strip_prefix(datadir).unwrap_or(path);
    let display_str = display.display().to_string();
    let path_str = path.to_string_lossy();

    let mut ts = match crate::cli::open_tablespace(&path_str, page_size_override, use_mmap) {
        Ok(t) => t,
        Err(e) => {
            return FileIntegrityResult {
                file: display_str,
                status: "error".to_string(),
                page_size: 0,
                total_pages: 0,
                empty_pages: 0,
                valid_pages: 0,
                invalid_pages: 0,
                lsn_mismatches: 0,
                error: Some(e.to_string()),
                corrupt_pages: Vec::new(),
            };
        }
    };

    if let Some(ref kp) = keyring {
        if crate::cli::setup_decryption(&mut ts, kp).is_err() {
            // Not encrypted or bad keyring — continue without decryption
        }
    }

    let page_size = ts.page_size();
    let page_count = ts.page_count();
    let vendor_info = ts.vendor_info().clone();

    // Read all pages for parallel validation
    let all_data = match ts.read_all_pages() {
        Ok(d) => d,
        Err(e) => {
            return FileIntegrityResult {
                file: display_str,
                status: "error".to_string(),
                page_size,
                total_pages: page_count,
                empty_pages: 0,
                valid_pages: 0,
                invalid_pages: 0,
                lsn_mismatches: 0,
                error: Some(e.to_string()),
                corrupt_pages: Vec::new(),
            };
        }
    };

    let ps = page_size as usize;

    // Validate pages in parallel
    let results: Vec<(u64, bool, bool, bool, Option<u64>)> = (0..page_count)
        .into_par_iter()
        .map(|page_num| {
            let offset = page_num as usize * ps;
            if offset + ps > all_data.len() {
                return (page_num, false, false, false, None);
            }
            let page_data = &all_data[offset..offset + ps];

            // Check for empty page
            if page_data.iter().all(|&b| b == 0) {
                return (page_num, true, true, true, None); // empty=true
            }

            let csum = validate_checksum(page_data, page_size, Some(&vendor_info));
            let lsn_ok = validate_lsn(page_data, page_size);

            let corrupt_page = if !csum.valid { Some(page_num) } else { None };
            // (page_num, is_valid_or_empty, is_empty, lsn_ok, corrupt_page_num)
            (page_num, csum.valid, false, lsn_ok, corrupt_page)
        })
        .collect();

    let mut valid = 0u64;
    let mut invalid = 0u64;
    let mut empty = 0u64;
    let mut lsn_mismatches = 0u64;
    let mut corrupt_pages = Vec::new();

    for &(_, is_valid, is_empty, lsn_ok, ref corrupt) in &results {
        if is_empty {
            empty += 1;
        } else if is_valid {
            valid += 1;
        } else {
            invalid += 1;
        }
        if !lsn_ok && !is_empty && is_valid {
            lsn_mismatches += 1;
        }
        if let Some(pn) = corrupt {
            corrupt_pages.push(*pn);
        }
    }

    let status = if invalid > 0 { "FAIL" } else { "PASS" };

    FileIntegrityResult {
        file: display_str,
        status: status.to_string(),
        page_size,
        total_pages: page_count,
        empty_pages: empty,
        valid_pages: valid,
        invalid_pages: invalid,
        lsn_mismatches,
        error: None,
        corrupt_pages,
    }
}

/// Audit a single file for health metrics.
fn audit_file_health(
    path: &Path,
    datadir: &Path,
    page_size_override: Option<u32>,
    keyring: &Option<String>,
    use_mmap: bool,
) -> FileHealthResult {
    let display = path.strip_prefix(datadir).unwrap_or(path);
    let display_str = display.display().to_string();
    let path_str = path.to_string_lossy();

    let mut ts = match crate::cli::open_tablespace(&path_str, page_size_override, use_mmap) {
        Ok(t) => t,
        Err(e) => {
            return FileHealthResult {
                file: display_str,
                avg_fill_factor: 0.0,
                avg_fragmentation: 0.0,
                avg_garbage_ratio: 0.0,
                index_count: 0,
                total_index_pages: 0,
                error: Some(e.to_string()),
            };
        }
    };

    if let Some(ref kp) = keyring {
        let _ = crate::cli::setup_decryption(&mut ts, kp);
    }

    let page_size = ts.page_size();
    let total_pages = ts.page_count();

    let mut snapshots = Vec::new();
    let mut empty_pages = 0u64;

    let scan_result = ts.for_each_page(|page_num, data| {
        if data.iter().all(|&b| b == 0) {
            empty_pages += 1;
        } else if let Some(snap) = health::extract_index_page_snapshot(data, page_num) {
            snapshots.push(snap);
        }
        Ok(())
    });

    if let Err(e) = scan_result {
        return FileHealthResult {
            file: display_str,
            avg_fill_factor: 0.0,
            avg_fragmentation: 0.0,
            avg_garbage_ratio: 0.0,
            index_count: 0,
            total_index_pages: 0,
            error: Some(e.to_string()),
        };
    }

    let report = health::analyze_health(snapshots, page_size, total_pages, empty_pages, &path_str);

    FileHealthResult {
        file: display_str,
        avg_fill_factor: report.summary.avg_fill_factor,
        avg_fragmentation: report.summary.avg_fragmentation,
        avg_garbage_ratio: report.summary.avg_garbage_ratio,
        index_count: report.summary.index_count,
        total_index_pages: report.summary.index_pages,
        error: None,
    }
}

/// Scan a single file for checksum mismatches only.
fn audit_file_mismatches(
    path: &Path,
    datadir: &Path,
    page_size_override: Option<u32>,
    keyring: &Option<String>,
    use_mmap: bool,
) -> (Vec<MismatchEntry>, u64) {
    let display = path.strip_prefix(datadir).unwrap_or(path);
    let display_str = display.display().to_string();
    let path_str = path.to_string_lossy();

    let mut ts = match crate::cli::open_tablespace(&path_str, page_size_override, use_mmap) {
        Ok(t) => t,
        Err(_) => return (Vec::new(), 0),
    };

    if let Some(ref kp) = keyring {
        let _ = crate::cli::setup_decryption(&mut ts, kp);
    }

    let page_size = ts.page_size();
    let page_count = ts.page_count();
    let vendor_info = ts.vendor_info().clone();

    let all_data = match ts.read_all_pages() {
        Ok(d) => d,
        Err(_) => return (Vec::new(), page_count),
    };

    let ps = page_size as usize;

    let mismatches: Vec<MismatchEntry> = (0..page_count)
        .into_par_iter()
        .filter_map(|page_num| {
            let offset = page_num as usize * ps;
            if offset + ps > all_data.len() {
                return None;
            }
            let page_data = &all_data[offset..offset + ps];

            if page_data.iter().all(|&b| b == 0) {
                return None;
            }

            let csum = validate_checksum(page_data, page_size, Some(&vendor_info));
            if csum.valid {
                return None;
            }

            Some(MismatchEntry {
                file: display_str.clone(),
                page_number: page_num,
                stored_checksum: csum.stored_checksum,
                calculated_checksum: csum.calculated_checksum,
                algorithm: algorithm_name(csum.algorithm).to_string(),
            })
        })
        .collect();

    (mismatches, page_count)
}

fn algorithm_name(algo: crate::innodb::checksum::ChecksumAlgorithm) -> &'static str {
    match algo {
        crate::innodb::checksum::ChecksumAlgorithm::Crc32c => "crc32c",
        crate::innodb::checksum::ChecksumAlgorithm::InnoDB => "innodb",
        crate::innodb::checksum::ChecksumAlgorithm::MariaDbFullCrc32 => "mariadb_full_crc32",
        crate::innodb::checksum::ChecksumAlgorithm::None => "none",
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Audit a MySQL data directory for integrity, health metrics, or corrupt pages.
pub fn execute(opts: &AuditOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    if opts.prometheus && (opts.json || opts.csv) {
        return Err(IdbError::Argument(
            "--prometheus cannot be combined with JSON or CSV output".to_string(),
        ));
    }

    // Validate mutually exclusive flags
    if opts.health && opts.checksum_mismatch {
        return Err(IdbError::Argument(
            "--health and --checksum-mismatch are mutually exclusive".to_string(),
        ));
    }

    let datadir = Path::new(&opts.datadir);
    if !datadir.is_dir() {
        return Err(IdbError::Argument(format!(
            "Data directory does not exist: {}",
            opts.datadir
        )));
    }

    let ibd_files = find_tablespace_files(datadir, &["ibd"], opts.depth)?;

    if ibd_files.is_empty() {
        if opts.prometheus {
            // Empty Prometheus output — valid exposition format (no metrics emitted)
            return Ok(());
        } else if opts.json {
            if opts.health {
                let report = HealthAuditReport {
                    datadir: opts.datadir.clone(),
                    tablespaces: Vec::new(),
                    summary: DirectoryHealthSummary {
                        total_files: 0,
                        total_index_pages: 0,
                        avg_fill_factor: 0.0,
                        avg_fragmentation: 0.0,
                        avg_garbage_ratio: 0.0,
                    },
                };
                let json = serde_json::to_string_pretty(&report)
                    .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
                wprintln!(writer, "{}", json)?;
            } else if opts.checksum_mismatch {
                let report = MismatchReport {
                    datadir: opts.datadir.clone(),
                    mismatches: Vec::new(),
                    total_files_scanned: 0,
                    total_pages_scanned: 0,
                };
                let json = serde_json::to_string_pretty(&report)
                    .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
                wprintln!(writer, "{}", json)?;
            } else {
                let report = AuditReport {
                    datadir: opts.datadir.clone(),
                    files: Vec::new(),
                    summary: AuditSummary {
                        total_files: 0,
                        files_passed: 0,
                        files_failed: 0,
                        files_error: 0,
                        total_pages: 0,
                        corrupt_pages: 0,
                        integrity_pct: 100.0,
                    },
                };
                let json = serde_json::to_string_pretty(&report)
                    .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
                wprintln!(writer, "{}", json)?;
            }
        } else {
            wprintln!(writer, "No .ibd files found in {}", opts.datadir)?;
        }
        return Ok(());
    }

    if opts.health {
        execute_health(opts, &ibd_files, datadir, writer)
    } else if opts.checksum_mismatch {
        execute_mismatch(opts, &ibd_files, datadir, writer)
    } else {
        execute_integrity(opts, &ibd_files, datadir, writer)
    }
}

// ---------------------------------------------------------------------------
// Default integrity mode (#83)
// ---------------------------------------------------------------------------

fn execute_integrity(
    opts: &AuditOptions,
    ibd_files: &[std::path::PathBuf],
    datadir: &Path,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    let start = Instant::now();

    let pb = if !opts.json && !opts.csv && !opts.prometheus {
        Some(create_progress_bar(ibd_files.len() as u64, "files"))
    } else {
        None
    };

    let page_size = opts.page_size;
    let keyring = opts.keyring.clone();
    let use_mmap = opts.mmap;

    let mut results: Vec<FileIntegrityResult> = ibd_files
        .par_iter()
        .map(|path| {
            let r = audit_file(path, datadir, page_size, &keyring, use_mmap);
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

    // Compute summary
    let total_files = results.len();
    let files_passed = results.iter().filter(|r| r.status == "PASS").count();
    let files_failed = results.iter().filter(|r| r.status == "FAIL").count();
    let files_error = results.iter().filter(|r| r.status == "error").count();
    let total_pages: u64 = results.iter().map(|r| r.total_pages).sum();
    let corrupt_pages: u64 = results.iter().map(|r| r.invalid_pages).sum();
    let valid_pages: u64 = results.iter().map(|r| r.valid_pages).sum();
    let checked_pages = valid_pages + corrupt_pages;
    let integrity_pct = if checked_pages > 0 {
        (valid_pages as f64 / checked_pages as f64) * 100.0
    } else {
        100.0
    };
    let integrity_pct = (integrity_pct * 100.0).round() / 100.0;

    if opts.prometheus {
        let duration_secs = start.elapsed().as_secs_f64();
        print_prometheus_integrity(
            writer,
            &IntegrityPrometheusParams {
                datadir: &opts.datadir,
                results: &results,
                total_pages,
                corrupt_pages,
                integrity_pct,
                duration_secs,
            },
        )?;

        if corrupt_pages > 0 {
            return Err(IdbError::Parse(format!(
                "{} corrupt pages found across {} files",
                corrupt_pages, files_failed
            )));
        }
        return Ok(());
    }

    if opts.json {
        let report = AuditReport {
            datadir: opts.datadir.clone(),
            files: results,
            summary: AuditSummary {
                total_files,
                files_passed,
                files_failed,
                files_error,
                total_pages,
                corrupt_pages,
                integrity_pct,
            },
        };
        let json = serde_json::to_string_pretty(&report)
            .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
        wprintln!(writer, "{}", json)?;
    } else if opts.csv {
        wprintln!(
            writer,
            "file,status,total_pages,empty_pages,valid_pages,invalid_pages,lsn_mismatches"
        )?;
        for r in &results {
            wprintln!(
                writer,
                "{},{},{},{},{},{},{}",
                csv_escape(&r.file),
                r.status,
                r.total_pages,
                r.empty_pages,
                r.valid_pages,
                r.invalid_pages,
                r.lsn_mismatches
            )?;
        }
    } else {
        wprintln!(
            writer,
            "Auditing {} ({} files)...\n",
            opts.datadir,
            total_files
        )?;

        for r in &results {
            let status_colored = match r.status.as_str() {
                "PASS" => "PASS".green().to_string(),
                "FAIL" => "FAIL".red().to_string(),
                _ => "ERROR".yellow().to_string(),
            };

            if r.status == "error" {
                wprintln!(
                    writer,
                    "  {:<40} {}   {}",
                    r.file,
                    status_colored,
                    r.error.as_deref().unwrap_or("unknown error")
                )?;
            } else if r.invalid_pages > 0 {
                wprintln!(
                    writer,
                    "  {:<40} {}   {} pages, {} corrupt",
                    r.file,
                    status_colored,
                    r.total_pages,
                    r.invalid_pages
                )?;
            } else {
                wprintln!(
                    writer,
                    "  {:<40} {}   {} pages",
                    r.file,
                    status_colored,
                    r.total_pages
                )?;
            }

            if opts.verbose && !r.corrupt_pages.is_empty() {
                wprintln!(writer, "    Corrupt pages: {:?}", r.corrupt_pages)?;
            }
        }

        wprintln!(writer)?;
        wprintln!(writer, "Summary:")?;
        wprintln!(
            writer,
            "  Files: {} ({} passed, {} failed{})",
            total_files,
            files_passed,
            files_failed,
            if files_error > 0 {
                format!(", {} error", files_error)
            } else {
                String::new()
            }
        )?;
        wprintln!(
            writer,
            "  Pages: {} total, {} corrupt",
            total_pages,
            corrupt_pages
        )?;
        wprintln!(writer, "  Integrity: {:.2}%", integrity_pct)?;
    }

    if corrupt_pages > 0 {
        return Err(IdbError::Parse(format!(
            "{} corrupt pages found across {} files",
            corrupt_pages, files_failed
        )));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Health mode (#84)
// ---------------------------------------------------------------------------

fn execute_health(
    opts: &AuditOptions,
    ibd_files: &[std::path::PathBuf],
    datadir: &Path,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    let start = Instant::now();

    let pb = if !opts.json && !opts.csv && !opts.prometheus {
        Some(create_progress_bar(ibd_files.len() as u64, "files"))
    } else {
        None
    };

    let page_size = opts.page_size;
    let keyring = opts.keyring.clone();
    let use_mmap = opts.mmap;

    let mut results: Vec<FileHealthResult> = ibd_files
        .par_iter()
        .map(|path| {
            let r = audit_file_health(path, datadir, page_size, &keyring, use_mmap);
            if let Some(ref pb) = pb {
                pb.inc(1);
            }
            r
        })
        .collect();

    if let Some(ref pb) = pb {
        pb.finish_and_clear();
    }

    // Compute directory-wide summary from ALL results (before filtering)
    let total_files = results.len();
    let total_index_pages: u64 = results.iter().map(|r| r.total_index_pages).sum();

    let valid_results: Vec<&FileHealthResult> =
        results.iter().filter(|r| r.error.is_none()).collect();
    let n = valid_results.len() as f64;

    let avg_fill = if n > 0.0 {
        valid_results.iter().map(|r| r.avg_fill_factor).sum::<f64>() / n
    } else {
        0.0
    };
    let avg_frag = if n > 0.0 {
        valid_results
            .iter()
            .map(|r| r.avg_fragmentation)
            .sum::<f64>()
            / n
    } else {
        0.0
    };
    let avg_garbage = if n > 0.0 {
        valid_results
            .iter()
            .map(|r| r.avg_garbage_ratio)
            .sum::<f64>()
            / n
    } else {
        0.0
    };

    // Prometheus output uses unfiltered results to avoid stale markers
    if opts.prometheus {
        let duration_secs = start.elapsed().as_secs_f64();
        print_prometheus_health(
            writer,
            &HealthPrometheusParams {
                datadir: &opts.datadir,
                results: &results,
                total_files,
                total_index_pages,
                avg_fill,
                avg_frag,
                avg_garbage,
                duration_secs,
            },
        )?;
        return Ok(());
    }

    // Filter by thresholds (values are 0-100 from CLI, compare as 0.0-1.0)
    if let Some(min_ff) = opts.min_fill_factor {
        let threshold = min_ff / 100.0;
        results.retain(|r| r.error.is_some() || r.avg_fill_factor < threshold);
    }
    if let Some(max_frag) = opts.max_fragmentation {
        let threshold = max_frag / 100.0;
        results.retain(|r| r.error.is_some() || r.avg_fragmentation > threshold);
    }

    // Sort worst-first by fragmentation (descending)
    results.sort_by(|a, b| {
        b.avg_fragmentation
            .partial_cmp(&a.avg_fragmentation)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    if opts.json {
        let report = HealthAuditReport {
            datadir: opts.datadir.clone(),
            tablespaces: results,
            summary: DirectoryHealthSummary {
                total_files,
                total_index_pages,
                avg_fill_factor: round2(avg_fill),
                avg_fragmentation: round2(avg_frag),
                avg_garbage_ratio: round2(avg_garbage),
            },
        };
        let json = serde_json::to_string_pretty(&report)
            .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
        wprintln!(writer, "{}", json)?;
    } else if opts.csv {
        wprintln!(
            writer,
            "file,avg_fill_factor,avg_fragmentation,avg_garbage_ratio,index_count,total_index_pages"
        )?;
        for r in &results {
            if r.error.is_some() {
                continue;
            }
            wprintln!(
                writer,
                "{},{:.1},{:.1},{:.1},{},{}",
                csv_escape(&r.file),
                r.avg_fill_factor * 100.0,
                r.avg_fragmentation * 100.0,
                r.avg_garbage_ratio * 100.0,
                r.index_count,
                r.total_index_pages
            )?;
        }
    } else {
        wprintln!(writer, "Directory Health: {}\n", opts.datadir)?;
        wprintln!(
            writer,
            "  {:<40} {:>6} {:>6} {:>6} {:>8} {:>6}",
            "File",
            "Fill%",
            "Frag%",
            "Garb%",
            "Indexes",
            "Pages"
        )?;

        for r in &results {
            if let Some(ref err) = r.error {
                wprintln!(
                    writer,
                    "  {:<40} {}",
                    r.file,
                    format!("ERROR: {}", err).yellow()
                )?;
            } else {
                wprintln!(
                    writer,
                    "  {:<40} {:>5.1}  {:>5.1}  {:>5.1}  {:>7}  {:>5}",
                    r.file,
                    r.avg_fill_factor * 100.0,
                    r.avg_fragmentation * 100.0,
                    r.avg_garbage_ratio * 100.0,
                    r.index_count,
                    r.total_index_pages
                )?;
            }
        }

        wprintln!(writer)?;
        wprintln!(
            writer,
            "Summary: {} files, avg fill {:.1}%, avg frag {:.1}%, avg garbage {:.1}%",
            total_files,
            avg_fill * 100.0,
            avg_frag * 100.0,
            avg_garbage * 100.0
        )?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Mismatch mode (#85)
// ---------------------------------------------------------------------------

fn execute_mismatch(
    opts: &AuditOptions,
    ibd_files: &[std::path::PathBuf],
    datadir: &Path,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    let pb = if !opts.json && !opts.csv {
        Some(create_progress_bar(ibd_files.len() as u64, "files"))
    } else {
        None
    };

    let page_size = opts.page_size;
    let keyring = opts.keyring.clone();
    let use_mmap = opts.mmap;

    let all_results: Vec<(Vec<MismatchEntry>, u64)> = ibd_files
        .par_iter()
        .map(|path| {
            let r = audit_file_mismatches(path, datadir, page_size, &keyring, use_mmap);
            if let Some(ref pb) = pb {
                pb.inc(1);
            }
            r
        })
        .collect();

    if let Some(ref pb) = pb {
        pb.finish_and_clear();
    }

    let total_files_scanned = ibd_files.len();
    let total_pages_scanned: u64 = all_results.iter().map(|(_, count)| count).sum();

    let mut mismatches: Vec<MismatchEntry> = all_results
        .into_iter()
        .flat_map(|(entries, _)| entries)
        .collect();
    mismatches.sort_by(|a, b| (&a.file, a.page_number).cmp(&(&b.file, b.page_number)));

    if opts.json {
        let report = MismatchReport {
            datadir: opts.datadir.clone(),
            mismatches: mismatches.clone(),
            total_files_scanned,
            total_pages_scanned,
        };
        let json = serde_json::to_string_pretty(&report)
            .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
        wprintln!(writer, "{}", json)?;
    } else if opts.csv {
        wprintln!(
            writer,
            "file,page_number,stored_checksum,calculated_checksum,algorithm"
        )?;
        for m in &mismatches {
            wprintln!(
                writer,
                "{},{},{},{},{}",
                csv_escape(&m.file),
                m.page_number,
                m.stored_checksum,
                m.calculated_checksum,
                m.algorithm
            )?;
        }
    } else if mismatches.is_empty() {
        wprintln!(
            writer,
            "No checksum mismatches found ({} files, {} pages scanned).",
            total_files_scanned,
            total_pages_scanned
        )?;
    } else {
        wprintln!(
            writer,
            "{:<40} {:>6} {:>12} {:>12} {:>12}",
            "FILE",
            "PAGE",
            "STORED",
            "CALCULATED",
            "ALGORITHM"
        )?;
        for m in &mismatches {
            wprintln!(
                writer,
                "{:<40} {:>6} {:>12} {:>12} {:>12}",
                m.file,
                m.page_number,
                format!("0x{:08X}", m.stored_checksum),
                format!("0x{:08X}", m.calculated_checksum),
                m.algorithm
            )?;
        }
    }

    if !mismatches.is_empty() {
        return Err(IdbError::Parse(format!(
            "{} checksum mismatches found",
            mismatches.len()
        )));
    }

    Ok(())
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

// ---------------------------------------------------------------------------
// Prometheus exposition format output
// ---------------------------------------------------------------------------

struct IntegrityPrometheusParams<'a> {
    datadir: &'a str,
    results: &'a [FileIntegrityResult],
    total_pages: u64,
    corrupt_pages: u64,
    integrity_pct: f64,
    duration_secs: f64,
}

/// Print integrity audit results in Prometheus exposition format.
fn print_prometheus_integrity(
    writer: &mut dyn Write,
    params: &IntegrityPrometheusParams<'_>,
) -> Result<(), IdbError> {
    let datadir = params.datadir;
    let results = params.results;
    let total_pages = params.total_pages;
    let corrupt_pages = params.corrupt_pages;
    let integrity_pct = params.integrity_pct;
    let duration_secs = params.duration_secs;
    // innodb_pages per file
    wprintln!(
        writer,
        "{}",
        prom::help_line("innodb_pages", "Total pages in tablespace")
    )?;
    wprintln!(writer, "{}", prom::type_line("innodb_pages", "gauge"))?;
    for r in results {
        if r.status == "error" {
            continue;
        }
        wprintln!(
            writer,
            "{}",
            prom::format_gauge_int(
                "innodb_pages",
                &[("datadir", datadir), ("file", &r.file)],
                r.total_pages
            )
        )?;
    }

    // innodb_corrupt_pages per file
    wprintln!(
        writer,
        "{}",
        prom::help_line(
            "innodb_corrupt_pages",
            "Number of corrupt pages in tablespace"
        )
    )?;
    wprintln!(
        writer,
        "{}",
        prom::type_line("innodb_corrupt_pages", "gauge")
    )?;
    for r in results {
        if r.status == "error" {
            continue;
        }
        wprintln!(
            writer,
            "{}",
            prom::format_gauge_int(
                "innodb_corrupt_pages",
                &[("datadir", datadir), ("file", &r.file)],
                r.invalid_pages
            )
        )?;
    }

    // innodb_empty_pages per file
    wprintln!(
        writer,
        "{}",
        prom::help_line("innodb_empty_pages", "Number of empty pages in tablespace")
    )?;
    wprintln!(writer, "{}", prom::type_line("innodb_empty_pages", "gauge"))?;
    for r in results {
        if r.status == "error" {
            continue;
        }
        wprintln!(
            writer,
            "{}",
            prom::format_gauge_int(
                "innodb_empty_pages",
                &[("datadir", datadir), ("file", &r.file)],
                r.empty_pages
            )
        )?;
    }

    // innodb_audit_integrity_pct — directory-wide
    wprintln!(
        writer,
        "{}",
        prom::help_line(
            "innodb_audit_integrity_pct",
            "Directory-wide integrity percentage"
        )
    )?;
    wprintln!(
        writer,
        "{}",
        prom::type_line("innodb_audit_integrity_pct", "gauge")
    )?;
    wprintln!(
        writer,
        "{}",
        prom::format_gauge(
            "innodb_audit_integrity_pct",
            &[("datadir", datadir)],
            integrity_pct
        )
    )?;

    // innodb_audit_pages — directory-wide
    wprintln!(
        writer,
        "{}",
        prom::help_line(
            "innodb_audit_pages",
            "Total pages scanned across data directory"
        )
    )?;
    wprintln!(writer, "{}", prom::type_line("innodb_audit_pages", "gauge"))?;
    wprintln!(
        writer,
        "{}",
        prom::format_gauge_int("innodb_audit_pages", &[("datadir", datadir)], total_pages)
    )?;

    // innodb_audit_corrupt_pages — directory-wide
    wprintln!(
        writer,
        "{}",
        prom::help_line(
            "innodb_audit_corrupt_pages",
            "Total corrupt pages across data directory"
        )
    )?;
    wprintln!(
        writer,
        "{}",
        prom::type_line("innodb_audit_corrupt_pages", "gauge")
    )?;
    wprintln!(
        writer,
        "{}",
        prom::format_gauge_int(
            "innodb_audit_corrupt_pages",
            &[("datadir", datadir)],
            corrupt_pages
        )
    )?;

    // innodb_scan_duration_seconds — directory-wide
    wprintln!(
        writer,
        "{}",
        prom::help_line(
            "innodb_scan_duration_seconds",
            "Time spent scanning the data directory"
        )
    )?;
    wprintln!(
        writer,
        "{}",
        prom::type_line("innodb_scan_duration_seconds", "gauge")
    )?;
    wprintln!(
        writer,
        "{}",
        prom::format_gauge(
            "innodb_scan_duration_seconds",
            &[("datadir", datadir)],
            duration_secs
        )
    )?;

    Ok(())
}

struct HealthPrometheusParams<'a> {
    datadir: &'a str,
    results: &'a [FileHealthResult],
    total_files: usize,
    total_index_pages: u64,
    avg_fill: f64,
    avg_frag: f64,
    avg_garbage: f64,
    duration_secs: f64,
}

/// Print health audit results in Prometheus exposition format.
fn print_prometheus_health(
    writer: &mut dyn Write,
    params: &HealthPrometheusParams<'_>,
) -> Result<(), IdbError> {
    let datadir = params.datadir;
    let results = params.results;
    let total_files = params.total_files;
    let total_index_pages = params.total_index_pages;
    let avg_fill = params.avg_fill;
    let avg_frag = params.avg_frag;
    let avg_garbage = params.avg_garbage;
    let duration_secs = params.duration_secs;
    // Per-file fill factor
    wprintln!(
        writer,
        "{}",
        prom::help_line(
            "innodb_fill_factor",
            "Average B+Tree fill factor for tablespace"
        )
    )?;
    wprintln!(writer, "{}", prom::type_line("innodb_fill_factor", "gauge"))?;
    for r in results {
        if r.error.is_some() {
            continue;
        }
        wprintln!(
            writer,
            "{}",
            prom::format_gauge(
                "innodb_fill_factor",
                &[("datadir", datadir), ("file", &r.file)],
                r.avg_fill_factor
            )
        )?;
    }

    // Per-file fragmentation
    wprintln!(
        writer,
        "{}",
        prom::help_line(
            "innodb_fragmentation_ratio",
            "Average fragmentation ratio for tablespace"
        )
    )?;
    wprintln!(
        writer,
        "{}",
        prom::type_line("innodb_fragmentation_ratio", "gauge")
    )?;
    for r in results {
        if r.error.is_some() {
            continue;
        }
        wprintln!(
            writer,
            "{}",
            prom::format_gauge(
                "innodb_fragmentation_ratio",
                &[("datadir", datadir), ("file", &r.file)],
                r.avg_fragmentation
            )
        )?;
    }

    // Per-file garbage ratio
    wprintln!(
        writer,
        "{}",
        prom::help_line(
            "innodb_garbage_ratio",
            "Average garbage ratio for tablespace"
        )
    )?;
    wprintln!(
        writer,
        "{}",
        prom::type_line("innodb_garbage_ratio", "gauge")
    )?;
    for r in results {
        if r.error.is_some() {
            continue;
        }
        wprintln!(
            writer,
            "{}",
            prom::format_gauge(
                "innodb_garbage_ratio",
                &[("datadir", datadir), ("file", &r.file)],
                r.avg_garbage_ratio
            )
        )?;
    }

    // Per-file index page count
    wprintln!(
        writer,
        "{}",
        prom::help_line("innodb_index_pages", "Total INDEX pages in tablespace")
    )?;
    wprintln!(writer, "{}", prom::type_line("innodb_index_pages", "gauge"))?;
    for r in results {
        if r.error.is_some() {
            continue;
        }
        wprintln!(
            writer,
            "{}",
            prom::format_gauge_int(
                "innodb_index_pages",
                &[("datadir", datadir), ("file", &r.file)],
                r.total_index_pages
            )
        )?;
    }

    // Directory-wide summary metrics
    wprintln!(
        writer,
        "{}",
        prom::help_line("innodb_audit_files", "Total tablespace files scanned")
    )?;
    wprintln!(writer, "{}", prom::type_line("innodb_audit_files", "gauge"))?;
    wprintln!(
        writer,
        "{}",
        prom::format_gauge_int(
            "innodb_audit_files",
            &[("datadir", datadir)],
            total_files as u64
        )
    )?;

    wprintln!(
        writer,
        "{}",
        prom::help_line(
            "innodb_audit_index_pages",
            "Total INDEX pages across data directory"
        )
    )?;
    wprintln!(
        writer,
        "{}",
        prom::type_line("innodb_audit_index_pages", "gauge")
    )?;
    wprintln!(
        writer,
        "{}",
        prom::format_gauge_int(
            "innodb_audit_index_pages",
            &[("datadir", datadir)],
            total_index_pages
        )
    )?;

    wprintln!(
        writer,
        "{}",
        prom::help_line(
            "innodb_audit_avg_fill_factor",
            "Directory-wide average fill factor"
        )
    )?;
    wprintln!(
        writer,
        "{}",
        prom::type_line("innodb_audit_avg_fill_factor", "gauge")
    )?;
    wprintln!(
        writer,
        "{}",
        prom::format_gauge(
            "innodb_audit_avg_fill_factor",
            &[("datadir", datadir)],
            avg_fill
        )
    )?;

    wprintln!(
        writer,
        "{}",
        prom::help_line(
            "innodb_audit_avg_fragmentation",
            "Directory-wide average fragmentation"
        )
    )?;
    wprintln!(
        writer,
        "{}",
        prom::type_line("innodb_audit_avg_fragmentation", "gauge")
    )?;
    wprintln!(
        writer,
        "{}",
        prom::format_gauge(
            "innodb_audit_avg_fragmentation",
            &[("datadir", datadir)],
            avg_frag
        )
    )?;

    wprintln!(
        writer,
        "{}",
        prom::help_line(
            "innodb_audit_avg_garbage_ratio",
            "Directory-wide average garbage ratio"
        )
    )?;
    wprintln!(
        writer,
        "{}",
        prom::type_line("innodb_audit_avg_garbage_ratio", "gauge")
    )?;
    wprintln!(
        writer,
        "{}",
        prom::format_gauge(
            "innodb_audit_avg_garbage_ratio",
            &[("datadir", datadir)],
            avg_garbage
        )
    )?;

    // innodb_scan_duration_seconds — directory-wide
    wprintln!(
        writer,
        "{}",
        prom::help_line(
            "innodb_scan_duration_seconds",
            "Time spent scanning the data directory"
        )
    )?;
    wprintln!(
        writer,
        "{}",
        prom::type_line("innodb_scan_duration_seconds", "gauge")
    )?;
    wprintln!(
        writer,
        "{}",
        prom::format_gauge(
            "innodb_scan_duration_seconds",
            &[("datadir", datadir)],
            duration_secs
        )
    )?;

    Ok(())
}
