use std::io::Write;

use colored::Colorize;
use serde::Serialize;

use crate::cli::{create_progress_bar, wprintln};
use crate::innodb::checksum::{validate_checksum, validate_lsn, ChecksumAlgorithm};
use crate::innodb::write;
use crate::IdbError;

/// Options for the `inno repair` subcommand.
pub struct RepairOptions {
    /// Path to the InnoDB tablespace file (.ibd).
    pub file: String,
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

/// Recalculate and write correct checksums for corrupt pages.
pub fn execute(opts: &RepairOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    // Open tablespace read-only to get metadata
    let mut ts = crate::cli::open_tablespace(&opts.file, opts.page_size, opts.mmap)?;

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
        let path = write::create_backup(&opts.file)?;
        if !opts.json {
            wprintln!(writer, "Backup created: {}", path.display())?;
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

    let algo_name = match algorithm {
        ChecksumAlgorithm::Crc32c => "crc32c",
        ChecksumAlgorithm::InnoDB => "innodb",
        ChecksumAlgorithm::MariaDbFullCrc32 => "full_crc32",
        ChecksumAlgorithm::None => "none",
    };

    if !opts.json && !opts.dry_run {
        wprintln!(
            writer,
            "Repairing {} using {} algorithm...",
            opts.file,
            algo_name
        )?;
    } else if !opts.json && opts.dry_run {
        wprintln!(
            writer,
            "Dry run: scanning {} using {} algorithm...",
            opts.file,
            algo_name
        )?;
    }

    let mut already_valid = 0u64;
    let mut repaired = 0u64;
    let mut empty = 0u64;
    let mut page_details: Vec<PageRepairInfo> = Vec::new();

    for page_num in start_page..end_page {
        let mut page_data = write::read_page_raw(&opts.file, page_num, page_size)?;

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
            write::write_page(&opts.file, page_num, page_size, &page_data)?;
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
            file: opts.file.clone(),
            backup_path: backup_path.map(|p| p.display().to_string()),
            algorithm: algo_name.to_string(),
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
        wprintln!(writer, "  Algorithm:     {}", algo_name)?;
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
