use std::io::Write;

use colored::Colorize;
use serde::Serialize;

use crate::cli::{create_progress_bar, wprintln};
use crate::innodb::checksum::{validate_checksum, validate_lsn, ChecksumAlgorithm};
use crate::innodb::page::FilHeader;
use crate::innodb::tablespace::Tablespace;
use crate::IdbError;

/// Options for the `inno checksum` subcommand.
pub struct ChecksumOptions {
    /// Path to the InnoDB tablespace file (.ibd).
    pub file: String,
    /// Show per-page checksum details.
    pub verbose: bool,
    /// Emit output as JSON.
    pub json: bool,
    /// Override the auto-detected page size.
    pub page_size: Option<u32>,
}

#[derive(Serialize)]
struct ChecksumSummaryJson {
    file: String,
    page_size: u32,
    total_pages: u64,
    empty_pages: u64,
    valid_pages: u64,
    invalid_pages: u64,
    lsn_mismatches: u64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pages: Vec<PageChecksumJson>,
}

#[derive(Serialize)]
struct PageChecksumJson {
    page_number: u64,
    status: String,
    algorithm: String,
    stored_checksum: u32,
    calculated_checksum: u32,
    lsn_valid: bool,
}

/// Validate page checksums for every page in an InnoDB tablespace.
///
/// Iterates over all pages and validates the stored checksum (bytes 0–3 of the
/// FIL header) against two algorithms: **CRC-32C** (MySQL 5.7.7+), which XORs
/// two independent CRC-32C values computed over bytes \[4..26) and
/// \[38..page_size-8); and **legacy InnoDB**, which uses `ut_fold_ulint_pair`
/// with u32 wrapping arithmetic over the same two byte ranges. A page is
/// considered valid if either algorithm matches the stored value.
///
/// Additionally checks **LSN consistency**: the low 32 bits of the header LSN
/// (bytes 16–23) must match the LSN value in the 8-byte FIL trailer at the
/// end of the page. All-zero pages are counted as empty and skipped entirely.
///
/// Prints a summary with total, empty, valid, and invalid page counts. In
/// `--verbose` mode, every non-empty page is printed with its algorithm,
/// stored and calculated checksum values, and LSN status. The process exits
/// with code 1 if any page has an invalid checksum, making this suitable for
/// scripted integrity checks.
pub fn execute(opts: &ChecksumOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    let mut ts = match opts.page_size {
        Some(ps) => Tablespace::open_with_page_size(&opts.file, ps)?,
        None => Tablespace::open(&opts.file)?,
    };

    let page_size = ts.page_size();
    let page_count = ts.page_count();

    if opts.json {
        return execute_json(opts, &mut ts, page_size, page_count, writer);
    }

    wprintln!(
        writer,
        "Validating checksums for {} ({} pages, page size {})...",
        opts.file,
        page_count,
        page_size
    )?;
    wprintln!(writer)?;

    let mut valid_count = 0u64;
    let mut invalid_count = 0u64;
    let mut empty_count = 0u64;
    let mut lsn_mismatch_count = 0u64;

    let pb = create_progress_bar(page_count, "pages");

    for page_num in 0..page_count {
        pb.inc(1);
        let page_data = ts.read_page(page_num)?;

        let header = match FilHeader::parse(&page_data) {
            Some(h) => h,
            None => {
                eprintln!("Page {}: Could not parse FIL header", page_num);
                invalid_count += 1;
                continue;
            }
        };

        // Skip all-zero pages
        if header.checksum == 0 && page_data.iter().all(|&b| b == 0) {
            empty_count += 1;
            if opts.verbose {
                wprintln!(writer, "Page {}: EMPTY", page_num)?;
            }
            continue;
        }

        let csum_result = validate_checksum(&page_data, page_size, Some(ts.vendor_info()));
        let lsn_valid = validate_lsn(&page_data, page_size);

        if csum_result.valid {
            valid_count += 1;
            if opts.verbose {
                wprintln!(
                    writer,
                    "Page {}: {} ({:?}, stored={}, calculated={})",
                    page_num,
                    "OK".green(),
                    csum_result.algorithm,
                    csum_result.stored_checksum,
                    csum_result.calculated_checksum,
                )?;
            }
        } else {
            invalid_count += 1;
            wprintln!(
                writer,
                "Page {}: {} checksum (stored={}, calculated={}, algorithm={:?})",
                page_num,
                "INVALID".red(),
                csum_result.stored_checksum,
                csum_result.calculated_checksum,
                csum_result.algorithm,
            )?;
        }

        // Check LSN consistency
        if !lsn_valid {
            lsn_mismatch_count += 1;
            if csum_result.valid {
                wprintln!(
                    writer,
                    "Page {}: {} - header LSN low32 does not match trailer",
                    page_num,
                    "LSN MISMATCH".yellow(),
                )?;
            }
        }
    }

    pb.finish_and_clear();

    wprintln!(writer)?;
    wprintln!(writer, "Summary:")?;
    wprintln!(writer, "  Total pages: {}", page_count)?;
    wprintln!(writer, "  Empty pages: {}", empty_count)?;
    wprintln!(writer, "  Valid checksums: {}", valid_count)?;
    if invalid_count > 0 {
        wprintln!(
            writer,
            "  Invalid checksums: {}",
            format!("{}", invalid_count).red()
        )?;
    } else {
        wprintln!(
            writer,
            "  Invalid checksums: {}",
            format!("{}", invalid_count).green()
        )?;
    }
    if lsn_mismatch_count > 0 {
        wprintln!(
            writer,
            "  LSN mismatches: {}",
            format!("{}", lsn_mismatch_count).yellow()
        )?;
    }

    if invalid_count > 0 {
        return Err(IdbError::Parse(format!(
            "{} pages with invalid checksums",
            invalid_count
        )));
    }

    Ok(())
}

fn execute_json(
    opts: &ChecksumOptions,
    ts: &mut Tablespace,
    page_size: u32,
    page_count: u64,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    let mut valid_count = 0u64;
    let mut invalid_count = 0u64;
    let mut empty_count = 0u64;
    let mut lsn_mismatch_count = 0u64;
    let mut pages = Vec::new();

    for page_num in 0..page_count {
        let page_data = ts.read_page(page_num)?;

        let header = match FilHeader::parse(&page_data) {
            Some(h) => h,
            None => {
                invalid_count += 1;
                if opts.verbose {
                    pages.push(PageChecksumJson {
                        page_number: page_num,
                        status: "error".to_string(),
                        algorithm: "unknown".to_string(),
                        stored_checksum: 0,
                        calculated_checksum: 0,
                        lsn_valid: false,
                    });
                }
                continue;
            }
        };

        if header.checksum == 0 && page_data.iter().all(|&b| b == 0) {
            empty_count += 1;
            continue;
        }

        let csum_result = validate_checksum(&page_data, page_size, Some(ts.vendor_info()));
        let lsn_valid = validate_lsn(&page_data, page_size);

        if csum_result.valid {
            valid_count += 1;
        } else {
            invalid_count += 1;
        }
        if !lsn_valid {
            lsn_mismatch_count += 1;
        }

        // In verbose JSON mode, include all pages; otherwise only invalid
        if opts.verbose || !csum_result.valid || !lsn_valid {
            let algorithm_name = match csum_result.algorithm {
                ChecksumAlgorithm::Crc32c => "crc32c",
                ChecksumAlgorithm::InnoDB => "innodb",
                ChecksumAlgorithm::MariaDbFullCrc32 => "mariadb_full_crc32",
                ChecksumAlgorithm::None => "none",
            };
            pages.push(PageChecksumJson {
                page_number: page_num,
                status: if csum_result.valid {
                    "valid".to_string()
                } else {
                    "invalid".to_string()
                },
                algorithm: algorithm_name.to_string(),
                stored_checksum: csum_result.stored_checksum,
                calculated_checksum: csum_result.calculated_checksum,
                lsn_valid,
            });
        }
    }

    let summary = ChecksumSummaryJson {
        file: opts.file.clone(),
        page_size,
        total_pages: page_count,
        empty_pages: empty_count,
        valid_pages: valid_count,
        invalid_pages: invalid_count,
        lsn_mismatches: lsn_mismatch_count,
        pages,
    };

    let json = serde_json::to_string_pretty(&summary)
        .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
    wprintln!(writer, "{}", json)?;

    if invalid_count > 0 {
        return Err(IdbError::Parse(format!(
            "{} pages with invalid checksums",
            invalid_count
        )));
    }

    Ok(())
}
