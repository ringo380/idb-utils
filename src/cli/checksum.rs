use std::io::Write;

use colored::Colorize;
use rayon::prelude::*;
use serde::Serialize;

use crate::cli::{create_progress_bar, wprintln};
use crate::innodb::checksum::{validate_checksum, validate_lsn, ChecksumAlgorithm, ChecksumResult};
use crate::innodb::page::FilHeader;
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
    /// Path to MySQL keyring file for decrypting encrypted tablespaces.
    pub keyring: Option<String>,
    /// Number of threads for parallel processing (0 = auto-detect).
    pub threads: usize,
    /// Use memory-mapped I/O for file access.
    pub mmap: bool,
    /// Stream results incrementally for lower memory usage.
    pub streaming: bool,
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

/// Result of validating a single page's checksum, used for parallel processing.
enum PageResult {
    /// Page header could not be parsed.
    ParseError,
    /// Page is all zeros (empty/allocated).
    Empty,
    /// Page was validated successfully.
    Validated {
        csum_result: ChecksumResult,
        lsn_valid: bool,
    },
}

/// Validate a single page's checksum and LSN. Pure function safe for parallel execution.
fn validate_page(
    page_data: &[u8],
    page_size: u32,
    vendor_info: &crate::innodb::vendor::VendorInfo,
) -> PageResult {
    let header = match FilHeader::parse(page_data) {
        Some(h) => h,
        None => return PageResult::ParseError,
    };

    if header.checksum == 0 && page_data.iter().all(|&b| b == 0) {
        return PageResult::Empty;
    }

    let csum_result = validate_checksum(page_data, page_size, Some(vendor_info));
    let lsn_valid = validate_lsn(page_data, page_size);

    PageResult::Validated {
        csum_result,
        lsn_valid,
    }
}

/// Validate page checksums for every page in an InnoDB tablespace.
///
/// Iterates over all pages and validates the stored checksum (bytes 0-3 of the
/// FIL header) against two algorithms: **CRC-32C** (MySQL 5.7.7+), which XORs
/// two independent CRC-32C values computed over bytes \[4..26) and
/// \[38..page_size-8); and **legacy InnoDB**, which uses `ut_fold_ulint_pair`
/// with u32 wrapping arithmetic over the same two byte ranges. A page is
/// considered valid if either algorithm matches the stored value.
///
/// Additionally checks **LSN consistency**: the low 32 bits of the header LSN
/// (bytes 16-23) must match the LSN value in the 8-byte FIL trailer at the
/// end of the page. All-zero pages are counted as empty and skipped entirely.
///
/// When the tablespace has more than one page, all page data is read into memory
/// and checksums are validated in parallel using rayon. Results are collected in
/// page order for deterministic output.
///
/// Prints a summary with total, empty, valid, and invalid page counts. In
/// `--verbose` mode, every non-empty page is printed with its algorithm,
/// stored and calculated checksum values, and LSN status. The process exits
/// with code 1 if any page has an invalid checksum, making this suitable for
/// scripted integrity checks.
///
/// **Note**: When `--streaming` is combined with `--json`, the output uses
/// NDJSON (one JSON object per line) rather than a single JSON document.
pub fn execute(opts: &ChecksumOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    let mut ts = crate::cli::open_tablespace(&opts.file, opts.page_size, opts.mmap)?;

    if let Some(ref keyring_path) = opts.keyring {
        crate::cli::setup_decryption(&mut ts, keyring_path)?;
    }

    let page_size = ts.page_size();
    let page_count = ts.page_count();
    let vendor_info = ts.vendor_info().clone();

    // Streaming mode: process one page at a time, output immediately
    if opts.streaming {
        return execute_streaming(opts, &mut ts, page_size, page_count, &vendor_info, writer);
    }

    // Read all pages into memory for parallel processing
    let all_data = ts.read_all_pages()?;
    let ps = page_size as usize;

    if opts.json {
        return execute_json_parallel(
            opts,
            &all_data,
            ps,
            page_size,
            page_count,
            &vendor_info,
            writer,
        );
    }

    wprintln!(
        writer,
        "Validating checksums for {} ({} pages, page size {})...",
        opts.file,
        page_count,
        page_size
    )?;
    wprintln!(writer)?;

    // Create progress bar before parallel work so it tracks real progress
    let pb = create_progress_bar(page_count, "pages");

    // Process all pages in parallel
    let results: Vec<(u64, PageResult)> = (0..page_count)
        .into_par_iter()
        .map(|page_num| {
            let offset = page_num as usize * ps;
            if offset + ps > all_data.len() {
                pb.inc(1);
                return (page_num, PageResult::ParseError);
            }
            let page_data = &all_data[offset..offset + ps];
            let result = validate_page(page_data, page_size, &vendor_info);
            pb.inc(1);
            (page_num, result)
        })
        .collect();

    pb.finish_and_clear();

    // Output results sequentially in page order (rayon collect preserves order)
    let mut valid_count = 0u64;
    let mut invalid_count = 0u64;
    let mut empty_count = 0u64;
    let mut lsn_mismatch_count = 0u64;

    for (page_num, result) in &results {
        match result {
            PageResult::ParseError => {
                eprintln!("Page {}: Could not parse FIL header", page_num);
                invalid_count += 1;
            }
            PageResult::Empty => {
                empty_count += 1;
                if opts.verbose {
                    wprintln!(writer, "Page {}: EMPTY", page_num)?;
                }
            }
            PageResult::Validated {
                csum_result,
                lsn_valid,
            } => {
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
        }
    }

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

/// Return a short string name for a checksum algorithm.
fn algorithm_name(algo: ChecksumAlgorithm) -> &'static str {
    match algo {
        ChecksumAlgorithm::Crc32c => "crc32c",
        ChecksumAlgorithm::InnoDB => "innodb",
        ChecksumAlgorithm::MariaDbFullCrc32 => "mariadb_full_crc32",
        ChecksumAlgorithm::None => "none",
    }
}

/// Streaming mode: process pages one at a time via `for_each_page()`, writing
/// each result immediately. No progress bar, no bulk memory allocation.
/// JSON output uses NDJSON (one JSON object per line).
fn execute_streaming(
    opts: &ChecksumOptions,
    ts: &mut crate::innodb::tablespace::Tablespace,
    page_size: u32,
    page_count: u64,
    vendor_info: &crate::innodb::vendor::VendorInfo,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    let mut valid_count = 0u64;
    let mut invalid_count = 0u64;
    let mut empty_count = 0u64;
    let mut lsn_mismatch_count = 0u64;

    if !opts.json {
        wprintln!(
            writer,
            "Validating checksums for {} ({} pages, page size {})...",
            opts.file,
            page_count,
            page_size
        )?;
        wprintln!(writer)?;
    }

    ts.for_each_page(|page_num, page_data| {
        let result = validate_page(page_data, page_size, vendor_info);

        match &result {
            PageResult::ParseError => {
                invalid_count += 1;
                if opts.json {
                    let obj = PageChecksumJson {
                        page_number: page_num,
                        status: "error".to_string(),
                        algorithm: "unknown".to_string(),
                        stored_checksum: 0,
                        calculated_checksum: 0,
                        lsn_valid: false,
                    };
                    let line = serde_json::to_string(&obj)
                        .map_err(|e| IdbError::Parse(format!("JSON error: {}", e)))?;
                    wprintln!(writer, "{}", line)?;
                } else {
                    eprintln!("Page {}: Could not parse FIL header", page_num);
                }
            }
            PageResult::Empty => {
                empty_count += 1;
                // In streaming JSON mode, skip empty pages (same as non-streaming)
                if !opts.json && opts.verbose {
                    wprintln!(writer, "Page {}: EMPTY", page_num)?;
                }
            }
            PageResult::Validated {
                csum_result,
                lsn_valid,
            } => {
                if csum_result.valid {
                    valid_count += 1;
                } else {
                    invalid_count += 1;
                }
                if !lsn_valid {
                    lsn_mismatch_count += 1;
                }

                if opts.json {
                    if opts.verbose || !csum_result.valid || !lsn_valid {
                        let obj = PageChecksumJson {
                            page_number: page_num,
                            status: if csum_result.valid {
                                "valid".to_string()
                            } else {
                                "invalid".to_string()
                            },
                            algorithm: algorithm_name(csum_result.algorithm).to_string(),
                            stored_checksum: csum_result.stored_checksum,
                            calculated_checksum: csum_result.calculated_checksum,
                            lsn_valid: *lsn_valid,
                        };
                        let line = serde_json::to_string(&obj)
                            .map_err(|e| IdbError::Parse(format!("JSON error: {}", e)))?;
                        wprintln!(writer, "{}", line)?;
                    }
                } else {
                    if csum_result.valid {
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

                    if !lsn_valid && csum_result.valid {
                        wprintln!(
                            writer,
                            "Page {}: {} - header LSN low32 does not match trailer",
                            page_num,
                            "LSN MISMATCH".yellow(),
                        )?;
                    }
                }
            }
        }
        Ok(())
    })?;

    if !opts.json {
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
    }

    if invalid_count > 0 {
        return Err(IdbError::Parse(format!(
            "{} pages with invalid checksums",
            invalid_count
        )));
    }

    Ok(())
}

fn execute_json_parallel(
    opts: &ChecksumOptions,
    all_data: &[u8],
    ps: usize,
    page_size: u32,
    page_count: u64,
    vendor_info: &crate::innodb::vendor::VendorInfo,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    // Process all pages in parallel
    let results: Vec<(u64, PageResult)> = (0..page_count)
        .into_par_iter()
        .map(|page_num| {
            let offset = page_num as usize * ps;
            if offset + ps > all_data.len() {
                return (page_num, PageResult::ParseError);
            }
            let page_data = &all_data[offset..offset + ps];
            (page_num, validate_page(page_data, page_size, vendor_info))
        })
        .collect();

    let mut valid_count = 0u64;
    let mut invalid_count = 0u64;
    let mut empty_count = 0u64;
    let mut lsn_mismatch_count = 0u64;
    let mut pages = Vec::new();

    for (page_num, result) in &results {
        match result {
            PageResult::ParseError => {
                invalid_count += 1;
                if opts.verbose {
                    pages.push(PageChecksumJson {
                        page_number: *page_num,
                        status: "error".to_string(),
                        algorithm: "unknown".to_string(),
                        stored_checksum: 0,
                        calculated_checksum: 0,
                        lsn_valid: false,
                    });
                }
            }
            PageResult::Empty => {
                empty_count += 1;
            }
            PageResult::Validated {
                csum_result,
                lsn_valid,
            } => {
                if csum_result.valid {
                    valid_count += 1;
                } else {
                    invalid_count += 1;
                }
                if !lsn_valid {
                    lsn_mismatch_count += 1;
                }

                if opts.verbose || !csum_result.valid || !lsn_valid {
                    pages.push(PageChecksumJson {
                        page_number: *page_num,
                        status: if csum_result.valid {
                            "valid".to_string()
                        } else {
                            "invalid".to_string()
                        },
                        algorithm: algorithm_name(csum_result.algorithm).to_string(),
                        stored_checksum: csum_result.stored_checksum,
                        calculated_checksum: csum_result.calculated_checksum,
                        lsn_valid: *lsn_valid,
                    });
                }
            }
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
