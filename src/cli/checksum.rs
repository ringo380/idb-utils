use std::io::Write;

use colored::Colorize;
use serde::Serialize;

use indicatif::{ProgressBar, ProgressStyle};

use crate::cli::wprintln;
use crate::innodb::checksum::{validate_checksum, validate_lsn, ChecksumAlgorithm};
use crate::innodb::page::FilHeader;
use crate::innodb::tablespace::Tablespace;
use crate::IdbError;

pub struct ChecksumOptions {
    pub file: String,
    pub verbose: bool,
    pub json: bool,
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
        opts.file, page_count, page_size
    )?;
    wprintln!(writer)?;

    let mut valid_count = 0u64;
    let mut invalid_count = 0u64;
    let mut empty_count = 0u64;
    let mut lsn_mismatch_count = 0u64;

    let pb = ProgressBar::new(page_count);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} pages ({eta})")
            .unwrap()
            .progress_chars("#>-"),
    );

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

        let csum_result = validate_checksum(&page_data, page_size);
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
        std::process::exit(1);
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

        let csum_result = validate_checksum(&page_data, page_size);
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
        std::process::exit(1);
    }

    Ok(())
}
