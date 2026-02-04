use colored::Colorize;

use crate::innodb::checksum::{validate_checksum, validate_lsn};
use crate::innodb::page::FilHeader;
use crate::innodb::tablespace::Tablespace;
use crate::IdbError;

pub struct ChecksumOptions {
    pub file: String,
    pub page_size: Option<u32>,
}

pub fn execute(opts: &ChecksumOptions) -> Result<(), IdbError> {
    let mut ts = match opts.page_size {
        Some(ps) => Tablespace::open_with_page_size(&opts.file, ps)?,
        None => Tablespace::open(&opts.file)?,
    };

    let page_size = ts.page_size();
    let page_count = ts.page_count();

    println!(
        "Validating checksums for {} ({} pages, page size {})...",
        opts.file, page_count, page_size
    );
    println!();

    let mut valid_count = 0u64;
    let mut invalid_count = 0u64;
    let mut empty_count = 0u64;
    let mut lsn_mismatch_count = 0u64;

    for page_num in 0..page_count {
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
            continue;
        }

        let csum_result = validate_checksum(&page_data, page_size);

        if csum_result.valid {
            valid_count += 1;
        } else {
            invalid_count += 1;
            println!(
                "Page {}: {} checksum (stored={}, calculated={}, algorithm={:?})",
                page_num,
                "INVALID".red(),
                csum_result.stored_checksum,
                csum_result.calculated_checksum,
                csum_result.algorithm,
            );
        }

        // Check LSN consistency
        if !validate_lsn(&page_data, page_size) {
            lsn_mismatch_count += 1;
            if csum_result.valid {
                println!(
                    "Page {}: {} - header LSN low32 does not match trailer",
                    page_num,
                    "LSN MISMATCH".yellow(),
                );
            }
        }
    }

    println!();
    println!("Summary:");
    println!("  Total pages: {}", page_count);
    println!("  Empty pages: {}", empty_count);
    println!("  Valid checksums: {}", valid_count);
    if invalid_count > 0 {
        println!(
            "  Invalid checksums: {}",
            format!("{}", invalid_count).red()
        );
    } else {
        println!(
            "  Invalid checksums: {}",
            format!("{}", invalid_count).green()
        );
    }
    if lsn_mismatch_count > 0 {
        println!(
            "  LSN mismatches: {}",
            format!("{}", lsn_mismatch_count).yellow()
        );
    }

    if invalid_count > 0 {
        std::process::exit(1);
    }

    Ok(())
}
