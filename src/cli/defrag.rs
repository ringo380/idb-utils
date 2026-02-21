use std::io::Write;

use byteorder::{BigEndian, ByteOrder};
use colored::Colorize;
use serde::Serialize;

use crate::cli::wprintln;
use crate::innodb::checksum::{validate_checksum, ChecksumAlgorithm};
use crate::innodb::constants::*;
use crate::innodb::page::FilHeader;
use crate::innodb::page_types::PageType;
use crate::innodb::write;
use crate::IdbError;

/// Options for the `inno defrag` subcommand.
pub struct DefragOptions {
    /// Path to the source InnoDB tablespace file (.ibd).
    pub file: String,
    /// Path to output file (always creates a new file).
    pub output: String,
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
struct DefragReport {
    source: String,
    output: String,
    source_pages: u64,
    output_pages: u64,
    index_pages: u64,
    empty_removed: u64,
    corrupt_removed: u64,
    post_validation: PostValidation,
}

#[derive(Serialize)]
struct PostValidation {
    total: u64,
    valid: u64,
}

/// Information about an INDEX page used for sorting.
struct IndexPageInfo {
    original_page_num: u64,
    index_id: u64,
    level: u16,
    data: Vec<u8>,
}

/// Defragment a tablespace: remove empty/corrupt pages, sort INDEX pages,
/// fix prev/next chains, and write a clean output file.
pub fn execute(opts: &DefragOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    let mut ts = crate::cli::open_tablespace(&opts.file, opts.page_size, opts.mmap)?;

    if let Some(ref keyring_path) = opts.keyring {
        crate::cli::setup_decryption(&mut ts, keyring_path)?;
    }

    let page_size = ts.page_size();
    let page_count = ts.page_count();
    let vendor_info = ts.vendor_info().clone();
    let all_data = ts.read_all_pages()?;
    let ps = page_size as usize;

    if !opts.json {
        wprintln!(
            writer,
            "Defragmenting {} ({} pages)...",
            opts.file,
            page_count
        )?;
    }

    // Read page 0 metadata
    let page0_data = if ps <= all_data.len() {
        &all_data[..ps]
    } else {
        return Err(IdbError::Parse(
            "File too small to contain page 0".to_string(),
        ));
    };

    let space_id = BigEndian::read_u32(&page0_data[FIL_PAGE_SPACE_ID..]);
    let fsp_offset = FIL_PAGE_DATA;
    let flags = BigEndian::read_u32(&page0_data[fsp_offset + FSP_SPACE_FLAGS..]);

    // Detect algorithm
    let algorithm = write::detect_algorithm(page0_data, page_size, Some(&vendor_info));
    let algorithm = if algorithm == ChecksumAlgorithm::None {
        ChecksumAlgorithm::Crc32c
    } else {
        algorithm
    };

    // Classify pages
    let mut index_pages: Vec<IndexPageInfo> = Vec::new();
    let mut other_pages: Vec<Vec<u8>> = Vec::new();
    let mut empty_removed = 0u64;
    let mut corrupt_removed = 0u64;
    let mut max_lsn = 0u64;

    for page_num in 1..page_count {
        // Skip page 0 — we'll build a new one
        let offset = page_num as usize * ps;
        if offset + ps > all_data.len() {
            break;
        }
        let page_data = &all_data[offset..offset + ps];

        // Empty page?
        if page_data.iter().all(|&b| b == 0) {
            empty_removed += 1;
            continue;
        }

        // Parse header
        let header = match FilHeader::parse(page_data) {
            Some(h) => h,
            None => {
                corrupt_removed += 1;
                continue;
            }
        };

        // Validate checksum
        let csum = validate_checksum(page_data, page_size, Some(&vendor_info));
        if !csum.valid {
            corrupt_removed += 1;
            if opts.verbose && !opts.json {
                wprintln!(
                    writer,
                    "Page {:>4}: {} (corrupt checksum)",
                    page_num,
                    "removed".red()
                )?;
            }
            continue;
        }

        if header.lsn > max_lsn {
            max_lsn = header.lsn;
        }

        if header.page_type == PageType::Index {
            // Extract index_id and level from the INDEX page header
            let ph = FIL_PAGE_DATA;
            let index_id = BigEndian::read_u64(&page_data[ph + PAGE_INDEX_ID..]);
            let level = BigEndian::read_u16(&page_data[ph + PAGE_LEVEL..]);

            index_pages.push(IndexPageInfo {
                original_page_num: page_num,
                index_id,
                level,
                data: page_data.to_vec(),
            });
        } else {
            other_pages.push(page_data.to_vec());
        }
    }

    // Sort INDEX pages by (index_id, level, original_page_num)
    index_pages.sort_by(|a, b| {
        a.index_id
            .cmp(&b.index_id)
            .then(a.level.cmp(&b.level))
            .then(a.original_page_num.cmp(&b.original_page_num))
    });

    // Build output: page 0 + INDEX pages (sorted) + other pages (original order)
    let total_output = 1 + index_pages.len() + other_pages.len();
    let page0 = write::build_fsp_page(
        space_id,
        total_output as u32,
        flags,
        max_lsn,
        page_size,
        algorithm,
    );

    let mut output_pages: Vec<Vec<u8>> = Vec::with_capacity(total_output);
    output_pages.push(page0);

    // Assign new page numbers to INDEX pages and fix prev/next chains
    let index_start_page = 1u32;
    for (i, idx) in index_pages.iter_mut().enumerate() {
        let new_page_num = index_start_page + i as u32;
        BigEndian::write_u32(&mut idx.data[FIL_PAGE_OFFSET..], new_page_num);
    }

    // Fix prev/next chain pointers within each (index_id, level) group
    let mut group_start = 0usize;
    while group_start < index_pages.len() {
        let group_index_id = index_pages[group_start].index_id;
        let group_level = index_pages[group_start].level;

        // Find end of group
        let mut group_end = group_start + 1;
        while group_end < index_pages.len()
            && index_pages[group_end].index_id == group_index_id
            && index_pages[group_end].level == group_level
        {
            group_end += 1;
        }

        // Set prev/next within the group
        #[allow(clippy::needless_range_loop)]
        for j in group_start..group_end {
            let prev = if j == group_start {
                FIL_NULL
            } else {
                index_start_page + j as u32 - 1
            };
            let next = if j == group_end - 1 {
                FIL_NULL
            } else {
                index_start_page + j as u32 + 1
            };
            BigEndian::write_u32(&mut index_pages[j].data[FIL_PAGE_PREV..], prev);
            BigEndian::write_u32(&mut index_pages[j].data[FIL_PAGE_NEXT..], next);
        }

        group_start = group_end;
    }

    // Recalculate checksums for INDEX pages and add to output
    for idx in &mut index_pages {
        write::fix_page_checksum(&mut idx.data, page_size, algorithm);
        output_pages.push(idx.data.clone());
    }

    // Assign new page numbers to other pages and add to output
    let other_start_page = index_start_page + index_pages.len() as u32;
    for (i, mut page) in other_pages.into_iter().enumerate() {
        let new_page_num = other_start_page + i as u32;
        BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], new_page_num);
        // Clear prev/next for non-INDEX pages (they're not part of B+Tree chains)
        BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
        BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
        write::fix_page_checksum(&mut page, page_size, algorithm);
        output_pages.push(page);
    }

    // Write output
    write::write_tablespace(&opts.output, &output_pages)?;

    // Post-validate
    let output_count = output_pages.len() as u64;
    let mut valid_count = 0u64;
    for i in 0..output_count {
        let page = write::read_page_raw(&opts.output, i, page_size)?;
        if validate_checksum(&page, page_size, Some(&vendor_info)).valid {
            valid_count += 1;
        }
    }

    let index_count = index_pages.len() as u64;

    if opts.json {
        let report = DefragReport {
            source: opts.file.clone(),
            output: opts.output.clone(),
            source_pages: page_count,
            output_pages: output_count,
            index_pages: index_count,
            empty_removed,
            corrupt_removed,
            post_validation: PostValidation {
                total: output_count,
                valid: valid_count,
            },
        };
        let json = serde_json::to_string_pretty(&report)
            .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
        wprintln!(writer, "{}", json)?;
    } else {
        wprintln!(writer)?;
        wprintln!(writer, "Defrag Summary:")?;
        wprintln!(
            writer,
            "  Source:          {} ({} pages)",
            opts.file,
            page_count
        )?;
        wprintln!(
            writer,
            "  Output:          {} ({} pages)",
            opts.output,
            output_count
        )?;
        wprintln!(writer, "  INDEX pages:     {}", index_count)?;
        if empty_removed > 0 {
            wprintln!(writer, "  Empty removed:   {}", empty_removed)?;
        }
        if corrupt_removed > 0 {
            wprintln!(
                writer,
                "  Corrupt removed: {}",
                format!("{}", corrupt_removed).red()
            )?;
        }
        wprintln!(
            writer,
            "  Post-validation: {}/{} valid checksums",
            valid_count,
            output_count
        )?;
    }

    Ok(())
}
