use std::collections::HashMap;

use byteorder::{BigEndian, ByteOrder};
use colored::Colorize;

use crate::innodb::checksum;
use crate::innodb::page::{FilHeader, FspHeader};
use crate::innodb::page_types::PageType;
use crate::innodb::tablespace::Tablespace;
use crate::util::hex::format_offset;
use crate::IdbError;

/// Options for the parse subcommand.
pub struct ParseOptions {
    pub file: String,
    pub page: Option<u64>,
    pub verbose: bool,
    pub no_empty: bool,
    pub page_size: Option<u32>,
    pub json: bool,
}

/// JSON-serializable page info.
#[derive(serde::Serialize)]
struct PageJson {
    page_number: u64,
    header: FilHeader,
    page_type_name: String,
    page_type_description: String,
    byte_start: u64,
    byte_end: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    fsp_header: Option<crate::innodb::page::FspHeader>,
}

/// Execute the parse subcommand.
pub fn execute(opts: &ParseOptions) -> Result<(), IdbError> {
    let mut ts = match opts.page_size {
        Some(ps) => Tablespace::open_with_page_size(&opts.file, ps)?,
        None => Tablespace::open(&opts.file)?,
    };

    let page_size = ts.page_size();

    if opts.json {
        return execute_json(opts, &mut ts, page_size);
    }

    if let Some(page_num) = opts.page {
        // Single page mode
        let page_data = ts.read_page(page_num)?;
        print_page_info(&page_data, page_num, page_size, opts.verbose);
    } else {
        // All pages mode
        // Print FSP header first
        let page0 = ts.read_page(0)?;
        if let Some(fsp) = FspHeader::parse(&page0) {
            print_fsp_header(&fsp);
            println!();
        }

        println!(
            "Pages in {} ({} pages, page size {}):",
            opts.file,
            ts.page_count(),
            page_size
        );
        println!("{}", "-".repeat(50));

        let mut type_counts: HashMap<PageType, u64> = HashMap::new();

        for page_num in 0..ts.page_count() {
            let page_data = ts.read_page(page_num)?;
            let header = match FilHeader::parse(&page_data) {
                Some(h) => h,
                None => continue,
            };

            *type_counts.entry(header.page_type).or_insert(0) += 1;

            // Skip empty pages if --no-empty
            if opts.no_empty && header.checksum == 0 && header.page_type == PageType::Allocated {
                continue;
            }

            // Skip pages with zero checksum unless they are page 0
            if header.checksum == 0 && page_num != 0 && !opts.verbose {
                continue;
            }

            print_page_info(&page_data, page_num, page_size, opts.verbose);
        }

        // Print page type summary
        println!();
        println!("{}", "Page Type Summary".bold());
        let mut sorted_types: Vec<_> = type_counts.iter().collect();
        sorted_types.sort_by(|a, b| b.1.cmp(a.1));
        for (pt, count) in sorted_types {
            let label = if *count == 1 { "page" } else { "pages" };
            println!("  {:20} {:>6} {}", pt.name(), count, label);
        }
    }

    Ok(())
}

/// Execute parse in JSON output mode.
fn execute_json(
    opts: &ParseOptions,
    ts: &mut Tablespace,
    page_size: u32,
) -> Result<(), IdbError> {
    let mut pages = Vec::new();

    let range: Box<dyn Iterator<Item = u64>> = if let Some(p) = opts.page {
        Box::new(std::iter::once(p))
    } else {
        Box::new(0..ts.page_count())
    };

    for page_num in range {
        let page_data = ts.read_page(page_num)?;
        let header = match FilHeader::parse(&page_data) {
            Some(h) => h,
            None => continue,
        };

        if opts.no_empty && header.checksum == 0 && header.page_type == PageType::Allocated {
            continue;
        }

        let pt = header.page_type;
        let byte_start = page_num * page_size as u64;

        let fsp_header = if page_num == 0 {
            FspHeader::parse(&page_data)
        } else {
            None
        };

        pages.push(PageJson {
            page_number: page_num,
            page_type_name: pt.name().to_string(),
            page_type_description: pt.description().to_string(),
            byte_start,
            byte_end: byte_start + page_size as u64,
            header,
            fsp_header,
        });
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&pages).unwrap_or_else(|_| "[]".to_string())
    );
    Ok(())
}

/// Print detailed information about a single page.
fn print_page_info(page_data: &[u8], page_num: u64, page_size: u32, verbose: bool) {
    let header = match FilHeader::parse(page_data) {
        Some(h) => h,
        None => {
            eprintln!("Could not parse FIL header for page {}", page_num);
            return;
        }
    };

    let byte_start = page_num * page_size as u64;
    let byte_end = byte_start + page_size as u64;

    let pt = header.page_type;

    println!("Page: {}", header.page_number);
    println!("{}", "-".repeat(20));
    println!("{}", "HEADER".bold());
    println!("Byte Start: {}", format_offset(byte_start));
    println!(
        "Page Type: {}\n-- {}: {} - {}",
        pt.as_u16(),
        pt.name(),
        pt.description(),
        pt.usage()
    );

    if verbose {
        println!("PAGE_N_HEAP (Amount of records in page): {}", read_page_n_heap(page_data));
    }

    print!("Prev Page: ");
    if !header.has_prev() {
        println!("Not used.");
    } else {
        println!("{}", header.prev_page);
    }

    print!("Next Page: ");
    if !header.has_next() {
        println!("Not used.");
    } else {
        println!("{}", header.next_page);
    }

    println!("LSN: {}", header.lsn);
    println!("Space ID: {}", header.space_id);
    println!("Checksum: {}", header.checksum);

    // Checksum validation
    let csum_result = checksum::validate_checksum(page_data, page_size);
    if verbose {
        let status = if csum_result.valid {
            "OK".green().to_string()
        } else {
            "MISMATCH".red().to_string()
        };
        println!(
            "Checksum Status: {} ({:?}, stored={}, calculated={})",
            status, csum_result.algorithm, csum_result.stored_checksum, csum_result.calculated_checksum
        );
    }

    println!();

    // Trailer
    let ps = page_size as usize;
    if page_data.len() >= ps {
        let trailer_offset = ps - 8;
        if let Some(trailer) = crate::innodb::page::FilTrailer::parse(&page_data[trailer_offset..]) {
            println!("{}", "TRAILER".bold());
            println!("Old-style Checksum: {}", trailer.checksum);
            println!("Low 32 bits of LSN: {}", trailer.lsn_low32);
            println!("Byte End: {}", format_offset(byte_end));

            // LSN validation
            if verbose {
                let lsn_valid = checksum::validate_lsn(page_data, page_size);
                let lsn_status = if lsn_valid {
                    "OK".green().to_string()
                } else {
                    "MISMATCH".red().to_string()
                };
                println!("LSN Consistency: {}", lsn_status);
            }
        }
    }
    println!("{}", "-".repeat(20));
}

/// Print FSP header information.
fn print_fsp_header(fsp: &FspHeader) {
    println!("{}", "-".repeat(20));
    println!("{}", "FSP_HDR - Filespace Header".bold());
    println!("{}", "-".repeat(20));
    println!("Space ID: {}", fsp.space_id);
    println!("Size (pages): {}", fsp.size);
    println!("Page Free Limit: {}", fsp.free_limit);
    println!("Flags: {}", fsp.flags);
}

/// Read PAGE_N_HEAP from the page header (INDEX page specific).
fn read_page_n_heap(page_data: &[u8]) -> u16 {
    let offset = crate::innodb::constants::FIL_PAGE_DATA + 4; // PAGE_N_HEAP is at FIL_PAGE_DATA + 4
    if page_data.len() < offset + 2 {
        return 0;
    }
    BigEndian::read_u16(&page_data[offset..])
}
