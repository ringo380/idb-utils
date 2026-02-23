use std::collections::HashMap;
use std::io::Write;

use byteorder::{BigEndian, ByteOrder};
use colored::Colorize;
use rayon::prelude::*;

use crate::cli::{create_progress_bar, wprint, wprintln};
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
    /// Output as CSV.
    pub csv: bool,
    pub keyring: Option<String>,
    /// Number of threads for parallel processing (0 = auto-detect).
    pub threads: usize,
    /// Use memory-mapped I/O for file access.
    pub mmap: bool,
    /// Stream results incrementally for lower memory usage.
    pub streaming: bool,
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

/// Pre-parsed page data for parallel processing.
struct ParsedPage {
    page_num: u64,
    header: Option<FilHeader>,
    page_type: PageType,
}

/// Parse an InnoDB tablespace file and display page headers with a type summary.
///
/// Opens the tablespace, auto-detects (or uses the overridden) page size, then
/// iterates over every page reading the 38-byte FIL header to extract the
/// checksum, page number, prev/next page pointers, LSN, page type, and space ID.
/// Page 0 additionally displays the FSP header (space ID, tablespace size,
/// free-page limit, and flags).
///
/// When the tablespace has more than one page, all page data is read into memory
/// and headers are parsed in parallel using rayon. Results are collected in
/// page order for deterministic output.
///
/// In **single-page mode** (`-p N`), only the specified page is printed with
/// its full FIL header and trailer. In **full-file mode** (the default), all
/// pages are listed and a frequency summary table is appended showing how many
/// pages of each type exist. Pages with zero checksum and type `Allocated` are
/// skipped by default unless `--verbose` is set; `--no-empty` additionally
/// filters these from `--json` output.
///
/// With `--verbose`, each page also shows checksum validation status (algorithm,
/// stored vs. calculated values) and LSN consistency between header and trailer.
pub fn execute(opts: &ParseOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    let mut ts = crate::cli::open_tablespace(&opts.file, opts.page_size, opts.mmap)?;

    if let Some(ref keyring_path) = opts.keyring {
        crate::cli::setup_decryption(&mut ts, keyring_path)?;
    }

    let page_size = ts.page_size();

    // Streaming mode: process one page at a time, output immediately
    if opts.streaming && opts.page.is_none() {
        if opts.json {
            return execute_streaming_json(opts, &mut ts, page_size, writer);
        }
        return execute_streaming_text(opts, &mut ts, page_size, writer);
    }

    if opts.json {
        return execute_json(opts, &mut ts, page_size, writer);
    }

    if opts.csv {
        return execute_csv(opts, &mut ts, page_size, writer);
    }

    if let Some(page_num) = opts.page {
        // Single page mode — no parallelism needed
        let page_data = ts.read_page(page_num)?;
        print_page_info(writer, &page_data, page_num, page_size, opts.verbose)?;
    } else {
        // All pages mode — use parallel processing
        let page_count = ts.page_count();
        let ps = page_size as usize;

        // Read all pages into memory
        let all_data = ts.read_all_pages()?;

        // Print FSP header first
        let page0_data = &all_data[0..ps];
        if let Some(fsp) = FspHeader::parse(page0_data) {
            print_fsp_header(writer, &fsp)?;
            wprintln!(writer)?;
        }

        wprintln!(
            writer,
            "Pages in {} ({} pages, page size {}):",
            opts.file,
            page_count,
            page_size
        )?;
        wprintln!(writer, "{}", "-".repeat(50))?;

        // Create progress bar before parallel work so it tracks real progress
        let pb = create_progress_bar(page_count, "pages");

        // Parse headers in parallel to build type counts
        let parsed_pages: Vec<ParsedPage> = (0..page_count)
            .into_par_iter()
            .map(|page_num| {
                let offset = page_num as usize * ps;
                if offset + ps > all_data.len() {
                    pb.inc(1);
                    return ParsedPage {
                        page_num,
                        header: None,
                        page_type: PageType::Unknown(0),
                    };
                }
                let page_data = &all_data[offset..offset + ps];
                let header = FilHeader::parse(page_data);
                let page_type = header
                    .as_ref()
                    .map(|h| h.page_type)
                    .unwrap_or(PageType::Unknown(0));
                pb.inc(1);
                ParsedPage {
                    page_num,
                    header,
                    page_type,
                }
            })
            .collect();

        pb.finish_and_clear();

        let mut type_counts: HashMap<PageType, u64> = HashMap::new();

        for pp in &parsed_pages {
            let header = match &pp.header {
                Some(h) => h,
                None => continue,
            };

            *type_counts.entry(pp.page_type).or_insert(0) += 1;

            // Skip empty pages if --no-empty
            if opts.no_empty && header.checksum == 0 && header.page_type == PageType::Allocated {
                continue;
            }

            // Skip pages with zero checksum unless they are page 0
            if header.checksum == 0 && pp.page_num != 0 && !opts.verbose {
                continue;
            }

            let offset = pp.page_num as usize * ps;
            let page_data = &all_data[offset..offset + ps];
            print_page_info(writer, page_data, pp.page_num, page_size, opts.verbose)?;
        }

        // Print page type summary
        wprintln!(writer)?;
        wprintln!(writer, "{}", "Page Type Summary".bold())?;
        let mut sorted_types: Vec<_> = type_counts.iter().collect();
        sorted_types.sort_by(|a, b| b.1.cmp(a.1));
        for (pt, count) in sorted_types {
            let label = if *count == 1 { "page" } else { "pages" };
            wprintln!(writer, "  {:20} {:>6} {}", pt.name(), count, label)?;
        }
    }

    Ok(())
}

/// Streaming text mode: process pages one at a time via `for_each_page()`,
/// writing each result immediately. No progress bar, no bulk memory allocation.
fn execute_streaming_text(
    opts: &ParseOptions,
    ts: &mut Tablespace,
    page_size: u32,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    let page_count = ts.page_count();

    // Print FSP header from page 0 first
    let page0_data = ts.read_page(0)?;
    if let Some(fsp) = FspHeader::parse(&page0_data) {
        print_fsp_header(writer, &fsp)?;
        wprintln!(writer)?;
    }

    wprintln!(
        writer,
        "Pages in {} ({} pages, page size {}):",
        opts.file,
        page_count,
        page_size
    )?;
    wprintln!(writer, "{}", "-".repeat(50))?;

    let mut type_counts: HashMap<PageType, u64> = HashMap::new();

    ts.for_each_page(|page_num, page_data| {
        let header = match FilHeader::parse(page_data) {
            Some(h) => h,
            None => return Ok(()),
        };

        let page_type = header.page_type;
        *type_counts.entry(page_type).or_insert(0) += 1;

        // Skip empty pages if --no-empty
        if opts.no_empty && header.checksum == 0 && header.page_type == PageType::Allocated {
            return Ok(());
        }

        // Skip pages with zero checksum unless they are page 0
        if header.checksum == 0 && page_num != 0 && !opts.verbose {
            return Ok(());
        }

        print_page_info(writer, page_data, page_num, page_size, opts.verbose)?;
        Ok(())
    })?;

    // Print page type summary
    wprintln!(writer)?;
    wprintln!(writer, "{}", "Page Type Summary".bold())?;
    let mut sorted_types: Vec<_> = type_counts.iter().collect();
    sorted_types.sort_by(|a, b| b.1.cmp(a.1));
    for (pt, count) in sorted_types {
        let label = if *count == 1 { "page" } else { "pages" };
        wprintln!(writer, "  {:20} {:>6} {}", pt.name(), count, label)?;
    }

    Ok(())
}

/// Streaming JSON mode: output NDJSON (one JSON object per line per page).
fn execute_streaming_json(
    opts: &ParseOptions,
    ts: &mut Tablespace,
    page_size: u32,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    ts.for_each_page(|page_num, page_data| {
        let header = match FilHeader::parse(page_data) {
            Some(h) => h,
            None => return Ok(()),
        };

        if opts.no_empty && header.checksum == 0 && header.page_type == PageType::Allocated {
            return Ok(());
        }

        let pt = header.page_type;
        let byte_start = page_num * page_size as u64;
        let fsp_header = if page_num == 0 {
            FspHeader::parse(page_data)
        } else {
            None
        };

        let page_json = PageJson {
            page_number: page_num,
            page_type_name: pt.name().to_string(),
            page_type_description: pt.description().to_string(),
            byte_start,
            byte_end: byte_start + page_size as u64,
            header,
            fsp_header,
        };

        let line = serde_json::to_string(&page_json)
            .map_err(|e| IdbError::Parse(format!("JSON error: {}", e)))?;
        wprintln!(writer, "{}", line)?;
        Ok(())
    })?;

    Ok(())
}

/// Execute parse in CSV output mode.
fn execute_csv(
    opts: &ParseOptions,
    ts: &mut Tablespace,
    _page_size: u32,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    wprintln!(
        writer,
        "page_number,checksum,page_type,lsn,space_id,prev_page,next_page"
    )?;

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

        let prev = if header.has_prev() {
            header.prev_page.to_string()
        } else {
            String::new()
        };
        let next = if header.has_next() {
            header.next_page.to_string()
        } else {
            String::new()
        };

        wprintln!(
            writer,
            "{},{},{},{},{},{},{}",
            page_num,
            header.checksum,
            crate::cli::csv_escape(header.page_type.name()),
            header.lsn,
            header.space_id,
            prev,
            next
        )?;
    }
    Ok(())
}

/// Execute parse in JSON output mode.
fn execute_json(
    opts: &ParseOptions,
    ts: &mut Tablespace,
    page_size: u32,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    if let Some(p) = opts.page {
        // Single page — no parallelism
        let page_data = ts.read_page(p)?;
        let header = match FilHeader::parse(&page_data) {
            Some(h) => h,
            None => {
                wprintln!(writer, "[]")?;
                return Ok(());
            }
        };

        let pt = header.page_type;
        let byte_start = p * page_size as u64;
        let fsp_header = if p == 0 {
            FspHeader::parse(&page_data)
        } else {
            None
        };

        let pages = vec![PageJson {
            page_number: p,
            page_type_name: pt.name().to_string(),
            page_type_description: pt.description().to_string(),
            byte_start,
            byte_end: byte_start + page_size as u64,
            header,
            fsp_header,
        }];

        let json = serde_json::to_string_pretty(&pages)
            .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
        wprintln!(writer, "{}", json)?;
        return Ok(());
    }

    // Full tablespace — read all then process in parallel
    let page_count = ts.page_count();
    let ps = page_size as usize;
    let all_data = ts.read_all_pages()?;

    let pages: Vec<Option<PageJson>> = (0..page_count)
        .into_par_iter()
        .map(|page_num| {
            let offset = page_num as usize * ps;
            if offset + ps > all_data.len() {
                return None;
            }
            let page_data = &all_data[offset..offset + ps];
            let header = match FilHeader::parse(page_data) {
                Some(h) => h,
                None => return None,
            };

            if opts.no_empty && header.checksum == 0 && header.page_type == PageType::Allocated {
                return None;
            }

            let pt = header.page_type;
            let byte_start = page_num * page_size as u64;
            let fsp_header = if page_num == 0 {
                FspHeader::parse(page_data)
            } else {
                None
            };

            Some(PageJson {
                page_number: page_num,
                page_type_name: pt.name().to_string(),
                page_type_description: pt.description().to_string(),
                byte_start,
                byte_end: byte_start + page_size as u64,
                header,
                fsp_header,
            })
        })
        .collect();

    let pages: Vec<PageJson> = pages.into_iter().flatten().collect();

    let json = serde_json::to_string_pretty(&pages)
        .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
    wprintln!(writer, "{}", json)?;
    Ok(())
}

/// Print detailed information about a single page.
fn print_page_info(
    writer: &mut dyn Write,
    page_data: &[u8],
    page_num: u64,
    page_size: u32,
    verbose: bool,
) -> Result<(), IdbError> {
    let header = match FilHeader::parse(page_data) {
        Some(h) => h,
        None => {
            eprintln!("Could not parse FIL header for page {}", page_num);
            return Ok(());
        }
    };

    let byte_start = page_num * page_size as u64;
    let byte_end = byte_start + page_size as u64;

    let pt = header.page_type;

    wprintln!(writer, "Page: {}", header.page_number)?;
    wprintln!(writer, "{}", "-".repeat(20))?;
    wprintln!(writer, "{}", "HEADER".bold())?;
    wprintln!(writer, "Byte Start: {}", format_offset(byte_start))?;
    wprintln!(
        writer,
        "Page Type: {}\n-- {}: {} - {}",
        pt.as_u16(),
        pt.name(),
        pt.description(),
        pt.usage()
    )?;

    if verbose {
        wprintln!(
            writer,
            "PAGE_N_HEAP (Amount of records in page): {}",
            read_page_n_heap(page_data)
        )?;
    }

    wprint!(writer, "Prev Page: ")?;
    if !header.has_prev() {
        wprintln!(writer, "Not used.")?;
    } else {
        wprintln!(writer, "{}", header.prev_page)?;
    }

    wprint!(writer, "Next Page: ")?;
    if !header.has_next() {
        wprintln!(writer, "Not used.")?;
    } else {
        wprintln!(writer, "{}", header.next_page)?;
    }

    wprintln!(writer, "LSN: {}", header.lsn)?;
    wprintln!(writer, "Space ID: {}", header.space_id)?;
    wprintln!(writer, "Checksum: {}", header.checksum)?;

    // Checksum validation
    let csum_result = checksum::validate_checksum(page_data, page_size, None);
    if verbose {
        let status = if csum_result.valid {
            "OK".green().to_string()
        } else {
            "MISMATCH".red().to_string()
        };
        wprintln!(
            writer,
            "Checksum Status: {} ({:?}, stored={}, calculated={})",
            status,
            csum_result.algorithm,
            csum_result.stored_checksum,
            csum_result.calculated_checksum
        )?;
    }

    wprintln!(writer)?;

    // Trailer
    let ps = page_size as usize;
    if page_data.len() >= ps {
        let trailer_offset = ps - 8;
        if let Some(trailer) = crate::innodb::page::FilTrailer::parse(&page_data[trailer_offset..])
        {
            wprintln!(writer, "{}", "TRAILER".bold())?;
            wprintln!(writer, "Old-style Checksum: {}", trailer.checksum)?;
            wprintln!(writer, "Low 32 bits of LSN: {}", trailer.lsn_low32)?;
            wprintln!(writer, "Byte End: {}", format_offset(byte_end))?;

            // LSN validation
            if verbose {
                let lsn_valid = checksum::validate_lsn(page_data, page_size);
                let lsn_status = if lsn_valid {
                    "OK".green().to_string()
                } else {
                    "MISMATCH".red().to_string()
                };
                wprintln!(writer, "LSN Consistency: {}", lsn_status)?;
            }
        }
    }
    wprintln!(writer, "{}", "-".repeat(20))?;
    Ok(())
}

/// Print FSP header information.
fn print_fsp_header(writer: &mut dyn Write, fsp: &FspHeader) -> Result<(), IdbError> {
    wprintln!(writer, "{}", "-".repeat(20))?;
    wprintln!(writer, "{}", "FSP_HDR - Filespace Header".bold())?;
    wprintln!(writer, "{}", "-".repeat(20))?;
    wprintln!(writer, "Space ID: {}", fsp.space_id)?;
    wprintln!(writer, "Size (pages): {}", fsp.size)?;
    wprintln!(writer, "Page Free Limit: {}", fsp.free_limit)?;
    wprintln!(writer, "Flags: {}", fsp.flags)?;
    Ok(())
}

/// Read PAGE_N_HEAP from the page header (INDEX page specific).
fn read_page_n_heap(page_data: &[u8]) -> u16 {
    let offset = crate::innodb::constants::FIL_PAGE_DATA + 4; // PAGE_N_HEAP is at FIL_PAGE_DATA + 4
    if page_data.len() < offset + 2 {
        return 0;
    }
    BigEndian::read_u16(&page_data[offset..])
}
