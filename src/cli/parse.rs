use std::collections::HashMap;
use std::io::Write;

use byteorder::{BigEndian, ByteOrder};
use colored::Colorize;

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

/// Parse an InnoDB tablespace file and display page headers with a type summary.
///
/// Opens the tablespace, auto-detects (or uses the overridden) page size, then
/// iterates over every page reading the 38-byte FIL header to extract the
/// checksum, page number, prev/next page pointers, LSN, page type, and space ID.
/// Page 0 additionally displays the FSP header (space ID, tablespace size,
/// free-page limit, and flags).
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
    let mut ts = match opts.page_size {
        Some(ps) => Tablespace::open_with_page_size(&opts.file, ps)?,
        None => Tablespace::open(&opts.file)?,
    };

    let page_size = ts.page_size();

    if opts.json {
        return execute_json(opts, &mut ts, page_size, writer);
    }

    if let Some(page_num) = opts.page {
        // Single page mode
        let page_data = ts.read_page(page_num)?;
        print_page_info(writer, &page_data, page_num, page_size, opts.verbose)?;
    } else {
        // All pages mode
        // Print FSP header first
        let page0 = ts.read_page(0)?;
        if let Some(fsp) = FspHeader::parse(&page0) {
            print_fsp_header(writer, &fsp)?;
            wprintln!(writer)?;
        }

        wprintln!(
            writer,
            "Pages in {} ({} pages, page size {}):",
            opts.file,
            ts.page_count(),
            page_size
        )?;
        wprintln!(writer, "{}", "-".repeat(50))?;

        let mut type_counts: HashMap<PageType, u64> = HashMap::new();

        let pb = create_progress_bar(ts.page_count(), "pages");

        for page_num in 0..ts.page_count() {
            pb.inc(1);
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

            print_page_info(writer, &page_data, page_num, page_size, opts.verbose)?;
        }

        pb.finish_and_clear();

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

/// Execute parse in JSON output mode.
fn execute_json(
    opts: &ParseOptions,
    ts: &mut Tablespace,
    page_size: u32,
    writer: &mut dyn Write,
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
