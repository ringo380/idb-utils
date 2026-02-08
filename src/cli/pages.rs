use std::io::Write;

use colored::Colorize;
use serde::Serialize;

use crate::cli::{wprint, wprintln};
use crate::innodb::checksum;
use crate::innodb::compression;
use crate::innodb::encryption;
use crate::innodb::index::{FsegHeader, IndexHeader, SystemRecords};
use crate::innodb::lob::{BlobPageHeader, LobFirstPageHeader};
use crate::innodb::page::{FilHeader, FspHeader};
use crate::innodb::page_types::PageType;
use crate::innodb::tablespace::Tablespace;
use crate::innodb::undo::{UndoPageHeader, UndoSegmentHeader};
use crate::util::hex::format_offset;
use crate::IdbError;

/// Options for the `inno pages` subcommand.
pub struct PagesOptions {
    /// Path to the InnoDB tablespace file (.ibd).
    pub file: String,
    /// If set, display only this specific page number.
    pub page: Option<u64>,
    /// Show additional detail (checksum status, FSEG internals).
    pub verbose: bool,
    /// Include empty/allocated pages in the output.
    pub show_empty: bool,
    /// Use compact one-line-per-page list format.
    pub list_mode: bool,
    /// Filter output to pages matching this type name (e.g. "INDEX", "UNDO").
    pub filter_type: Option<String>,
    /// Override the auto-detected page size.
    pub page_size: Option<u32>,
    /// Emit output as JSON.
    pub json: bool,
}

/// JSON-serializable detailed page info.
#[derive(Serialize)]
struct PageDetailJson {
    page_number: u64,
    header: FilHeader,
    page_type_name: String,
    page_type_description: String,
    byte_start: u64,
    byte_end: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    index_header: Option<IndexHeader>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fsp_header: Option<FspHeader>,
}

/// Perform deep structural analysis of pages in an InnoDB tablespace.
///
/// Unlike `parse` which only decodes FIL headers, this command dives into
/// page-type-specific internal structures:
///
/// - **INDEX pages** (type 17855): Decodes the index header (index ID, B+Tree
///   level, record counts, heap top, garbage bytes, insert direction), FSEG
///   inode pointers for leaf and non-leaf segments, and infimum/supremum system
///   record metadata.
/// - **UNDO pages** (type 2): Shows the undo page header (type, start/free
///   offsets, used bytes) and segment header (state, last log offset).
/// - **BLOB/ZBLOB pages** (types 10, 11, 12): Shows data length and next-page
///   chain pointer for old-style externally stored columns.
/// - **LOB_FIRST pages** (MySQL 8.0+): Shows version, flags, total data length,
///   and transaction ID for new-style LOB first pages.
/// - **Page 0** (FSP_HDR): Shows extended FSP header fields including
///   compression algorithm, encryption flags, and first unused segment ID.
///
/// In **list mode** (`-l`), output is a compact one-line-per-page summary
/// showing page number, type, description, and byte offset. In **detail mode**
/// (the default), each page gets a full multi-section breakdown. Use `-t` to
/// filter by page type name (supports aliases like "undo", "blob", "lob",
/// "sdi", "compressed", "encrypted").
pub fn execute(opts: &PagesOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    let mut ts = match opts.page_size {
        Some(ps) => Tablespace::open_with_page_size(&opts.file, ps)?,
        None => Tablespace::open(&opts.file)?,
    };

    let page_size = ts.page_size();

    if opts.json {
        return execute_json(opts, &mut ts, page_size, writer);
    }

    if let Some(page_num) = opts.page {
        let page_data = ts.read_page(page_num)?;
        print_full_page(&page_data, page_num, page_size, opts.verbose, writer)?;
        return Ok(());
    }

    // Print FSP header unless filtering by type
    if opts.filter_type.is_none() {
        let page0 = ts.read_page(0)?;
        if let Some(fsp) = FspHeader::parse(&page0) {
            print_fsp_header_detail(&fsp, &page0, opts.verbose, writer)?;
        }
    }

    for page_num in 0..ts.page_count() {
        let page_data = ts.read_page(page_num)?;
        let header = match FilHeader::parse(&page_data) {
            Some(h) => h,
            None => continue,
        };

        // Skip empty pages unless --show-empty
        if !opts.show_empty && header.checksum == 0 && header.page_type == PageType::Allocated {
            continue;
        }

        // Filter by type
        if let Some(ref filter) = opts.filter_type {
            if !matches_page_type_filter(&header.page_type, filter) {
                continue;
            }
        }

        if opts.list_mode {
            print_list_line(&page_data, page_num, page_size, writer)?;
        } else {
            print_full_page(&page_data, page_num, page_size, opts.verbose, writer)?;
        }
    }

    Ok(())
}

/// Execute pages in JSON output mode.
fn execute_json(
    opts: &PagesOptions,
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

        if !opts.show_empty && header.checksum == 0 && header.page_type == PageType::Allocated {
            continue;
        }

        if let Some(ref filter) = opts.filter_type {
            if !matches_page_type_filter(&header.page_type, filter) {
                continue;
            }
        }

        let pt = header.page_type;
        let byte_start = page_num * page_size as u64;

        let index_header = if pt == PageType::Index {
            IndexHeader::parse(&page_data)
        } else {
            None
        };

        let fsp_header = if page_num == 0 {
            FspHeader::parse(&page_data)
        } else {
            None
        };

        pages.push(PageDetailJson {
            page_number: page_num,
            page_type_name: pt.name().to_string(),
            page_type_description: pt.description().to_string(),
            byte_start,
            byte_end: byte_start + page_size as u64,
            header,
            index_header,
            fsp_header,
        });
    }

    let json = serde_json::to_string_pretty(&pages)
        .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
    wprintln!(writer, "{}", json)?;
    Ok(())
}

/// Print a compact one-line summary per page (list mode).
fn print_list_line(
    page_data: &[u8],
    page_num: u64,
    page_size: u32,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    let header = match FilHeader::parse(page_data) {
        Some(h) => h,
        None => return Ok(()),
    };

    let pt = header.page_type;
    let byte_start = page_num * page_size as u64;

    wprint!(
        writer,
        "-- Page {} - {}: {}",
        page_num,
        pt.name(),
        pt.description()
    )?;

    if pt == PageType::Index {
        if let Some(idx) = IndexHeader::parse(page_data) {
            wprint!(writer, ", Index ID: {}", idx.index_id)?;
        }
    }

    wprintln!(writer, ", Byte Start: {}", format_offset(byte_start))?;
    Ok(())
}

/// Print full detailed information about a page.
fn print_full_page(
    page_data: &[u8],
    page_num: u64,
    page_size: u32,
    verbose: bool,
    writer: &mut dyn Write,
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

    // FIL Header
    wprintln!(writer)?;
    wprintln!(writer, "=== HEADER: Page {}", header.page_number)?;
    wprintln!(writer, "Byte Start: {}", format_offset(byte_start))?;
    wprintln!(
        writer,
        "Page Type: {}\n-- {}: {} - {}",
        pt.as_u16(),
        pt.name(),
        pt.description(),
        pt.usage()
    )?;

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

    // INDEX-specific headers
    if pt == PageType::Index {
        if let Some(idx) = IndexHeader::parse(page_data) {
            wprintln!(writer)?;
            print_index_header(&idx, header.page_number, verbose, writer)?;

            wprintln!(writer)?;
            print_fseg_headers(page_data, header.page_number, &idx, verbose, writer)?;

            wprintln!(writer)?;
            print_system_records(page_data, header.page_number, writer)?;
        }
    }

    // BLOB page-specific headers (old-style)
    if matches!(pt, PageType::Blob | PageType::ZBlob | PageType::ZBlob2) {
        if let Some(blob_hdr) = BlobPageHeader::parse(page_data) {
            wprintln!(writer)?;
            wprintln!(writer, "=== BLOB Header: Page {}", header.page_number)?;
            wprintln!(writer, "Data Length: {} bytes", blob_hdr.part_len)?;
            if blob_hdr.has_next() {
                wprintln!(writer, "Next BLOB Page: {}", blob_hdr.next_page_no)?;
            } else {
                wprintln!(writer, "Next BLOB Page: None (last in chain)")?;
            }
        }
    }

    // LOB first page header (MySQL 8.0+ new-style)
    if pt == PageType::LobFirst {
        if let Some(lob_hdr) = LobFirstPageHeader::parse(page_data) {
            wprintln!(writer)?;
            wprintln!(
                writer,
                "=== LOB First Page Header: Page {}",
                header.page_number
            )?;
            wprintln!(writer, "Version: {}", lob_hdr.version)?;
            wprintln!(writer, "Flags: {}", lob_hdr.flags)?;
            wprintln!(writer, "Total Data Length: {} bytes", lob_hdr.data_len)?;
            if lob_hdr.trx_id > 0 {
                wprintln!(writer, "Transaction ID: {}", lob_hdr.trx_id)?;
            }
        }
    }

    // Undo log page-specific headers
    if pt == PageType::UndoLog {
        if let Some(undo_hdr) = UndoPageHeader::parse(page_data) {
            wprintln!(writer)?;
            wprintln!(writer, "=== UNDO Header: Page {}", header.page_number)?;
            wprintln!(
                writer,
                "Undo Type: {} ({})",
                undo_hdr.page_type.name(),
                undo_hdr.page_type.name()
            )?;
            wprintln!(writer, "Log Start Offset: {}", undo_hdr.start)?;
            wprintln!(writer, "Free Offset: {}", undo_hdr.free)?;
            wprintln!(
                writer,
                "Used Bytes: {}",
                undo_hdr.free.saturating_sub(undo_hdr.start)
            )?;

            if let Some(seg_hdr) = UndoSegmentHeader::parse(page_data) {
                wprintln!(writer, "Segment State: {}", seg_hdr.state.name())?;
                wprintln!(writer, "Last Log Offset: {}", seg_hdr.last_log)?;
            }
        }
    }

    // FIL Trailer
    wprintln!(writer)?;
    let ps = page_size as usize;
    if page_data.len() >= ps {
        let trailer_offset = ps - 8;
        if let Some(trailer) = crate::innodb::page::FilTrailer::parse(&page_data[trailer_offset..])
        {
            wprintln!(writer, "=== TRAILER: Page {}", header.page_number)?;
            wprintln!(writer, "Old-style Checksum: {}", trailer.checksum)?;
            wprintln!(writer, "Low 32 bits of LSN: {}", trailer.lsn_low32)?;
            wprintln!(writer, "Byte End: {}", format_offset(byte_end))?;

            if verbose {
                let csum_result = checksum::validate_checksum(page_data, page_size);
                let status = if csum_result.valid {
                    "OK".green().to_string()
                } else {
                    "MISMATCH".red().to_string()
                };
                wprintln!(
                    writer,
                    "Checksum Status: {} ({:?})",
                    status,
                    csum_result.algorithm
                )?;

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

    Ok(())
}

/// Print the INDEX page header details.
fn print_index_header(
    idx: &IndexHeader,
    page_num: u32,
    verbose: bool,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    wprintln!(writer, "=== INDEX Header: Page {}", page_num)?;
    wprintln!(writer, "Index ID: {}", idx.index_id)?;
    wprintln!(writer, "Node Level: {}", idx.level)?;

    if idx.max_trx_id > 0 {
        wprintln!(writer, "Max Transaction ID: {}", idx.max_trx_id)?;
    } else {
        wprintln!(writer, "-- Secondary Index")?;
    }

    wprintln!(writer, "Directory Slots: {}", idx.n_dir_slots)?;
    if verbose {
        wprintln!(writer, "-- Number of slots in page directory")?;
    }

    wprintln!(writer, "Heap Top: {}", idx.heap_top)?;
    if verbose {
        wprintln!(writer, "-- Pointer to record heap top")?;
    }

    wprintln!(writer, "Records in Page: {}", idx.n_recs)?;
    wprintln!(
        writer,
        "Records in Heap: {} (compact: {})",
        idx.n_heap(),
        idx.is_compact()
    )?;
    if verbose {
        wprintln!(writer, "-- Number of records in heap")?;
    }

    wprintln!(writer, "Start of Free Record List: {}", idx.free)?;
    wprintln!(writer, "Garbage Bytes: {}", idx.garbage)?;
    if verbose {
        wprintln!(writer, "-- Number of bytes in deleted records.")?;
    }

    wprintln!(writer, "Last Insert: {}", idx.last_insert)?;
    wprintln!(
        writer,
        "Last Insert Direction: {} - {}",
        idx.direction,
        idx.direction_name()
    )?;
    wprintln!(writer, "Inserts in this direction: {}", idx.n_direction)?;
    if verbose {
        wprintln!(
            writer,
            "-- Number of consecutive inserts in this direction."
        )?;
    }

    Ok(())
}

/// Print FSEG (file segment) header details.
fn print_fseg_headers(
    page_data: &[u8],
    page_num: u32,
    idx: &IndexHeader,
    verbose: bool,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    wprintln!(
        writer,
        "=== FSEG_HDR - File Segment Header: Page {}",
        page_num
    )?;

    if let Some(leaf) = FsegHeader::parse_leaf(page_data) {
        wprintln!(writer, "Inode Space ID: {}", leaf.space_id)?;
        wprintln!(writer, "Inode Page Number: {}", leaf.page_no)?;
        wprintln!(writer, "Inode Offset: {}", leaf.offset)?;
    }

    if idx.is_leaf() {
        if let Some(internal) = FsegHeader::parse_internal(page_data) {
            wprintln!(writer, "Non-leaf Space ID: {}", internal.space_id)?;
            if verbose {
                wprintln!(writer, "Non-leaf Page Number: {}", internal.page_no)?;
                wprintln!(writer, "Non-leaf Offset: {}", internal.offset)?;
            }
        }
    }

    Ok(())
}

/// Print system records (infimum/supremum) info.
fn print_system_records(
    page_data: &[u8],
    page_num: u32,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    let sys = match SystemRecords::parse(page_data) {
        Some(s) => s,
        None => return Ok(()),
    };

    wprintln!(writer, "=== INDEX System Records: Page {}", page_num)?;
    wprintln!(
        writer,
        "Index Record Status: {} - (Decimal: {}) {}",
        sys.rec_status,
        sys.rec_status,
        sys.rec_status_name()
    )?;
    wprintln!(writer, "Number of records owned: {}", sys.n_owned)?;
    wprintln!(writer, "Deleted: {}", if sys.deleted { "1" } else { "0" })?;
    wprintln!(writer, "Heap Number: {}", sys.heap_no)?;
    wprintln!(writer, "Next Record Offset (Infimum): {}", sys.infimum_next)?;
    wprintln!(
        writer,
        "Next Record Offset (Supremum): {}",
        sys.supremum_next
    )?;
    wprintln!(
        writer,
        "Left-most node on non-leaf level: {}",
        if sys.min_rec { "1" } else { "0" }
    )?;

    Ok(())
}

/// Print detailed FSP header with additional fields.
fn print_fsp_header_detail(
    fsp: &FspHeader,
    page0: &[u8],
    verbose: bool,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    wprintln!(writer, "=== File Header")?;
    wprintln!(writer, "Space ID: {}", fsp.space_id)?;
    if verbose {
        wprintln!(writer, "-- Offset 38, Length 4")?;
    }
    wprintln!(writer, "Size: {}", fsp.size)?;
    wprintln!(writer, "Flags: {}", fsp.flags)?;
    wprintln!(
        writer,
        "Page Free Limit: {} (this should always be 64 on a single-table file)",
        fsp.free_limit
    )?;

    // Compression and encryption detection from flags
    let comp = compression::detect_compression(fsp.flags);
    let enc = encryption::detect_encryption(fsp.flags);
    if comp != compression::CompressionAlgorithm::None {
        wprintln!(writer, "Compression: {}", comp)?;
    }
    if enc != encryption::EncryptionAlgorithm::None {
        wprintln!(writer, "Encryption: {}", enc)?;
    }

    // Try to read the first unused segment ID (at FSP offset 72, 8 bytes)
    let seg_id_offset = crate::innodb::constants::FIL_PAGE_DATA + 72;
    if page0.len() >= seg_id_offset + 8 {
        use byteorder::ByteOrder;
        let seg_id = byteorder::BigEndian::read_u64(&page0[seg_id_offset..]);
        wprintln!(writer, "First Unused Segment ID: {}", seg_id)?;
    }

    Ok(())
}

/// Check if a page type matches the user-provided filter string.
///
/// Matches against the page type name (case-insensitive). Supports
/// short aliases like "index", "undo", "blob", "sdi", etc.
fn matches_page_type_filter(page_type: &PageType, filter: &str) -> bool {
    let filter_upper = filter.to_uppercase();
    let type_name = page_type.name();

    // Exact match on type name
    if type_name == filter_upper {
        return true;
    }

    // Common aliases and prefix matching
    match filter_upper.as_str() {
        "UNDO" => *page_type == PageType::UndoLog,
        "BLOB" => matches!(
            page_type,
            PageType::Blob | PageType::ZBlob | PageType::ZBlob2
        ),
        "LOB" => matches!(
            page_type,
            PageType::LobIndex | PageType::LobData | PageType::LobFirst
        ),
        "SDI" => matches!(page_type, PageType::Sdi | PageType::SdiBlob),
        "COMPRESSED" | "COMP" => matches!(
            page_type,
            PageType::Compressed | PageType::CompressedEncrypted
        ),
        "ENCRYPTED" | "ENC" => matches!(
            page_type,
            PageType::Encrypted | PageType::CompressedEncrypted | PageType::EncryptedRtree
        ),
        _ => type_name.contains(&filter_upper),
    }
}
