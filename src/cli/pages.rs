use colored::Colorize;
use serde::Serialize;

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

pub struct PagesOptions {
    pub file: String,
    pub page: Option<u64>,
    pub verbose: bool,
    pub show_empty: bool,
    pub list_mode: bool,
    pub filter_type: Option<String>,
    pub page_size: Option<u32>,
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

pub fn execute(opts: &PagesOptions) -> Result<(), IdbError> {
    let mut ts = match opts.page_size {
        Some(ps) => Tablespace::open_with_page_size(&opts.file, ps)?,
        None => Tablespace::open(&opts.file)?,
    };

    let page_size = ts.page_size();

    if opts.json {
        return execute_json(opts, &mut ts, page_size);
    }

    if let Some(page_num) = opts.page {
        let page_data = ts.read_page(page_num)?;
        print_full_page(&page_data, page_num, page_size, opts.verbose);
        return Ok(());
    }

    // Print FSP header unless filtering by type
    if opts.filter_type.is_none() {
        let page0 = ts.read_page(0)?;
        if let Some(fsp) = FspHeader::parse(&page0) {
            print_fsp_header_detail(&fsp, &page0, opts.verbose);
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
            print_list_line(&page_data, page_num, page_size);
        } else {
            print_full_page(&page_data, page_num, page_size, opts.verbose);
        }
    }

    Ok(())
}

/// Execute pages in JSON output mode.
fn execute_json(
    opts: &PagesOptions,
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

    println!(
        "{}",
        serde_json::to_string_pretty(&pages).unwrap_or_else(|_| "[]".to_string())
    );
    Ok(())
}

/// Print a compact one-line summary per page (list mode).
fn print_list_line(page_data: &[u8], page_num: u64, page_size: u32) {
    let header = match FilHeader::parse(page_data) {
        Some(h) => h,
        None => return,
    };

    let pt = header.page_type;
    let byte_start = page_num * page_size as u64;

    print!(
        "-- Page {} - {}: {}",
        page_num,
        pt.name(),
        pt.description()
    );

    if pt == PageType::Index {
        if let Some(idx) = IndexHeader::parse(page_data) {
            print!(", Index ID: {}", idx.index_id);
        }
    }

    println!(", Byte Start: {}", format_offset(byte_start));
}

/// Print full detailed information about a page.
fn print_full_page(page_data: &[u8], page_num: u64, page_size: u32, verbose: bool) {
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

    // FIL Header
    println!();
    println!("=== HEADER: Page {}", header.page_number);
    println!("Byte Start: {}", format_offset(byte_start));
    println!(
        "Page Type: {}\n-- {}: {} - {}",
        pt.as_u16(),
        pt.name(),
        pt.description(),
        pt.usage()
    );

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

    // INDEX-specific headers
    if pt == PageType::Index {
        if let Some(idx) = IndexHeader::parse(page_data) {
            println!();
            print_index_header(&idx, header.page_number, verbose);

            println!();
            print_fseg_headers(page_data, header.page_number, &idx, verbose);

            println!();
            print_system_records(page_data, header.page_number);
        }
    }

    // BLOB page-specific headers (old-style)
    if matches!(pt, PageType::Blob | PageType::ZBlob | PageType::ZBlob2) {
        if let Some(blob_hdr) = BlobPageHeader::parse(page_data) {
            println!();
            println!("=== BLOB Header: Page {}", header.page_number);
            println!("Data Length: {} bytes", blob_hdr.part_len);
            if blob_hdr.has_next() {
                println!("Next BLOB Page: {}", blob_hdr.next_page_no);
            } else {
                println!("Next BLOB Page: None (last in chain)");
            }
        }
    }

    // LOB first page header (MySQL 8.0+ new-style)
    if pt == PageType::LobFirst {
        if let Some(lob_hdr) = LobFirstPageHeader::parse(page_data) {
            println!();
            println!("=== LOB First Page Header: Page {}", header.page_number);
            println!("Version: {}", lob_hdr.version);
            println!("Flags: {}", lob_hdr.flags);
            println!("Total Data Length: {} bytes", lob_hdr.data_len);
            if lob_hdr.trx_id > 0 {
                println!("Transaction ID: {}", lob_hdr.trx_id);
            }
        }
    }

    // Undo log page-specific headers
    if pt == PageType::UndoLog {
        if let Some(undo_hdr) = UndoPageHeader::parse(page_data) {
            println!();
            println!("=== UNDO Header: Page {}", header.page_number);
            println!("Undo Type: {} ({})", undo_hdr.page_type.name(), undo_hdr.page_type.name());
            println!("Log Start Offset: {}", undo_hdr.start);
            println!("Free Offset: {}", undo_hdr.free);
            println!(
                "Used Bytes: {}",
                undo_hdr.free.saturating_sub(undo_hdr.start)
            );

            if let Some(seg_hdr) = UndoSegmentHeader::parse(page_data) {
                println!("Segment State: {}", seg_hdr.state.name());
                println!("Last Log Offset: {}", seg_hdr.last_log);
            }
        }
    }

    // FIL Trailer
    println!();
    let ps = page_size as usize;
    if page_data.len() >= ps {
        let trailer_offset = ps - 8;
        if let Some(trailer) =
            crate::innodb::page::FilTrailer::parse(&page_data[trailer_offset..])
        {
            println!("=== TRAILER: Page {}", header.page_number);
            println!("Old-style Checksum: {}", trailer.checksum);
            println!("Low 32 bits of LSN: {}", trailer.lsn_low32);
            println!("Byte End: {}", format_offset(byte_end));

            if verbose {
                let csum_result = checksum::validate_checksum(page_data, page_size);
                let status = if csum_result.valid {
                    "OK".green().to_string()
                } else {
                    "MISMATCH".red().to_string()
                };
                println!(
                    "Checksum Status: {} ({:?})",
                    status, csum_result.algorithm
                );

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
}

/// Print the INDEX page header details.
fn print_index_header(idx: &IndexHeader, page_num: u32, verbose: bool) {
    println!("=== INDEX Header: Page {}", page_num);
    println!("Index ID: {}", idx.index_id);
    println!("Node Level: {}", idx.level);

    if idx.max_trx_id > 0 {
        println!("Max Transaction ID: {}", idx.max_trx_id);
    } else {
        println!("-- Secondary Index");
    }

    println!("Directory Slots: {}", idx.n_dir_slots);
    if verbose {
        println!("-- Number of slots in page directory");
    }

    println!("Heap Top: {}", idx.heap_top);
    if verbose {
        println!("-- Pointer to record heap top");
    }

    println!("Records in Page: {}", idx.n_recs);
    println!(
        "Records in Heap: {} (compact: {})",
        idx.n_heap(),
        idx.is_compact()
    );
    if verbose {
        println!("-- Number of records in heap");
    }

    println!("Start of Free Record List: {}", idx.free);
    println!("Garbage Bytes: {}", idx.garbage);
    if verbose {
        println!("-- Number of bytes in deleted records.");
    }

    println!("Last Insert: {}", idx.last_insert);
    println!(
        "Last Insert Direction: {} - {}",
        idx.direction,
        idx.direction_name()
    );
    println!("Inserts in this direction: {}", idx.n_direction);
    if verbose {
        println!("-- Number of consecutive inserts in this direction.");
    }
}

/// Print FSEG (file segment) header details.
fn print_fseg_headers(page_data: &[u8], page_num: u32, idx: &IndexHeader, verbose: bool) {
    println!("=== FSEG_HDR - File Segment Header: Page {}", page_num);

    if let Some(leaf) = FsegHeader::parse_leaf(page_data) {
        println!("Inode Space ID: {}", leaf.space_id);
        println!("Inode Page Number: {}", leaf.page_no);
        println!("Inode Offset: {}", leaf.offset);
    }

    if idx.is_leaf() {
        if let Some(internal) = FsegHeader::parse_internal(page_data) {
            println!("Non-leaf Space ID: {}", internal.space_id);
            if verbose {
                println!("Non-leaf Page Number: {}", internal.page_no);
                println!("Non-leaf Offset: {}", internal.offset);
            }
        }
    }
}

/// Print system records (infimum/supremum) info.
fn print_system_records(page_data: &[u8], page_num: u32) {
    let sys = match SystemRecords::parse(page_data) {
        Some(s) => s,
        None => return,
    };

    println!("=== INDEX System Records: Page {}", page_num);
    println!(
        "Index Record Status: {} - (Decimal: {}) {}",
        sys.rec_status,
        sys.rec_status,
        sys.rec_status_name()
    );
    println!("Number of records owned: {}", sys.n_owned);
    println!("Deleted: {}", if sys.deleted { "1" } else { "0" });
    println!("Heap Number: {}", sys.heap_no);
    println!("Next Record Offset (Infimum): {}", sys.infimum_next);
    println!("Next Record Offset (Supremum): {}", sys.supremum_next);
    println!(
        "Left-most node on non-leaf level: {}",
        if sys.min_rec { "1" } else { "0" }
    );
}

/// Print detailed FSP header with additional fields.
fn print_fsp_header_detail(fsp: &FspHeader, page0: &[u8], verbose: bool) {
    println!("=== File Header");
    println!("Space ID: {}", fsp.space_id);
    if verbose {
        println!("-- Offset 38, Length 4");
    }
    println!("Size: {}", fsp.size);
    println!("Flags: {}", fsp.flags);
    println!("Page Free Limit: {} (this should always be 64 on a single-table file)", fsp.free_limit);

    // Compression and encryption detection from flags
    let comp = compression::detect_compression(fsp.flags);
    let enc = encryption::detect_encryption(fsp.flags);
    if comp != compression::CompressionAlgorithm::None {
        println!("Compression: {}", comp);
    }
    if enc != encryption::EncryptionAlgorithm::None {
        println!("Encryption: {}", enc);
    }

    // Try to read the first unused segment ID (at FSP offset 72, 8 bytes)
    let seg_id_offset = crate::innodb::constants::FIL_PAGE_DATA + 72;
    if page0.len() >= seg_id_offset + 8 {
        use byteorder::ByteOrder;
        let seg_id = byteorder::BigEndian::read_u64(&page0[seg_id_offset..]);
        println!("First Unused Segment ID: {}", seg_id);
    }
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
        "BLOB" => matches!(page_type, PageType::Blob | PageType::ZBlob | PageType::ZBlob2),
        "LOB" => matches!(page_type, PageType::LobIndex | PageType::LobData | PageType::LobFirst),
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
