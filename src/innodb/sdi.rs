use byteorder::{BigEndian, ByteOrder};
use flate2::read::ZlibDecoder;
use std::io::Read;

use crate::innodb::constants::{FIL_PAGE_DATA, FSP_HEADER_SIZE, SIZE_FIL_HEAD, SIZE_FIL_TRAILER};
use crate::innodb::index::IndexHeader;
use crate::innodb::page::FilHeader;
use crate::innodb::page_types::PageType;
use crate::innodb::record::{walk_compact_records, RecordType};

/// An extracted SDI record from a tablespace.
#[derive(Debug, Clone)]
pub struct SdiRecord {
    /// SDI type (1 = table, 2 = tablespace).
    pub sdi_type: u32,
    /// SDI object ID.
    pub sdi_id: u64,
    /// Uncompressed data length.
    pub uncompressed_len: u32,
    /// Compressed data length.
    pub compressed_len: u32,
    /// Decompressed JSON string.
    pub data: String,
}

/// Parsed fields from an SDI record before decompression.
#[derive(Debug, Clone)]
pub struct SdiRecordHeader {
    pub sdi_type: u32,
    pub sdi_id: u64,
    pub uncompressed_len: u32,
    pub compressed_len: u32,
    /// Offset within the page where compressed data starts.
    pub data_offset_in_page: usize,
    /// Whether the data fits entirely within the page.
    pub data_complete: bool,
}

/// SDI record field offsets relative to the record origin.
///
/// SDI clustered index record layout (compact format):
///   [record origin]
///   Key:    type (4 bytes BE) + id (8 bytes BE) = 12 bytes
///   System: trx_id (6 bytes BE) + roll_ptr (7 bytes BE) = 13 bytes
///   Data:   uncompressed_len (4 bytes BE) + compressed_len (4 bytes BE) + blob data
const SDI_TYPE_OFFSET: usize = 0;
const SDI_TYPE_LEN: usize = 4;
const SDI_ID_OFFSET: usize = SDI_TYPE_OFFSET + SDI_TYPE_LEN; // 4
const SDI_ID_LEN: usize = 8;
const SDI_TRX_ID_OFFSET: usize = SDI_ID_OFFSET + SDI_ID_LEN; // 12
const SDI_TRX_ID_LEN: usize = 6;
const SDI_ROLL_PTR_OFFSET: usize = SDI_TRX_ID_OFFSET + SDI_TRX_ID_LEN; // 18
const SDI_ROLL_PTR_LEN: usize = 7;
const SDI_UNCOMP_LEN_OFFSET: usize = SDI_ROLL_PTR_OFFSET + SDI_ROLL_PTR_LEN; // 25
const SDI_UNCOMP_LEN_SIZE: usize = 4;
const SDI_COMP_LEN_OFFSET: usize = SDI_UNCOMP_LEN_OFFSET + SDI_UNCOMP_LEN_SIZE; // 29
const SDI_COMP_LEN_SIZE: usize = 4;
const SDI_DATA_OFFSET: usize = SDI_COMP_LEN_OFFSET + SDI_COMP_LEN_SIZE; // 33

/// Extract all SDI records from a single SDI page.
///
/// Returns None if the page is not an SDI page or has no records.
pub fn extract_sdi_from_page(page_data: &[u8]) -> Option<Vec<SdiRecord>> {
    let header = FilHeader::parse(page_data)?;
    if header.page_type != PageType::Sdi {
        return None;
    }

    let idx_header = IndexHeader::parse(page_data)?;

    // Only leaf pages contain actual SDI data
    if !idx_header.is_leaf() {
        return None;
    }

    if idx_header.n_recs == 0 {
        return Some(Vec::new());
    }

    let records = walk_compact_records(page_data);
    let mut sdi_records = Vec::new();

    for rec in &records {
        if rec.header.rec_type != RecordType::Ordinary {
            continue;
        }

        if let Some(sdi_rec) = parse_sdi_record(page_data, rec.offset) {
            sdi_records.push(sdi_rec);
        }
    }

    Some(sdi_records)
}

/// Extract SDI records with multi-page reassembly support.
///
/// When a record's compressed data spans beyond the current page, this function
/// follows the next-page chain to collect all compressed bytes before decompression.
pub fn extract_sdi_from_pages(
    ts: &mut crate::innodb::tablespace::Tablespace,
    sdi_pages: &[u64],
) -> Result<Vec<SdiRecord>, crate::IdbError> {
    let mut all_records = Vec::new();

    for &page_num in sdi_pages {
        let page_data = ts.read_page(page_num)?;

        let header = match FilHeader::parse(&page_data) {
            Some(h) => h,
            None => continue,
        };
        if header.page_type != PageType::Sdi {
            continue;
        }

        let idx_header = match IndexHeader::parse(&page_data) {
            Some(h) => h,
            None => continue,
        };

        if !idx_header.is_leaf() || idx_header.n_recs == 0 {
            continue;
        }

        let records = walk_compact_records(&page_data);

        for rec in &records {
            if rec.header.rec_type != RecordType::Ordinary {
                continue;
            }

            if let Some(rec_header) = parse_sdi_record_header(&page_data, rec.offset) {
                if rec_header.data_complete {
                    // Data fits in single page
                    let data_start = rec.offset + SDI_DATA_OFFSET;
                    let compressed = &page_data[data_start..data_start + rec_header.compressed_len as usize];
                    let json = decompress_sdi_data(compressed, rec_header.uncompressed_len)
                        .unwrap_or_default();
                    all_records.push(SdiRecord {
                        sdi_type: rec_header.sdi_type,
                        sdi_id: rec_header.sdi_id,
                        uncompressed_len: rec_header.uncompressed_len,
                        compressed_len: rec_header.compressed_len,
                        data: json,
                    });
                } else {
                    // Data spans multiple pages — collect from current and continuation pages
                    let data_start = rec.offset + SDI_DATA_OFFSET;
                    let available_this_page = page_data.len() - data_start;
                    let mut compressed_data = Vec::with_capacity(rec_header.compressed_len as usize);
                    compressed_data.extend_from_slice(&page_data[data_start..data_start + available_this_page]);

                    let remaining = rec_header.compressed_len as usize - available_this_page;
                    collect_continuation_data(ts, header.next_page, remaining, &mut compressed_data)?;

                    let json = decompress_sdi_data(&compressed_data, rec_header.uncompressed_len)
                        .unwrap_or_default();
                    all_records.push(SdiRecord {
                        sdi_type: rec_header.sdi_type,
                        sdi_id: rec_header.sdi_id,
                        uncompressed_len: rec_header.uncompressed_len,
                        compressed_len: rec_header.compressed_len,
                        data: json,
                    });
                }
            }
        }
    }

    Ok(all_records)
}

/// Collect continuation data from linked SDI pages.
///
/// Reads data from the page body (after FIL header, before FIL trailer) of successive pages.
fn collect_continuation_data(
    ts: &mut crate::innodb::tablespace::Tablespace,
    mut next_page: u32,
    mut remaining: usize,
    buf: &mut Vec<u8>,
) -> Result<(), crate::IdbError> {
    while remaining > 0 && next_page != crate::innodb::constants::FIL_NULL && next_page != 0 {
        let page_data = ts.read_page(next_page as u64)?;
        let page_size = page_data.len();

        // Data area: after FIL header, before FIL trailer
        let data_start = SIZE_FIL_HEAD;
        let data_end = page_size - SIZE_FIL_TRAILER;
        let available = data_end - data_start;
        let to_read = remaining.min(available);

        buf.extend_from_slice(&page_data[data_start..data_start + to_read]);
        remaining -= to_read;

        // Follow the chain
        let header = match FilHeader::parse(&page_data) {
            Some(h) => h,
            None => break,
        };
        next_page = header.next_page;
    }

    Ok(())
}

/// Parse SDI record header fields without decompressing data.
fn parse_sdi_record_header(page_data: &[u8], origin: usize) -> Option<SdiRecordHeader> {
    if origin + SDI_DATA_OFFSET > page_data.len() {
        return None;
    }

    let data = &page_data[origin..];

    let sdi_type = BigEndian::read_u32(&data[SDI_TYPE_OFFSET..]);
    let sdi_id = BigEndian::read_u64(&data[SDI_ID_OFFSET..]);
    let uncompressed_len = BigEndian::read_u32(&data[SDI_UNCOMP_LEN_OFFSET..]);
    let compressed_len = BigEndian::read_u32(&data[SDI_COMP_LEN_OFFSET..]);

    if compressed_len == 0 {
        return None;
    }

    let data_end = origin + SDI_DATA_OFFSET + compressed_len as usize;
    let data_complete = data_end <= page_data.len();

    Some(SdiRecordHeader {
        sdi_type,
        sdi_id,
        uncompressed_len,
        compressed_len,
        data_offset_in_page: origin + SDI_DATA_OFFSET,
        data_complete,
    })
}

/// Parse a single SDI record from the page data at the given origin offset.
fn parse_sdi_record(page_data: &[u8], origin: usize) -> Option<SdiRecord> {
    // Check we have enough data for the fixed fields
    if origin + SDI_DATA_OFFSET > page_data.len() {
        return None;
    }

    let data = &page_data[origin..];

    let sdi_type = BigEndian::read_u32(&data[SDI_TYPE_OFFSET..]);
    let sdi_id = BigEndian::read_u64(&data[SDI_ID_OFFSET..]);
    let uncompressed_len = BigEndian::read_u32(&data[SDI_UNCOMP_LEN_OFFSET..]);
    let compressed_len = BigEndian::read_u32(&data[SDI_COMP_LEN_OFFSET..]);

    if compressed_len == 0 {
        return None;
    }

    // Bounds check for compressed data
    let data_end = origin + SDI_DATA_OFFSET + compressed_len as usize;
    if data_end > page_data.len() {
        // Data spans multiple pages — extract what we can from this page
        let available = page_data.len() - (origin + SDI_DATA_OFFSET);
        let compressed = &data[SDI_DATA_OFFSET..SDI_DATA_OFFSET + available];
        let json = decompress_sdi_data(compressed, uncompressed_len).unwrap_or_default();

        return Some(SdiRecord {
            sdi_type,
            sdi_id,
            uncompressed_len,
            compressed_len,
            data: json,
        });
    }

    let compressed = &data[SDI_DATA_OFFSET..SDI_DATA_OFFSET + compressed_len as usize];
    let json = decompress_sdi_data(compressed, uncompressed_len).unwrap_or_default();

    Some(SdiRecord {
        sdi_type,
        sdi_id,
        uncompressed_len,
        compressed_len,
        data: json,
    })
}

/// Decompress zlib-compressed SDI data.
fn decompress_sdi_data(compressed: &[u8], _uncompressed_len: u32) -> Option<String> {
    let mut decoder = ZlibDecoder::new(compressed);
    let mut decompressed = String::new();
    decoder.read_to_string(&mut decompressed).ok()?;
    Some(decompressed)
}

/// SDI type constants.
pub fn sdi_type_name(sdi_type: u32) -> &'static str {
    match sdi_type {
        1 => "Table",
        2 => "Tablespace",
        _ => "Unknown",
    }
}

/// Check if a page is an SDI page.
pub fn is_sdi_page(page_data: &[u8]) -> bool {
    FilHeader::parse(page_data)
        .map(|h| h.page_type == PageType::Sdi)
        .unwrap_or(false)
}

/// Size of each XDES (extent descriptor) entry in bytes.
const XDES_SIZE: usize = 40;

/// SDI header fields (8 bytes total on page 0, after XDES array).
const SDI_VERSION_SIZE: usize = 4;

/// Expected SDI version value.
const SDI_VERSION_EXPECTED: u32 = 1;

/// Compute the number of pages per extent for a given page size.
fn pages_per_extent(page_size: u32) -> u32 {
    if page_size <= 16384 {
        1048576 / page_size // 1MB extents for page sizes <= 16K
    } else {
        64 // 64 pages per extent for larger page sizes
    }
}

/// Compute the number of XDES entries on page 0 for a given page size.
fn xdes_arr_size(page_size: u32) -> u32 {
    page_size / pages_per_extent(page_size)
}

/// Compute the byte offset of the SDI header on page 0.
///
/// The SDI header is located after the FIL header, FSP header, and XDES array.
/// Layout: FIL_PAGE_DATA(38) + FSP_HEADER(112) + XDES_ARRAY(variable)
fn sdi_header_offset(page_size: u32) -> usize {
    let xdes_arr_offset = FIL_PAGE_DATA + FSP_HEADER_SIZE;
    let xdes_entries = xdes_arr_size(page_size) as usize;
    xdes_arr_offset + xdes_entries * XDES_SIZE
}

/// Read the SDI root page number from page 0.
///
/// Returns Some(page_num) if SDI version marker is found and the root page
/// number is valid (non-zero and within the tablespace). Returns None if
/// SDI is not present or the offset doesn't contain valid SDI info.
pub fn read_sdi_root_page(page0: &[u8], page_size: u32, page_count: u64) -> Option<u64> {
    let offset = sdi_header_offset(page_size);

    // Need at least SDI_VERSION_SIZE + 4 bytes at the offset
    if page0.len() < offset + SDI_VERSION_SIZE + 4 {
        return None;
    }

    let version = BigEndian::read_u32(&page0[offset..]);
    if version != SDI_VERSION_EXPECTED {
        return None;
    }

    let root_page = BigEndian::read_u32(&page0[offset + SDI_VERSION_SIZE..]) as u64;
    if root_page == 0 || root_page >= page_count {
        return None;
    }

    Some(root_page)
}

/// Find SDI pages in a tablespace.
///
/// First tries to read the SDI root page number from page 0 (fast path),
/// then falls back to scanning all pages if that fails.
pub fn find_sdi_pages(
    ts: &mut crate::innodb::tablespace::Tablespace,
) -> Result<Vec<u64>, crate::IdbError> {
    let page_size = ts.page_size();
    let page_count = ts.page_count();

    // Fast path: read SDI root page from page 0
    let page0 = ts.read_page(0)?;
    if let Some(root_page) = read_sdi_root_page(&page0, page_size, page_count) {
        // Verify the root page is actually an SDI page
        let root_data = ts.read_page(root_page)?;
        if is_sdi_page(&root_data) {
            // Walk the B+tree: collect root and any linked leaf pages
            let mut sdi_pages = vec![root_page];
            collect_linked_sdi_pages(ts, &root_data, &mut sdi_pages)?;
            return Ok(sdi_pages);
        }
    }

    // Fallback: scan all pages
    let mut sdi_pages = Vec::new();
    for page_num in 0..page_count {
        let page_data = ts.read_page(page_num)?;
        if is_sdi_page(&page_data) {
            sdi_pages.push(page_num);
        }
    }

    Ok(sdi_pages)
}

/// Walk the prev/next linked list from a starting SDI page to find all leaf pages.
fn collect_linked_sdi_pages(
    ts: &mut crate::innodb::tablespace::Tablespace,
    start_page: &[u8],
    pages: &mut Vec<u64>,
) -> Result<(), crate::IdbError> {
    let header = match FilHeader::parse(start_page) {
        Some(h) => h,
        None => return Ok(()),
    };

    // Follow next pointers
    let mut next = header.next_page;
    while next != crate::innodb::constants::FIL_NULL && next != 0 {
        let next_page = next as u64;
        if pages.contains(&next_page) {
            break; // Avoid cycles
        }
        let data = ts.read_page(next_page)?;
        if !is_sdi_page(&data) {
            break;
        }
        pages.push(next_page);
        next = match FilHeader::parse(&data) {
            Some(h) => h.next_page,
            None => break,
        };
    }

    // Follow prev pointers from original page
    let mut prev = header.prev_page;
    while prev != crate::innodb::constants::FIL_NULL && prev != 0 {
        let prev_page = prev as u64;
        if pages.contains(&prev_page) {
            break;
        }
        let data = ts.read_page(prev_page)?;
        if !is_sdi_page(&data) {
            break;
        }
        pages.push(prev_page);
        prev = match FilHeader::parse(&data) {
            Some(h) => h.prev_page,
            None => break,
        };
    }

    pages.sort();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sdi_type_name() {
        assert_eq!(sdi_type_name(1), "Table");
        assert_eq!(sdi_type_name(2), "Tablespace");
        assert_eq!(sdi_type_name(99), "Unknown");
    }

    #[test]
    fn test_decompress_sdi_data() {
        // Create a simple zlib-compressed string
        use flate2::write::ZlibEncoder;
        use flate2::Compression;
        use std::io::Write;

        let original = r#"{"dd_object_type": "Table", "dd_object": {"name": "test"}}"#;
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(original.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();

        let result = decompress_sdi_data(&compressed, original.len() as u32);
        assert_eq!(result.unwrap(), original);
    }

    #[test]
    fn test_is_sdi_page() {
        use crate::innodb::constants::FIL_PAGE_TYPE;

        // Non-SDI page
        let page = vec![0u8; 256];
        assert!(!is_sdi_page(&page));

        // SDI page (type 17853 at offset 24-25)
        let mut page = vec![0u8; 256];
        BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17853);
        assert!(is_sdi_page(&page));
    }

    #[test]
    fn test_sdi_header_offset() {
        // 16K pages: 38 + 112 + 256*40 = 10390
        assert_eq!(sdi_header_offset(16384), 10390);
        // 4K pages: 38 + 112 + 16*40 = 790
        assert_eq!(sdi_header_offset(4096), 790);
        // 8K pages: 38 + 112 + 64*40 = 2710
        assert_eq!(sdi_header_offset(8192), 2710);
        // 32K pages: 38 + 112 + 512*40 = 20630
        assert_eq!(sdi_header_offset(32768), 20630);
        // 64K pages: 38 + 112 + 1024*40 = 41110
        assert_eq!(sdi_header_offset(65536), 41110);
    }

    #[test]
    fn test_xdes_arr_size() {
        assert_eq!(xdes_arr_size(4096), 16);
        assert_eq!(xdes_arr_size(8192), 64);
        assert_eq!(xdes_arr_size(16384), 256);
        assert_eq!(xdes_arr_size(32768), 512);
        assert_eq!(xdes_arr_size(65536), 1024);
    }

    #[test]
    fn test_read_sdi_root_page() {
        let page_size = 16384u32;
        let mut page0 = vec![0u8; page_size as usize];
        let offset = sdi_header_offset(page_size);

        // Write SDI version = 1
        BigEndian::write_u32(&mut page0[offset..], 1);
        // Write SDI root page = 3
        BigEndian::write_u32(&mut page0[offset + 4..], 3);

        let result = read_sdi_root_page(&page0, page_size, 100);
        assert_eq!(result, Some(3));

        // Wrong version
        BigEndian::write_u32(&mut page0[offset..], 0);
        assert_eq!(read_sdi_root_page(&page0, page_size, 100), None);

        // Root page out of range
        BigEndian::write_u32(&mut page0[offset..], 1);
        BigEndian::write_u32(&mut page0[offset + 4..], 200); // > page_count=100
        assert_eq!(read_sdi_root_page(&page0, page_size, 100), None);
    }

    #[test]
    fn test_parse_sdi_record_header() {
        use flate2::write::ZlibEncoder;
        use flate2::Compression;
        use std::io::Write;

        let json = r#"{"test": true}"#;
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(json.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();

        // Build a mock page with an SDI record at offset 200
        let mut page = vec![0u8; 16384];
        let origin = 200;

        BigEndian::write_u32(&mut page[origin + SDI_TYPE_OFFSET..], 1); // Table
        BigEndian::write_u64(&mut page[origin + SDI_ID_OFFSET..], 42);
        BigEndian::write_u32(&mut page[origin + SDI_UNCOMP_LEN_OFFSET..], json.len() as u32);
        BigEndian::write_u32(&mut page[origin + SDI_COMP_LEN_OFFSET..], compressed.len() as u32);
        page[origin + SDI_DATA_OFFSET..origin + SDI_DATA_OFFSET + compressed.len()]
            .copy_from_slice(&compressed);

        let header = parse_sdi_record_header(&page, origin).unwrap();
        assert_eq!(header.sdi_type, 1);
        assert_eq!(header.sdi_id, 42);
        assert!(header.data_complete);
        assert_eq!(header.compressed_len, compressed.len() as u32);
    }
}
