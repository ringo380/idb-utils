//! Integration tests for idb-utils.
//!
//! These tests construct synthetic InnoDB tablespace files (.ibd) with valid
//! page structures and run the full parsing/validation pipeline against them.

use byteorder::{BigEndian, ByteOrder};
use std::io::Write;
use tempfile::NamedTempFile;

use idb::innodb::checksum::{validate_checksum, validate_lsn, ChecksumAlgorithm};
use idb::innodb::constants::*;
use idb::innodb::page::FilHeader;
use idb::innodb::page_types::PageType;
use idb::innodb::tablespace::Tablespace;

const PAGE_SIZE: u32 = 16384;
const PS: usize = PAGE_SIZE as usize;

/// Build a minimal valid FSP_HDR page (page 0) with CRC-32C checksum.
fn build_fsp_hdr_page(space_id: u32, total_pages: u32) -> Vec<u8> {
    let mut page = vec![0u8; PS];

    // FIL header
    BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], 0); // page number 0
    BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], 1000);
    BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 8); // FSP_HDR
    BigEndian::write_u64(&mut page[FIL_PAGE_FILE_FLUSH_LSN..], 1000);
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);

    // FSP header at FIL_PAGE_DATA (offset 38)
    let fsp = FIL_PAGE_DATA;
    BigEndian::write_u32(&mut page[fsp + FSP_SPACE_ID..], space_id);
    BigEndian::write_u32(&mut page[fsp + FSP_SIZE..], total_pages);
    BigEndian::write_u32(&mut page[fsp + FSP_FREE_LIMIT..], total_pages);
    BigEndian::write_u32(&mut page[fsp + FSP_SPACE_FLAGS..], 0); // flags=0 => default 16K

    // FIL trailer: low 32 bits of LSN at trailer+4
    let trailer = PS - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut page[trailer + 4..], 1000 & 0xFFFFFFFF);

    // Calculate and write CRC-32C checksum
    write_crc32c_checksum(&mut page);

    page
}

/// Build a minimal INDEX page with CRC-32C checksum.
fn build_index_page(page_num: u32, space_id: u32, lsn: u64) -> Vec<u8> {
    let mut page = vec![0u8; PS];

    // FIL header
    BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], page_num);
    BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], lsn);
    BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855); // INDEX
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);

    // INDEX page header at FIL_PAGE_DATA (offset 38)
    let ph = FIL_PAGE_DATA;
    BigEndian::write_u16(&mut page[ph + PAGE_N_DIR_SLOTS..], 2);
    BigEndian::write_u16(&mut page[ph + PAGE_N_HEAP..], 0x8002); // compact + 2 records
    BigEndian::write_u16(&mut page[ph + PAGE_N_RECS..], 0);
    BigEndian::write_u16(&mut page[ph + PAGE_LEVEL..], 0); // leaf
    BigEndian::write_u64(&mut page[ph + PAGE_INDEX_ID..], 42);

    // FIL trailer
    let trailer = PS - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut page[trailer + 4..], (lsn & 0xFFFFFFFF) as u32);

    write_crc32c_checksum(&mut page);

    page
}

/// Build an ALLOCATED (empty) page — all zeros with valid CRC-32C.
fn build_allocated_page(_page_num: u32, _space_id: u32) -> Vec<u8> {
    // All-zero pages are valid with checksum 0
    vec![0u8; PS]
}

/// Calculate and write CRC-32C into the checksum field (bytes 0-3).
///
/// MySQL CRC-32C covers:
///   Range 1: bytes 4..26 (FIL_PAGE_OFFSET to FIL_PAGE_FILE_FLUSH_LSN)
///   Range 2: bytes 38..(page_size-8) (FIL_PAGE_DATA to end before trailer)
fn write_crc32c_checksum(page: &mut [u8]) {
    let end = PS - SIZE_FIL_TRAILER;
    let crc1 = crc32c::crc32c(&page[FIL_PAGE_OFFSET..FIL_PAGE_FILE_FLUSH_LSN]);
    let crc2 = crc32c::crc32c(&page[FIL_PAGE_DATA..end]);
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_OR_CHKSUM..], crc1 ^ crc2);
}

/// Write a multi-page synthetic tablespace to a temp file.
fn write_tablespace(pages: &[Vec<u8>]) -> NamedTempFile {
    let mut tmp = NamedTempFile::new().expect("create temp file");
    for page in pages {
        tmp.write_all(page).expect("write page");
    }
    tmp.flush().expect("flush");
    tmp
}

// ---------- Tablespace open + page size detection ----------

#[test]
fn test_open_tablespace_detects_page_size() {
    let page0 = build_fsp_hdr_page(42, 4);
    let page1 = build_index_page(1, 42, 2000);
    let page2 = build_index_page(2, 42, 3000);
    let page3 = build_allocated_page(3, 42);

    let tmp = write_tablespace(&[page0, page1, page2, page3]);
    let ts = Tablespace::open(tmp.path()).expect("open tablespace");

    assert_eq!(ts.page_size(), PAGE_SIZE);
    assert_eq!(ts.page_count(), 4);
    assert_eq!(ts.file_size(), 4 * PS as u64);

    let fsp = ts.fsp_header().expect("FSP header present");
    assert_eq!(fsp.space_id, 42);
    assert_eq!(fsp.size, 4);
}

// ---------- Page iteration ----------

#[test]
fn test_iterate_all_pages() {
    let page0 = build_fsp_hdr_page(10, 3);
    let page1 = build_index_page(1, 10, 5000);
    let page2 = build_index_page(2, 10, 6000);

    let tmp = write_tablespace(&[page0, page1, page2]);
    let mut ts = Tablespace::open(tmp.path()).expect("open tablespace");

    let mut page_types = Vec::new();
    ts.for_each_page(|page_num, data| {
        let hdr = FilHeader::parse(data).unwrap();
        page_types.push((page_num, hdr.page_type));
        Ok(())
    })
    .expect("iterate pages");

    assert_eq!(page_types.len(), 3);
    assert_eq!(page_types[0], (0, PageType::FspHdr));
    assert_eq!(page_types[1], (1, PageType::Index));
    assert_eq!(page_types[2], (2, PageType::Index));
}

// ---------- Checksum validation ----------

#[test]
fn test_crc32c_checksum_valid_pages() {
    let page0 = build_fsp_hdr_page(1, 2);
    let page1 = build_index_page(1, 1, 2000);

    // Verify page 0
    let result = validate_checksum(&page0, PAGE_SIZE);
    assert!(result.valid, "FSP_HDR page checksum should be valid");
    assert_eq!(result.algorithm, ChecksumAlgorithm::Crc32c);

    // Verify page 1
    let result = validate_checksum(&page1, PAGE_SIZE);
    assert!(result.valid, "INDEX page checksum should be valid");
    assert_eq!(result.algorithm, ChecksumAlgorithm::Crc32c);
}

#[test]
fn test_checksum_detects_corruption() {
    let mut page = build_index_page(1, 1, 2000);

    // Verify it's valid first
    let result = validate_checksum(&page, PAGE_SIZE);
    assert!(result.valid);

    // Corrupt a byte in the data area
    page[100] ^= 0xFF;

    // Checksum should now fail
    let result = validate_checksum(&page, PAGE_SIZE);
    assert!(!result.valid);
}

#[test]
fn test_lsn_consistency_valid() {
    let page = build_index_page(1, 1, 0x00000001_AABBCCDD);
    assert!(validate_lsn(&page, PAGE_SIZE));
}

#[test]
fn test_lsn_consistency_corrupted() {
    let mut page = build_index_page(1, 1, 0x00000001_AABBCCDD);
    // Corrupt the trailer LSN
    let trailer = PS - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut page[trailer + 4..], 0x12345678);
    assert!(!validate_lsn(&page, PAGE_SIZE));
}

// ---------- Read specific page ----------

#[test]
fn test_read_specific_page() {
    let page0 = build_fsp_hdr_page(7, 3);
    let page1 = build_index_page(1, 7, 8000);
    let page2 = build_index_page(2, 7, 9000);

    let tmp = write_tablespace(&[page0, page1, page2]);
    let mut ts = Tablespace::open(tmp.path()).expect("open");

    let data = ts.read_page(1).expect("read page 1");
    let hdr = FilHeader::parse(&data).unwrap();
    assert_eq!(hdr.page_number, 1);
    assert_eq!(hdr.page_type, PageType::Index);
    assert_eq!(hdr.lsn, 8000);
    assert_eq!(hdr.space_id, 7);
}

#[test]
fn test_read_page_out_of_range() {
    let page0 = build_fsp_hdr_page(1, 1);
    let tmp = write_tablespace(&[page0]);
    let mut ts = Tablespace::open(tmp.path()).expect("open");

    let result = ts.read_page(5);
    assert!(result.is_err());
}

// ---------- CLI parse pipeline ----------

#[test]
fn test_parse_execute_succeeds() {
    let page0 = build_fsp_hdr_page(99, 3);
    let page1 = build_index_page(1, 99, 5000);
    let page2 = build_allocated_page(2, 99);

    let tmp = write_tablespace(&[page0, page1, page2]);

    let opts = idb::cli::parse::ParseOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        verbose: false,
        no_empty: false,
        page_size: None,
        json: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::parse::execute(&opts, &mut out);
    assert!(result.is_ok(), "parse execute should succeed: {:?}", result.err());
}

#[test]
fn test_parse_single_page() {
    let page0 = build_fsp_hdr_page(1, 2);
    let page1 = build_index_page(1, 1, 2000);

    let tmp = write_tablespace(&[page0, page1]);

    let opts = idb::cli::parse::ParseOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: Some(0),
        verbose: false,
        no_empty: false,
        page_size: None,
        json: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::parse::execute(&opts, &mut out);
    assert!(result.is_ok());
}

// ---------- CLI checksum pipeline ----------

#[test]
fn test_checksum_execute_all_valid() {
    let page0 = build_fsp_hdr_page(1, 2);
    let page1 = build_index_page(1, 1, 2000);

    let tmp = write_tablespace(&[page0, page1]);

    let opts = idb::cli::checksum::ChecksumOptions {
        file: tmp.path().to_string_lossy().to_string(),
        verbose: false,
        json: false,
        page_size: None,
    };

    let mut out = Vec::new();
    let result = idb::cli::checksum::execute(&opts, &mut out);
    assert!(result.is_ok(), "checksum should succeed: {:?}", result.err());
}

#[test]
fn test_checksum_json_output() {
    let page0 = build_fsp_hdr_page(1, 2);
    let page1 = build_index_page(1, 1, 2000);

    let tmp = write_tablespace(&[page0, page1]);

    let opts = idb::cli::checksum::ChecksumOptions {
        file: tmp.path().to_string_lossy().to_string(),
        verbose: false,
        json: true,
        page_size: None,
    };

    let mut out = Vec::new();
    let result = idb::cli::checksum::execute(&opts, &mut out);
    assert!(result.is_ok());
}

// ---------- CLI dump pipeline ----------

#[test]
fn test_dump_execute_page_zero() {
    let page0 = build_fsp_hdr_page(1, 1);
    let tmp = write_tablespace(&[page0]);

    let opts = idb::cli::dump::DumpOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: Some(0),
        offset: None,
        length: Some(64),
        raw: false,
        page_size: None,
    };

    let mut out = Vec::new();
    let result = idb::cli::dump::execute(&opts, &mut out);
    assert!(result.is_ok(), "dump should succeed: {:?}", result.err());
}

// ---------- Hex dump utility ----------

#[test]
fn test_hex_dump_format() {
    let data: Vec<u8> = (0..32).collect();
    let dump = idb::util::hex::hex_dump(&data, 0);

    // Should contain two lines of 16 bytes each
    let lines: Vec<&str> = dump.trim().lines().collect();
    assert_eq!(lines.len(), 2);

    // First line starts at offset 00000000
    assert!(lines[0].starts_with("00000000"));
    // Second line starts at offset 00000010
    assert!(lines[1].starts_with("00000010"));
}
