#![cfg(feature = "cli")]
//! Integration tests for idb-utils.
//!
//! These tests construct synthetic InnoDB tablespace files (.ibd) with valid
//! page structures and run the full parsing/validation pipeline against them.

use byteorder::{BigEndian, ByteOrder};
use std::io::Write;
use tempfile::NamedTempFile;

use idb::innodb::checksum::{validate_checksum, validate_lsn, ChecksumAlgorithm};
use idb::innodb::constants::*;
use idb::innodb::log::{LOG_BLOCK_CHECKSUM_OFFSET, LOG_BLOCK_HDR_SIZE, LOG_BLOCK_SIZE};
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
    let result = validate_checksum(&page0, PAGE_SIZE, None);
    assert!(result.valid, "FSP_HDR page checksum should be valid");
    assert_eq!(result.algorithm, ChecksumAlgorithm::Crc32c);

    // Verify page 1
    let result = validate_checksum(&page1, PAGE_SIZE, None);
    assert!(result.valid, "INDEX page checksum should be valid");
    assert_eq!(result.algorithm, ChecksumAlgorithm::Crc32c);
}

#[test]
fn test_checksum_detects_corruption() {
    let mut page = build_index_page(1, 1, 2000);

    // Verify it's valid first
    let result = validate_checksum(&page, PAGE_SIZE, None);
    assert!(result.valid);

    // Corrupt a byte in the data area
    page[100] ^= 0xFF;

    // Checksum should now fail
    let result = validate_checksum(&page, PAGE_SIZE, None);
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
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::parse::execute(&opts, &mut out);
    assert!(
        result.is_ok(),
        "parse execute should succeed: {:?}",
        result.err()
    );
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
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: false,
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
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::checksum::execute(&opts, &mut out);
    assert!(
        result.is_ok(),
        "checksum should succeed: {:?}",
        result.err()
    );
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
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: false,
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
        keyring: None,
        decrypt: false,
        mmap: false,
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

// ---------- CLI pages pipeline ----------

#[test]
fn test_pages_execute_succeeds() {
    let page0 = build_fsp_hdr_page(1, 3);
    let page1 = build_index_page(1, 1, 2000);
    let page2 = build_allocated_page(2, 1);

    let tmp = write_tablespace(&[page0, page1, page2]);

    let opts = idb::cli::pages::PagesOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        verbose: false,
        show_empty: false,
        list_mode: false,
        filter_type: None,
        page_size: None,
        json: false,
        keyring: None,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::pages::execute(&opts, &mut out);
    assert!(
        result.is_ok(),
        "pages execute should succeed: {:?}",
        result.err()
    );
}

#[test]
fn test_pages_list_mode() {
    let page0 = build_fsp_hdr_page(1, 2);
    let page1 = build_index_page(1, 1, 2000);

    let tmp = write_tablespace(&[page0, page1]);

    let opts = idb::cli::pages::PagesOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        verbose: false,
        show_empty: false,
        list_mode: true,
        filter_type: None,
        page_size: None,
        json: false,
        keyring: None,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::pages::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    assert!(
        output.contains("INDEX"),
        "list mode should show INDEX pages"
    );
}

#[test]
fn test_pages_json_output() {
    let page0 = build_fsp_hdr_page(1, 2);
    let page1 = build_index_page(1, 1, 2000);

    let tmp = write_tablespace(&[page0, page1]);

    let opts = idb::cli::pages::PagesOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        verbose: false,
        show_empty: false,
        list_mode: false,
        filter_type: None,
        page_size: None,
        json: true,
        keyring: None,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::pages::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    // Verify it's valid JSON
    let parsed: serde_json::Value =
        serde_json::from_str(&output).expect("output should be valid JSON");
    assert!(parsed.is_array());
}

// ---------- CLI corrupt pipeline ----------

#[test]
fn test_corrupt_execute_basic() {
    let page0 = build_fsp_hdr_page(1, 2);
    let page1 = build_index_page(1, 1, 2000);

    let tmp = write_tablespace(&[page0, page1]);
    // corrupt needs a writable file; NamedTempFile is writable
    let path = tmp.path().to_string_lossy().to_string();

    let opts = idb::cli::corrupt::CorruptOptions {
        file: path,
        page: Some(1),
        bytes: 4,
        header: false,
        records: false,
        offset: None,
        verify: false,
        json: false,
        page_size: None,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::corrupt::execute(&opts, &mut out);
    assert!(result.is_ok(), "corrupt should succeed: {:?}", result.err());
}

#[test]
fn test_corrupt_verify_detects_change() {
    let page0 = build_fsp_hdr_page(1, 2);
    let page1 = build_index_page(1, 1, 2000);

    let tmp = write_tablespace(&[page0, page1]);
    let path = tmp.path().to_string_lossy().to_string();

    let opts = idb::cli::corrupt::CorruptOptions {
        file: path.clone(),
        page: Some(1),
        bytes: 4,
        header: false,
        records: true,
        offset: None,
        verify: true,
        json: false,
        page_size: None,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::corrupt::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    assert!(
        output.contains("Verification"),
        "should show verification output"
    );
}

// ---------- CLI find pipeline ----------

#[test]
fn test_find_execute_basic() {
    // Create a temp datadir with subdirectory containing .ibd files
    let datadir = tempfile::tempdir().expect("create temp dir");
    let subdir = datadir.path().join("testdb");
    std::fs::create_dir(&subdir).expect("create subdir");

    let page0 = build_fsp_hdr_page(50, 2);
    let page1 = build_index_page(1, 50, 3000);

    let ibd_path = subdir.join("test_table.ibd");
    let mut f = std::fs::File::create(&ibd_path).expect("create ibd");
    f.write_all(&page0).unwrap();
    f.write_all(&page1).unwrap();
    f.flush().unwrap();

    let opts = idb::cli::find::FindOptions {
        datadir: datadir.path().to_string_lossy().to_string(),
        page: 1,
        checksum: None,
        space_id: None,
        first: false,
        json: false,
        page_size: None,
        threads: 0,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::find::execute(&opts, &mut out);
    assert!(result.is_ok(), "find should succeed: {:?}", result.err());
    let output = String::from_utf8(out).unwrap();
    assert!(
        output.contains("Found page 1"),
        "should find page 1 in the .ibd file"
    );
}

// ---------- CLI tsid pipeline ----------

#[test]
fn test_tsid_list_mode() {
    let datadir = tempfile::tempdir().expect("create temp dir");
    let subdir = datadir.path().join("testdb");
    std::fs::create_dir(&subdir).expect("create subdir");

    let page0 = build_fsp_hdr_page(77, 1);
    let ibd_path = subdir.join("t1.ibd");
    let mut f = std::fs::File::create(&ibd_path).expect("create ibd");
    f.write_all(&page0).unwrap();
    f.flush().unwrap();

    let opts = idb::cli::tsid::TsidOptions {
        datadir: datadir.path().to_string_lossy().to_string(),
        list: true,
        tablespace_id: None,
        json: false,
        page_size: None,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::tsid::execute(&opts, &mut out);
    assert!(
        result.is_ok(),
        "tsid list should succeed: {:?}",
        result.err()
    );
    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("77"), "should show space_id 77");
}

#[test]
fn test_tsid_lookup() {
    let datadir = tempfile::tempdir().expect("create temp dir");
    let subdir = datadir.path().join("testdb");
    std::fs::create_dir(&subdir).expect("create subdir");

    let page0 = build_fsp_hdr_page(88, 1);
    let ibd_path = subdir.join("t1.ibd");
    let mut f = std::fs::File::create(&ibd_path).expect("create ibd");
    f.write_all(&page0).unwrap();
    f.flush().unwrap();

    let opts = idb::cli::tsid::TsidOptions {
        datadir: datadir.path().to_string_lossy().to_string(),
        list: false,
        tablespace_id: Some(88),
        json: false,
        page_size: None,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::tsid::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("88"), "should find space_id 88");
}

// ---------- CLI sdi pipeline ----------

#[test]
fn test_sdi_execute_no_sdi_pages() {
    // Tablespace without SDI pages — should succeed gracefully
    let page0 = build_fsp_hdr_page(1, 2);
    let page1 = build_index_page(1, 1, 2000);

    let tmp = write_tablespace(&[page0, page1]);

    let opts = idb::cli::sdi::SdiOptions {
        file: tmp.path().to_string_lossy().to_string(),
        pretty: false,
        page_size: None,
        keyring: None,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::sdi::execute(&opts, &mut out);
    assert!(
        result.is_ok(),
        "sdi should succeed with no SDI pages: {:?}",
        result.err()
    );
    let output = String::from_utf8(out).unwrap();
    assert!(
        output.contains("No SDI pages found"),
        "should report no SDI pages"
    );
}

// ---------- CLI dump raw mode ----------

#[test]
fn test_dump_raw_mode() {
    let page0 = build_fsp_hdr_page(1, 1);
    let tmp = write_tablespace(&[page0.clone()]);

    let opts = idb::cli::dump::DumpOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: Some(0),
        offset: None,
        length: Some(64),
        raw: true,
        page_size: None,
        keyring: None,
        decrypt: false,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::dump::execute(&opts, &mut out);
    assert!(
        result.is_ok(),
        "dump raw should succeed: {:?}",
        result.err()
    );
    // Raw mode outputs exact bytes, not hex
    assert_eq!(out.len(), 64);
    assert_eq!(&out[..64], &page0[..64]);
}

// ---------- Error path tests ----------

#[test]
fn test_parse_nonexistent_file() {
    let opts = idb::cli::parse::ParseOptions {
        file: "/nonexistent/file.ibd".to_string(),
        page: None,
        verbose: false,
        no_empty: false,
        page_size: None,
        json: false,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::parse::execute(&opts, &mut out);
    assert!(result.is_err(), "parse should fail on nonexistent file");
}

#[test]
fn test_open_empty_file() {
    let tmp = NamedTempFile::new().expect("create temp file");
    let result = Tablespace::open(tmp.path());
    assert!(result.is_err(), "should reject empty file");
}

#[test]
fn test_open_truncated_file() {
    // File smaller than FIL header + FSP header
    let mut tmp = NamedTempFile::new().expect("create temp file");
    tmp.write_all(&[0u8; 20]).unwrap();
    tmp.flush().unwrap();
    let result = Tablespace::open(tmp.path());
    assert!(result.is_err(), "should reject truncated file");
}

#[test]
fn test_parse_json_validates_output() {
    let page0 = build_fsp_hdr_page(1, 2);
    let page1 = build_index_page(1, 1, 2000);

    let tmp = write_tablespace(&[page0, page1]);

    let opts = idb::cli::parse::ParseOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        verbose: false,
        no_empty: false,
        page_size: None,
        json: true,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::parse::execute(&opts, &mut out);
    assert!(result.is_ok());

    let output = String::from_utf8(out).unwrap();
    let parsed: serde_json::Value =
        serde_json::from_str(&output).expect("parse --json should produce valid JSON");
    assert!(parsed.is_array());
    let arr = parsed.as_array().unwrap();
    assert_eq!(arr.len(), 2, "should have 2 pages in JSON output");
}

#[test]
fn test_checksum_json_validates_output() {
    let page0 = build_fsp_hdr_page(1, 2);
    let page1 = build_index_page(1, 1, 2000);

    let tmp = write_tablespace(&[page0, page1]);

    let opts = idb::cli::checksum::ChecksumOptions {
        file: tmp.path().to_string_lossy().to_string(),
        verbose: true,
        json: true,
        page_size: None,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::checksum::execute(&opts, &mut out);
    assert!(result.is_ok());

    let output = String::from_utf8(out).unwrap();
    let parsed: serde_json::Value =
        serde_json::from_str(&output).expect("checksum --json should produce valid JSON");
    assert!(parsed.is_object());
    assert!(parsed.get("total_pages").is_some());
    assert!(parsed.get("valid_pages").is_some());
}

// ---------- CLI find with JSON ----------

#[test]
fn test_find_json_output() {
    let datadir = tempfile::tempdir().expect("create temp dir");
    let subdir = datadir.path().join("testdb");
    std::fs::create_dir(&subdir).expect("create subdir");

    let page0 = build_fsp_hdr_page(50, 1);
    let ibd_path = subdir.join("test.ibd");
    let mut f = std::fs::File::create(&ibd_path).expect("create ibd");
    f.write_all(&page0).unwrap();
    f.flush().unwrap();

    let opts = idb::cli::find::FindOptions {
        datadir: datadir.path().to_string_lossy().to_string(),
        page: 0,
        checksum: None,
        space_id: None,
        first: false,
        json: true,
        page_size: None,
        threads: 0,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::find::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    let parsed: serde_json::Value =
        serde_json::from_str(&output).expect("find --json should produce valid JSON");
    assert!(parsed.get("matches").is_some());
}

// ---------- CLI recover pipeline ----------

#[test]
fn test_recover_all_intact() {
    let page0 = build_fsp_hdr_page(1, 3);
    let page1 = build_index_page(1, 1, 2000);
    let page2 = build_index_page(2, 1, 3000);

    let tmp = write_tablespace(&[page0, page1, page2]);

    let opts = idb::cli::recover::RecoverOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        verbose: false,
        json: false,
        force: false,
        page_size: None,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: false,
        rebuild: None,
    };

    let mut out = Vec::new();
    let result = idb::cli::recover::execute(&opts, &mut out);
    assert!(result.is_ok(), "recover should succeed: {:?}", result.err());
    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("Recovery Analysis"));
    assert!(output.contains("Intact:"));
    assert!(output.contains("100.0% of pages intact"));
}

#[test]
fn test_recover_with_corrupt_page() {
    let page0 = build_fsp_hdr_page(1, 3);
    let page1 = build_index_page(1, 1, 2000);
    let mut page2 = build_index_page(2, 1, 3000);
    // Corrupt page 2's checksum
    BigEndian::write_u32(&mut page2[FIL_PAGE_SPACE_OR_CHKSUM..], 0xBADBAD);

    let tmp = write_tablespace(&[page0, page1, page2]);

    let opts = idb::cli::recover::RecoverOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        verbose: false,
        json: false,
        force: false,
        page_size: None,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: false,
        rebuild: None,
    };

    let mut out = Vec::new();
    let result = idb::cli::recover::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("Corrupt:"));
    assert!(output.contains("page"));
}

#[test]
fn test_recover_with_empty_page() {
    let page0 = build_fsp_hdr_page(1, 3);
    let page1 = build_index_page(1, 1, 2000);
    let page2 = build_allocated_page(2, 1);

    let tmp = write_tablespace(&[page0, page1, page2]);

    let opts = idb::cli::recover::RecoverOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        verbose: false,
        json: false,
        force: false,
        page_size: None,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: false,
        rebuild: None,
    };

    let mut out = Vec::new();
    let result = idb::cli::recover::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("Empty:"));
}

#[test]
fn test_recover_single_page() {
    let page0 = build_fsp_hdr_page(1, 2);
    let page1 = build_index_page(1, 1, 2000);

    let tmp = write_tablespace(&[page0, page1]);

    let opts = idb::cli::recover::RecoverOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: Some(1),
        verbose: false,
        json: false,
        force: false,
        page_size: None,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: false,
        rebuild: None,
    };

    let mut out = Vec::new();
    let result = idb::cli::recover::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("1 pages x"));
}

#[test]
fn test_recover_json_output() {
    let page0 = build_fsp_hdr_page(1, 3);
    let page1 = build_index_page(1, 1, 2000);
    let page2 = build_allocated_page(2, 1);

    let tmp = write_tablespace(&[page0, page1, page2]);

    let opts = idb::cli::recover::RecoverOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        verbose: false,
        json: true,
        force: false,
        page_size: None,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: false,
        rebuild: None,
    };

    let mut out = Vec::new();
    let result = idb::cli::recover::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    let parsed: serde_json::Value =
        serde_json::from_str(&output).expect("recover --json should produce valid JSON");
    assert!(parsed.is_object());
    assert!(parsed.get("summary").is_some());
    assert!(parsed.get("total_pages").is_some());
    assert!(parsed.get("recoverable_records").is_some());
    assert_eq!(parsed["total_pages"], 3);
}

#[test]
fn test_recover_json_verbose_includes_pages() {
    let page0 = build_fsp_hdr_page(1, 2);
    let page1 = build_index_page(1, 1, 2000);

    let tmp = write_tablespace(&[page0, page1]);

    let opts = idb::cli::recover::RecoverOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        verbose: true,
        json: true,
        force: false,
        page_size: None,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: false,
        rebuild: None,
    };

    let mut out = Vec::new();
    let result = idb::cli::recover::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
    let pages = parsed.get("pages").and_then(|v| v.as_array());
    assert!(pages.is_some(), "verbose JSON should include pages array");
    assert_eq!(pages.unwrap().len(), 2);
}

#[test]
fn test_recover_force_extracts_corrupt_records() {
    let page0 = build_fsp_hdr_page(1, 2);
    let mut page1 = build_index_page(1, 1, 2000);
    // Corrupt the checksum but leave the header valid
    BigEndian::write_u32(&mut page1[FIL_PAGE_SPACE_OR_CHKSUM..], 0xBADBAD);

    let tmp = write_tablespace(&[page0, page1]);

    // Without force
    let opts = idb::cli::recover::RecoverOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        verbose: false,
        json: true,
        force: false,
        page_size: None,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: false,
        rebuild: None,
    };
    let mut out = Vec::new();
    idb::cli::recover::execute(&opts, &mut out).unwrap();
    let output = String::from_utf8(out).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
    // Without force, might have force_recoverable_records field
    assert!(
        parsed.get("force_recoverable_records").is_some() || parsed["recoverable_records"] == 0
    );

    // With force
    let opts_force = idb::cli::recover::RecoverOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        verbose: false,
        json: true,
        force: true,
        page_size: None,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: false,
        rebuild: None,
    };
    let mut out2 = Vec::new();
    idb::cli::recover::execute(&opts_force, &mut out2).unwrap();
    let output2 = String::from_utf8(out2).unwrap();
    let parsed2: serde_json::Value = serde_json::from_str(&output2).unwrap();
    // force_recoverable_records should NOT be present when force is used
    assert!(parsed2.get("force_recoverable_records").is_none());
}

#[test]
fn test_recover_page_size_override() {
    let page0 = build_fsp_hdr_page(1, 2);
    let page1 = build_index_page(1, 1, 2000);

    let tmp = write_tablespace(&[page0, page1]);

    let opts = idb::cli::recover::RecoverOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        verbose: false,
        json: true,
        force: false,
        page_size: Some(16384),
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: false,
        rebuild: None,
    };

    let mut out = Vec::new();
    let result = idb::cli::recover::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert_eq!(parsed["page_size"], 16384);
    assert_eq!(parsed["page_size_source"], "user-specified");
}

#[test]
fn test_recover_verbose_text_output() {
    let page0 = build_fsp_hdr_page(1, 2);
    let page1 = build_index_page(1, 1, 2000);

    let tmp = write_tablespace(&[page0, page1]);

    let opts = idb::cli::recover::RecoverOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        verbose: true,
        json: false,
        force: false,
        page_size: None,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: false,
        rebuild: None,
    };

    let mut out = Vec::new();
    let result = idb::cli::recover::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("Page    0:"));
    assert!(output.contains("Page    1:"));
    assert!(output.contains("FSP_HDR"));
    assert!(output.contains("INDEX"));
    assert!(output.contains("LSN="));
}

// ---------- CLI tsid with JSON ----------

#[test]
fn test_tsid_json_output() {
    let datadir = tempfile::tempdir().expect("create temp dir");
    let subdir = datadir.path().join("testdb");
    std::fs::create_dir(&subdir).expect("create subdir");

    let page0 = build_fsp_hdr_page(33, 1);
    let ibd_path = subdir.join("t1.ibd");
    let mut f = std::fs::File::create(&ibd_path).expect("create ibd");
    f.write_all(&page0).unwrap();
    f.flush().unwrap();

    let opts = idb::cli::tsid::TsidOptions {
        datadir: datadir.path().to_string_lossy().to_string(),
        list: true,
        tablespace_id: None,
        json: true,
        page_size: None,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::tsid::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    let parsed: serde_json::Value =
        serde_json::from_str(&output).expect("tsid --json should produce valid JSON");
    assert!(parsed.get("tablespaces").is_some());
}

// ========== Redo log builder helpers ==========

/// Build a redo log header block (block 0) with valid CRC-32C.
///
/// Offset 0: format_version (u32), Offset 4: log_uuid (u32),
/// Offset 8: start_lsn (u64), Offset 16: creator (32 bytes).
fn build_log_header_block(
    format_version: u32,
    start_lsn: u64,
    log_uuid: u32,
    creator: &str,
) -> Vec<u8> {
    let mut block = vec![0u8; LOG_BLOCK_SIZE];
    BigEndian::write_u32(&mut block[0..], format_version);
    BigEndian::write_u32(&mut block[4..], log_uuid);
    BigEndian::write_u64(&mut block[8..], start_lsn);
    let creator_bytes = creator.as_bytes();
    let len = creator_bytes.len().min(32);
    block[16..16 + len].copy_from_slice(&creator_bytes[..len]);
    write_log_block_checksum(&mut block);
    block
}

/// Build a checkpoint block with valid CRC-32C.
fn build_checkpoint_block(number: u64, lsn: u64, offset: u32, buf_size: u32) -> Vec<u8> {
    let mut block = vec![0u8; LOG_BLOCK_SIZE];
    BigEndian::write_u64(&mut block[0..], number);
    BigEndian::write_u64(&mut block[8..], lsn);
    BigEndian::write_u32(&mut block[16..], offset);
    BigEndian::write_u32(&mut block[20..], buf_size);
    write_log_block_checksum(&mut block);
    block
}

/// Build a redo log data block with valid CRC-32C.
fn build_log_data_block(block_no: u32, data_len: u16, epoch_no: u32) -> Vec<u8> {
    let mut block = vec![0u8; LOG_BLOCK_SIZE];
    BigEndian::write_u32(&mut block[0..], block_no);
    BigEndian::write_u16(&mut block[4..], data_len);
    BigEndian::write_u16(&mut block[6..], LOG_BLOCK_HDR_SIZE as u16); // first_rec_group
    BigEndian::write_u32(&mut block[8..], epoch_no);
    // Fill some data bytes
    if data_len as usize > LOG_BLOCK_HDR_SIZE {
        for i in LOG_BLOCK_HDR_SIZE..(data_len as usize).min(LOG_BLOCK_SIZE - 4) {
            block[i] = (i % 256) as u8;
        }
    }
    write_log_block_checksum(&mut block);
    block
}

/// Calculate and write CRC-32C checksum for a log block (bytes 508-511 over bytes [0..508)).
fn write_log_block_checksum(block: &mut [u8]) {
    let crc = crc32c::crc32c(&block[..LOG_BLOCK_CHECKSUM_OFFSET]);
    BigEndian::write_u32(&mut block[LOG_BLOCK_CHECKSUM_OFFSET..], crc);
}

/// Write a synthetic redo log file (4 header blocks + N data blocks).
fn write_redo_log(data_blocks: &[Vec<u8>]) -> NamedTempFile {
    let header = build_log_header_block(1, 8704, 0, "MySQL 8.0.32");
    let cp1 = build_checkpoint_block(42, 8704, 2048, 65536);
    let reserved = vec![0u8; LOG_BLOCK_SIZE]; // block 2 reserved
    let cp2 = build_checkpoint_block(41, 8500, 2048, 65536);

    let mut tmp = NamedTempFile::new().expect("create temp file");
    tmp.write_all(&header).unwrap();
    tmp.write_all(&cp1).unwrap();
    tmp.write_all(&reserved).unwrap();
    tmp.write_all(&cp2).unwrap();
    for block in data_blocks {
        tmp.write_all(block).unwrap();
    }
    tmp.flush().unwrap();
    tmp
}

// ---------- CLI log pipeline (Phase 2.1) ----------

#[test]
fn test_log_basic_parse() {
    let b1 = build_log_data_block(5, 100, 42);
    let b2 = build_log_data_block(6, 200, 42);
    let b3 = build_log_data_block(7, LOG_BLOCK_HDR_SIZE as u16, 42); // empty (data_len == header size)

    let tmp = write_redo_log(&[b1, b2, b3]);

    let opts = idb::cli::log::LogOptions {
        file: tmp.path().to_string_lossy().to_string(),
        blocks: None,
        no_empty: false,
        verbose: false,
        json: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::log::execute(&opts, &mut out);
    assert!(
        result.is_ok(),
        "log execute should succeed: {:?}",
        result.err()
    );
    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("InnoDB Redo Log File"));
    assert!(output.contains("MySQL 8.0.32"));
    assert!(output.contains("Checkpoint 1"));
}

#[test]
fn test_log_json_output() {
    let b1 = build_log_data_block(5, 100, 42);
    let b2 = build_log_data_block(6, 200, 42);

    let tmp = write_redo_log(&[b1, b2]);

    let opts = idb::cli::log::LogOptions {
        file: tmp.path().to_string_lossy().to_string(),
        blocks: None,
        no_empty: false,
        verbose: false,
        json: true,
    };

    let mut out = Vec::new();
    let result = idb::cli::log::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    let parsed: serde_json::Value =
        serde_json::from_str(&output).expect("log --json should produce valid JSON");
    assert!(parsed.is_object());
    assert!(parsed.get("header").is_some());
    assert!(parsed.get("data_blocks").is_some());
    assert!(parsed.get("blocks").is_some());
}

#[test]
fn test_log_blocks_limit() {
    let blocks: Vec<Vec<u8>> = (0..5)
        .map(|i| build_log_data_block(i + 5, 100, 42))
        .collect();

    let tmp = write_redo_log(&blocks);

    let opts = idb::cli::log::LogOptions {
        file: tmp.path().to_string_lossy().to_string(),
        blocks: Some(2),
        no_empty: false,
        verbose: false,
        json: true,
    };

    let mut out = Vec::new();
    let result = idb::cli::log::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
    let blocks_arr = parsed["blocks"].as_array().unwrap();
    assert_eq!(blocks_arr.len(), 2, "should limit to 2 blocks");
}

#[test]
fn test_log_verbose_mode() {
    let b1 = build_log_data_block(5, 100, 42);

    let tmp = write_redo_log(&[b1]);

    let opts = idb::cli::log::LogOptions {
        file: tmp.path().to_string_lossy().to_string(),
        blocks: None,
        no_empty: false,
        verbose: true,
        json: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::log::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    assert!(
        output.contains("record types:"),
        "verbose should show record types"
    );
}

#[test]
fn test_log_nonexistent_file() {
    let opts = idb::cli::log::LogOptions {
        file: "/nonexistent/ib_logfile0".to_string(),
        blocks: None,
        no_empty: false,
        verbose: false,
        json: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::log::execute(&opts, &mut out);
    assert!(result.is_err(), "log should fail on nonexistent file");
}

// ---------- Error path tests (Phase 2.2) ----------

#[test]
fn test_dump_nonexistent_file() {
    let opts = idb::cli::dump::DumpOptions {
        file: "/nonexistent/test.ibd".to_string(),
        page: Some(0),
        offset: None,
        length: Some(64),
        raw: false,
        page_size: None,
        keyring: None,
        decrypt: false,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::dump::execute(&opts, &mut out);
    assert!(result.is_err(), "dump should fail on nonexistent file");
}

#[test]
fn test_checksum_invalid_returns_error() {
    let page0 = build_fsp_hdr_page(1, 2);
    let mut page1 = build_index_page(1, 1, 2000);
    // Corrupt page 1's checksum (avoid 0xDEADBEEF which is BUF_NO_CHECKSUM_MAGIC)
    BigEndian::write_u32(&mut page1[FIL_PAGE_SPACE_OR_CHKSUM..], 0xBADBAD00);

    let tmp = write_tablespace(&[page0, page1]);

    let opts = idb::cli::checksum::ChecksumOptions {
        file: tmp.path().to_string_lossy().to_string(),
        verbose: false,
        json: false,
        page_size: None,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::checksum::execute(&opts, &mut out);
    assert!(
        result.is_err(),
        "checksum should return Err when pages are invalid"
    );
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("invalid checksums"),
        "error should mention invalid checksums"
    );
}

#[test]
fn test_find_nonexistent_directory() {
    let opts = idb::cli::find::FindOptions {
        datadir: "/nonexistent/datadir".to_string(),
        page: 0,
        checksum: None,
        space_id: None,
        first: false,
        json: false,
        page_size: None,
        threads: 0,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::find::execute(&opts, &mut out);
    assert!(result.is_err(), "find should fail on nonexistent directory");
}

// ---------- CLI corrupt JSON output (Phase 2.3) ----------

#[test]
fn test_corrupt_json_output() {
    let page0 = build_fsp_hdr_page(1, 2);
    let page1 = build_index_page(1, 1, 2000);

    let tmp = write_tablespace(&[page0, page1]);
    let path = tmp.path().to_string_lossy().to_string();

    let opts = idb::cli::corrupt::CorruptOptions {
        file: path,
        page: Some(1),
        bytes: 4,
        header: false,
        records: false,
        offset: None,
        verify: false,
        json: true,
        page_size: None,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::corrupt::execute(&opts, &mut out);
    assert!(
        result.is_ok(),
        "corrupt JSON should succeed: {:?}",
        result.err()
    );
    let output = String::from_utf8(out).unwrap();
    let parsed: serde_json::Value =
        serde_json::from_str(&output).expect("corrupt --json should produce valid JSON");
    assert!(parsed.is_object());
    assert!(parsed.get("offset").is_some());
    assert!(parsed.get("data").is_some());
    assert!(parsed.get("bytes_written").is_some());
}

// ========== Info subcommand helpers ==========

/// Build a synthetic data directory with ibdata1 (single FSP_HDR page, space_id=0).
/// Returns the TempDir handle (must be kept alive for the duration of the test).
fn build_info_datadir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("create temp dir");
    let ibdata_path = dir.path().join("ibdata1");
    let page0 = build_fsp_hdr_page(0, 1);
    let mut f = std::fs::File::create(&ibdata_path).expect("create ibdata1");
    f.write_all(&page0).unwrap();
    f.flush().unwrap();
    dir
}

/// Build a synthetic redo log file at a specific path (not a temp file).
fn write_redo_log_to_path(path: &std::path::Path, data_blocks: &[Vec<u8>]) {
    let header = build_log_header_block(1, 1000, 0, "MySQL 8.0.32");
    // Checkpoint 1: LSN matching ibdata1 page0 LSN (1000)
    let cp1 = build_checkpoint_block(42, 1000, 2048, 65536);
    let reserved = vec![0u8; LOG_BLOCK_SIZE];
    let cp2 = build_checkpoint_block(41, 900, 2048, 65536);

    let mut f = std::fs::File::create(path).expect("create redo log");
    f.write_all(&header).unwrap();
    f.write_all(&cp1).unwrap();
    f.write_all(&reserved).unwrap();
    f.write_all(&cp2).unwrap();
    for block in data_blocks {
        f.write_all(block).unwrap();
    }
    f.flush().unwrap();
}

// ========== Phase 1: Info subcommand tests ==========

#[test]
fn test_info_ibdata_basic() {
    let datadir = build_info_datadir();

    let opts = idb::cli::info::InfoOptions {
        ibdata: true,
        lsn_check: false,
        datadir: Some(datadir.path().to_string_lossy().to_string()),
        database: None,
        table: None,
        host: None,
        port: None,
        user: None,
        password: None,
        defaults_file: None,
        json: false,
        page_size: None,
    };

    let mut out = Vec::new();
    let result = idb::cli::info::execute(&opts, &mut out);
    assert!(
        result.is_ok(),
        "info ibdata should succeed: {:?}",
        result.err()
    );
    let output = String::from_utf8(out).unwrap();
    assert!(
        output.contains("ibdata1 Page 0 Header"),
        "should show ibdata1 header"
    );
    assert!(output.contains("Checksum:"), "should show checksum");
    assert!(output.contains("LSN:"), "should show LSN");
    assert!(output.contains("Space ID:"), "should show space ID");
}

#[test]
fn test_info_ibdata_json() {
    let datadir = build_info_datadir();

    let opts = idb::cli::info::InfoOptions {
        ibdata: true,
        lsn_check: false,
        datadir: Some(datadir.path().to_string_lossy().to_string()),
        database: None,
        table: None,
        host: None,
        port: None,
        user: None,
        password: None,
        defaults_file: None,
        json: true,
        page_size: None,
    };

    let mut out = Vec::new();
    let result = idb::cli::info::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    let parsed: serde_json::Value =
        serde_json::from_str(&output).expect("info --json should produce valid JSON");
    assert!(parsed.is_object());
    assert!(parsed.get("ibdata_file").is_some());
    assert!(parsed.get("lsn").is_some());
    assert!(parsed.get("space_id").is_some());
    assert_eq!(parsed["space_id"], 0);
}

#[test]
fn test_info_ibdata_with_redo_log() {
    let datadir = build_info_datadir();
    // Add ib_logfile0 alongside ibdata1
    let logfile_path = datadir.path().join("ib_logfile0");
    write_redo_log_to_path(&logfile_path, &[]);

    let opts = idb::cli::info::InfoOptions {
        ibdata: true,
        lsn_check: false,
        datadir: Some(datadir.path().to_string_lossy().to_string()),
        database: None,
        table: None,
        host: None,
        port: None,
        user: None,
        password: None,
        defaults_file: None,
        json: false,
        page_size: None,
    };

    let mut out = Vec::new();
    let result = idb::cli::info::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    assert!(
        output.contains("Redo Log Checkpoint"),
        "should show checkpoint LSN from redo log"
    );
}

#[test]
fn test_info_lsn_check() {
    let datadir = build_info_datadir();
    let logfile_path = datadir.path().join("ib_logfile0");
    write_redo_log_to_path(&logfile_path, &[]);

    let opts = idb::cli::info::InfoOptions {
        ibdata: false,
        lsn_check: true,
        datadir: Some(datadir.path().to_string_lossy().to_string()),
        database: None,
        table: None,
        host: None,
        port: None,
        user: None,
        password: None,
        defaults_file: None,
        json: false,
        page_size: None,
    };

    let mut out = Vec::new();
    let result = idb::cli::info::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    assert!(
        output.contains("LSN Sync Check"),
        "should show LSN sync check header"
    );
    // ibdata1 LSN=1000 and checkpoint1 LSN=1000, so should be IN SYNC
    assert!(
        output.contains("IN SYNC"),
        "should be in sync since LSNs match"
    );
}

#[test]
fn test_info_lsn_check_json() {
    let datadir = build_info_datadir();
    let logfile_path = datadir.path().join("ib_logfile0");
    write_redo_log_to_path(&logfile_path, &[]);

    let opts = idb::cli::info::InfoOptions {
        ibdata: false,
        lsn_check: true,
        datadir: Some(datadir.path().to_string_lossy().to_string()),
        database: None,
        table: None,
        host: None,
        port: None,
        user: None,
        password: None,
        defaults_file: None,
        json: true,
        page_size: None,
    };

    let mut out = Vec::new();
    let result = idb::cli::info::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    let parsed: serde_json::Value =
        serde_json::from_str(&output).expect("lsn-check --json should produce valid JSON");
    assert!(parsed.is_object());
    assert!(parsed.get("ibdata_lsn").is_some());
    assert!(parsed.get("redo_checkpoint_lsn").is_some());
    assert!(parsed.get("in_sync").is_some());
    assert_eq!(parsed["in_sync"], true);
}

#[test]
fn test_info_nonexistent_datadir() {
    let opts = idb::cli::info::InfoOptions {
        ibdata: true,
        lsn_check: false,
        datadir: Some("/nonexistent/datadir".to_string()),
        database: None,
        table: None,
        host: None,
        port: None,
        user: None,
        password: None,
        defaults_file: None,
        json: false,
        page_size: None,
    };

    let mut out = Vec::new();
    let result = idb::cli::info::execute(&opts, &mut out);
    assert!(result.is_err(), "info should fail on nonexistent datadir");
}

#[test]
fn test_info_no_mode() {
    let opts = idb::cli::info::InfoOptions {
        ibdata: false,
        lsn_check: false,
        datadir: None,
        database: None,
        table: None,
        host: None,
        port: None,
        user: None,
        password: None,
        defaults_file: None,
        json: false,
        page_size: None,
    };

    let mut out = Vec::new();
    let result = idb::cli::info::execute(&opts, &mut out);
    assert!(result.is_ok(), "no-mode should print usage and succeed");
    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("Usage:"), "should show usage info");
}

// ========== Phase 2: parse flag tests ==========

#[test]
fn test_parse_verbose_output() {
    let page0 = build_fsp_hdr_page(1, 2);
    let page1 = build_index_page(1, 1, 2000);

    let tmp = write_tablespace(&[page0, page1]);

    let opts = idb::cli::parse::ParseOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: Some(1),
        verbose: true,
        no_empty: false,
        page_size: None,
        json: false,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::parse::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    assert!(
        output.contains("Checksum Status:"),
        "verbose should show checksum status"
    );
    assert!(
        output.contains("LSN Consistency:"),
        "verbose should show LSN consistency"
    );
}

#[test]
fn test_parse_no_empty() {
    let page0 = build_fsp_hdr_page(1, 3);
    let page1 = build_index_page(1, 1, 2000);
    let page2 = build_allocated_page(2, 1);

    let tmp = write_tablespace(&[page0, page1, page2]);

    // With no_empty, ALLOCATED page should be excluded from JSON output
    let opts = idb::cli::parse::ParseOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        verbose: false,
        no_empty: true,
        page_size: None,
        json: true,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::parse::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
    let arr = parsed.as_array().unwrap();
    // Should have 2 pages (FSP_HDR + INDEX), not 3 (ALLOCATED excluded)
    assert_eq!(arr.len(), 2, "no_empty should exclude ALLOCATED pages");
}

// ========== Phase 2: pages flag tests ==========

#[test]
fn test_pages_single_page() {
    let page0 = build_fsp_hdr_page(1, 3);
    let page1 = build_index_page(1, 1, 2000);
    let page2 = build_index_page(2, 1, 3000);

    let tmp = write_tablespace(&[page0, page1, page2]);

    let opts = idb::cli::pages::PagesOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: Some(1),
        verbose: false,
        show_empty: false,
        list_mode: false,
        filter_type: None,
        page_size: None,
        json: false,
        keyring: None,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::pages::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("Page 1"), "should show page 1");
    // Should NOT show page 0 or page 2
    assert!(!output.contains("Page 0"), "should not show page 0");
    assert!(!output.contains("Page 2"), "should not show page 2");
}

#[test]
fn test_pages_verbose_output() {
    let page0 = build_fsp_hdr_page(1, 2);
    let page1 = build_index_page(1, 1, 2000);

    let tmp = write_tablespace(&[page0, page1]);

    let opts = idb::cli::pages::PagesOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: Some(1),
        verbose: true,
        show_empty: false,
        list_mode: false,
        filter_type: None,
        page_size: None,
        json: false,
        keyring: None,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::pages::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    assert!(
        output.contains("Checksum Status:"),
        "verbose should show checksum status"
    );
    assert!(
        output.contains("LSN Consistency:"),
        "verbose should show LSN consistency"
    );
}

#[test]
fn test_pages_show_empty() {
    let page0 = build_fsp_hdr_page(1, 3);
    let page1 = build_index_page(1, 1, 2000);
    let page2 = build_allocated_page(2, 1);

    let tmp = write_tablespace(&[page0, page1, page2]);

    // Without show_empty, allocated page is skipped
    let opts_no_empty = idb::cli::pages::PagesOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        verbose: false,
        show_empty: false,
        list_mode: true,
        filter_type: None,
        page_size: None,
        json: false,
        keyring: None,
        mmap: false,
    };

    let mut out1 = Vec::new();
    idb::cli::pages::execute(&opts_no_empty, &mut out1).unwrap();
    let output_no_empty = String::from_utf8(out1).unwrap();

    // With show_empty, allocated page appears
    let opts_show_empty = idb::cli::pages::PagesOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        verbose: false,
        show_empty: true,
        list_mode: true,
        filter_type: None,
        page_size: None,
        json: false,
        keyring: None,
        mmap: false,
    };

    let mut out2 = Vec::new();
    idb::cli::pages::execute(&opts_show_empty, &mut out2).unwrap();
    let output_show_empty = String::from_utf8(out2).unwrap();

    // show_empty output should have more lines (includes ALLOCATED page)
    let lines_without = output_no_empty.lines().count();
    let lines_with = output_show_empty.lines().count();
    assert!(
        lines_with > lines_without,
        "show_empty should produce more output (got {} vs {} lines)",
        lines_with,
        lines_without
    );
}

#[test]
fn test_pages_filter_type() {
    let page0 = build_fsp_hdr_page(1, 3);
    let page1 = build_index_page(1, 1, 2000);
    let page2 = build_index_page(2, 1, 3000);

    let tmp = write_tablespace(&[page0, page1, page2]);

    let opts = idb::cli::pages::PagesOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        verbose: false,
        show_empty: false,
        list_mode: true,
        filter_type: Some("INDEX".to_string()),
        page_size: None,
        json: false,
        keyring: None,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::pages::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("INDEX"), "should show INDEX pages");
    assert!(
        !output.contains("FSP_HDR"),
        "should not show FSP_HDR when filtering by INDEX"
    );
}

// ========== Phase 2: dump flag tests ==========

#[test]
fn test_dump_offset_mode() {
    let page0 = build_fsp_hdr_page(1, 2);
    let page1 = build_index_page(1, 1, 2000);

    let tmp = write_tablespace(&[page0, page1]);

    let opts = idb::cli::dump::DumpOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        offset: Some(100),
        length: Some(32),
        raw: false,
        page_size: None,
        keyring: None,
        decrypt: false,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::dump::execute(&opts, &mut out);
    assert!(
        result.is_ok(),
        "dump offset mode should succeed: {:?}",
        result.err()
    );
    let output = String::from_utf8(out).unwrap();
    assert!(
        output.contains("offset 100"),
        "should mention offset in output"
    );
}

#[test]
fn test_dump_offset_raw_mode() {
    let page0 = build_fsp_hdr_page(1, 1);
    let tmp = write_tablespace(&[page0.clone()]);

    let opts = idb::cli::dump::DumpOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        offset: Some(0),
        length: Some(16),
        raw: true,
        page_size: None,
        keyring: None,
        decrypt: false,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::dump::execute(&opts, &mut out);
    assert!(result.is_ok());
    assert_eq!(out.len(), 16, "raw mode should output exactly 16 bytes");
    assert_eq!(
        &out[..16],
        &page0[..16],
        "raw bytes should match file content"
    );
}

#[test]
fn test_dump_default_page_zero() {
    let page0 = build_fsp_hdr_page(1, 1);
    let tmp = write_tablespace(&[page0]);

    // page=None should default to page 0
    let opts = idb::cli::dump::DumpOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        offset: None,
        length: Some(64),
        raw: false,
        page_size: None,
        keyring: None,
        decrypt: false,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::dump::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("page 0"), "should default to page 0");
}

#[test]
fn test_dump_length_truncation() {
    let page0 = build_fsp_hdr_page(1, 1);
    let tmp = write_tablespace(&[page0]);

    // Full page dump
    let opts_full = idb::cli::dump::DumpOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: Some(0),
        offset: None,
        length: None, // full page
        raw: true,
        page_size: None,
        keyring: None,
        decrypt: false,
        mmap: false,
    };

    let mut out_full = Vec::new();
    idb::cli::dump::execute(&opts_full, &mut out_full).unwrap();

    // Truncated dump
    let opts_short = idb::cli::dump::DumpOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: Some(0),
        offset: None,
        length: Some(32),
        raw: true,
        page_size: None,
        keyring: None,
        decrypt: false,
        mmap: false,
    };

    let mut out_short = Vec::new();
    idb::cli::dump::execute(&opts_short, &mut out_short).unwrap();

    assert_eq!(out_short.len(), 32, "truncated dump should be 32 bytes");
    assert!(
        out_full.len() > out_short.len(),
        "full dump ({}) should be larger than truncated ({})",
        out_full.len(),
        out_short.len()
    );
}

// ========== Phase 3: corrupt flag tests ==========

#[test]
fn test_corrupt_header_mode() {
    let page0 = build_fsp_hdr_page(1, 2);
    let page1 = build_index_page(1, 1, 2000);

    let tmp = write_tablespace(&[page0, page1]);
    let path = tmp.path().to_string_lossy().to_string();

    let opts = idb::cli::corrupt::CorruptOptions {
        file: path.clone(),
        page: Some(1),
        bytes: 4,
        header: true,
        records: false,
        offset: None,
        verify: true,
        json: true,
        page_size: None,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::corrupt::execute(&opts, &mut out);
    assert!(
        result.is_ok(),
        "corrupt header mode should succeed: {:?}",
        result.err()
    );
    let output = String::from_utf8(out).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
    // Offset should be within the first 38 bytes of page 1 (bytes 16384..16422)
    let offset = parsed["offset"].as_u64().unwrap();
    let page1_start = PS as u64;
    assert!(
        offset >= page1_start && offset < page1_start + 38,
        "header mode offset {} should be within FIL header of page 1 ({}-{})",
        offset,
        page1_start,
        page1_start + 38
    );
}

#[test]
fn test_corrupt_offset_mode() {
    let page0 = build_fsp_hdr_page(1, 2);
    let page1 = build_index_page(1, 1, 2000);

    let tmp = write_tablespace(&[page0, page1]);
    let path = tmp.path().to_string_lossy().to_string();

    let opts = idb::cli::corrupt::CorruptOptions {
        file: path.clone(),
        page: None,
        bytes: 4,
        header: false,
        records: false,
        offset: Some(100),
        verify: false,
        json: true,
        page_size: None,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::corrupt::execute(&opts, &mut out);
    assert!(
        result.is_ok(),
        "corrupt offset mode should succeed: {:?}",
        result.err()
    );
    let output = String::from_utf8(out).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert_eq!(parsed["offset"], 100, "should corrupt at exact offset 100");
    assert!(
        parsed["page"].is_null(),
        "offset mode should have null page"
    );
}

#[test]
fn test_corrupt_random_page() {
    let page0 = build_fsp_hdr_page(1, 3);
    let page1 = build_index_page(1, 1, 2000);
    let page2 = build_index_page(2, 1, 3000);

    let tmp = write_tablespace(&[page0, page1, page2]);
    let path = tmp.path().to_string_lossy().to_string();

    // page=None should choose a random page
    let opts = idb::cli::corrupt::CorruptOptions {
        file: path,
        page: None,
        bytes: 4,
        header: false,
        records: false,
        offset: None,
        verify: false,
        json: true,
        page_size: None,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::corrupt::execute(&opts, &mut out);
    assert!(
        result.is_ok(),
        "random page corrupt should succeed: {:?}",
        result.err()
    );
    let output = String::from_utf8(out).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert!(
        parsed["page"].is_number(),
        "should have a page number in JSON"
    );
    let page = parsed["page"].as_u64().unwrap();
    assert!(page < 3, "random page should be within range 0..3");
}

// ========== Phase 3: find flag tests ==========

#[test]
fn test_find_checksum_filter() {
    let datadir = tempfile::tempdir().expect("create temp dir");
    let subdir = datadir.path().join("testdb");
    std::fs::create_dir(&subdir).expect("create subdir");

    let page0 = build_fsp_hdr_page(50, 2);
    let page1 = build_index_page(1, 50, 3000);

    let ibd_path = subdir.join("test_table.ibd");
    let mut f = std::fs::File::create(&ibd_path).expect("create ibd");
    f.write_all(&page0).unwrap();
    f.write_all(&page1).unwrap();
    f.flush().unwrap();

    // Get the actual checksum from page 0
    let stored_checksum = BigEndian::read_u32(&page0[FIL_PAGE_SPACE_OR_CHKSUM..]);

    // Search with matching checksum — should find it
    let opts_match = idb::cli::find::FindOptions {
        datadir: datadir.path().to_string_lossy().to_string(),
        page: 0,
        checksum: Some(stored_checksum),
        space_id: None,
        first: false,
        json: true,
        page_size: None,
        threads: 0,
        mmap: false,
    };

    let mut out1 = Vec::new();
    idb::cli::find::execute(&opts_match, &mut out1).unwrap();
    let output1 = String::from_utf8(out1).unwrap();
    let parsed1: serde_json::Value = serde_json::from_str(&output1).unwrap();
    let matches1 = parsed1["matches"].as_array().unwrap();
    assert!(
        !matches1.is_empty(),
        "should find page with matching checksum"
    );

    // Search with wrong checksum — should find nothing
    let opts_no_match = idb::cli::find::FindOptions {
        datadir: datadir.path().to_string_lossy().to_string(),
        page: 0,
        checksum: Some(0xDEADBEEF),
        space_id: None,
        first: false,
        json: true,
        page_size: None,
        threads: 0,
        mmap: false,
    };

    let mut out2 = Vec::new();
    idb::cli::find::execute(&opts_no_match, &mut out2).unwrap();
    let output2 = String::from_utf8(out2).unwrap();
    let parsed2: serde_json::Value = serde_json::from_str(&output2).unwrap();
    let matches2 = parsed2["matches"].as_array().unwrap();
    assert!(
        matches2.is_empty(),
        "should not find page with wrong checksum"
    );
}

#[test]
fn test_find_space_id_filter() {
    let datadir = tempfile::tempdir().expect("create temp dir");
    let subdir = datadir.path().join("testdb");
    std::fs::create_dir(&subdir).expect("create subdir");

    let page0 = build_fsp_hdr_page(50, 1);
    let ibd_path = subdir.join("test.ibd");
    let mut f = std::fs::File::create(&ibd_path).expect("create ibd");
    f.write_all(&page0).unwrap();
    f.flush().unwrap();

    // Matching space_id
    let opts_match = idb::cli::find::FindOptions {
        datadir: datadir.path().to_string_lossy().to_string(),
        page: 0,
        checksum: None,
        space_id: Some(50),
        first: false,
        json: true,
        page_size: None,
        threads: 0,
        mmap: false,
    };

    let mut out1 = Vec::new();
    idb::cli::find::execute(&opts_match, &mut out1).unwrap();
    let parsed1: serde_json::Value =
        serde_json::from_str(&String::from_utf8(out1).unwrap()).unwrap();
    assert!(
        !parsed1["matches"].as_array().unwrap().is_empty(),
        "should find page with matching space_id"
    );

    // Non-matching space_id
    let opts_no_match = idb::cli::find::FindOptions {
        datadir: datadir.path().to_string_lossy().to_string(),
        page: 0,
        checksum: None,
        space_id: Some(999),
        first: false,
        json: true,
        page_size: None,
        threads: 0,
        mmap: false,
    };

    let mut out2 = Vec::new();
    idb::cli::find::execute(&opts_no_match, &mut out2).unwrap();
    let parsed2: serde_json::Value =
        serde_json::from_str(&String::from_utf8(out2).unwrap()).unwrap();
    assert!(
        parsed2["matches"].as_array().unwrap().is_empty(),
        "should not find page with wrong space_id"
    );
}

#[test]
fn test_find_first_flag() {
    let datadir = tempfile::tempdir().expect("create temp dir");

    // Create two .ibd files, each with page 0
    for (i, name) in ["db1", "db2"].iter().enumerate() {
        let subdir = datadir.path().join(name);
        std::fs::create_dir(&subdir).expect("create subdir");
        let page0 = build_fsp_hdr_page((i + 10) as u32, 1);
        let ibd_path = subdir.join("t.ibd");
        let mut f = std::fs::File::create(&ibd_path).expect("create ibd");
        f.write_all(&page0).unwrap();
        f.flush().unwrap();
    }

    // Without --first, should find 2 matches for page 0
    let opts_all = idb::cli::find::FindOptions {
        datadir: datadir.path().to_string_lossy().to_string(),
        page: 0,
        checksum: None,
        space_id: None,
        first: false,
        json: true,
        page_size: None,
        threads: 0,
        mmap: false,
    };

    let mut out_all = Vec::new();
    idb::cli::find::execute(&opts_all, &mut out_all).unwrap();
    let parsed_all: serde_json::Value =
        serde_json::from_str(&String::from_utf8(out_all).unwrap()).unwrap();
    let all_matches = parsed_all["matches"].as_array().unwrap().len();
    assert_eq!(all_matches, 2, "should find page 0 in both files");

    // With --first, should find only 1
    let opts_first = idb::cli::find::FindOptions {
        datadir: datadir.path().to_string_lossy().to_string(),
        page: 0,
        checksum: None,
        space_id: None,
        first: true,
        json: true,
        page_size: None,
        threads: 0,
        mmap: false,
    };

    let mut out_first = Vec::new();
    idb::cli::find::execute(&opts_first, &mut out_first).unwrap();
    let parsed_first: serde_json::Value =
        serde_json::from_str(&String::from_utf8(out_first).unwrap()).unwrap();
    let first_matches = parsed_first["matches"].as_array().unwrap().len();
    assert_eq!(first_matches, 1, "first flag should stop after 1 match");
}

// ========== Phase 3: tsid/sdi/log tests ==========

#[test]
fn test_tsid_nonexistent_directory() {
    let opts = idb::cli::tsid::TsidOptions {
        datadir: "/nonexistent/datadir".to_string(),
        list: true,
        tablespace_id: None,
        json: false,
        page_size: None,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::tsid::execute(&opts, &mut out);
    assert!(result.is_err(), "tsid should fail on nonexistent directory");
}

#[test]
fn test_tsid_json_lookup() {
    let datadir = tempfile::tempdir().expect("create temp dir");
    let subdir = datadir.path().join("testdb");
    std::fs::create_dir(&subdir).expect("create subdir");

    let page0 = build_fsp_hdr_page(88, 1);
    let ibd_path = subdir.join("t1.ibd");
    let mut f = std::fs::File::create(&ibd_path).expect("create ibd");
    f.write_all(&page0).unwrap();
    f.flush().unwrap();

    let opts = idb::cli::tsid::TsidOptions {
        datadir: datadir.path().to_string_lossy().to_string(),
        list: false,
        tablespace_id: Some(88),
        json: true,
        page_size: None,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::tsid::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    let parsed: serde_json::Value =
        serde_json::from_str(&output).expect("tsid --json lookup should produce valid JSON");
    assert!(parsed.get("tablespaces").is_some());
    let tablespaces = parsed["tablespaces"].as_array().unwrap();
    assert_eq!(tablespaces.len(), 1);
    assert_eq!(tablespaces[0]["space_id"], 88);
}

#[test]
fn test_sdi_nonexistent_file() {
    let opts = idb::cli::sdi::SdiOptions {
        file: "/nonexistent/test.ibd".to_string(),
        pretty: false,
        page_size: None,
        keyring: None,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::sdi::execute(&opts, &mut out);
    assert!(result.is_err(), "sdi should fail on nonexistent file");
}

#[test]
fn test_log_no_empty_filter() {
    // 3 data blocks: 2 with data, 1 empty (data_len == LOG_BLOCK_HDR_SIZE)
    let b1 = build_log_data_block(5, 100, 42);
    let b2 = build_log_data_block(6, LOG_BLOCK_HDR_SIZE as u16, 42); // empty
    let b3 = build_log_data_block(7, 200, 42);

    let tmp = write_redo_log(&[b1, b2, b3]);

    let opts = idb::cli::log::LogOptions {
        file: tmp.path().to_string_lossy().to_string(),
        blocks: None,
        no_empty: true,
        verbose: false,
        json: true,
    };

    let mut out = Vec::new();
    let result = idb::cli::log::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
    let blocks = parsed["blocks"].as_array().unwrap();
    assert_eq!(
        blocks.len(),
        2,
        "no_empty should filter out the empty block, leaving 2"
    );
    // Verify all remaining blocks have data_len > LOG_BLOCK_HDR_SIZE
    for block in blocks {
        assert!(
            block["data_len"].as_u64().unwrap() > LOG_BLOCK_HDR_SIZE as u64,
            "filtered blocks should all have data"
        );
    }
}

// ========== Phase 4: Error path tests ==========

#[test]
fn test_recover_nonexistent_file() {
    let opts = idb::cli::recover::RecoverOptions {
        file: "/nonexistent/test.ibd".to_string(),
        page: None,
        verbose: false,
        json: false,
        force: false,
        page_size: None,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: false,
        rebuild: None,
    };

    let mut out = Vec::new();
    let result = idb::cli::recover::execute(&opts, &mut out);
    assert!(result.is_err(), "recover should fail on nonexistent file");
}

#[test]
fn test_corrupt_nonexistent_file() {
    let opts = idb::cli::corrupt::CorruptOptions {
        file: "/nonexistent/test.ibd".to_string(),
        page: Some(0),
        bytes: 4,
        header: false,
        records: false,
        offset: None,
        verify: false,
        json: false,
        page_size: None,
        mmap: false,
    };

    let mut out = Vec::new();
    let result = idb::cli::corrupt::execute(&opts, &mut out);
    assert!(result.is_err(), "corrupt should fail on nonexistent file");
}

#[test]
fn test_info_missing_ibdata1() {
    // Valid directory but no ibdata1 file
    let datadir = tempfile::tempdir().expect("create temp dir");

    let opts = idb::cli::info::InfoOptions {
        ibdata: true,
        lsn_check: false,
        datadir: Some(datadir.path().to_string_lossy().to_string()),
        database: None,
        table: None,
        host: None,
        port: None,
        user: None,
        password: None,
        defaults_file: None,
        json: false,
        page_size: None,
    };

    let mut out = Vec::new();
    let result = idb::cli::info::execute(&opts, &mut out);
    assert!(result.is_err(), "info should fail when ibdata1 is missing");
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("ibdata1 not found"),
        "error should mention ibdata1 not found, got: {}",
        err_msg
    );
}

// ══════════════════════════════════════════════════════════════════════
// MySQL 9.x fixture tests
// ══════════════════════════════════════════════════════════════════════
//
// These tests use real .ibd files extracted from MySQL 9.0.1 and 9.1.0
// Docker containers. They validate that the parser correctly handles
// MySQL 9.x tablespace formats.

const MYSQL9_FIXTURE_DIR: &str = "tests/fixtures/mysql9";

// ── MySQL 9.0 standard tablespace ───────────────────────────────────

#[test]
fn test_mysql90_standard_opens() {
    let path = format!("{}/mysql90_standard.ibd", MYSQL9_FIXTURE_DIR);
    let ts = Tablespace::open(&path).expect("should open MySQL 9.0 standard tablespace");
    assert_eq!(ts.page_size(), 16384, "standard table should use 16K pages");
    assert_eq!(ts.page_count(), 7, "standard table should have 7 pages");
}

#[test]
fn test_mysql90_standard_checksums_valid() {
    let path = format!("{}/mysql90_standard.ibd", MYSQL9_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");
    let vendor = ts.vendor_info().clone();
    let page_size = ts.page_size();

    ts.for_each_page(|page_num, data| {
        // Skip all-zero (ALLOCATED) pages
        if data.iter().all(|&b| b == 0) {
            return Ok(());
        }
        let result = validate_checksum(data, page_size, Some(&vendor));
        assert!(
            result.valid,
            "page {} checksum should be valid (algo={:?})",
            page_num, result.algorithm
        );
        assert_eq!(result.algorithm, ChecksumAlgorithm::Crc32c);
        Ok(())
    })
    .expect("iterate pages");
}

#[test]
fn test_mysql90_standard_page_types() {
    let path = format!("{}/mysql90_standard.ibd", MYSQL9_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");

    let mut types = Vec::new();
    ts.for_each_page(|_page_num, data| {
        let hdr = FilHeader::parse(data).unwrap();
        types.push(hdr.page_type);
        Ok(())
    })
    .expect("iterate");

    assert!(
        types.contains(&PageType::FspHdr),
        "should contain FSP_HDR page"
    );
    assert!(
        types.contains(&PageType::Index),
        "should contain INDEX page"
    );
    assert!(types.contains(&PageType::Sdi), "should contain SDI page");
    assert!(
        types.contains(&PageType::Inode),
        "should contain INODE page"
    );
}

#[test]
fn test_mysql90_standard_sdi_extraction() {
    use idb::innodb::sdi::{extract_sdi_from_pages, find_sdi_pages};

    let path = format!("{}/mysql90_standard.ibd", MYSQL9_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");

    let sdi_pages = find_sdi_pages(&mut ts).expect("find SDI pages");
    assert!(!sdi_pages.is_empty(), "should find at least one SDI page");

    let records = extract_sdi_from_pages(&mut ts, &sdi_pages).expect("extract SDI");
    assert!(
        !records.is_empty(),
        "should extract at least one SDI record"
    );

    // Table SDI record (type 1)
    let table_rec = records.iter().find(|r| r.sdi_type == 1);
    assert!(table_rec.is_some(), "should have a Table SDI record");

    let data = &table_rec.unwrap().data;
    assert!(
        data.contains("\"name\": \"standard\"") || data.contains("\"name\":\"standard\""),
        "SDI data should contain table name 'standard'"
    );
}

#[test]
fn test_mysql90_standard_vendor_detection() {
    use idb::innodb::vendor::InnoDbVendor;

    let path = format!("{}/mysql90_standard.ibd", MYSQL9_FIXTURE_DIR);
    let ts = Tablespace::open(&path).expect("open");
    assert_eq!(
        ts.vendor_info().vendor,
        InnoDbVendor::MySQL,
        "should detect MySQL vendor"
    );
}

#[test]
fn test_mysql90_standard_fsp_header() {
    let path = format!("{}/mysql90_standard.ibd", MYSQL9_FIXTURE_DIR);
    let ts = Tablespace::open(&path).expect("open");
    let fsp = ts.fsp_header().expect("FSP header present");
    assert!(fsp.space_id > 0, "space_id should be nonzero");
    assert_eq!(fsp.size, 7, "FSP size should match page count");
}

// ── MySQL 9.1 standard tablespace ───────────────────────────────────

#[test]
fn test_mysql91_standard_opens() {
    let path = format!("{}/mysql91_standard.ibd", MYSQL9_FIXTURE_DIR);
    let ts = Tablespace::open(&path).expect("should open MySQL 9.1 standard tablespace");
    assert_eq!(ts.page_size(), 16384);
    assert_eq!(ts.page_count(), 7);
}

#[test]
fn test_mysql91_standard_checksums_valid() {
    let path = format!("{}/mysql91_standard.ibd", MYSQL9_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");
    let vendor = ts.vendor_info().clone();
    let page_size = ts.page_size();

    ts.for_each_page(|page_num, data| {
        if data.iter().all(|&b| b == 0) {
            return Ok(());
        }
        let result = validate_checksum(data, page_size, Some(&vendor));
        assert!(result.valid, "page {} checksum should be valid", page_num);
        Ok(())
    })
    .expect("iterate pages");
}

#[test]
fn test_mysql91_standard_sdi_extraction() {
    use idb::innodb::sdi::{extract_sdi_from_pages, find_sdi_pages};

    let path = format!("{}/mysql91_standard.ibd", MYSQL9_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");

    let sdi_pages = find_sdi_pages(&mut ts).expect("find SDI pages");
    assert!(!sdi_pages.is_empty(), "should find at least one SDI page");

    let records = extract_sdi_from_pages(&mut ts, &sdi_pages).expect("extract SDI");
    assert!(
        !records.is_empty(),
        "should extract at least one SDI record"
    );

    let table_rec = records.iter().find(|r| r.sdi_type == 1);
    assert!(table_rec.is_some(), "should have a Table SDI record");

    let data = &table_rec.unwrap().data;
    assert!(
        data.contains("\"name\": \"standard\"") || data.contains("\"name\":\"standard\""),
        "SDI data should contain table name 'standard'"
    );
}

#[test]
fn test_mysql91_standard_page_types() {
    let path = format!("{}/mysql91_standard.ibd", MYSQL9_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");

    let mut has_fsp_hdr = false;
    let mut has_index = false;
    let mut has_sdi = false;
    ts.for_each_page(|_page_num, data| {
        let hdr = FilHeader::parse(data).unwrap();
        match hdr.page_type {
            PageType::FspHdr => has_fsp_hdr = true,
            PageType::Index => has_index = true,
            PageType::Sdi => has_sdi = true,
            _ => {}
        }
        Ok(())
    })
    .expect("iterate");

    assert!(has_fsp_hdr, "should contain FSP_HDR page");
    assert!(has_index, "should contain INDEX page");
    assert!(has_sdi, "should contain SDI page");
}

// ── MySQL 9.0 compressed tablespace ─────────────────────────────────

#[test]
fn test_mysql90_compressed_opens() {
    let path = format!("{}/mysql90_compressed.ibd", MYSQL9_FIXTURE_DIR);
    let ts = Tablespace::open(&path).expect("should open MySQL 9.0 compressed tablespace");
    // The file is 56K. The parser reads at the detected page size.
    // Compressed tablespace with KEY_BLOCK_SIZE=8 has 8K physical pages,
    // but page 0 (FSP_HDR) is always stored at the logical page size.
    assert_eq!(
        ts.page_size(),
        16384,
        "logical page size should be 16K even for compressed tablespaces"
    );
    assert!(
        ts.page_count() > 1,
        "should have FSP_HDR plus at least one data page"
    );
}

#[test]
fn test_mysql90_compressed_fsp_header() {
    let path = format!("{}/mysql90_compressed.ibd", MYSQL9_FIXTURE_DIR);
    let ts = Tablespace::open(&path).expect("open");
    let fsp = ts.fsp_header().expect("FSP header present");
    assert!(fsp.space_id > 0, "space_id should be nonzero");
    // FSP flags should indicate compressed format
    assert!(
        fsp.flags > 0,
        "flags should be nonzero for compressed table"
    );
}

// ── MySQL 9.1 compressed tablespace ─────────────────────────────────

#[test]
fn test_mysql91_compressed_opens() {
    let path = format!("{}/mysql91_compressed.ibd", MYSQL9_FIXTURE_DIR);
    let ts = Tablespace::open(&path).expect("should open MySQL 9.1 compressed tablespace");
    assert_eq!(
        ts.page_size(),
        16384,
        "logical page size should be 16K even for compressed tablespaces"
    );
    assert!(
        ts.page_count() > 1,
        "should have FSP_HDR plus at least one data page"
    );
}

#[test]
fn test_mysql91_compressed_fsp_header() {
    let path = format!("{}/mysql91_compressed.ibd", MYSQL9_FIXTURE_DIR);
    let ts = Tablespace::open(&path).expect("open");
    let fsp = ts.fsp_header().expect("FSP header present");
    assert!(fsp.space_id > 0, "space_id should be nonzero");
    assert!(
        fsp.flags > 0,
        "flags should be nonzero for compressed table"
    );
}

// ── MySQL 9.0 multipage tablespace ──────────────────────────────────

#[test]
fn test_mysql90_multipage_opens() {
    let path = format!("{}/mysql90_multipage.ibd", MYSQL9_FIXTURE_DIR);
    let ts = Tablespace::open(&path).expect("should open MySQL 9.0 multipage tablespace");
    assert_eq!(ts.page_size(), 16384);
    assert_eq!(
        ts.page_count(),
        640,
        "multipage table should have 640 pages"
    );
}

#[test]
fn test_mysql90_multipage_checksums_valid() {
    let path = format!("{}/mysql90_multipage.ibd", MYSQL9_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");
    let vendor = ts.vendor_info().clone();
    let page_size = ts.page_size();

    let mut valid_count = 0u32;
    let mut empty_count = 0u32;
    ts.for_each_page(|page_num, data| {
        if data.iter().all(|&b| b == 0) {
            empty_count += 1;
            return Ok(());
        }
        let result = validate_checksum(data, page_size, Some(&vendor));
        assert!(result.valid, "page {} checksum should be valid", page_num);
        valid_count += 1;
        Ok(())
    })
    .expect("iterate pages");

    assert!(
        valid_count > 100,
        "should have many valid pages, got {}",
        valid_count
    );
    assert!(empty_count > 0, "should have some empty (ALLOCATED) pages");
}

#[test]
fn test_mysql90_multipage_has_many_index_pages() {
    let path = format!("{}/mysql90_multipage.ibd", MYSQL9_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");

    let mut index_count = 0u32;
    ts.for_each_page(|_page_num, data| {
        let hdr = FilHeader::parse(data).unwrap();
        if hdr.page_type == PageType::Index {
            index_count += 1;
        }
        Ok(())
    })
    .expect("iterate");

    assert!(
        index_count >= 100,
        "multipage table should have at least 100 INDEX pages, got {}",
        index_count
    );
}

#[test]
fn test_mysql90_multipage_sdi_extraction() {
    use idb::innodb::sdi::{extract_sdi_from_pages, find_sdi_pages};

    let path = format!("{}/mysql90_multipage.ibd", MYSQL9_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");

    let sdi_pages = find_sdi_pages(&mut ts).expect("find SDI pages");
    assert!(!sdi_pages.is_empty(), "should find SDI pages");

    let records = extract_sdi_from_pages(&mut ts, &sdi_pages).expect("extract SDI");
    let table_rec = records.iter().find(|r| r.sdi_type == 1);
    assert!(table_rec.is_some(), "should have Table SDI record");

    let data = &table_rec.unwrap().data;
    assert!(
        data.contains("\"name\": \"multipage\"") || data.contains("\"name\":\"multipage\""),
        "SDI should contain table name 'multipage'"
    );
}

// ── MySQL 9.1 multipage tablespace ──────────────────────────────────

#[test]
fn test_mysql91_multipage_opens() {
    let path = format!("{}/mysql91_multipage.ibd", MYSQL9_FIXTURE_DIR);
    let ts = Tablespace::open(&path).expect("should open MySQL 9.1 multipage tablespace");
    assert_eq!(ts.page_size(), 16384);
    assert_eq!(ts.page_count(), 640);
}

#[test]
fn test_mysql91_multipage_checksums_valid() {
    let path = format!("{}/mysql91_multipage.ibd", MYSQL9_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");
    let vendor = ts.vendor_info().clone();
    let page_size = ts.page_size();

    let mut valid_count = 0u32;
    ts.for_each_page(|page_num, data| {
        if data.iter().all(|&b| b == 0) {
            return Ok(());
        }
        let result = validate_checksum(data, page_size, Some(&vendor));
        assert!(result.valid, "page {} checksum should be valid", page_num);
        valid_count += 1;
        Ok(())
    })
    .expect("iterate pages");

    assert!(
        valid_count > 100,
        "should have many valid pages, got {}",
        valid_count
    );
}

#[test]
fn test_mysql91_multipage_has_many_index_pages() {
    let path = format!("{}/mysql91_multipage.ibd", MYSQL9_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");

    let mut index_count = 0u32;
    ts.for_each_page(|_page_num, data| {
        let hdr = FilHeader::parse(data).unwrap();
        if hdr.page_type == PageType::Index {
            index_count += 1;
        }
        Ok(())
    })
    .expect("iterate");

    assert!(
        index_count >= 100,
        "multipage table should have at least 100 INDEX pages, got {}",
        index_count
    );
}

#[test]
fn test_mysql91_multipage_sdi_extraction() {
    use idb::innodb::sdi::{extract_sdi_from_pages, find_sdi_pages};

    let path = format!("{}/mysql91_multipage.ibd", MYSQL9_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");

    let sdi_pages = find_sdi_pages(&mut ts).expect("find SDI pages");
    assert!(!sdi_pages.is_empty(), "should find SDI pages");

    let records = extract_sdi_from_pages(&mut ts, &sdi_pages).expect("extract SDI");
    let table_rec = records.iter().find(|r| r.sdi_type == 1);
    assert!(table_rec.is_some(), "should have Table SDI record");

    let data = &table_rec.unwrap().data;
    assert!(
        data.contains("\"name\": \"multipage\"") || data.contains("\"name\":\"multipage\""),
        "SDI should contain table name 'multipage'"
    );
}

// ── MySQL 9.x redo log tests ────────────────────────────────────────

#[test]
fn test_mysql90_redo_log_opens() {
    use idb::innodb::log::LogFile;
    use idb::innodb::vendor::{detect_vendor_from_created_by, InnoDbVendor};

    let path = format!("{}/mysql90_redo_9", MYSQL9_FIXTURE_DIR);
    let mut log = LogFile::open(&path).expect("should open MySQL 9.0 redo log");
    let header = log.read_header().expect("should read header");

    assert!(
        header.created_by.contains("9.0"),
        "created_by should mention 9.0, got: {}",
        header.created_by
    );
    assert_eq!(
        detect_vendor_from_created_by(&header.created_by),
        InnoDbVendor::MySQL,
        "should detect MySQL vendor from redo log"
    );
}

#[test]
fn test_mysql91_redo_log_opens() {
    use idb::innodb::log::LogFile;
    use idb::innodb::vendor::{detect_vendor_from_created_by, InnoDbVendor};

    let path = format!("{}/mysql91_redo_9", MYSQL9_FIXTURE_DIR);
    let mut log = LogFile::open(&path).expect("should open MySQL 9.1 redo log");
    let header = log.read_header().expect("should read header");

    assert!(
        header.created_by.contains("9.1"),
        "created_by should mention 9.1, got: {}",
        header.created_by
    );
    assert_eq!(
        detect_vendor_from_created_by(&header.created_by),
        InnoDbVendor::MySQL,
    );
}

// ── Cross-version comparison tests ──────────────────────────────────

#[test]
fn test_mysql9_standard_cross_version_page_structure() {
    // Both MySQL 9.0 and 9.1 standard tables should have the same page layout
    let path90 = format!("{}/mysql90_standard.ibd", MYSQL9_FIXTURE_DIR);
    let path91 = format!("{}/mysql91_standard.ibd", MYSQL9_FIXTURE_DIR);

    let mut ts90 = Tablespace::open(&path90).expect("open 9.0");
    let mut ts91 = Tablespace::open(&path91).expect("open 9.1");

    assert_eq!(
        ts90.page_size(),
        ts91.page_size(),
        "page sizes should match"
    );
    assert_eq!(
        ts90.page_count(),
        ts91.page_count(),
        "page counts should match"
    );

    let mut types90 = Vec::new();
    let mut types91 = Vec::new();

    ts90.for_each_page(|_n, data| {
        types90.push(FilHeader::parse(data).unwrap().page_type);
        Ok(())
    })
    .expect("iterate 9.0");

    ts91.for_each_page(|_n, data| {
        types91.push(FilHeader::parse(data).unwrap().page_type);
        Ok(())
    })
    .expect("iterate 9.1");

    assert_eq!(
        types90, types91,
        "page type sequences should be identical across 9.0 and 9.1"
    );
}

#[test]
fn test_mysql9_multipage_cross_version_structure() {
    let path90 = format!("{}/mysql90_multipage.ibd", MYSQL9_FIXTURE_DIR);
    let path91 = format!("{}/mysql91_multipage.ibd", MYSQL9_FIXTURE_DIR);

    let ts90 = Tablespace::open(&path90).expect("open 9.0");
    let ts91 = Tablespace::open(&path91).expect("open 9.1");

    assert_eq!(ts90.page_size(), ts91.page_size());
    assert_eq!(ts90.page_count(), ts91.page_count());
}

// ── LSN validation tests ────────────────────────────────────────────

#[test]
fn test_mysql90_standard_lsn_valid() {
    let path = format!("{}/mysql90_standard.ibd", MYSQL9_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");
    let page_size = ts.page_size();

    ts.for_each_page(|page_num, data| {
        if data.iter().all(|&b| b == 0) {
            return Ok(());
        }
        assert!(
            validate_lsn(data, page_size),
            "page {} LSN should be consistent",
            page_num
        );
        Ok(())
    })
    .expect("iterate");
}

#[test]
fn test_mysql91_standard_lsn_valid() {
    let path = format!("{}/mysql91_standard.ibd", MYSQL9_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");
    let page_size = ts.page_size();

    ts.for_each_page(|page_num, data| {
        if data.iter().all(|&b| b == 0) {
            return Ok(());
        }
        assert!(
            validate_lsn(data, page_size),
            "page {} LSN should be consistent",
            page_num
        );
        Ok(())
    })
    .expect("iterate");
}

// ── MySQL 9.x redo log file 10 tests ────────────────────────────────

#[test]
fn test_mysql90_redo_log_10_opens() {
    use idb::innodb::log::LogFile;
    use idb::innodb::vendor::{detect_vendor_from_created_by, InnoDbVendor};

    let path = format!("{}/mysql90_redo_10", MYSQL9_FIXTURE_DIR);
    let mut log = LogFile::open(&path).expect("should open MySQL 9.0 redo log file 10");
    let header = log.read_header().expect("should read header");

    assert!(
        header.created_by.contains("9.0"),
        "created_by should mention 9.0, got: {}",
        header.created_by
    );
    assert_eq!(
        detect_vendor_from_created_by(&header.created_by),
        InnoDbVendor::MySQL,
        "should detect MySQL vendor from redo log"
    );
}

#[test]
fn test_mysql91_redo_log_10_opens() {
    use idb::innodb::log::LogFile;
    use idb::innodb::vendor::{detect_vendor_from_created_by, InnoDbVendor};

    let path = format!("{}/mysql91_redo_10", MYSQL9_FIXTURE_DIR);
    let mut log = LogFile::open(&path).expect("should open MySQL 9.1 redo log file 10");
    let header = log.read_header().expect("should read header");

    assert!(
        header.created_by.contains("9.1"),
        "created_by should mention 9.1, got: {}",
        header.created_by
    );
    assert_eq!(
        detect_vendor_from_created_by(&header.created_by),
        InnoDbVendor::MySQL,
    );
}

// ── MySQL 9.x redo log comprehensive tests ──────────────────────────

#[test]
fn test_mysql90_redo_log_format_version() {
    use idb::innodb::log::LogFile;

    let path = format!("{}/mysql90_redo_9", MYSQL9_FIXTURE_DIR);
    let mut log = LogFile::open(&path).expect("should open MySQL 9.0 redo log");
    let header = log.read_header().expect("should read header");

    // MySQL 9.x uses redo log format version 6 (introduced in MySQL 8.0.30)
    assert_eq!(
        header.format_version, 6,
        "MySQL 9.0 should use format version 6, got {}",
        header.format_version
    );
    // Log UUID should be non-zero for format version 6
    assert_ne!(
        header.log_uuid, 0,
        "log_uuid should be non-zero for format version 6"
    );
    // Start LSN should be reasonable (non-zero)
    assert!(
        header.start_lsn > 0,
        "start_lsn should be non-zero, got {}",
        header.start_lsn
    );
}

#[test]
fn test_mysql91_redo_log_format_version() {
    use idb::innodb::log::LogFile;

    let path = format!("{}/mysql91_redo_9", MYSQL9_FIXTURE_DIR);
    let mut log = LogFile::open(&path).expect("should open MySQL 9.1 redo log");
    let header = log.read_header().expect("should read header");

    assert_eq!(
        header.format_version, 6,
        "MySQL 9.1 should use format version 6, got {}",
        header.format_version
    );
    assert_ne!(
        header.log_uuid, 0,
        "log_uuid should be non-zero for format version 6"
    );
    assert!(
        header.start_lsn > 0,
        "start_lsn should be non-zero, got {}",
        header.start_lsn
    );
}

#[test]
fn test_mysql9_redo_log_checkpoints() {
    use idb::innodb::log::LogFile;

    // Test both MySQL 9.0 and 9.1
    for (version, fixture) in &[("9.0", "mysql90_redo_9"), ("9.1", "mysql91_redo_9")] {
        let path = format!("{}/{}", MYSQL9_FIXTURE_DIR, fixture);
        let mut log = LogFile::open(&path).unwrap_or_else(|e| {
            panic!("should open {} redo log: {}", version, e);
        });

        // Read checkpoint 1 (block 1)
        let cp1 = log
            .read_checkpoint(0)
            .unwrap_or_else(|e| panic!("{} checkpoint 1 should be readable: {}", version, e));
        assert!(
            cp1.lsn > 0,
            "{} checkpoint 1 LSN should be non-zero, got {}",
            version,
            cp1.lsn
        );

        // In MySQL 8.0.30+ (format version 6), the checkpoint block only stores
        // the LSN at offset 8. The number, offset, buf_size, archived_lsn fields
        // are not written. However, we just parse whatever is in those bytes.
        // The key assertion is that LSN is valid.

        // Read checkpoint 2 (block 3)
        let cp2 = log
            .read_checkpoint(1)
            .unwrap_or_else(|e| panic!("{} checkpoint 2 should be readable: {}", version, e));
        assert!(
            cp2.lsn > 0,
            "{} checkpoint 2 LSN should be non-zero, got {}",
            version,
            cp2.lsn
        );
    }
}

#[test]
fn test_mysql9_redo_log_block_checksums() {
    use idb::innodb::log::{
        validate_log_block_checksum, LogBlockHeader, LogFile, LOG_FILE_HDR_BLOCKS,
    };

    // Verify that block checksums validate correctly on MySQL 9.x redo logs
    for (version, fixture) in &[("9.0", "mysql90_redo_9"), ("9.1", "mysql91_redo_9")] {
        let path = format!("{}/{}", MYSQL9_FIXTURE_DIR, fixture);
        let mut log = LogFile::open(&path).unwrap_or_else(|e| {
            panic!("should open {} redo log: {}", version, e);
        });

        let data_blocks = log.data_block_count();
        // Check at least the first 50 data blocks (or all if fewer)
        let limit = data_blocks.min(50);
        let mut valid_count = 0;
        let mut data_count = 0;

        for i in 0..limit {
            let block_idx = LOG_FILE_HDR_BLOCKS + i;
            let block_data = log
                .read_block(block_idx)
                .unwrap_or_else(|e| panic!("{} block {} read failed: {}", version, block_idx, e));

            let hdr = LogBlockHeader::parse(&block_data);
            if let Some(hdr) = hdr {
                if hdr.has_data() {
                    data_count += 1;
                    if validate_log_block_checksum(&block_data) {
                        valid_count += 1;
                    }
                }
            }
        }

        // All non-empty data blocks should have valid checksums
        assert_eq!(
            valid_count, data_count,
            "{} redo log: {}/{} blocks had valid checksums",
            version, valid_count, data_count
        );
    }
}

#[test]
fn test_mysql9_redo_log_data_blocks_parseable() {
    use idb::innodb::log::{LogBlockHeader, LogFile, LOG_BLOCK_HDR_SIZE, LOG_FILE_HDR_BLOCKS};

    // Verify that all data blocks can be parsed without errors
    for (version, fixture) in &[("9.0", "mysql90_redo_9"), ("9.1", "mysql91_redo_9")] {
        let path = format!("{}/{}", MYSQL9_FIXTURE_DIR, fixture);
        let mut log = LogFile::open(&path).unwrap_or_else(|e| {
            panic!("should open {} redo log: {}", version, e);
        });

        let data_blocks = log.data_block_count();
        assert!(
            data_blocks > 0,
            "{} redo log should have data blocks",
            version
        );

        let limit = data_blocks.min(100);
        let mut has_data_count = 0;

        for i in 0..limit {
            let block_idx = LOG_FILE_HDR_BLOCKS + i;
            let block_data = log
                .read_block(block_idx)
                .unwrap_or_else(|e| panic!("{} block {} read failed: {}", version, block_idx, e));

            let hdr = LogBlockHeader::parse(&block_data)
                .unwrap_or_else(|| panic!("{} block {} header parse failed", version, block_idx));

            // data_len should be within valid range
            assert!(
                (hdr.data_len as usize) <= 512,
                "{} block {} data_len {} exceeds block size",
                version,
                block_idx,
                hdr.data_len
            );

            if hdr.has_data() {
                has_data_count += 1;
                // data_len should be at least the header size
                assert!(
                    hdr.data_len as usize >= LOG_BLOCK_HDR_SIZE,
                    "{} block {} data_len {} < header size {}",
                    version,
                    block_idx,
                    hdr.data_len,
                    LOG_BLOCK_HDR_SIZE
                );
            }
        }

        // There should be at least some non-empty data blocks in an active redo log
        assert!(
            has_data_count > 0,
            "{} redo log should have non-empty data blocks",
            version
        );
    }
}

#[test]
fn test_mysql9_redo_log_json_output() {
    use idb::innodb::log::LOG_BLOCK_HDR_SIZE;

    // Test the `inno log --json` output against MySQL 9.x fixtures
    for (version, fixture) in &[("9.0", "mysql90_redo_9"), ("9.1", "mysql91_redo_9")] {
        let path = format!("{}/{}", MYSQL9_FIXTURE_DIR, fixture);
        let opts = idb::cli::log::LogOptions {
            file: path.clone(),
            blocks: Some(20),
            no_empty: false,
            verbose: true,
            json: true,
        };
        let mut out = Vec::new();
        idb::cli::log::execute(&opts, &mut out)
            .unwrap_or_else(|e| panic!("{} log JSON failed: {}", version, e));

        let output = String::from_utf8(out).expect("valid UTF-8");
        let json: serde_json::Value = serde_json::from_str(&output).expect("should parse as JSON");

        // Validate top-level fields
        assert!(
            json.get("file_size").is_some(),
            "{} missing file_size",
            version
        );
        assert!(
            json.get("total_blocks").is_some(),
            "{} missing total_blocks",
            version
        );
        assert!(
            json.get("data_blocks").is_some(),
            "{} missing data_blocks",
            version
        );

        // Validate header fields
        let header = json.get("header").expect("missing header");
        assert_eq!(
            header["format_version"].as_u64().unwrap(),
            6,
            "{} format_version should be 6",
            version
        );
        assert!(
            header["start_lsn"].as_u64().unwrap() > 0,
            "{} start_lsn should be > 0",
            version
        );
        let created = header["created_by"].as_str().unwrap();
        assert!(
            created.contains(version),
            "{} created_by should contain version string, got: {}",
            version,
            created
        );

        // Validate checkpoint fields
        let cp1 = json.get("checkpoint_1").expect("missing checkpoint_1");
        assert!(
            cp1["lsn"].as_u64().unwrap() > 0,
            "{} checkpoint_1 LSN should be > 0",
            version
        );

        // Validate blocks array
        let blocks = json["blocks"].as_array().expect("blocks should be array");
        assert!(
            !blocks.is_empty(),
            "{} blocks array should not be empty",
            version
        );

        for block in blocks {
            // Each block should have all required fields
            assert!(block.get("block_index").is_some());
            assert!(block.get("block_no").is_some());
            assert!(block.get("flush_flag").is_some());
            assert!(block.get("data_len").is_some());
            assert!(block.get("first_rec_group").is_some());
            assert!(block.get("epoch_no").is_some());
            assert!(block.get("checksum_valid").is_some());
            assert!(block.get("record_types").is_some());

            let data_len = block["data_len"].as_u64().unwrap();
            let has_data = data_len > LOG_BLOCK_HDR_SIZE as u64;
            if has_data {
                // Non-empty blocks should have valid checksums
                assert!(
                    block["checksum_valid"].as_bool().unwrap(),
                    "{} block {} should have valid checksum",
                    version,
                    block["block_index"]
                );
            }
        }
    }
}

#[test]
fn test_mysql9_redo_log_cross_version_consistency() {
    use idb::innodb::log::LogFile;

    // MySQL 9.0 and 9.1 redo logs should have the same format version
    let path90 = format!("{}/mysql90_redo_9", MYSQL9_FIXTURE_DIR);
    let path91 = format!("{}/mysql91_redo_9", MYSQL9_FIXTURE_DIR);

    let mut log90 = LogFile::open(&path90).expect("open 9.0 redo log");
    let mut log91 = LogFile::open(&path91).expect("open 9.1 redo log");

    let hdr90 = log90.read_header().expect("read 9.0 header");
    let hdr91 = log91.read_header().expect("read 9.1 header");

    // Both should use format version 6
    assert_eq!(
        hdr90.format_version, hdr91.format_version,
        "MySQL 9.0 and 9.1 should use the same format version"
    );
    assert_eq!(hdr90.format_version, 6);

    // Both should have non-zero log UUIDs (though they'll differ)
    assert_ne!(hdr90.log_uuid, 0, "9.0 log_uuid should be non-zero");
    assert_ne!(hdr91.log_uuid, 0, "9.1 log_uuid should be non-zero");

    // Block counts should be equal (same file size)
    assert_eq!(
        log90.block_count(),
        log91.block_count(),
        "9.0 and 9.1 redo logs should have the same block count"
    );
}

#[test]
fn test_mysql9_redo_log_file_10_format_version() {
    use idb::innodb::log::LogFile;

    // Verify format version for the second redo log file as well
    for (version, fixture) in &[("9.0", "mysql90_redo_10"), ("9.1", "mysql91_redo_10")] {
        let path = format!("{}/{}", MYSQL9_FIXTURE_DIR, fixture);
        let mut log = LogFile::open(&path).unwrap_or_else(|e| {
            panic!("should open {} redo log 10: {}", version, e);
        });
        let header = log.read_header().expect("should read header");

        assert_eq!(
            header.format_version, 6,
            "{} redo log 10 should use format version 6",
            version
        );
        assert_ne!(
            header.log_uuid, 0,
            "{} redo log 10 log_uuid should be non-zero",
            version
        );
    }
}

#[test]
fn test_mysql9_redo_log_backward_compat_accessors() {
    use idb::innodb::log::{LogBlockHeader, LogFile, LOG_FILE_HDR_BLOCKS};

    // Test that backward-compatibility accessors work
    let path = format!("{}/mysql90_redo_9", MYSQL9_FIXTURE_DIR);
    let mut log = LogFile::open(&path).expect("open redo log");

    // Test LogFileHeader::group_id() == format_version
    let header = log.read_header().expect("read header");
    assert_eq!(
        header.group_id(),
        header.format_version,
        "group_id() should equal format_version"
    );

    // Test LogBlockHeader::checkpoint_no() == epoch_no
    let block_data = log
        .read_block(LOG_FILE_HDR_BLOCKS)
        .expect("read first data block");
    if let Some(hdr) = LogBlockHeader::parse(&block_data) {
        assert_eq!(
            hdr.checkpoint_no(),
            hdr.epoch_no,
            "checkpoint_no() should equal epoch_no"
        );
    }
}

// ── MySQL 9.x recovery assessment tests ─────────────────────────────

/// Run `inno recover --json` on a MySQL 9.x fixture and return the parsed JSON.
fn run_mysql9_recovery_json(fixture_path: &str) -> serde_json::Value {
    let path = format!("{}/{}", MYSQL9_FIXTURE_DIR, fixture_path);
    let opts = idb::cli::recover::RecoverOptions {
        file: path.clone(),
        page: None,
        verbose: false,
        json: true,
        force: false,
        page_size: None,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: false,
        rebuild: None,
    };
    let mut out = Vec::new();
    idb::cli::recover::execute(&opts, &mut out)
        .unwrap_or_else(|e| panic!("recover failed on {}: {}", path, e));
    let output = String::from_utf8(out).expect("valid UTF-8 output");
    serde_json::from_str(&output).expect("valid JSON output")
}

#[test]
fn test_mysql90_standard_recovery_assessment() {
    let parsed = run_mysql9_recovery_json("mysql90_standard.ibd");
    assert!(parsed.get("summary").is_some());
    assert!(parsed.get("total_pages").is_some());
    assert!(parsed.get("recoverable_records").is_some());
    assert_eq!(
        parsed["total_pages"], 7,
        "standard fixture should have 7 pages"
    );
    assert!(
        parsed["summary"].get("intact").is_some(),
        "summary should contain intact count"
    );
}

#[test]
fn test_mysql91_standard_recovery_assessment() {
    let parsed = run_mysql9_recovery_json("mysql91_standard.ibd");
    assert!(parsed.get("summary").is_some());
    assert!(parsed.get("total_pages").is_some());
    assert!(parsed.get("recoverable_records").is_some());
    assert_eq!(
        parsed["total_pages"], 7,
        "standard fixture should have 7 pages"
    );
    assert!(
        parsed["summary"].get("intact").is_some(),
        "summary should contain intact count"
    );
}

#[test]
fn test_mysql90_multipage_recovery_assessment() {
    let parsed = run_mysql9_recovery_json("mysql90_multipage.ibd");
    assert!(
        parsed["total_pages"].as_u64().unwrap() > 7,
        "multipage fixture should have many pages"
    );
    assert!(parsed.get("recoverable_records").is_some());
}

#[test]
fn test_mysql91_multipage_recovery_assessment() {
    let parsed = run_mysql9_recovery_json("mysql91_multipage.ibd");
    assert!(
        parsed["total_pages"].as_u64().unwrap() > 7,
        "multipage fixture should have many pages"
    );
    assert!(parsed.get("recoverable_records").is_some());
}

// ========== MySQL 9.x SDI JSON schema validation ==========
//
// These tests verify the SDI JSON structure from MySQL 9.x tablespaces in
// detail — validating that the SDI page format, zlib decompression, and JSON
// schema are fully compatible with MySQL 9.0 and 9.1.
//
// SDI format verified against MySQL 9.0.1 and 9.1.0:
//   - sdi_version remains 80019 (unchanged from MySQL 8.0)
//   - dd_version bumped to 90000
//   - Binary record layout (type/id/trx_id/roll_ptr/lengths/data) unchanged
//   - JSON schema fields identical to MySQL 8.0

/// Helper: extract SDI Table record JSON from a fixture file.
fn extract_sdi_table_json(fixture_name: &str) -> serde_json::Value {
    use idb::innodb::sdi::{extract_sdi_from_pages, find_sdi_pages};

    let path = format!("{}/{}", MYSQL9_FIXTURE_DIR, fixture_name);
    let mut ts = Tablespace::open(&path).expect("open tablespace");

    let sdi_pages = find_sdi_pages(&mut ts).expect("find SDI pages");
    assert!(!sdi_pages.is_empty(), "should find SDI pages");

    let records = extract_sdi_from_pages(&mut ts, &sdi_pages).expect("extract SDI");
    let table_rec = records
        .iter()
        .find(|r| r.sdi_type == 1)
        .expect("should have Table SDI record (type=1)");

    assert!(!table_rec.data.is_empty(), "SDI data should not be empty");

    serde_json::from_str(&table_rec.data).expect("SDI data should be valid JSON")
}

/// Helper: extract SDI Tablespace record JSON from a fixture file.
fn extract_sdi_tablespace_json(fixture_name: &str) -> serde_json::Value {
    use idb::innodb::sdi::{extract_sdi_from_pages, find_sdi_pages};

    let path = format!("{}/{}", MYSQL9_FIXTURE_DIR, fixture_name);
    let mut ts = Tablespace::open(&path).expect("open tablespace");

    let sdi_pages = find_sdi_pages(&mut ts).expect("find SDI pages");
    let records = extract_sdi_from_pages(&mut ts, &sdi_pages).expect("extract SDI");
    let ts_rec = records
        .iter()
        .find(|r| r.sdi_type == 2)
        .expect("should have Tablespace SDI record (type=2)");

    assert!(!ts_rec.data.is_empty(), "SDI data should not be empty");

    serde_json::from_str(&ts_rec.data).expect("SDI data should be valid JSON")
}

/// Validate top-level SDI envelope fields common to all MySQL 9.x records.
fn assert_sdi_envelope(json: &serde_json::Value, expected_type: &str) {
    assert_eq!(
        json["dd_object_type"].as_str().unwrap(),
        expected_type,
        "dd_object_type mismatch"
    );

    let sdi_version = json["sdi_version"].as_u64().unwrap();
    assert_eq!(sdi_version, 80019, "sdi_version should be 80019");

    let dd_version = json["dd_version"].as_u64().unwrap();
    assert!(
        dd_version >= 90000,
        "dd_version should be >= 90000 for MySQL 9.x, got {}",
        dd_version
    );

    let mysqld_version = json["mysqld_version_id"].as_u64().unwrap();
    assert!(
        mysqld_version >= 90000,
        "mysqld_version_id should be >= 90000, got {}",
        mysqld_version
    );

    assert!(json.get("dd_object").is_some(), "dd_object must exist");
}

/// Validate Table dd_object fields for MySQL 9.x.
fn assert_table_dd_object(dd: &serde_json::Value, expected_name: &str) {
    // Required string/number fields
    assert_eq!(
        dd["name"].as_str().unwrap(),
        expected_name,
        "table name mismatch"
    );
    assert_eq!(dd["engine"].as_str().unwrap(), "InnoDB");
    assert!(dd["collation_id"].as_u64().is_some(), "collation_id");
    assert!(dd["created"].as_u64().is_some(), "created timestamp");
    assert!(dd["last_altered"].as_u64().is_some(), "last_altered");
    assert!(dd["se_private_id"].as_u64().is_some(), "se_private_id");
    assert!(dd["row_format"].as_u64().is_some(), "row_format");
    assert!(dd["hidden"].as_u64().is_some(), "hidden");
    assert!(dd["schema_ref"].as_str().is_some(), "schema_ref");

    let mysql_ver = dd["mysql_version_id"].as_u64().unwrap();
    assert!(
        mysql_ver >= 90000,
        "mysql_version_id should be >= 90000, got {}",
        mysql_ver
    );

    // Required arrays
    assert!(dd["columns"].is_array(), "columns must be an array");
    assert!(dd["indexes"].is_array(), "indexes must be an array");
    assert!(
        dd["columns"].as_array().unwrap().len() >= 3,
        "should have at least 3 columns (id, name, data or similar)"
    );
    assert!(
        dd["indexes"].as_array().unwrap().len() >= 1,
        "should have at least 1 index"
    );
    assert!(dd["foreign_keys"].is_array(), "foreign_keys must be array");
    assert!(
        dd["check_constraints"].is_array(),
        "check_constraints must be array"
    );
    assert!(dd["partitions"].is_array(), "partitions must be array");

    // Optional string fields that should exist (even if empty)
    for field in &[
        "comment",
        "engine_attribute",
        "secondary_engine_attribute",
        "se_private_data",
        "options",
        "partition_expression",
        "partition_expression_utf8",
        "subpartition_expression",
        "subpartition_expression_utf8",
    ] {
        assert!(
            dd.get(*field).is_some(),
            "field '{}' should exist in dd_object",
            field
        );
    }
}

/// Validate column fields in the SDI JSON.
fn assert_column_fields(col: &serde_json::Value) {
    let expected_fields = [
        "name",
        "type",
        "char_length",
        "collation_id",
        "column_key",
        "column_type_utf8",
        "comment",
        "datetime_precision",
        "datetime_precision_null",
        "default_option",
        "default_value",
        "default_value_null",
        "default_value_utf8",
        "default_value_utf8_null",
        "elements",
        "engine_attribute",
        "generation_expression",
        "generation_expression_utf8",
        "has_no_default",
        "hidden",
        "is_auto_increment",
        "is_explicit_collation",
        "is_nullable",
        "is_unsigned",
        "is_virtual",
        "is_zerofill",
        "numeric_precision",
        "numeric_scale",
        "numeric_scale_null",
        "options",
        "ordinal_position",
        "se_private_data",
        "secondary_engine_attribute",
        "srs_id",
        "srs_id_null",
        "update_option",
    ];

    for field in &expected_fields {
        assert!(
            col.get(*field).is_some(),
            "column '{}' missing field '{}'",
            col["name"].as_str().unwrap_or("<unknown>"),
            field
        );
    }

    // name must be a string
    assert!(col["name"].as_str().is_some(), "column name must be string");
    // ordinal_position must be a number
    assert!(
        col["ordinal_position"].as_u64().is_some(),
        "ordinal_position must be number"
    );
}

/// Validate index fields in the SDI JSON.
fn assert_index_fields(idx: &serde_json::Value) {
    let expected_fields = [
        "name",
        "type",
        "algorithm",
        "comment",
        "elements",
        "engine",
        "engine_attribute",
        "hidden",
        "is_algorithm_explicit",
        "is_generated",
        "is_visible",
        "options",
        "ordinal_position",
        "se_private_data",
        "secondary_engine_attribute",
        "tablespace_ref",
    ];

    for field in &expected_fields {
        assert!(
            idx.get(*field).is_some(),
            "index '{}' missing field '{}'",
            idx["name"].as_str().unwrap_or("<unknown>"),
            field
        );
    }

    assert!(idx["elements"].is_array(), "index elements must be array");
    assert!(
        !idx["elements"].as_array().unwrap().is_empty(),
        "index should have at least one element"
    );

    // Validate index element fields
    let elem = &idx["elements"].as_array().unwrap()[0];
    for field in &[
        "column_opx",
        "hidden",
        "length",
        "order",
        "ordinal_position",
    ] {
        assert!(
            elem.get(*field).is_some(),
            "index element missing field '{}'",
            field
        );
    }
}

/// Validate Tablespace dd_object fields for MySQL 9.x.
fn assert_tablespace_dd_object(dd: &serde_json::Value) {
    assert!(dd["name"].as_str().is_some(), "tablespace name");
    assert_eq!(dd["engine"].as_str().unwrap(), "InnoDB");
    assert!(dd["files"].is_array(), "files must be array");
    assert!(
        !dd["files"].as_array().unwrap().is_empty(),
        "should have at least one file"
    );

    let file = &dd["files"].as_array().unwrap()[0];
    assert!(file["filename"].as_str().is_some(), "filename");
    assert!(
        file["ordinal_position"].as_u64().is_some(),
        "ordinal_position"
    );
    assert!(
        file["se_private_data"].as_str().is_some(),
        "se_private_data"
    );

    for field in &["comment", "engine_attribute", "options", "se_private_data"] {
        assert!(
            dd.get(*field).is_some(),
            "tablespace dd_object missing '{}'",
            field
        );
    }
}

// ── MySQL 9.0 standard SDI schema validation ────────────────────────

#[test]
fn test_mysql90_standard_sdi_table_json_schema() {
    let json = extract_sdi_table_json("mysql90_standard.ibd");
    assert_sdi_envelope(&json, "Table");

    let dd = &json["dd_object"];
    assert_table_dd_object(dd, "standard");

    // Validate all columns
    for col in dd["columns"].as_array().unwrap() {
        assert_column_fields(col);
    }

    // Validate all indexes
    for idx in dd["indexes"].as_array().unwrap() {
        assert_index_fields(idx);
    }

    // MySQL 9.0 specific version checks
    assert_eq!(json["mysqld_version_id"].as_u64().unwrap(), 90001);
    assert_eq!(dd["mysql_version_id"].as_u64().unwrap(), 90001);
}

#[test]
fn test_mysql90_standard_sdi_tablespace_json_schema() {
    let json = extract_sdi_tablespace_json("mysql90_standard.ibd");
    assert_sdi_envelope(&json, "Tablespace");
    assert_tablespace_dd_object(&json["dd_object"]);

    // Verify server_version in se_private_data
    let se_data = json["dd_object"]["se_private_data"].as_str().unwrap();
    assert!(
        se_data.contains("server_version=90001"),
        "se_private_data should reference server_version=90001, got: {}",
        se_data
    );
}

// ── MySQL 9.1 standard SDI schema validation ────────────────────────

#[test]
fn test_mysql91_standard_sdi_table_json_schema() {
    let json = extract_sdi_table_json("mysql91_standard.ibd");
    assert_sdi_envelope(&json, "Table");

    let dd = &json["dd_object"];
    assert_table_dd_object(dd, "standard");

    for col in dd["columns"].as_array().unwrap() {
        assert_column_fields(col);
    }
    for idx in dd["indexes"].as_array().unwrap() {
        assert_index_fields(idx);
    }

    // MySQL 9.1 specific version checks
    assert_eq!(json["mysqld_version_id"].as_u64().unwrap(), 90100);
    assert_eq!(dd["mysql_version_id"].as_u64().unwrap(), 90100);
}

#[test]
fn test_mysql91_standard_sdi_tablespace_json_schema() {
    let json = extract_sdi_tablespace_json("mysql91_standard.ibd");
    assert_sdi_envelope(&json, "Tablespace");
    assert_tablespace_dd_object(&json["dd_object"]);

    let se_data = json["dd_object"]["se_private_data"].as_str().unwrap();
    assert!(
        se_data.contains("server_version=90100"),
        "se_private_data should reference server_version=90100, got: {}",
        se_data
    );
}

// ── MySQL 9.0 multipage SDI schema validation ───────────────────────

#[test]
fn test_mysql90_multipage_sdi_table_json_schema() {
    let json = extract_sdi_table_json("mysql90_multipage.ibd");
    assert_sdi_envelope(&json, "Table");

    let dd = &json["dd_object"];
    assert_table_dd_object(dd, "multipage");

    for col in dd["columns"].as_array().unwrap() {
        assert_column_fields(col);
    }
    for idx in dd["indexes"].as_array().unwrap() {
        assert_index_fields(idx);
    }
}

#[test]
fn test_mysql90_multipage_sdi_tablespace_json_schema() {
    let json = extract_sdi_tablespace_json("mysql90_multipage.ibd");
    assert_sdi_envelope(&json, "Tablespace");
    assert_tablespace_dd_object(&json["dd_object"]);
}

// ── MySQL 9.1 multipage SDI schema validation ───────────────────────

#[test]
fn test_mysql91_multipage_sdi_table_json_schema() {
    let json = extract_sdi_table_json("mysql91_multipage.ibd");
    assert_sdi_envelope(&json, "Table");

    let dd = &json["dd_object"];
    assert_table_dd_object(dd, "multipage");

    for col in dd["columns"].as_array().unwrap() {
        assert_column_fields(col);
    }
    for idx in dd["indexes"].as_array().unwrap() {
        assert_index_fields(idx);
    }
}

#[test]
fn test_mysql91_multipage_sdi_tablespace_json_schema() {
    let json = extract_sdi_tablespace_json("mysql91_multipage.ibd");
    assert_sdi_envelope(&json, "Tablespace");
    assert_tablespace_dd_object(&json["dd_object"]);
}

// ── Cross-version SDI schema consistency ────────────────────────────

#[test]
fn test_mysql9_sdi_schema_consistent_across_versions() {
    // Verify that MySQL 9.0 and 9.1 produce identical SDI JSON schema
    // (same field names, same structure — only values like version IDs differ)
    let json90 = extract_sdi_table_json("mysql90_standard.ibd");
    let json91 = extract_sdi_table_json("mysql91_standard.ibd");

    // Top-level keys must match
    let keys90: std::collections::BTreeSet<String> =
        json90.as_object().unwrap().keys().cloned().collect();
    let keys91: std::collections::BTreeSet<String> =
        json91.as_object().unwrap().keys().cloned().collect();
    assert_eq!(keys90, keys91, "top-level keys differ between 9.0 and 9.1");

    // dd_object keys must match
    let dd_keys90: std::collections::BTreeSet<String> = json90["dd_object"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    let dd_keys91: std::collections::BTreeSet<String> = json91["dd_object"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    assert_eq!(
        dd_keys90, dd_keys91,
        "dd_object keys differ between 9.0 and 9.1"
    );

    // Column field names must match (compare first user column)
    let col90 = &json90["dd_object"]["columns"][0];
    let col91 = &json91["dd_object"]["columns"][0];
    let col_keys90: std::collections::BTreeSet<String> =
        col90.as_object().unwrap().keys().cloned().collect();
    let col_keys91: std::collections::BTreeSet<String> =
        col91.as_object().unwrap().keys().cloned().collect();
    assert_eq!(
        col_keys90, col_keys91,
        "column field names differ between 9.0 and 9.1"
    );

    // Index field names must match
    let idx90 = &json90["dd_object"]["indexes"][0];
    let idx91 = &json91["dd_object"]["indexes"][0];
    let idx_keys90: std::collections::BTreeSet<String> =
        idx90.as_object().unwrap().keys().cloned().collect();
    let idx_keys91: std::collections::BTreeSet<String> =
        idx91.as_object().unwrap().keys().cloned().collect();
    assert_eq!(
        idx_keys90, idx_keys91,
        "index field names differ between 9.0 and 9.1"
    );

    // sdi_version must be the same across both versions
    assert_eq!(
        json90["sdi_version"].as_u64().unwrap(),
        json91["sdi_version"].as_u64().unwrap(),
        "sdi_version should be identical across MySQL 9.x versions"
    );

    // dd_version must be the same across both versions
    assert_eq!(
        json90["dd_version"].as_u64().unwrap(),
        json91["dd_version"].as_u64().unwrap(),
        "dd_version should be identical across MySQL 9.x versions"
    );
}

#[test]
fn test_mysql9_sdi_record_types() {
    // Verify both type=1 (Table) and type=2 (Tablespace) records exist in all fixtures
    use idb::innodb::sdi::{extract_sdi_from_pages, find_sdi_pages};

    for fixture in &[
        "mysql90_standard.ibd",
        "mysql91_standard.ibd",
        "mysql90_multipage.ibd",
        "mysql91_multipage.ibd",
    ] {
        let path = format!("{}/{}", MYSQL9_FIXTURE_DIR, fixture);
        let mut ts = Tablespace::open(&path).expect("open");
        let sdi_pages = find_sdi_pages(&mut ts).expect("find SDI pages");
        let records = extract_sdi_from_pages(&mut ts, &sdi_pages).expect("extract SDI");

        let has_table = records.iter().any(|r| r.sdi_type == 1);
        let has_tablespace = records.iter().any(|r| r.sdi_type == 2);

        assert!(
            has_table,
            "{}: should have Table SDI record (type=1)",
            fixture
        );
        assert!(
            has_tablespace,
            "{}: should have Tablespace SDI record (type=2)",
            fixture
        );
        assert_eq!(
            records.len(),
            2,
            "{}: should have exactly 2 SDI records (1 Table + 1 Tablespace)",
            fixture
        );
    }
}

// ══════════════════════════════════════════════════════════════════════
// Percona Server fixture tests
// ══════════════════════════════════════════════════════════════════════
//
// These tests use real .ibd files extracted from Percona Server 8.0.45
// and 8.4.7 Docker containers. They validate that the parser correctly
// handles Percona Server tablespace formats, which are binary-compatible
// with MySQL but may contain Percona-specific XtraDB features.

const PERCONA_FIXTURE_DIR: &str = "tests/fixtures/percona";

// ── Percona 8.0 standard tablespace ─────────────────────────────────

#[test]
fn test_percona80_standard_opens() {
    let path = format!("{}/percona80_standard.ibd", PERCONA_FIXTURE_DIR);
    let ts = Tablespace::open(&path).expect("should open Percona 8.0 standard tablespace");
    assert_eq!(ts.page_size(), 16384, "standard table should use 16K pages");
    assert_eq!(ts.page_count(), 7, "standard table should have 7 pages");
}

#[test]
fn test_percona80_standard_checksums_valid() {
    let path = format!("{}/percona80_standard.ibd", PERCONA_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");
    let vendor = ts.vendor_info().clone();
    let page_size = ts.page_size();

    ts.for_each_page(|page_num, data| {
        // Skip all-zero (ALLOCATED) pages
        if data.iter().all(|&b| b == 0) {
            return Ok(());
        }
        let result = validate_checksum(data, page_size, Some(&vendor));
        assert!(
            result.valid,
            "page {} checksum should be valid (algo={:?})",
            page_num, result.algorithm
        );
        assert_eq!(result.algorithm, ChecksumAlgorithm::Crc32c);
        Ok(())
    })
    .expect("iterate pages");
}

#[test]
fn test_percona80_standard_page_types() {
    let path = format!("{}/percona80_standard.ibd", PERCONA_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");

    let mut types = Vec::new();
    ts.for_each_page(|_page_num, data| {
        let hdr = FilHeader::parse(data).unwrap();
        types.push(hdr.page_type);
        Ok(())
    })
    .expect("iterate");

    assert!(
        types.contains(&PageType::FspHdr),
        "should contain FSP_HDR page"
    );
    assert!(
        types.contains(&PageType::Index),
        "should contain INDEX page"
    );
    assert!(types.contains(&PageType::Sdi), "should contain SDI page");
    assert!(
        types.contains(&PageType::Inode),
        "should contain INODE page"
    );
}

#[test]
fn test_percona80_standard_sdi_extraction() {
    use idb::innodb::sdi::{extract_sdi_from_pages, find_sdi_pages};

    let path = format!("{}/percona80_standard.ibd", PERCONA_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");

    let sdi_pages = find_sdi_pages(&mut ts).expect("find SDI pages");
    assert!(!sdi_pages.is_empty(), "should find at least one SDI page");

    let records = extract_sdi_from_pages(&mut ts, &sdi_pages).expect("extract SDI");
    assert!(
        !records.is_empty(),
        "should extract at least one SDI record"
    );

    // Table SDI record (type 1)
    let table_rec = records.iter().find(|r| r.sdi_type == 1);
    assert!(table_rec.is_some(), "should have a Table SDI record");

    let data = &table_rec.unwrap().data;
    assert!(
        data.contains("\"name\": \"standard\"") || data.contains("\"name\":\"standard\""),
        "SDI data should contain table name 'standard'"
    );
}

#[test]
fn test_percona80_standard_vendor_detection() {
    use idb::innodb::vendor::InnoDbVendor;

    let path = format!("{}/percona80_standard.ibd", PERCONA_FIXTURE_DIR);
    let ts = Tablespace::open(&path).expect("open");
    // Percona is binary-compatible with MySQL; vendor detection from FSP flags
    // alone cannot distinguish Percona from MySQL.
    assert_eq!(
        ts.vendor_info().vendor,
        InnoDbVendor::MySQL,
        "Percona should be detected as MySQL from FSP flags (binary-compatible)"
    );
}

#[test]
fn test_percona80_standard_fsp_header() {
    let path = format!("{}/percona80_standard.ibd", PERCONA_FIXTURE_DIR);
    let ts = Tablespace::open(&path).expect("open");
    let fsp = ts.fsp_header().expect("FSP header present");
    assert!(fsp.space_id > 0, "space_id should be nonzero");
    assert_eq!(fsp.size, 7, "FSP size should match page count");
}

#[test]
fn test_percona80_standard_lsn_valid() {
    let path = format!("{}/percona80_standard.ibd", PERCONA_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");
    let page_size = ts.page_size();

    ts.for_each_page(|page_num, data| {
        if data.iter().all(|&b| b == 0) {
            return Ok(());
        }
        assert!(
            validate_lsn(data, page_size),
            "page {} LSN should be consistent",
            page_num
        );
        Ok(())
    })
    .expect("iterate");
}

// ── Percona 8.4 standard tablespace ─────────────────────────────────

#[test]
fn test_percona84_standard_opens() {
    let path = format!("{}/percona84_standard.ibd", PERCONA_FIXTURE_DIR);
    let ts = Tablespace::open(&path).expect("should open Percona 8.4 standard tablespace");
    assert_eq!(ts.page_size(), 16384);
    assert_eq!(ts.page_count(), 7);
}

#[test]
fn test_percona84_standard_checksums_valid() {
    let path = format!("{}/percona84_standard.ibd", PERCONA_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");
    let vendor = ts.vendor_info().clone();
    let page_size = ts.page_size();

    ts.for_each_page(|page_num, data| {
        if data.iter().all(|&b| b == 0) {
            return Ok(());
        }
        let result = validate_checksum(data, page_size, Some(&vendor));
        assert!(result.valid, "page {} checksum should be valid", page_num);
        Ok(())
    })
    .expect("iterate pages");
}

#[test]
fn test_percona84_standard_sdi_extraction() {
    use idb::innodb::sdi::{extract_sdi_from_pages, find_sdi_pages};

    let path = format!("{}/percona84_standard.ibd", PERCONA_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");

    let sdi_pages = find_sdi_pages(&mut ts).expect("find SDI pages");
    assert!(!sdi_pages.is_empty(), "should find at least one SDI page");

    let records = extract_sdi_from_pages(&mut ts, &sdi_pages).expect("extract SDI");
    assert!(
        !records.is_empty(),
        "should extract at least one SDI record"
    );

    let table_rec = records.iter().find(|r| r.sdi_type == 1);
    assert!(table_rec.is_some(), "should have a Table SDI record");

    let data = &table_rec.unwrap().data;
    assert!(
        data.contains("\"name\": \"standard\"") || data.contains("\"name\":\"standard\""),
        "SDI data should contain table name 'standard'"
    );
}

#[test]
fn test_percona84_standard_page_types() {
    let path = format!("{}/percona84_standard.ibd", PERCONA_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");

    let mut has_fsp_hdr = false;
    let mut has_index = false;
    let mut has_sdi = false;
    ts.for_each_page(|_page_num, data| {
        let hdr = FilHeader::parse(data).unwrap();
        match hdr.page_type {
            PageType::FspHdr => has_fsp_hdr = true,
            PageType::Index => has_index = true,
            PageType::Sdi => has_sdi = true,
            _ => {}
        }
        Ok(())
    })
    .expect("iterate");

    assert!(has_fsp_hdr, "should contain FSP_HDR page");
    assert!(has_index, "should contain INDEX page");
    assert!(has_sdi, "should contain SDI page");
}

#[test]
fn test_percona84_standard_lsn_valid() {
    let path = format!("{}/percona84_standard.ibd", PERCONA_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");
    let page_size = ts.page_size();

    ts.for_each_page(|page_num, data| {
        if data.iter().all(|&b| b == 0) {
            return Ok(());
        }
        assert!(
            validate_lsn(data, page_size),
            "page {} LSN should be consistent",
            page_num
        );
        Ok(())
    })
    .expect("iterate");
}

// ── Percona 8.0 compressed tablespace ───────────────────────────────

#[test]
fn test_percona80_compressed_opens() {
    let path = format!("{}/percona80_compressed.ibd", PERCONA_FIXTURE_DIR);
    let ts = Tablespace::open(&path).expect("should open Percona 8.0 compressed tablespace");
    assert_eq!(
        ts.page_size(),
        16384,
        "logical page size should be 16K even for compressed tablespaces"
    );
    assert!(
        ts.page_count() > 1,
        "should have FSP_HDR plus at least one data page"
    );
}

#[test]
fn test_percona80_compressed_fsp_header() {
    let path = format!("{}/percona80_compressed.ibd", PERCONA_FIXTURE_DIR);
    let ts = Tablespace::open(&path).expect("open");
    let fsp = ts.fsp_header().expect("FSP header present");
    assert!(fsp.space_id > 0, "space_id should be nonzero");
    // FSP flags should indicate compressed format
    assert!(
        fsp.flags > 0,
        "flags should be nonzero for compressed table"
    );
}

// ── Percona 8.4 compressed tablespace ───────────────────────────────

#[test]
fn test_percona84_compressed_opens() {
    let path = format!("{}/percona84_compressed.ibd", PERCONA_FIXTURE_DIR);
    let ts = Tablespace::open(&path).expect("should open Percona 8.4 compressed tablespace");
    assert_eq!(
        ts.page_size(),
        16384,
        "logical page size should be 16K even for compressed tablespaces"
    );
    assert!(
        ts.page_count() > 1,
        "should have FSP_HDR plus at least one data page"
    );
}

#[test]
fn test_percona84_compressed_fsp_header() {
    let path = format!("{}/percona84_compressed.ibd", PERCONA_FIXTURE_DIR);
    let ts = Tablespace::open(&path).expect("open");
    let fsp = ts.fsp_header().expect("FSP header present");
    assert!(fsp.space_id > 0, "space_id should be nonzero");
    assert!(
        fsp.flags > 0,
        "flags should be nonzero for compressed table"
    );
}

// ── Percona 8.0 multipage tablespace ────────────────────────────────

#[test]
fn test_percona80_multipage_opens() {
    let path = format!("{}/percona80_multipage.ibd", PERCONA_FIXTURE_DIR);
    let ts = Tablespace::open(&path).expect("should open Percona 8.0 multipage tablespace");
    assert_eq!(ts.page_size(), 16384);
    assert_eq!(
        ts.page_count(),
        576,
        "multipage table should have 576 pages"
    );
}

#[test]
fn test_percona80_multipage_checksums_valid() {
    let path = format!("{}/percona80_multipage.ibd", PERCONA_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");
    let vendor = ts.vendor_info().clone();
    let page_size = ts.page_size();

    let mut valid_count = 0u32;
    let mut empty_count = 0u32;
    ts.for_each_page(|page_num, data| {
        if data.iter().all(|&b| b == 0) {
            empty_count += 1;
            return Ok(());
        }
        let result = validate_checksum(data, page_size, Some(&vendor));
        assert!(result.valid, "page {} checksum should be valid", page_num);
        valid_count += 1;
        Ok(())
    })
    .expect("iterate pages");

    assert!(
        valid_count > 50,
        "should have many valid pages, got {}",
        valid_count
    );
    assert!(empty_count > 0, "should have some empty (ALLOCATED) pages");
}

#[test]
fn test_percona80_multipage_has_many_index_pages() {
    let path = format!("{}/percona80_multipage.ibd", PERCONA_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");

    let mut index_count = 0u32;
    ts.for_each_page(|_page_num, data| {
        let hdr = FilHeader::parse(data).unwrap();
        if hdr.page_type == PageType::Index {
            index_count += 1;
        }
        Ok(())
    })
    .expect("iterate");

    assert!(
        index_count >= 50,
        "multipage table should have at least 50 INDEX pages, got {}",
        index_count
    );
}

#[test]
fn test_percona80_multipage_sdi_extraction() {
    use idb::innodb::sdi::{extract_sdi_from_pages, find_sdi_pages};

    let path = format!("{}/percona80_multipage.ibd", PERCONA_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");

    let sdi_pages = find_sdi_pages(&mut ts).expect("find SDI pages");
    assert!(!sdi_pages.is_empty(), "should find SDI pages");

    let records = extract_sdi_from_pages(&mut ts, &sdi_pages).expect("extract SDI");
    let table_rec = records.iter().find(|r| r.sdi_type == 1);
    assert!(table_rec.is_some(), "should have Table SDI record");

    let data = &table_rec.unwrap().data;
    assert!(
        data.contains("\"name\": \"multipage\"") || data.contains("\"name\":\"multipage\""),
        "SDI should contain table name 'multipage'"
    );
}

// ── Percona 8.4 multipage tablespace ────────────────────────────────

#[test]
fn test_percona84_multipage_opens() {
    let path = format!("{}/percona84_multipage.ibd", PERCONA_FIXTURE_DIR);
    let ts = Tablespace::open(&path).expect("should open Percona 8.4 multipage tablespace");
    assert_eq!(ts.page_size(), 16384);
    assert_eq!(ts.page_count(), 576);
}

#[test]
fn test_percona84_multipage_checksums_valid() {
    let path = format!("{}/percona84_multipage.ibd", PERCONA_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");
    let vendor = ts.vendor_info().clone();
    let page_size = ts.page_size();

    let mut valid_count = 0u32;
    ts.for_each_page(|page_num, data| {
        if data.iter().all(|&b| b == 0) {
            return Ok(());
        }
        let result = validate_checksum(data, page_size, Some(&vendor));
        assert!(result.valid, "page {} checksum should be valid", page_num);
        valid_count += 1;
        Ok(())
    })
    .expect("iterate pages");

    assert!(
        valid_count > 50,
        "should have many valid pages, got {}",
        valid_count
    );
}

#[test]
fn test_percona84_multipage_has_many_index_pages() {
    let path = format!("{}/percona84_multipage.ibd", PERCONA_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");

    let mut index_count = 0u32;
    ts.for_each_page(|_page_num, data| {
        let hdr = FilHeader::parse(data).unwrap();
        if hdr.page_type == PageType::Index {
            index_count += 1;
        }
        Ok(())
    })
    .expect("iterate");

    assert!(
        index_count >= 50,
        "multipage table should have at least 50 INDEX pages, got {}",
        index_count
    );
}

#[test]
fn test_percona84_multipage_sdi_extraction() {
    use idb::innodb::sdi::{extract_sdi_from_pages, find_sdi_pages};

    let path = format!("{}/percona84_multipage.ibd", PERCONA_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");

    let sdi_pages = find_sdi_pages(&mut ts).expect("find SDI pages");
    assert!(!sdi_pages.is_empty(), "should find SDI pages");

    let records = extract_sdi_from_pages(&mut ts, &sdi_pages).expect("extract SDI");
    let table_rec = records.iter().find(|r| r.sdi_type == 1);
    assert!(table_rec.is_some(), "should have Table SDI record");

    let data = &table_rec.unwrap().data;
    assert!(
        data.contains("\"name\": \"multipage\"") || data.contains("\"name\":\"multipage\""),
        "SDI should contain table name 'multipage'"
    );
}

// ── Percona cross-version comparison tests ──────────────────────────

#[test]
fn test_percona_standard_cross_version_page_structure() {
    // Both Percona 8.0 and 8.4 standard tables should have the same page layout
    let path80 = format!("{}/percona80_standard.ibd", PERCONA_FIXTURE_DIR);
    let path84 = format!("{}/percona84_standard.ibd", PERCONA_FIXTURE_DIR);

    let mut ts80 = Tablespace::open(&path80).expect("open 8.0");
    let mut ts84 = Tablespace::open(&path84).expect("open 8.4");

    assert_eq!(
        ts80.page_size(),
        ts84.page_size(),
        "page sizes should match"
    );
    assert_eq!(
        ts80.page_count(),
        ts84.page_count(),
        "page counts should match"
    );

    let mut types80 = Vec::new();
    let mut types84 = Vec::new();

    ts80.for_each_page(|_n, data| {
        types80.push(FilHeader::parse(data).unwrap().page_type);
        Ok(())
    })
    .expect("iterate 8.0");

    ts84.for_each_page(|_n, data| {
        types84.push(FilHeader::parse(data).unwrap().page_type);
        Ok(())
    })
    .expect("iterate 8.4");

    assert_eq!(
        types80, types84,
        "page type sequences should be identical across Percona 8.0 and 8.4"
    );
}

#[test]
fn test_percona_multipage_cross_version_structure() {
    let path80 = format!("{}/percona80_multipage.ibd", PERCONA_FIXTURE_DIR);
    let path84 = format!("{}/percona84_multipage.ibd", PERCONA_FIXTURE_DIR);

    let ts80 = Tablespace::open(&path80).expect("open 8.0");
    let ts84 = Tablespace::open(&path84).expect("open 8.4");

    assert_eq!(ts80.page_size(), ts84.page_size());
    assert_eq!(ts80.page_count(), ts84.page_count());
}

// ── Percona recovery assessment tests ───────────────────────────────

/// Run `inno recover --json` on a Percona fixture and return the parsed JSON.
fn run_percona_recovery_json(fixture_path: &str) -> serde_json::Value {
    let path = format!("{}/{}", PERCONA_FIXTURE_DIR, fixture_path);
    let opts = idb::cli::recover::RecoverOptions {
        file: path.clone(),
        page: None,
        verbose: false,
        json: true,
        force: false,
        page_size: None,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: false,
        rebuild: None,
    };
    let mut out = Vec::new();
    idb::cli::recover::execute(&opts, &mut out)
        .unwrap_or_else(|e| panic!("recover failed on {}: {}", path, e));
    let output = String::from_utf8(out).expect("valid UTF-8 output");
    serde_json::from_str(&output).expect("valid JSON output")
}

#[test]
fn test_percona80_standard_recovery_assessment() {
    let parsed = run_percona_recovery_json("percona80_standard.ibd");
    assert!(parsed.get("summary").is_some());
    assert!(parsed.get("total_pages").is_some());
    assert!(parsed.get("recoverable_records").is_some());
    assert_eq!(
        parsed["total_pages"], 7,
        "standard fixture should have 7 pages"
    );
    assert!(
        parsed["summary"].get("intact").is_some(),
        "summary should contain intact count"
    );
}

#[test]
fn test_percona84_standard_recovery_assessment() {
    let parsed = run_percona_recovery_json("percona84_standard.ibd");
    assert!(parsed.get("summary").is_some());
    assert!(parsed.get("total_pages").is_some());
    assert!(parsed.get("recoverable_records").is_some());
    assert_eq!(
        parsed["total_pages"], 7,
        "standard fixture should have 7 pages"
    );
    assert!(
        parsed["summary"].get("intact").is_some(),
        "summary should contain intact count"
    );
}

#[test]
fn test_percona80_multipage_recovery_assessment() {
    let parsed = run_percona_recovery_json("percona80_multipage.ibd");
    assert!(
        parsed["total_pages"].as_u64().unwrap() > 7,
        "multipage fixture should have many pages"
    );
    assert!(parsed.get("recoverable_records").is_some());
}

#[test]
fn test_percona84_multipage_recovery_assessment() {
    let parsed = run_percona_recovery_json("percona84_multipage.ibd");
    assert!(
        parsed["total_pages"].as_u64().unwrap() > 7,
        "multipage fixture should have many pages"
    );
    assert!(parsed.get("recoverable_records").is_some());
}

// ── Percona SDI metadata version validation ─────────────────────────

#[test]
fn test_percona80_standard_sdi_version_fields() {
    use idb::innodb::sdi::{extract_sdi_from_pages, find_sdi_pages};

    let path = format!("{}/percona80_standard.ibd", PERCONA_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");

    let sdi_pages = find_sdi_pages(&mut ts).expect("find SDI pages");
    let records = extract_sdi_from_pages(&mut ts, &sdi_pages).expect("extract SDI");
    let table_rec = records.iter().find(|r| r.sdi_type == 1).unwrap();
    let json: serde_json::Value = serde_json::from_str(&table_rec.data).unwrap();

    // Percona 8.0.45 is based on MySQL 8.0.x
    assert_eq!(json["sdi_version"], 80019, "sdi_version should be 80019");
    assert!(
        json["mysqld_version_id"].as_u64().unwrap() >= 80045,
        "mysqld_version_id should be >= 80045 for Percona 8.0.45"
    );
    assert!(
        json["dd_object"]["name"].as_str().unwrap() == "standard",
        "table name should be 'standard'"
    );
}

#[test]
fn test_percona84_standard_sdi_version_fields() {
    use idb::innodb::sdi::{extract_sdi_from_pages, find_sdi_pages};

    let path = format!("{}/percona84_standard.ibd", PERCONA_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");

    let sdi_pages = find_sdi_pages(&mut ts).expect("find SDI pages");
    let records = extract_sdi_from_pages(&mut ts, &sdi_pages).expect("extract SDI");
    let table_rec = records.iter().find(|r| r.sdi_type == 1).unwrap();
    let json: serde_json::Value = serde_json::from_str(&table_rec.data).unwrap();

    // Percona 8.4.7 is based on MySQL 8.4.x
    assert_eq!(json["sdi_version"], 80019, "sdi_version should be 80019");
    assert!(
        json["mysqld_version_id"].as_u64().unwrap() >= 80400,
        "mysqld_version_id should be >= 80400 for Percona 8.4.7"
    );
    assert!(
        json["dd_object"]["name"].as_str().unwrap() == "standard",
        "table name should be 'standard'"
    );
}

#[test]
fn test_percona_sdi_record_types() {
    use idb::innodb::sdi::{extract_sdi_from_pages, find_sdi_pages};

    // All Percona fixtures should have exactly 2 SDI records: Table (type=1) + Tablespace (type=2)
    for fixture in &[
        "percona80_standard.ibd",
        "percona84_standard.ibd",
        "percona80_multipage.ibd",
        "percona84_multipage.ibd",
    ] {
        let path = format!("{}/{}", PERCONA_FIXTURE_DIR, fixture);
        let mut ts = Tablespace::open(&path).expect("open");
        let sdi_pages = find_sdi_pages(&mut ts).expect("find SDI pages");
        let records = extract_sdi_from_pages(&mut ts, &sdi_pages).expect("extract SDI");

        let has_table = records.iter().any(|r| r.sdi_type == 1);
        let has_tablespace = records.iter().any(|r| r.sdi_type == 2);

        assert!(
            has_table,
            "{}: should have Table SDI record (type=1)",
            fixture
        );
        assert!(
            has_tablespace,
            "{}: should have Tablespace SDI record (type=2)",
            fixture
        );
        assert_eq!(
            records.len(),
            2,
            "{}: should have exactly 2 SDI records (1 Table + 1 Tablespace)",
            fixture
        );
    }
}

// ========================================================================
// Streaming mode tests
// ========================================================================

#[test]
fn test_streaming_checksum_text_matches_nonstreaming() {
    let page0 = build_fsp_hdr_page(1, 3);
    let page1 = build_index_page(1, 1, 2000);
    let page2 = build_index_page(2, 1, 3000);

    let tmp = write_tablespace(&[page0, page1, page2]);
    let path = tmp.path().to_string_lossy().to_string();

    // Non-streaming verbose
    let opts_normal = idb::cli::checksum::ChecksumOptions {
        file: path.clone(),
        verbose: true,
        json: false,
        page_size: None,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: false,
    };
    let mut out_normal = Vec::new();
    idb::cli::checksum::execute(&opts_normal, &mut out_normal).unwrap();

    // Streaming verbose
    let opts_stream = idb::cli::checksum::ChecksumOptions {
        file: path,
        verbose: true,
        json: false,
        page_size: None,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: true,
    };
    let mut out_stream = Vec::new();
    idb::cli::checksum::execute(&opts_stream, &mut out_stream).unwrap();

    // Both should produce the same output (same header, summary, per-page lines)
    let normal_str = String::from_utf8(out_normal).unwrap();
    let stream_str = String::from_utf8(out_stream).unwrap();
    assert_eq!(normal_str, stream_str);
}

#[test]
fn test_streaming_checksum_json_is_valid_ndjson() {
    let page0 = build_fsp_hdr_page(1, 3);
    let page1 = build_index_page(1, 1, 2000);
    let page2 = build_index_page(2, 1, 3000);

    let tmp = write_tablespace(&[page0, page1, page2]);

    let opts = idb::cli::checksum::ChecksumOptions {
        file: tmp.path().to_string_lossy().to_string(),
        verbose: true,
        json: true,
        page_size: None,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: true,
    };

    let mut out = Vec::new();
    idb::cli::checksum::execute(&opts, &mut out).unwrap();

    let output = String::from_utf8(out).unwrap();
    let lines: Vec<&str> = output.lines().collect();

    // With verbose + streaming JSON, should have one line per non-empty page
    assert!(
        !lines.is_empty(),
        "streaming JSON should produce at least one line"
    );

    // Each line should be valid JSON
    for (i, line) in lines.iter().enumerate() {
        let parsed: Result<serde_json::Value, _> = serde_json::from_str(line);
        assert!(
            parsed.is_ok(),
            "Line {} should be valid JSON: {:?}\nContent: {}",
            i,
            parsed.err(),
            line
        );
    }
}

#[test]
fn test_streaming_parse_json_is_valid_ndjson() {
    let page0 = build_fsp_hdr_page(1, 3);
    let page1 = build_index_page(1, 1, 2000);
    let page2 = build_index_page(2, 1, 3000);

    let tmp = write_tablespace(&[page0, page1, page2]);

    let opts = idb::cli::parse::ParseOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        verbose: false,
        no_empty: false,
        page_size: None,
        json: true,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: true,
    };

    let mut out = Vec::new();
    idb::cli::parse::execute(&opts, &mut out).unwrap();

    let output = String::from_utf8(out).unwrap();
    let lines: Vec<&str> = output.lines().collect();

    // Should have 3 lines (one per page)
    assert_eq!(
        lines.len(),
        3,
        "streaming parse JSON should produce one line per page"
    );

    // Each line should be valid JSON with expected fields
    for (i, line) in lines.iter().enumerate() {
        let parsed: serde_json::Value = serde_json::from_str(line).unwrap_or_else(|e| {
            panic!("Line {} should be valid JSON: {}\nContent: {}", i, e, line);
        });
        assert!(
            parsed.get("page_number").is_some(),
            "Line {} should have page_number field",
            i
        );
        assert!(
            parsed.get("page_type_name").is_some(),
            "Line {} should have page_type_name field",
            i
        );
    }
}

#[test]
fn test_streaming_parse_json_line_count_matches_pages() {
    let page0 = build_fsp_hdr_page(1, 5);
    let page1 = build_index_page(1, 1, 2000);
    let page2 = build_index_page(2, 1, 3000);
    let page3 = build_index_page(3, 1, 4000);
    let page4 = build_index_page(4, 1, 5000);

    let tmp = write_tablespace(&[page0, page1, page2, page3, page4]);

    let opts = idb::cli::parse::ParseOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        verbose: false,
        no_empty: false,
        page_size: None,
        json: true,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: true,
    };

    let mut out = Vec::new();
    idb::cli::parse::execute(&opts, &mut out).unwrap();

    let output = String::from_utf8(out).unwrap();
    let lines: Vec<&str> = output.lines().collect();

    assert_eq!(
        lines.len(),
        5,
        "streaming parse JSON line count should match page count"
    );
}

#[test]
fn test_streaming_parse_json_no_empty_filters_pages() {
    let page0 = build_fsp_hdr_page(1, 3);
    let page1 = build_index_page(1, 1, 2000);
    let page2 = build_allocated_page(2, 1); // empty page

    let tmp = write_tablespace(&[page0, page1, page2]);

    let opts = idb::cli::parse::ParseOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        verbose: false,
        no_empty: true,
        page_size: None,
        json: true,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: true,
    };

    let mut out = Vec::new();
    idb::cli::parse::execute(&opts, &mut out).unwrap();

    let output = String::from_utf8(out).unwrap();
    let lines: Vec<&str> = output.lines().collect();

    // Empty page should be filtered: page0 (FSP_HDR), page1 (INDEX) = 2 lines
    assert_eq!(
        lines.len(),
        2,
        "streaming parse JSON with --no-empty should filter allocated pages"
    );
}

#[test]
fn test_streaming_recover_text_produces_summary() {
    let page0 = build_fsp_hdr_page(1, 3);
    let page1 = build_index_page(1, 1, 2000);
    let page2 = build_index_page(2, 1, 3000);

    let tmp = write_tablespace(&[page0, page1, page2]);

    let opts = idb::cli::recover::RecoverOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        verbose: false,
        json: false,
        force: false,
        page_size: None,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: true,
        rebuild: None,
    };

    let mut out = Vec::new();
    idb::cli::recover::execute(&opts, &mut out).unwrap();

    let output = String::from_utf8(out).unwrap();
    assert!(
        output.contains("Page Status Summary:"),
        "streaming recover should produce summary"
    );
    assert!(
        output.contains("Intact:"),
        "streaming recover should show intact count"
    );
}

#[test]
fn test_streaming_recover_json_is_valid_ndjson() {
    let page0 = build_fsp_hdr_page(1, 3);
    let page1 = build_index_page(1, 1, 2000);
    let page2 = build_index_page(2, 1, 3000);

    let tmp = write_tablespace(&[page0, page1, page2]);

    let opts = idb::cli::recover::RecoverOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        verbose: true,
        json: true,
        force: false,
        page_size: None,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: true,
        rebuild: None,
    };

    let mut out = Vec::new();
    idb::cli::recover::execute(&opts, &mut out).unwrap();

    let output = String::from_utf8(out).unwrap();
    let lines: Vec<&str> = output.lines().collect();

    // Should have per-page lines + 1 summary line
    assert!(
        lines.len() >= 2,
        "streaming recover JSON should produce per-page lines + summary"
    );

    // Each line should be valid JSON
    for (i, line) in lines.iter().enumerate() {
        let parsed: Result<serde_json::Value, _> = serde_json::from_str(line);
        assert!(
            parsed.is_ok(),
            "Line {} should be valid JSON: {:?}\nContent: {}",
            i,
            parsed.err(),
            line
        );
    }

    // Last line should be the summary
    let last: serde_json::Value = serde_json::from_str(lines.last().unwrap()).unwrap();
    assert_eq!(
        last.get("type").and_then(|v| v.as_str()),
        Some("summary"),
        "last NDJSON line should be the summary"
    );
    assert!(
        last.get("total_pages").is_some(),
        "summary should have total_pages"
    );
}

#[test]
fn test_streaming_checksum_detects_invalid() {
    let page0 = build_fsp_hdr_page(1, 2);
    let mut page1 = build_index_page(1, 1, 2000);
    // Corrupt the checksum
    BigEndian::write_u32(&mut page1[FIL_PAGE_SPACE_OR_CHKSUM..], 0xDEAD);

    let tmp = write_tablespace(&[page0, page1]);

    let opts = idb::cli::checksum::ChecksumOptions {
        file: tmp.path().to_string_lossy().to_string(),
        verbose: false,
        json: false,
        page_size: None,
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: true,
    };

    let mut out = Vec::new();
    let result = idb::cli::checksum::execute(&opts, &mut out);
    assert!(
        result.is_err(),
        "streaming checksum should return error for invalid pages"
    );
    let output = String::from_utf8(out).unwrap();
    assert!(
        output.contains("INVALID"),
        "streaming checksum should report invalid pages"
    );
}

#[test]
fn test_streaming_single_page_mode_ignored() {
    // When --page is specified, --streaming is ignored (single page reads one page)
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
        keyring: None,
        threads: 0,
        mmap: false,
        streaming: true, // should be ignored since page=Some
    };

    let mut out = Vec::new();
    let result = idb::cli::parse::execute(&opts, &mut out);
    assert!(
        result.is_ok(),
        "streaming with single page should succeed: {:?}",
        result.err()
    );
}

// ============================================================
// Schema extraction (inno schema)
// ============================================================

#[test]
fn test_schema_mysql90_standard_extracts_ddl() {
    use idb::innodb::schema::extract_schema_from_sdi;
    use idb::innodb::sdi::{extract_sdi_from_pages, find_sdi_pages};

    let path = format!("{}/mysql90_standard.ibd", MYSQL9_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");

    let sdi_pages = find_sdi_pages(&mut ts).expect("find SDI pages");
    let records = extract_sdi_from_pages(&mut ts, &sdi_pages).expect("extract SDI");

    let table_records: Vec<_> = records.iter().filter(|r| r.sdi_type == 1).collect();
    assert_eq!(table_records.len(), 1, "should have exactly one Table SDI record");

    let schema = extract_schema_from_sdi(&table_records[0].data).expect("parse schema");
    assert_eq!(schema.table_name, "standard");
    assert_eq!(schema.schema_name, Some("fixtures".to_string()));
    assert_eq!(schema.engine, "InnoDB");
    assert_eq!(schema.source, "sdi");
    assert_eq!(schema.mysql_version, Some("9.0.1".to_string()));

    // Should have 3 visible columns (id, name, data) — DB_TRX_ID/DB_ROLL_PTR filtered
    assert_eq!(schema.columns.len(), 3);
    assert_eq!(schema.columns[0].name, "id");
    assert_eq!(schema.columns[0].column_type, "int");
    assert!(schema.columns[0].is_auto_increment);
    assert!(!schema.columns[0].is_nullable);
    assert_eq!(schema.columns[1].name, "name");
    assert_eq!(schema.columns[1].column_type, "varchar(100)");
    assert!(schema.columns[1].is_nullable);
    assert_eq!(schema.columns[2].name, "data");
    assert_eq!(schema.columns[2].column_type, "text");

    // Should have PRIMARY KEY index
    assert_eq!(schema.indexes.len(), 1);
    assert_eq!(schema.indexes[0].index_type, "PRIMARY KEY");
    assert_eq!(schema.indexes[0].columns.len(), 1);
    assert_eq!(schema.indexes[0].columns[0].name, "id");

    // DDL should be valid
    assert!(schema.ddl.contains("CREATE TABLE `standard`"));
    assert!(schema.ddl.contains("PRIMARY KEY (`id`)"));
    assert!(schema.ddl.contains("ENGINE=InnoDB"));
}

#[test]
fn test_schema_mysql91_standard_extracts_ddl() {
    use idb::innodb::schema::extract_schema_from_sdi;
    use idb::innodb::sdi::{extract_sdi_from_pages, find_sdi_pages};

    let path = format!("{}/mysql91_standard.ibd", MYSQL9_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");

    let sdi_pages = find_sdi_pages(&mut ts).expect("find SDI pages");
    let records = extract_sdi_from_pages(&mut ts, &sdi_pages).expect("extract SDI");

    let table_records: Vec<_> = records.iter().filter(|r| r.sdi_type == 1).collect();
    assert!(!table_records.is_empty(), "should have Table SDI record");

    let schema = extract_schema_from_sdi(&table_records[0].data).expect("parse schema");
    assert_eq!(schema.table_name, "standard");
    assert_eq!(schema.mysql_version, Some("9.1.0".to_string()));
    assert_eq!(schema.columns.len(), 3);
}

#[test]
fn test_schema_percona84_standard_extracts_ddl() {
    use idb::innodb::schema::extract_schema_from_sdi;
    use idb::innodb::sdi::{extract_sdi_from_pages, find_sdi_pages};

    let path = "tests/fixtures/percona/percona84_standard.ibd";
    let mut ts = Tablespace::open(path).expect("open");

    let sdi_pages = find_sdi_pages(&mut ts).expect("find SDI pages");
    let records = extract_sdi_from_pages(&mut ts, &sdi_pages).expect("extract SDI");

    let table_records: Vec<_> = records.iter().filter(|r| r.sdi_type == 1).collect();
    assert!(!table_records.is_empty(), "should have Table SDI record");

    let schema = extract_schema_from_sdi(&table_records[0].data).expect("parse schema");
    assert_eq!(schema.table_name, "standard");
    assert_eq!(schema.engine, "InnoDB");
    assert_eq!(schema.columns.len(), 3);
    assert!(schema.ddl.contains("CREATE TABLE `standard`"));
}

#[test]
fn test_schema_compressed_falls_back_to_inference() {
    use idb::innodb::schema::infer_schema_from_pages;
    use idb::innodb::sdi::find_sdi_pages;

    let path = format!("{}/mysql90_compressed.ibd", MYSQL9_FIXTURE_DIR);
    let mut ts = Tablespace::open(&path).expect("open");

    let sdi_pages = find_sdi_pages(&mut ts).expect("find SDI pages");
    assert!(sdi_pages.is_empty(), "compressed fixture should have no SDI pages");

    let inferred = infer_schema_from_pages(&mut ts).expect("infer schema");
    assert!(
        inferred.source.contains("Inferred"),
        "source should indicate inference"
    );
    assert!(!inferred.indexes.is_empty(), "should detect at least one index");
}

#[test]
fn test_schema_cli_default_output() {
    let path = format!("{}/mysql90_standard.ibd", MYSQL9_FIXTURE_DIR);
    let opts = idb::cli::schema::SchemaOptions {
        file: path,
        verbose: false,
        json: false,
        page_size: None,
        keyring: None,
        mmap: false,
    };

    let mut out = Vec::new();
    idb::cli::schema::execute(&opts, &mut out).expect("execute schema");
    let output = String::from_utf8(out).expect("valid utf8");

    assert!(output.contains("CREATE TABLE `standard`"), "should contain DDL");
    assert!(output.contains("-- Table:"), "should contain table comment header");
    assert!(output.contains("-- Source: SDI"), "should contain source header");
}

#[test]
fn test_schema_cli_json_output() {
    let path = format!("{}/mysql90_standard.ibd", MYSQL9_FIXTURE_DIR);
    let opts = idb::cli::schema::SchemaOptions {
        file: path,
        verbose: false,
        json: true,
        page_size: None,
        keyring: None,
        mmap: false,
    };

    let mut out = Vec::new();
    idb::cli::schema::execute(&opts, &mut out).expect("execute schema --json");
    let output = String::from_utf8(out).expect("valid utf8");

    let parsed: serde_json::Value =
        serde_json::from_str(output.trim()).expect("should produce valid JSON");
    assert_eq!(parsed["table_name"], "standard");
    assert_eq!(parsed["source"], "sdi");
    assert!(parsed["ddl"].as_str().unwrap().contains("CREATE TABLE"));
}

#[test]
fn test_schema_cli_verbose_output() {
    let path = format!("{}/mysql90_standard.ibd", MYSQL9_FIXTURE_DIR);
    let opts = idb::cli::schema::SchemaOptions {
        file: path,
        verbose: true,
        json: false,
        page_size: None,
        keyring: None,
        mmap: false,
    };

    let mut out = Vec::new();
    idb::cli::schema::execute(&opts, &mut out).expect("execute schema -v");
    let output = String::from_utf8(out).expect("valid utf8");

    assert!(output.contains("Schema:"), "verbose should show schema name");
    assert!(output.contains("Table:"), "verbose should show table name");
    assert!(output.contains("Engine:"), "verbose should show engine");
    assert!(output.contains("Columns (3):"), "verbose should show column count");
    assert!(output.contains("Indexes (1):"), "verbose should show index count");
    assert!(output.contains("DDL:"), "verbose should show DDL header");
}

#[test]
fn test_schema_pre80_inference_synthetic() {
    use idb::innodb::schema::infer_schema_from_pages;

    // Build a synthetic tablespace with 2 INDEX pages
    let page0 = build_fsp_hdr_page(100, 4);
    let page1 = build_index_page(1, 100, 2000);
    let page2 = build_index_page(2, 100, 3000);
    let page3 = build_allocated_page(3, 100);

    let tmp = write_tablespace(&[page0, page1, page2, page3]);
    let mut ts = Tablespace::open(tmp.path()).expect("open synthetic");

    let inferred = infer_schema_from_pages(&mut ts).expect("infer schema");
    assert_eq!(inferred.record_format, "COMPACT");
    assert_eq!(inferred.indexes.len(), 1, "should detect one index");
    assert_eq!(inferred.indexes[0].index_id, 42); // from build_index_page
    assert_eq!(inferred.indexes[0].leaf_pages, 2);
}
