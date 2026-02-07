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
    };

    let mut out = Vec::new();
    let result = idb::cli::pages::execute(&opts, &mut out);
    assert!(result.is_ok(), "pages execute should succeed: {:?}", result.err());
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
    };

    let mut out = Vec::new();
    let result = idb::cli::pages::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("INDEX"), "list mode should show INDEX pages");
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
    };

    let mut out = Vec::new();
    let result = idb::cli::pages::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    // Verify it's valid JSON
    let parsed: serde_json::Value = serde_json::from_str(&output).expect("output should be valid JSON");
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
    };

    let mut out = Vec::new();
    let result = idb::cli::corrupt::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("Verification"), "should show verification output");
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
    };

    let mut out = Vec::new();
    let result = idb::cli::find::execute(&opts, &mut out);
    assert!(result.is_ok(), "find should succeed: {:?}", result.err());
    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("Found page 1"), "should find page 1 in the .ibd file");
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
    };

    let mut out = Vec::new();
    let result = idb::cli::tsid::execute(&opts, &mut out);
    assert!(result.is_ok(), "tsid list should succeed: {:?}", result.err());
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
    };

    let mut out = Vec::new();
    let result = idb::cli::sdi::execute(&opts, &mut out);
    assert!(result.is_ok(), "sdi should succeed with no SDI pages: {:?}", result.err());
    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("No SDI pages found"), "should report no SDI pages");
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
    };

    let mut out = Vec::new();
    let result = idb::cli::dump::execute(&opts, &mut out);
    assert!(result.is_ok(), "dump raw should succeed: {:?}", result.err());
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
    };

    let mut out = Vec::new();
    let result = idb::cli::parse::execute(&opts, &mut out);
    assert!(result.is_ok());

    let output = String::from_utf8(out).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&output).expect("parse --json should produce valid JSON");
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
    };

    let mut out = Vec::new();
    let result = idb::cli::checksum::execute(&opts, &mut out);
    assert!(result.is_ok());

    let output = String::from_utf8(out).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&output).expect("checksum --json should produce valid JSON");
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
    };

    let mut out = Vec::new();
    let result = idb::cli::find::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&output).expect("find --json should produce valid JSON");
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
    };
    let mut out = Vec::new();
    idb::cli::recover::execute(&opts, &mut out).unwrap();
    let output = String::from_utf8(out).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
    // Without force, might have force_recoverable_records field
    assert!(parsed.get("force_recoverable_records").is_some() || parsed["recoverable_records"] == 0);

    // With force
    let opts_force = idb::cli::recover::RecoverOptions {
        file: tmp.path().to_string_lossy().to_string(),
        page: None,
        verbose: false,
        json: true,
        force: true,
        page_size: None,
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
    };

    let mut out = Vec::new();
    let result = idb::cli::tsid::execute(&opts, &mut out);
    assert!(result.is_ok());
    let output = String::from_utf8(out).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&output).expect("tsid --json should produce valid JSON");
    assert!(parsed.get("tablespaces").is_some());
}
