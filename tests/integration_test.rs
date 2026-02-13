//! Integration tests for idb-utils.
//!
//! These tests construct synthetic InnoDB tablespace files (.ibd) with valid
//! page structures and run the full parsing/validation pipeline against them.

use byteorder::{BigEndian, ByteOrder};
use std::io::Write;
use tempfile::NamedTempFile;

use idb::innodb::checksum::{validate_checksum, validate_lsn, ChecksumAlgorithm};
use idb::innodb::constants::*;
use idb::innodb::log::{LOG_BLOCK_CHECKSUM_OFFSET, LOG_BLOCK_SIZE};
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
    let parsed: serde_json::Value =
        serde_json::from_str(&output).expect("tsid --json should produce valid JSON");
    assert!(parsed.get("tablespaces").is_some());
}

// ========== Redo log builder helpers ==========

/// Build a redo log header block (block 0) with valid CRC-32C.
fn build_log_header_block(group_id: u32, start_lsn: u64, file_no: u32, creator: &str) -> Vec<u8> {
    let mut block = vec![0u8; LOG_BLOCK_SIZE];
    BigEndian::write_u32(&mut block[0..], group_id);
    BigEndian::write_u64(&mut block[4..], start_lsn);
    BigEndian::write_u32(&mut block[12..], file_no);
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
fn build_log_data_block(block_no: u32, data_len: u16, checkpoint_no: u32) -> Vec<u8> {
    let mut block = vec![0u8; LOG_BLOCK_SIZE];
    BigEndian::write_u32(&mut block[0..], block_no);
    BigEndian::write_u16(&mut block[4..], data_len);
    BigEndian::write_u16(&mut block[6..], 14); // first_rec_group = header size
    BigEndian::write_u32(&mut block[8..], checkpoint_no);
    // Fill some data bytes
    if data_len as usize > 14 {
        for i in 14..(data_len as usize).min(LOG_BLOCK_SIZE - 4) {
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
    let b3 = build_log_data_block(7, 14, 42); // empty (data_len == header size)

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
    };

    let mut out = Vec::new();
    let result = idb::cli::sdi::execute(&opts, &mut out);
    assert!(result.is_err(), "sdi should fail on nonexistent file");
}

#[test]
fn test_log_no_empty_filter() {
    // 3 data blocks: 2 with data, 1 empty (data_len==14)
    let b1 = build_log_data_block(5, 100, 42);
    let b2 = build_log_data_block(6, 14, 42); // empty: data_len == LOG_BLOCK_HDR_SIZE
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
    // Verify all remaining blocks have data_len > 14
    for block in blocks {
        assert!(
            block["data_len"].as_u64().unwrap() > 14,
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
