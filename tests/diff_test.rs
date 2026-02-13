//! Integration tests for `inno diff` subcommand.

use byteorder::{BigEndian, ByteOrder};
use std::io::Write;
use tempfile::NamedTempFile;

use idb::cli::diff::{execute, DiffOptions};
use idb::innodb::constants::*;

const PAGE_SIZE: u32 = 16384;
const PS: usize = PAGE_SIZE as usize;

// ── Test helpers (duplicated from integration_test.rs since they're private) ──

fn build_fsp_hdr_page(space_id: u32, total_pages: u32) -> Vec<u8> {
    let mut page = vec![0u8; PS];
    BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], 0);
    BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], 1000);
    BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 8); // FSP_HDR
    BigEndian::write_u64(&mut page[FIL_PAGE_FILE_FLUSH_LSN..], 1000);
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);
    let fsp = FIL_PAGE_DATA;
    BigEndian::write_u32(&mut page[fsp + FSP_SPACE_ID..], space_id);
    BigEndian::write_u32(&mut page[fsp + FSP_SIZE..], total_pages);
    BigEndian::write_u32(&mut page[fsp + FSP_FREE_LIMIT..], total_pages);
    BigEndian::write_u32(&mut page[fsp + FSP_SPACE_FLAGS..], 0);
    let trailer = PS - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut page[trailer + 4..], 1000 & 0xFFFFFFFF);
    write_crc32c_checksum(&mut page);
    page
}

fn build_index_page(page_num: u32, space_id: u32, lsn: u64) -> Vec<u8> {
    let mut page = vec![0u8; PS];
    BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], page_num);
    BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], lsn);
    BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855); // INDEX
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);
    let trailer = PS - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut page[trailer + 4..], (lsn & 0xFFFFFFFF) as u32);
    write_crc32c_checksum(&mut page);
    page
}

fn write_crc32c_checksum(page: &mut [u8]) {
    let end = PS - SIZE_FIL_TRAILER;
    let crc1 = crc32c::crc32c(&page[FIL_PAGE_OFFSET..FIL_PAGE_FILE_FLUSH_LSN]);
    let crc2 = crc32c::crc32c(&page[FIL_PAGE_DATA..end]);
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_OR_CHKSUM..], crc1 ^ crc2);
}

fn write_tablespace(pages: &[Vec<u8>]) -> NamedTempFile {
    let mut tmp = NamedTempFile::new().expect("create temp file");
    for page in pages {
        tmp.write_all(page).expect("write page");
    }
    tmp.flush().expect("flush");
    tmp
}

fn run_diff(opts: &DiffOptions) -> String {
    let mut buf = Vec::new();
    execute(opts, &mut buf).expect("execute diff");
    String::from_utf8(buf).expect("valid utf8")
}

fn default_opts(file1: &str, file2: &str) -> DiffOptions {
    DiffOptions {
        file1: file1.to_string(),
        file2: file2.to_string(),
        verbose: false,
        byte_ranges: false,
        page: None,
        json: false,
        page_size: None,
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[test]
fn test_diff_identical_files() {
    let pages = vec![
        build_fsp_hdr_page(1, 4),
        build_index_page(1, 1, 2000),
        build_index_page(2, 1, 3000),
        build_index_page(3, 1, 4000),
    ];
    let tmp1 = write_tablespace(&pages);
    let tmp2 = write_tablespace(&pages);

    let opts = default_opts(
        tmp1.path().to_str().unwrap(),
        tmp2.path().to_str().unwrap(),
    );
    let output = run_diff(&opts);

    assert!(output.contains("Identical pages:  4"));
    assert!(output.contains("Modified pages:   0"));
    assert!(output.contains("Only in file 1:   0"));
    assert!(output.contains("Only in file 2:   0"));
}

#[test]
fn test_diff_different_lsn() {
    let pages1 = vec![
        build_fsp_hdr_page(1, 2),
        build_index_page(1, 1, 2000),
    ];
    let pages2 = vec![
        build_fsp_hdr_page(1, 2),
        build_index_page(1, 1, 5000), // different LSN
    ];
    let tmp1 = write_tablespace(&pages1);
    let tmp2 = write_tablespace(&pages2);

    let mut opts = default_opts(
        tmp1.path().to_str().unwrap(),
        tmp2.path().to_str().unwrap(),
    );
    opts.verbose = true;
    let output = run_diff(&opts);

    assert!(output.contains("Identical pages:  1"));
    assert!(output.contains("Modified pages:   1"));
    assert!(output.contains("LSN: 2000 -> 5000"));
}

#[test]
fn test_diff_different_page_types() {
    let pages1 = vec![
        build_fsp_hdr_page(1, 2),
        build_index_page(1, 1, 2000), // INDEX
    ];
    // Build page with ALLOCATED type (type 0)
    let mut alloc_page = vec![0u8; PS];
    BigEndian::write_u32(&mut alloc_page[FIL_PAGE_OFFSET..], 1);
    BigEndian::write_u32(&mut alloc_page[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut alloc_page[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut alloc_page[FIL_PAGE_LSN..], 2000);
    BigEndian::write_u16(&mut alloc_page[FIL_PAGE_TYPE..], 0); // ALLOCATED
    BigEndian::write_u32(&mut alloc_page[FIL_PAGE_SPACE_ID..], 1);
    let trailer = PS - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut alloc_page[trailer + 4..], 2000u32);
    write_crc32c_checksum(&mut alloc_page);

    let pages2 = vec![build_fsp_hdr_page(1, 2), alloc_page];
    let tmp1 = write_tablespace(&pages1);
    let tmp2 = write_tablespace(&pages2);

    let mut opts = default_opts(
        tmp1.path().to_str().unwrap(),
        tmp2.path().to_str().unwrap(),
    );
    opts.verbose = true;
    let output = run_diff(&opts);

    assert!(output.contains("Page Type: INDEX -> ALLOCATED"));
}

#[test]
fn test_diff_different_page_counts() {
    let pages1 = vec![
        build_fsp_hdr_page(1, 4),
        build_index_page(1, 1, 2000),
        build_index_page(2, 1, 3000),
        build_index_page(3, 1, 4000),
    ];
    let pages2 = vec![
        build_fsp_hdr_page(1, 6),
        build_index_page(1, 1, 2000),
        build_index_page(2, 1, 3000),
        build_index_page(3, 1, 4000),
        build_index_page(4, 1, 5000),
        build_index_page(5, 1, 6000),
    ];
    let tmp1 = write_tablespace(&pages1);
    let tmp2 = write_tablespace(&pages2);

    let opts = default_opts(
        tmp1.path().to_str().unwrap(),
        tmp2.path().to_str().unwrap(),
    );
    let output = run_diff(&opts);

    // page 0 differs (different FSP size field), pages 1-3 identical
    assert!(output.contains("Only in file 2:   2"));
    // Pages 1, 2, 3 should be identical
    assert!(output.contains("Identical pages:  3"));
}

#[test]
fn test_diff_single_page_mode() {
    let pages1 = vec![
        build_fsp_hdr_page(1, 3),
        build_index_page(1, 1, 2000),
        build_index_page(2, 1, 3000),
    ];
    let pages2 = vec![
        build_fsp_hdr_page(1, 3),
        build_index_page(1, 1, 9000), // different LSN on page 1
        build_index_page(2, 1, 3000),
    ];
    let tmp1 = write_tablespace(&pages1);
    let tmp2 = write_tablespace(&pages2);

    // Only compare page 1
    let mut opts = default_opts(
        tmp1.path().to_str().unwrap(),
        tmp2.path().to_str().unwrap(),
    );
    opts.page = Some(1);
    opts.verbose = true;
    let output = run_diff(&opts);

    assert!(output.contains("Modified pages:   1"));
    assert!(output.contains("Identical pages:  0"));
    assert!(output.contains("LSN: 2000 -> 9000"));
    // Should NOT mention page 0 or page 2
    assert!(!output.contains("Page 0:"));
    assert!(!output.contains("Page 2:"));
}

#[test]
fn test_diff_byte_ranges() {
    let mut page1 = build_index_page(1, 1, 2000);
    let mut page2 = page1.clone();

    // Modify bytes 100-109 in page2
    for i in 100..110 {
        page2[i] = 0xFF;
    }
    // Modify bytes 200-204
    for i in 200..205 {
        page2[i] = 0xAA;
    }
    // Recalculate checksum for page2
    write_crc32c_checksum(&mut page2);
    // Also recalculate page1 (already valid, but be safe)
    write_crc32c_checksum(&mut page1);

    let pages1 = vec![build_fsp_hdr_page(1, 2), page1];
    let pages2 = vec![build_fsp_hdr_page(1, 2), page2];
    let tmp1 = write_tablespace(&pages1);
    let tmp2 = write_tablespace(&pages2);

    let mut opts = default_opts(
        tmp1.path().to_str().unwrap(),
        tmp2.path().to_str().unwrap(),
    );
    opts.verbose = true;
    opts.byte_ranges = true;
    let output = run_diff(&opts);

    assert!(output.contains("Byte diff ranges:"));
    assert!(output.contains("100-110 (10 bytes)"));
    assert!(output.contains("200-205 (5 bytes)"));
}

#[test]
fn test_diff_json_output() {
    let pages1 = vec![
        build_fsp_hdr_page(1, 2),
        build_index_page(1, 1, 2000),
    ];
    let pages2 = vec![
        build_fsp_hdr_page(1, 2),
        build_index_page(1, 1, 5000), // different LSN
    ];
    let tmp1 = write_tablespace(&pages1);
    let tmp2 = write_tablespace(&pages2);

    let mut opts = default_opts(
        tmp1.path().to_str().unwrap(),
        tmp2.path().to_str().unwrap(),
    );
    opts.json = true;
    let output = run_diff(&opts);

    let json: serde_json::Value = serde_json::from_str(&output).expect("valid JSON");
    assert_eq!(json["summary"]["identical"], 1);
    assert_eq!(json["summary"]["modified"], 1);
    assert_eq!(json["file1"]["page_size"], 16384);
    assert_eq!(json["file2"]["page_size"], 16384);
    assert_eq!(json["page_size_mismatch"], false);

    let modified = json["modified_pages"].as_array().unwrap();
    assert_eq!(modified.len(), 1);
    assert_eq!(modified[0]["page_number"], 1);
}

#[test]
fn test_diff_page_size_mismatch() {
    // Create file1 with 16K pages (auto-detected)
    let pages1 = vec![
        build_fsp_hdr_page(1, 2),
        build_index_page(1, 1, 2000),
    ];
    let tmp1 = write_tablespace(&pages1);

    // Create file2 as 8K pages using page_size override
    let small_ps: usize = 8192;
    let mut page0 = vec![0u8; small_ps];
    BigEndian::write_u32(&mut page0[FIL_PAGE_OFFSET..], 0);
    BigEndian::write_u32(&mut page0[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut page0[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut page0[FIL_PAGE_LSN..], 1000);
    BigEndian::write_u16(&mut page0[FIL_PAGE_TYPE..], 8); // FSP_HDR
    BigEndian::write_u32(&mut page0[FIL_PAGE_SPACE_ID..], 1);
    let fsp = FIL_PAGE_DATA;
    BigEndian::write_u32(&mut page0[fsp + FSP_SPACE_ID..], 1);
    BigEndian::write_u32(&mut page0[fsp + FSP_SIZE..], 2);
    // Set ssize=4 => 8192 byte pages
    BigEndian::write_u32(&mut page0[fsp + FSP_SPACE_FLAGS..], 4 << FSP_FLAGS_POS_PAGE_SSIZE);
    let trailer = small_ps - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut page0[trailer + 4..], 1000u32);
    // CRC for 8K page
    let end = small_ps - SIZE_FIL_TRAILER;
    let crc1 = crc32c::crc32c(&page0[FIL_PAGE_OFFSET..FIL_PAGE_FILE_FLUSH_LSN]);
    let crc2 = crc32c::crc32c(&page0[FIL_PAGE_DATA..end]);
    BigEndian::write_u32(&mut page0[FIL_PAGE_SPACE_OR_CHKSUM..], crc1 ^ crc2);

    let mut page1 = vec![0u8; small_ps];
    BigEndian::write_u32(&mut page1[FIL_PAGE_OFFSET..], 1);
    BigEndian::write_u32(&mut page1[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut page1[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut page1[FIL_PAGE_LSN..], 2000);
    BigEndian::write_u16(&mut page1[FIL_PAGE_TYPE..], 17855);
    BigEndian::write_u32(&mut page1[FIL_PAGE_SPACE_ID..], 1);
    let trailer = small_ps - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut page1[trailer + 4..], 2000u32);
    let end = small_ps - SIZE_FIL_TRAILER;
    let crc1 = crc32c::crc32c(&page1[FIL_PAGE_OFFSET..FIL_PAGE_FILE_FLUSH_LSN]);
    let crc2 = crc32c::crc32c(&page1[FIL_PAGE_DATA..end]);
    BigEndian::write_u32(&mut page1[FIL_PAGE_SPACE_OR_CHKSUM..], crc1 ^ crc2);

    let mut tmp2 = NamedTempFile::new().unwrap();
    tmp2.write_all(&page0).unwrap();
    tmp2.write_all(&page1).unwrap();
    tmp2.flush().unwrap();

    let opts = default_opts(
        tmp1.path().to_str().unwrap(),
        tmp2.path().to_str().unwrap(),
    );
    let output = run_diff(&opts);

    assert!(output.contains("WARNING: Page size mismatch"));
}
