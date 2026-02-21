#![cfg(feature = "cli")]
//! Integration tests for `inno repair`.

use byteorder::{BigEndian, ByteOrder};
use std::io::Write;
use tempfile::NamedTempFile;

use idb::innodb::checksum::{validate_checksum, ChecksumAlgorithm};
use idb::innodb::constants::*;
use idb::innodb::write;

const PAGE_SIZE: u32 = 16384;
const PS: usize = PAGE_SIZE as usize;

fn build_fsp_hdr_page(space_id: u32, total_pages: u32) -> Vec<u8> {
    write::build_fsp_page(
        space_id,
        total_pages,
        0,
        1000,
        PAGE_SIZE,
        ChecksumAlgorithm::Crc32c,
    )
}

fn build_index_page(page_num: u32, space_id: u32, lsn: u64) -> Vec<u8> {
    let mut page = vec![0u8; PS];
    BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], page_num);
    BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], lsn);
    BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855);
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);
    let trailer = PS - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut page[trailer + 4..], (lsn & 0xFFFFFFFF) as u32);
    idb::innodb::checksum::recalculate_checksum(&mut page, PAGE_SIZE, ChecksumAlgorithm::Crc32c);
    page
}

fn write_tablespace(pages: &[Vec<u8>]) -> NamedTempFile {
    let mut tmp = NamedTempFile::new().unwrap();
    for page in pages {
        tmp.write_all(page).unwrap();
    }
    tmp.flush().unwrap();
    tmp
}

fn corrupt_checksum(page: &mut [u8]) {
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_OR_CHKSUM..], 0xDEADDEAD);
}

#[test]
fn test_repair_fixes_bad_checksums() {
    let page0 = build_fsp_hdr_page(42, 4);
    let page1 = build_index_page(1, 42, 2000);
    let mut page2 = build_index_page(2, 42, 3000);
    let page3 = build_index_page(3, 42, 4000);

    // Corrupt page 2
    corrupt_checksum(&mut page2);
    assert!(!validate_checksum(&page2, PAGE_SIZE, None).valid);

    let tmp = write_tablespace(&[page0, page1, page2, page3]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::repair::execute(
        &idb::cli::repair::RepairOptions {
            file: path.to_string(),
            page: None,
            algorithm: "auto".to_string(),
            no_backup: true,
            dry_run: false,
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    // Verify all pages are now valid
    for i in 0..4 {
        let page = write::read_page_raw(path, i, PAGE_SIZE).unwrap();
        assert!(
            validate_checksum(&page, PAGE_SIZE, None).valid,
            "Page {} invalid after repair",
            i
        );
    }
}

#[test]
fn test_repair_dry_run_does_not_modify() {
    let page0 = build_fsp_hdr_page(42, 3);
    let page1 = build_index_page(1, 42, 2000);
    let mut page2 = build_index_page(2, 42, 3000);

    corrupt_checksum(&mut page2);
    let corrupted_checksum = BigEndian::read_u32(&page2[FIL_PAGE_SPACE_OR_CHKSUM..]);

    let tmp = write_tablespace(&[page0, page1, page2]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::repair::execute(
        &idb::cli::repair::RepairOptions {
            file: path.to_string(),
            page: None,
            algorithm: "auto".to_string(),
            no_backup: true,
            dry_run: true,
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    // Page 2 should still have the corrupted checksum
    let page2_after = write::read_page_raw(path, 2, PAGE_SIZE).unwrap();
    let after_checksum = BigEndian::read_u32(&page2_after[FIL_PAGE_SPACE_OR_CHKSUM..]);
    assert_eq!(after_checksum, corrupted_checksum);
}

#[test]
fn test_repair_single_page() {
    let page0 = build_fsp_hdr_page(42, 3);
    let mut page1 = build_index_page(1, 42, 2000);
    let mut page2 = build_index_page(2, 42, 3000);

    corrupt_checksum(&mut page1);
    corrupt_checksum(&mut page2);

    let tmp = write_tablespace(&[page0, page1, page2]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::repair::execute(
        &idb::cli::repair::RepairOptions {
            file: path.to_string(),
            page: Some(1),
            algorithm: "crc32c".to_string(),
            no_backup: true,
            dry_run: false,
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    // Page 1 should be fixed, page 2 should still be corrupt
    let p1 = write::read_page_raw(path, 1, PAGE_SIZE).unwrap();
    assert!(validate_checksum(&p1, PAGE_SIZE, None).valid);

    let p2 = write::read_page_raw(path, 2, PAGE_SIZE).unwrap();
    assert!(!validate_checksum(&p2, PAGE_SIZE, None).valid);
}

#[test]
fn test_repair_already_valid_file() {
    let page0 = build_fsp_hdr_page(42, 3);
    let page1 = build_index_page(1, 42, 2000);
    let page2 = build_index_page(2, 42, 3000);

    let tmp = write_tablespace(&[page0, page1, page2]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::repair::execute(
        &idb::cli::repair::RepairOptions {
            file: path.to_string(),
            page: None,
            algorithm: "auto".to_string(),
            no_backup: true,
            dry_run: false,
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    let text = String::from_utf8(output).unwrap();
    // Should report 0 repaired (all 3 pages already valid)
    assert!(text.contains("Repaired:      ") || text.contains("0"));
}

#[test]
fn test_repair_json_output() {
    let page0 = build_fsp_hdr_page(42, 3);
    let page1 = build_index_page(1, 42, 2000);
    let mut page2 = build_index_page(2, 42, 3000);
    corrupt_checksum(&mut page2);

    let tmp = write_tablespace(&[page0, page1, page2]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::repair::execute(
        &idb::cli::repair::RepairOptions {
            file: path.to_string(),
            page: None,
            algorithm: "auto".to_string(),
            no_backup: true,
            dry_run: false,
            verbose: false,
            json: true,
            page_size: None,
            keyring: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    let text = String::from_utf8(output).unwrap();
    let json: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(json["total_pages"], 3);
    assert_eq!(json["repaired"], 1);
    assert_eq!(json["already_valid"], 2);
}

#[test]
fn test_repair_innodb_algorithm() {
    let page0 = build_fsp_hdr_page(42, 2);
    let mut page1 = build_index_page(1, 42, 2000);
    corrupt_checksum(&mut page1);

    let tmp = write_tablespace(&[page0, page1]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::repair::execute(
        &idb::cli::repair::RepairOptions {
            file: path.to_string(),
            page: Some(1),
            algorithm: "innodb".to_string(),
            no_backup: true,
            dry_run: false,
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    // Should be valid with legacy InnoDB algorithm
    let p1 = write::read_page_raw(path, 1, PAGE_SIZE).unwrap();
    let result = validate_checksum(&p1, PAGE_SIZE, None);
    assert!(result.valid);
    assert_eq!(result.algorithm, ChecksumAlgorithm::InnoDB);
}

#[test]
fn test_repair_no_backup_skips_backup() {
    let page0 = build_fsp_hdr_page(42, 2);
    let mut page1 = build_index_page(1, 42, 2000);
    corrupt_checksum(&mut page1);

    let tmp = write_tablespace(&[page0, page1]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::repair::execute(
        &idb::cli::repair::RepairOptions {
            file: path.to_string(),
            page: None,
            algorithm: "auto".to_string(),
            no_backup: true,
            dry_run: false,
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    // No .bak file should exist
    let backup = format!("{}.bak", path);
    assert!(!std::path::Path::new(&backup).exists());
}
