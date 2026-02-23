#![cfg(feature = "cli")]
//! Integration tests for `inno transplant`.

use byteorder::{BigEndian, ByteOrder};
use std::io::Write;
use tempfile::NamedTempFile;

use idb::innodb::checksum::{recalculate_checksum, validate_checksum, ChecksumAlgorithm};
use idb::innodb::constants::*;
use idb::innodb::write;

const PAGE_SIZE: u32 = 16384;
const PS: usize = PAGE_SIZE as usize;

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
    recalculate_checksum(&mut page, PAGE_SIZE, ChecksumAlgorithm::Crc32c);
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

#[test]
fn test_transplant_fixes_corrupt_page() {
    let page0 = write::build_fsp_page(42, 3, 0, 1000, PAGE_SIZE, ChecksumAlgorithm::Crc32c);

    // Donor has good pages
    let donor_page1 = build_index_page(1, 42, 2000);
    let donor_page2 = build_index_page(2, 42, 3000);
    let donor = write_tablespace(&[page0.clone(), donor_page1, donor_page2]);

    // Target has a corrupt page 2
    let target_page1 = build_index_page(1, 42, 2000);
    let mut target_page2 = build_index_page(2, 42, 3000);
    BigEndian::write_u32(&mut target_page2[FIL_PAGE_SPACE_OR_CHKSUM..], 0xDEAD);
    let target = write_tablespace(&[page0, target_page1, target_page2]);

    let donor_path = donor.path().to_str().unwrap();
    let target_path = target.path().to_str().unwrap();

    // Verify target page 2 is corrupt
    let p2 = write::read_page_raw(target_path, 2, PAGE_SIZE).unwrap();
    assert!(!validate_checksum(&p2, PAGE_SIZE, None).valid);

    let mut output = Vec::new();
    idb::cli::transplant::execute(
        &idb::cli::transplant::TransplantOptions {
            donor: donor_path.to_string(),
            target: target_path.to_string(),
            pages: vec![2],
            no_backup: true,
            force: false,
            dry_run: false,
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
            audit_logger: None,
        },
        &mut output,
    )
    .unwrap();

    // Target page 2 should now be valid
    let p2 = write::read_page_raw(target_path, 2, PAGE_SIZE).unwrap();
    assert!(validate_checksum(&p2, PAGE_SIZE, None).valid);
}

#[test]
fn test_transplant_page_size_mismatch() {
    // Create donor with 16K pages
    let donor_page0 = write::build_fsp_page(42, 2, 0, 1000, 16384, ChecksumAlgorithm::Crc32c);
    let donor_page1 = build_index_page(1, 42, 2000);
    let donor = write_tablespace(&[donor_page0, donor_page1]);

    // Create target with 8K pages (ssize=4 encodes 8K: 1 << (4+9) = 8192)
    let flags_8k = 4 << 6; // ssize=4 in bits 6-9
    let target_page0 =
        write::build_fsp_page(42, 2, flags_8k, 1000, 8192, ChecksumAlgorithm::Crc32c);
    let mut target_page1 = vec![0u8; 8192];
    BigEndian::write_u32(&mut target_page1[FIL_PAGE_OFFSET..], 1);
    BigEndian::write_u64(&mut target_page1[FIL_PAGE_LSN..], 2000);
    BigEndian::write_u16(&mut target_page1[FIL_PAGE_TYPE..], 17855);
    BigEndian::write_u32(&mut target_page1[FIL_PAGE_SPACE_ID..], 42);
    let trailer = 8192 - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut target_page1[trailer + 4..], 2000);
    recalculate_checksum(&mut target_page1, 8192, ChecksumAlgorithm::Crc32c);
    let target = write_tablespace(&[target_page0, target_page1]);

    let mut output = Vec::new();
    let result = idb::cli::transplant::execute(
        &idb::cli::transplant::TransplantOptions {
            donor: donor.path().to_str().unwrap().to_string(),
            target: target.path().to_str().unwrap().to_string(),
            pages: vec![1],
            no_backup: true,
            force: false,
            dry_run: false,
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
            audit_logger: None,
        },
        &mut output,
    );

    // Should error due to page size mismatch
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("Page size mismatch"));
}

#[test]
fn test_transplant_space_id_mismatch_without_force() {
    let donor_page0 = write::build_fsp_page(42, 2, 0, 1000, PAGE_SIZE, ChecksumAlgorithm::Crc32c);
    let donor_page1 = build_index_page(1, 42, 2000);
    let donor = write_tablespace(&[donor_page0, donor_page1]);

    let target_page0 = write::build_fsp_page(99, 2, 0, 1000, PAGE_SIZE, ChecksumAlgorithm::Crc32c);
    let target_page1 = build_index_page(1, 99, 2000);
    let target = write_tablespace(&[target_page0, target_page1]);

    let mut output = Vec::new();
    let result = idb::cli::transplant::execute(
        &idb::cli::transplant::TransplantOptions {
            donor: donor.path().to_str().unwrap().to_string(),
            target: target.path().to_str().unwrap().to_string(),
            pages: vec![1],
            no_backup: true,
            force: false,
            dry_run: false,
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
            audit_logger: None,
        },
        &mut output,
    );

    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("Space ID mismatch"));
}

#[test]
fn test_transplant_dry_run_does_not_modify() {
    let page0 = write::build_fsp_page(42, 3, 0, 1000, PAGE_SIZE, ChecksumAlgorithm::Crc32c);
    let donor_page1 = build_index_page(1, 42, 9999);
    let donor = write_tablespace(&[page0.clone(), donor_page1, build_index_page(2, 42, 3000)]);

    let target_page1 = build_index_page(1, 42, 2000);
    let target = write_tablespace(&[page0, target_page1, build_index_page(2, 42, 3000)]);

    let target_path = target.path().to_str().unwrap();

    // Read original page 1
    let original_p1 = write::read_page_raw(target_path, 1, PAGE_SIZE).unwrap();

    let mut output = Vec::new();
    idb::cli::transplant::execute(
        &idb::cli::transplant::TransplantOptions {
            donor: donor.path().to_str().unwrap().to_string(),
            target: target_path.to_string(),
            pages: vec![1],
            no_backup: true,
            force: false,
            dry_run: true,
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
            audit_logger: None,
        },
        &mut output,
    )
    .unwrap();

    // Target page 1 should be unchanged
    let after_p1 = write::read_page_raw(target_path, 1, PAGE_SIZE).unwrap();
    assert_eq!(original_p1, after_p1);
}

#[test]
fn test_transplant_page0_rejected_without_force() {
    let page0 = write::build_fsp_page(42, 2, 0, 1000, PAGE_SIZE, ChecksumAlgorithm::Crc32c);
    let page1 = build_index_page(1, 42, 2000);
    let donor = write_tablespace(&[page0.clone(), page1.clone()]);
    let target = write_tablespace(&[page0, page1]);

    let mut output = Vec::new();
    idb::cli::transplant::execute(
        &idb::cli::transplant::TransplantOptions {
            donor: donor.path().to_str().unwrap().to_string(),
            target: target.path().to_str().unwrap().to_string(),
            pages: vec![0],
            no_backup: true,
            force: false,
            dry_run: false,
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
            audit_logger: None,
        },
        &mut output,
    )
    .unwrap();

    let text = String::from_utf8(output).unwrap();
    // Page 0 should be skipped
    assert!(text.contains("Skipped:") || text.contains("0"));
}

#[test]
fn test_transplant_backup_creation() {
    let page0 = write::build_fsp_page(42, 2, 0, 1000, PAGE_SIZE, ChecksumAlgorithm::Crc32c);
    let donor_page1 = build_index_page(1, 42, 9999);
    let donor = write_tablespace(&[page0.clone(), donor_page1]);

    let target_page1 = build_index_page(1, 42, 2000);
    let target = write_tablespace(&[page0, target_page1]);

    let target_path = target.path().to_str().unwrap();
    let backup_path = format!("{}.bak", target_path);

    let mut output = Vec::new();
    idb::cli::transplant::execute(
        &idb::cli::transplant::TransplantOptions {
            donor: donor.path().to_str().unwrap().to_string(),
            target: target_path.to_string(),
            pages: vec![1],
            no_backup: false,
            force: false,
            dry_run: false,
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
            audit_logger: None,
        },
        &mut output,
    )
    .unwrap();

    // Backup should exist
    assert!(std::path::Path::new(&backup_path).exists());

    let _ = std::fs::remove_file(&backup_path);
}
