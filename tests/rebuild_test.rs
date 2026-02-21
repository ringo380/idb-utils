#![cfg(feature = "cli")]
//! Integration tests for `inno recover --rebuild`.

use byteorder::{BigEndian, ByteOrder};
use std::io::Write;
use tempfile::NamedTempFile;

use idb::innodb::checksum::{recalculate_checksum, validate_checksum, ChecksumAlgorithm};
use idb::innodb::constants::*;
use idb::innodb::tablespace::Tablespace;
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
fn test_rebuild_from_corrupt_file() {
    let page0 = write::build_fsp_page(42, 5, 0, 1000, PAGE_SIZE, ChecksumAlgorithm::Crc32c);
    let page1 = build_index_page(1, 42, 2000);
    let page2 = build_index_page(2, 42, 3000);
    let mut page3 = build_index_page(3, 42, 4000);
    let page4 = build_index_page(4, 42, 5000);

    // Corrupt page 3
    BigEndian::write_u32(&mut page3[FIL_PAGE_SPACE_OR_CHKSUM..], 0xDEAD);

    let tmp = write_tablespace(&[page0, page1, page2, page3, page4]);
    let path = tmp.path().to_str().unwrap();

    // Create output file path
    let output_tmp = NamedTempFile::new().unwrap();
    let output_path = output_tmp.path().to_str().unwrap().to_string();
    drop(output_tmp);

    let mut output = Vec::new();
    idb::cli::recover::execute(
        &idb::cli::recover::RecoverOptions {
            file: path.to_string(),
            page: None,
            verbose: false,
            json: false,
            force: false,
            page_size: None,
            keyring: None,
            threads: 0,
            mmap: false,
            streaming: false,
            rebuild: Some(output_path.clone()),
        },
        &mut output,
    )
    .unwrap();

    // Rebuilt file should be openable
    let ts = Tablespace::open(&output_path).unwrap();
    // Should have 4 pages (page0 + 3 intact data pages, corrupt page3 excluded)
    assert_eq!(ts.page_count(), 4);

    // All pages should have valid checksums
    for i in 0..ts.page_count() {
        let page = write::read_page_raw(&output_path, i, PAGE_SIZE).unwrap();
        assert!(
            validate_checksum(&page, PAGE_SIZE, None).valid,
            "Page {} invalid in rebuilt file",
            i
        );
    }

    let _ = std::fs::remove_file(&output_path);
}

#[test]
fn test_rebuild_with_force_includes_corrupt() {
    let page0 = write::build_fsp_page(42, 3, 0, 1000, PAGE_SIZE, ChecksumAlgorithm::Crc32c);
    let page1 = build_index_page(1, 42, 2000);
    let mut page2 = build_index_page(2, 42, 3000);

    // Corrupt page 2
    BigEndian::write_u32(&mut page2[FIL_PAGE_SPACE_OR_CHKSUM..], 0xDEAD);

    let tmp = write_tablespace(&[page0, page1, page2]);
    let path = tmp.path().to_str().unwrap();

    let output_tmp = NamedTempFile::new().unwrap();
    let output_path = output_tmp.path().to_str().unwrap().to_string();
    drop(output_tmp);

    let mut output = Vec::new();
    idb::cli::recover::execute(
        &idb::cli::recover::RecoverOptions {
            file: path.to_string(),
            page: None,
            verbose: false,
            json: false,
            force: true,
            page_size: None,
            keyring: None,
            threads: 0,
            mmap: false,
            streaming: false,
            rebuild: Some(output_path.clone()),
        },
        &mut output,
    )
    .unwrap();

    // With --force, corrupt page2 should also be included
    let ts = Tablespace::open(&output_path).unwrap();
    assert_eq!(ts.page_count(), 3); // page0 + page1 + corrupt page2

    let _ = std::fs::remove_file(&output_path);
}

#[test]
fn test_rebuild_all_valid_checksums() {
    let page0 = write::build_fsp_page(42, 3, 0, 1000, PAGE_SIZE, ChecksumAlgorithm::Crc32c);
    let page1 = build_index_page(1, 42, 2000);
    let page2 = build_index_page(2, 42, 3000);

    let tmp = write_tablespace(&[page0, page1, page2]);
    let path = tmp.path().to_str().unwrap();

    let output_tmp = NamedTempFile::new().unwrap();
    let output_path = output_tmp.path().to_str().unwrap().to_string();
    drop(output_tmp);

    let mut output = Vec::new();
    idb::cli::recover::execute(
        &idb::cli::recover::RecoverOptions {
            file: path.to_string(),
            page: None,
            verbose: false,
            json: false,
            force: false,
            page_size: None,
            keyring: None,
            threads: 0,
            mmap: false,
            streaming: false,
            rebuild: Some(output_path.clone()),
        },
        &mut output,
    )
    .unwrap();

    let ts = Tablespace::open(&output_path).unwrap();
    assert_eq!(ts.page_count(), 3);

    let _ = std::fs::remove_file(&output_path);
}
