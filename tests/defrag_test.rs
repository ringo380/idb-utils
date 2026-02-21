#![cfg(feature = "cli")]
//! Integration tests for `inno defrag`.

use byteorder::{BigEndian, ByteOrder};
use std::io::Write;
use tempfile::NamedTempFile;

use idb::innodb::checksum::{recalculate_checksum, validate_checksum, ChecksumAlgorithm};
use idb::innodb::constants::*;
use idb::innodb::tablespace::Tablespace;
use idb::innodb::write;

const PAGE_SIZE: u32 = 16384;
const PS: usize = PAGE_SIZE as usize;

fn build_index_page(page_num: u32, space_id: u32, lsn: u64, index_id: u64, level: u16) -> Vec<u8> {
    let mut page = vec![0u8; PS];
    BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], page_num);
    BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], lsn);
    BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855); // INDEX
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);

    // INDEX page header
    let ph = FIL_PAGE_DATA;
    BigEndian::write_u16(&mut page[ph + PAGE_N_DIR_SLOTS..], 2);
    BigEndian::write_u16(&mut page[ph + PAGE_N_HEAP..], 0x8002);
    BigEndian::write_u16(&mut page[ph + PAGE_LEVEL..], level);
    BigEndian::write_u64(&mut page[ph + PAGE_INDEX_ID..], index_id);

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
fn test_defrag_removes_empty_pages() {
    let page0 = write::build_fsp_page(42, 10, 0, 1000, PAGE_SIZE, ChecksumAlgorithm::Crc32c);
    let page1 = build_index_page(1, 42, 2000, 100, 0);
    let empty1 = vec![0u8; PS]; // empty
    let page3 = build_index_page(3, 42, 3000, 100, 0);
    let empty2 = vec![0u8; PS]; // empty
    let page5 = build_index_page(5, 42, 4000, 100, 0);
    let empty3 = vec![0u8; PS]; // empty
    let page7 = build_index_page(7, 42, 5000, 200, 0);
    let page8 = build_index_page(8, 42, 6000, 200, 0);
    let page9 = build_index_page(9, 42, 7000, 200, 0);

    let tmp = write_tablespace(&[
        page0, page1, empty1, page3, empty2, page5, empty3, page7, page8, page9,
    ]);
    let path = tmp.path().to_str().unwrap();

    let output_tmp = NamedTempFile::new().unwrap();
    let output_path = output_tmp.path().to_str().unwrap().to_string();
    drop(output_tmp);

    let mut output = Vec::new();
    idb::cli::defrag::execute(
        &idb::cli::defrag::DefragOptions {
            file: path.to_string(),
            output: output_path.clone(),
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    // Should have 7 pages: page0 + 6 data pages (3 empty removed)
    let ts = Tablespace::open(&output_path).unwrap();
    assert_eq!(ts.page_count(), 7);

    // All checksums valid
    for i in 0..7 {
        let page = write::read_page_raw(&output_path, i, PAGE_SIZE).unwrap();
        assert!(
            validate_checksum(&page, PAGE_SIZE, None).valid,
            "Page {} invalid in defrag output",
            i
        );
    }

    let _ = std::fs::remove_file(&output_path);
}

#[test]
fn test_defrag_sorts_index_pages_by_index_id() {
    let page0 = write::build_fsp_page(42, 5, 0, 1000, PAGE_SIZE, ChecksumAlgorithm::Crc32c);
    // Create pages with different index_ids in reverse order
    let page1 = build_index_page(1, 42, 2000, 300, 0); // index 300
    let page2 = build_index_page(2, 42, 3000, 100, 0); // index 100
    let page3 = build_index_page(3, 42, 4000, 200, 0); // index 200
    let page4 = build_index_page(4, 42, 5000, 100, 0); // index 100

    let tmp = write_tablespace(&[page0, page1, page2, page3, page4]);
    let path = tmp.path().to_str().unwrap();

    let output_tmp = NamedTempFile::new().unwrap();
    let output_path = output_tmp.path().to_str().unwrap().to_string();
    drop(output_tmp);

    let mut output = Vec::new();
    idb::cli::defrag::execute(
        &idb::cli::defrag::DefragOptions {
            file: path.to_string(),
            output: output_path.clone(),
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    // Verify pages are sorted by index_id
    let ts = Tablespace::open(&output_path).unwrap();
    assert_eq!(ts.page_count(), 5);

    // Read INDEX pages (1-4) and check index_id ordering
    let mut index_ids = Vec::new();
    for i in 1..5 {
        let page = write::read_page_raw(&output_path, i, PAGE_SIZE).unwrap();
        let ph = FIL_PAGE_DATA;
        let index_id = BigEndian::read_u64(&page[ph + PAGE_INDEX_ID..]);
        index_ids.push(index_id);
    }

    // Should be sorted: 100, 100, 200, 300
    assert_eq!(index_ids, vec![100, 100, 200, 300]);

    let _ = std::fs::remove_file(&output_path);
}

#[test]
fn test_defrag_fixes_prev_next_chains() {
    let page0 = write::build_fsp_page(42, 4, 0, 1000, PAGE_SIZE, ChecksumAlgorithm::Crc32c);
    // Three pages from same index, same level
    let page1 = build_index_page(1, 42, 2000, 100, 0);
    let page2 = build_index_page(2, 42, 3000, 100, 0);
    let page3 = build_index_page(3, 42, 4000, 100, 0);

    let tmp = write_tablespace(&[page0, page1, page2, page3]);
    let path = tmp.path().to_str().unwrap();

    let output_tmp = NamedTempFile::new().unwrap();
    let output_path = output_tmp.path().to_str().unwrap().to_string();
    drop(output_tmp);

    let mut output = Vec::new();
    idb::cli::defrag::execute(
        &idb::cli::defrag::DefragOptions {
            file: path.to_string(),
            output: output_path.clone(),
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    // Check prev/next chain: page1->page2->page3
    let p1 = write::read_page_raw(&output_path, 1, PAGE_SIZE).unwrap();
    let p2 = write::read_page_raw(&output_path, 2, PAGE_SIZE).unwrap();
    let p3 = write::read_page_raw(&output_path, 3, PAGE_SIZE).unwrap();

    // Page 1: prev=FIL_NULL, next=2
    assert_eq!(BigEndian::read_u32(&p1[FIL_PAGE_PREV..]), FIL_NULL);
    assert_eq!(BigEndian::read_u32(&p1[FIL_PAGE_NEXT..]), 2);

    // Page 2: prev=1, next=3
    assert_eq!(BigEndian::read_u32(&p2[FIL_PAGE_PREV..]), 1);
    assert_eq!(BigEndian::read_u32(&p2[FIL_PAGE_NEXT..]), 3);

    // Page 3: prev=2, next=FIL_NULL
    assert_eq!(BigEndian::read_u32(&p3[FIL_PAGE_PREV..]), 2);
    assert_eq!(BigEndian::read_u32(&p3[FIL_PAGE_NEXT..]), FIL_NULL);

    let _ = std::fs::remove_file(&output_path);
}

#[test]
fn test_defrag_json_output() {
    let page0 = write::build_fsp_page(42, 3, 0, 1000, PAGE_SIZE, ChecksumAlgorithm::Crc32c);
    let page1 = build_index_page(1, 42, 2000, 100, 0);
    let page2 = build_index_page(2, 42, 3000, 100, 0);

    let tmp = write_tablespace(&[page0, page1, page2]);
    let path = tmp.path().to_str().unwrap();

    let output_tmp = NamedTempFile::new().unwrap();
    let output_path = output_tmp.path().to_str().unwrap().to_string();
    drop(output_tmp);

    let mut output = Vec::new();
    idb::cli::defrag::execute(
        &idb::cli::defrag::DefragOptions {
            file: path.to_string(),
            output: output_path.clone(),
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
    assert_eq!(json["source_pages"], 3);
    assert_eq!(json["output_pages"], 3);
    assert_eq!(json["index_pages"], 2);
    assert_eq!(json["empty_removed"], 0);

    let _ = std::fs::remove_file(&output_path);
}
