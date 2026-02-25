#![cfg(feature = "cli")]
//! Integration tests for `inno health`.

use byteorder::{BigEndian, ByteOrder};
use std::io::Write;
use tempfile::NamedTempFile;

use idb::innodb::checksum::ChecksumAlgorithm;
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

fn build_index_page(
    page_num: u32,
    space_id: u32,
    lsn: u64,
    index_id: u64,
    level: u16,
    n_recs: u16,
    heap_top: u16,
    garbage: u16,
) -> Vec<u8> {
    let mut page = vec![0u8; PS];

    // FIL header
    BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], page_num);
    BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], lsn);
    BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855); // INDEX
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);

    // INDEX page header
    let ph = FIL_PAGE_DATA;
    BigEndian::write_u16(&mut page[ph + PAGE_N_DIR_SLOTS..], 2);
    BigEndian::write_u16(&mut page[ph + PAGE_HEAP_TOP..], heap_top);
    BigEndian::write_u16(&mut page[ph + PAGE_N_HEAP..], 0x8002); // compact
    BigEndian::write_u16(&mut page[ph + PAGE_GARBAGE..], garbage);
    BigEndian::write_u16(&mut page[ph + PAGE_N_RECS..], n_recs);
    BigEndian::write_u16(&mut page[ph + PAGE_LEVEL..], level);
    BigEndian::write_u64(&mut page[ph + PAGE_INDEX_ID..], index_id);

    // FIL trailer
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

#[test]
fn test_health_basic_text_output() {
    let page0 = build_fsp_hdr_page(42, 4);
    let page1 = build_index_page(1, 42, 2000, 100, 0, 50, 8000, 0);
    let page2 = build_index_page(2, 42, 3000, 100, 0, 50, 8000, 0);
    let page3 = build_index_page(3, 42, 4000, 100, 1, 2, 300, 0); // non-leaf

    let tmp = write_tablespace(&[page0, page1, page2, page3]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::health::execute(
        &idb::cli::health::HealthOptions {
            file: path.to_string(),
            verbose: false,
            json: false,
            csv: false,
            prometheus: false,
            page_size: None,
            keyring: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("Index 100"));
    assert!(text.contains("Tree depth"));
    assert!(text.contains("Fill factor"));
    assert!(text.contains("Fragmentation"));
    assert!(text.contains("Summary"));
}

#[test]
fn test_health_json_output() {
    let page0 = build_fsp_hdr_page(42, 3);
    let page1 = build_index_page(1, 42, 2000, 100, 0, 50, 8000, 100);
    let page2 = build_index_page(2, 42, 3000, 200, 0, 30, 4000, 0);

    let tmp = write_tablespace(&[page0, page1, page2]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::health::execute(
        &idb::cli::health::HealthOptions {
            file: path.to_string(),
            verbose: false,
            json: true,
            csv: false,
            prometheus: false,
            page_size: None,
            keyring: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert!(json.get("file").is_some());
    assert!(json.get("summary").is_some());
    assert!(json.get("indexes").is_some());

    let indexes = json["indexes"].as_array().unwrap();
    assert_eq!(indexes.len(), 2);

    // Check index 100
    let idx100 = indexes.iter().find(|i| i["index_id"] == 100).unwrap();
    assert_eq!(idx100["total_pages"], 1);
    assert_eq!(idx100["leaf_pages"], 1);
    assert_eq!(idx100["total_records"], 50);
    assert!(idx100["total_garbage_bytes"].as_u64().unwrap() > 0);

    // Check summary
    let summary = &json["summary"];
    assert_eq!(summary["total_pages"], 3);
    assert_eq!(summary["index_pages"], 2);
    assert_eq!(summary["index_count"], 2);
}

#[test]
fn test_health_verbose_output() {
    let page0 = build_fsp_hdr_page(42, 3);
    let page1 = build_index_page(1, 42, 2000, 100, 0, 50, 8000, 0);
    let page2 = build_index_page(2, 42, 3000, 100, 0, 0, 200, 0); // empty leaf

    let tmp = write_tablespace(&[page0, page1, page2]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::health::execute(
        &idb::cli::health::HealthOptions {
            file: path.to_string(),
            verbose: true,
            json: false,
            csv: false,
            prometheus: false,
            page_size: None,
            keyring: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("Total records"));
    assert!(text.contains("Empty leaves"));
}

#[test]
fn test_health_no_index_pages() {
    let page0 = build_fsp_hdr_page(42, 2);
    let page1 = vec![0u8; PS]; // empty page

    let tmp = write_tablespace(&[page0, page1]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::health::execute(
        &idb::cli::health::HealthOptions {
            file: path.to_string(),
            verbose: false,
            json: false,
            csv: false,
            prometheus: false,
            page_size: None,
            keyring: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("No INDEX pages found"));
}

#[test]
fn test_health_prometheus_output() {
    let page0 = build_fsp_hdr_page(42, 4);
    let page1 = build_index_page(1, 42, 2000, 100, 0, 50, 8000, 0);
    let page2 = build_index_page(2, 42, 3000, 100, 0, 50, 8000, 0);
    let page3 = build_index_page(3, 42, 4000, 100, 1, 2, 300, 0);

    let tmp = write_tablespace(&[page0, page1, page2, page3]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::health::execute(
        &idb::cli::health::HealthOptions {
            file: path.to_string(),
            verbose: false,
            json: false,
            csv: false,
            prometheus: true,
            page_size: None,
            keyring: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    let text = String::from_utf8(output).unwrap();
    assert!(
        text.contains("# TYPE innodb_fill_factor gauge"),
        "missing innodb_fill_factor TYPE line"
    );
    assert!(
        text.contains("# TYPE innodb_fragmentation_ratio gauge"),
        "missing innodb_fragmentation_ratio TYPE line"
    );
    assert!(
        text.contains("# TYPE innodb_pages gauge"),
        "missing innodb_pages TYPE line"
    );
    assert!(
        text.contains("innodb_scan_duration_seconds"),
        "missing innodb_scan_duration_seconds metric"
    );
    assert!(
        text.contains("# TYPE innodb_scan_duration_seconds gauge"),
        "missing innodb_scan_duration_seconds TYPE line"
    );
}

#[test]
fn test_health_multiple_indexes_json() {
    let page0 = build_fsp_hdr_page(42, 5);
    let page1 = build_index_page(1, 42, 2000, 100, 0, 50, 8000, 0);
    let page2 = build_index_page(2, 42, 3000, 100, 0, 50, 8000, 0);
    let page3 = build_index_page(3, 42, 4000, 200, 0, 30, 4000, 200);
    let page4 = build_index_page(4, 42, 5000, 300, 0, 10, 2000, 0);

    let tmp = write_tablespace(&[page0, page1, page2, page3, page4]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::health::execute(
        &idb::cli::health::HealthOptions {
            file: path.to_string(),
            verbose: false,
            json: true,
            csv: false,
            prometheus: false,
            page_size: None,
            keyring: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    let indexes = json["indexes"].as_array().unwrap();
    assert_eq!(indexes.len(), 3);
    assert_eq!(json["summary"]["index_count"], 3);
}
