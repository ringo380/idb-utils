#![cfg(feature = "cli")]
//! Integration tests for `inno find --corrupt`.

use byteorder::{BigEndian, ByteOrder};
use std::fs;
use std::io::Write;
use tempfile::TempDir;

use idb::innodb::checksum::{recalculate_checksum, ChecksumAlgorithm};
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
    BigEndian::write_u16(&mut page[ph + PAGE_HEAP_TOP..], 8000);
    BigEndian::write_u16(&mut page[ph + PAGE_N_HEAP..], 0x8002);
    BigEndian::write_u16(&mut page[ph + PAGE_N_RECS..], 50);
    BigEndian::write_u16(&mut page[ph + PAGE_LEVEL..], 0);
    BigEndian::write_u64(&mut page[ph + PAGE_INDEX_ID..], 100);

    // FIL trailer
    let trailer = PS - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut page[trailer + 4..], (lsn & 0xFFFFFFFF) as u32);

    recalculate_checksum(&mut page, PAGE_SIZE, ChecksumAlgorithm::Crc32c);
    page
}

fn write_ibd_file(dir: &std::path::Path, name: &str, pages: &[Vec<u8>]) {
    let path = dir.join(name);
    let mut f = fs::File::create(path).unwrap();
    for page in pages {
        f.write_all(page).unwrap();
    }
    f.flush().unwrap();
}

fn create_clean_datadir() -> TempDir {
    let dir = TempDir::new().unwrap();

    let db1 = dir.path().join("db1");
    fs::create_dir(&db1).unwrap();
    let p0 = build_fsp_hdr_page(10, 3);
    let p1 = build_index_page(1, 10, 2000);
    let p2 = build_index_page(2, 10, 3000);
    write_ibd_file(&db1, "orders.ibd", &[p0, p1, p2]);

    let db2 = dir.path().join("db2");
    fs::create_dir(&db2).unwrap();
    let p0 = build_fsp_hdr_page(20, 2);
    let p1 = build_index_page(1, 20, 2000);
    write_ibd_file(&db2, "users.ibd", &[p0, p1]);

    dir
}

fn find_opts(datadir: &str) -> idb::cli::find::FindOptions {
    idb::cli::find::FindOptions {
        datadir: datadir.to_string(),
        page: None,
        checksum: None,
        space_id: None,
        corrupt: false,
        first: false,
        json: false,
        page_size: None,
        threads: 0,
        mmap: false,
        depth: None,
    }
}

// -----------------------------------------------------------------------
// --corrupt on clean directory: no results
// -----------------------------------------------------------------------

#[test]
fn test_find_corrupt_all_clean() {
    let dir = create_clean_datadir();
    let datadir = dir.path().to_str().unwrap();

    let mut opts = find_opts(datadir);
    opts.corrupt = true;

    let mut output = Vec::new();
    let result = idb::cli::find::execute(&opts, &mut output);
    assert!(result.is_ok());

    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("No corrupt pages found"));
}

// -----------------------------------------------------------------------
// --corrupt detects corruption
// -----------------------------------------------------------------------

#[test]
fn test_find_corrupt_detects_bad_checksum() {
    let dir = create_clean_datadir();
    let datadir = dir.path().to_str().unwrap();

    // Corrupt page 1 in orders.ibd
    let orders_path = dir.path().join("db1").join("orders.ibd");
    let mut data = fs::read(&orders_path).unwrap();
    data[PS] = 0xFF;
    data[PS + 1] = 0xFF;
    data[PS + 2] = 0xFF;
    data[PS + 3] = 0xFF;
    fs::write(&orders_path, &data).unwrap();

    let mut opts = find_opts(datadir);
    opts.corrupt = true;

    let mut output = Vec::new();
    let result = idb::cli::find::execute(&opts, &mut output);
    assert!(result.is_ok());

    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("Corrupt page"));
    assert!(text.contains("orders.ibd"));
    assert!(text.contains("0x"));
    assert!(text.contains("Found 1 corrupt page(s)"));
}

// -----------------------------------------------------------------------
// --corrupt --first: at most 1 result per file
// -----------------------------------------------------------------------

#[test]
fn test_find_corrupt_first_limits_results() {
    let dir = create_clean_datadir();
    let datadir = dir.path().to_str().unwrap();

    // Corrupt two pages in orders.ibd
    let orders_path = dir.path().join("db1").join("orders.ibd");
    let mut data = fs::read(&orders_path).unwrap();
    // Corrupt page 1
    data[PS] = 0xFF;
    data[PS + 1] = 0xFF;
    data[PS + 2] = 0xFF;
    data[PS + 3] = 0xFF;
    // Corrupt page 2
    data[PS * 2] = 0xFF;
    data[PS * 2 + 1] = 0xFF;
    data[PS * 2 + 2] = 0xFF;
    data[PS * 2 + 3] = 0xFF;
    fs::write(&orders_path, &data).unwrap();

    let mut opts = find_opts(datadir);
    opts.corrupt = true;
    opts.first = true;

    let mut output = Vec::new();
    let result = idb::cli::find::execute(&opts, &mut output);
    assert!(result.is_ok());

    let text = String::from_utf8(output).unwrap();
    // Should find exactly 1 corrupt page (first per file, and --first stops globally)
    assert!(text.contains("Found 1 corrupt page(s)"));
}

// -----------------------------------------------------------------------
// --corrupt --json: structured output
// -----------------------------------------------------------------------

#[test]
fn test_find_corrupt_json_output() {
    let dir = create_clean_datadir();
    let datadir = dir.path().to_str().unwrap();

    // Corrupt page 1 in orders.ibd
    let orders_path = dir.path().join("db1").join("orders.ibd");
    let mut data = fs::read(&orders_path).unwrap();
    data[PS] = 0xFF;
    data[PS + 1] = 0xFF;
    data[PS + 2] = 0xFF;
    data[PS + 3] = 0xFF;
    fs::write(&orders_path, &data).unwrap();

    let mut opts = find_opts(datadir);
    opts.corrupt = true;
    opts.json = true;

    let mut output = Vec::new();
    let result = idb::cli::find::execute(&opts, &mut output);
    assert!(result.is_ok());

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert!(json.get("datadir").is_some());
    assert!(json.get("corrupt_pages").is_some());
    assert!(json.get("files_searched").is_some());
    assert!(json.get("total_corrupt").is_some());

    let pages = json["corrupt_pages"].as_array().unwrap();
    assert!(!pages.is_empty());

    let p = &pages[0];
    assert!(p.get("file").is_some());
    assert!(p.get("page_number").is_some());
    assert!(p.get("stored_checksum").is_some());
    assert!(p.get("calculated_checksum").is_some());
    assert!(p.get("algorithm").is_some());
    assert!(p.get("corruption_pattern").is_some());

    assert_eq!(json["total_corrupt"], pages.len());
}

// -----------------------------------------------------------------------
// --corrupt --json on clean directory: empty result
// -----------------------------------------------------------------------

#[test]
fn test_find_corrupt_json_clean() {
    let dir = create_clean_datadir();
    let datadir = dir.path().to_str().unwrap();

    let mut opts = find_opts(datadir);
    opts.corrupt = true;
    opts.json = true;

    let mut output = Vec::new();
    idb::cli::find::execute(&opts, &mut output).unwrap();

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(json["total_corrupt"], 0);
    assert!(json["corrupt_pages"].as_array().unwrap().is_empty());
}

// -----------------------------------------------------------------------
// --corrupt --space-id: filters by tablespace
// -----------------------------------------------------------------------

#[test]
fn test_find_corrupt_with_space_id_filter() {
    let dir = create_clean_datadir();
    let datadir = dir.path().to_str().unwrap();

    // Corrupt page 1 in orders.ibd (space_id=10)
    let orders_path = dir.path().join("db1").join("orders.ibd");
    let mut data = fs::read(&orders_path).unwrap();
    data[PS] = 0xFF;
    data[PS + 1] = 0xFF;
    data[PS + 2] = 0xFF;
    data[PS + 3] = 0xFF;
    fs::write(&orders_path, &data).unwrap();

    // Search with space_id=20 (users.ibd) — should find nothing
    let mut opts = find_opts(datadir);
    opts.corrupt = true;
    opts.space_id = Some(20);
    opts.json = true;

    let mut output = Vec::new();
    idb::cli::find::execute(&opts, &mut output).unwrap();

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(json["total_corrupt"], 0);

    // Search with space_id=10 (orders.ibd) — should find the corrupt page
    let mut opts2 = find_opts(datadir);
    opts2.corrupt = true;
    opts2.space_id = Some(10);
    opts2.json = true;

    let mut output2 = Vec::new();
    idb::cli::find::execute(&opts2, &mut output2).unwrap();

    let json2: serde_json::Value = serde_json::from_slice(&output2).unwrap();
    assert!(json2["total_corrupt"].as_u64().unwrap() > 0);
}

// -----------------------------------------------------------------------
// --page + --corrupt: mutually exclusive error
// -----------------------------------------------------------------------

#[test]
fn test_find_page_and_corrupt_mutually_exclusive() {
    let dir = create_clean_datadir();
    let datadir = dir.path().to_str().unwrap();

    let mut opts = find_opts(datadir);
    opts.page = Some(1);
    opts.corrupt = true;

    let mut output = Vec::new();
    let result = idb::cli::find::execute(&opts, &mut output);
    assert!(result.is_err());

    let err = result.unwrap_err().to_string();
    assert!(err.contains("mutually exclusive"));
}

// -----------------------------------------------------------------------
// --checksum + --corrupt: incompatible error
// -----------------------------------------------------------------------

#[test]
fn test_find_checksum_and_corrupt_incompatible() {
    let dir = create_clean_datadir();
    let datadir = dir.path().to_str().unwrap();

    let mut opts = find_opts(datadir);
    opts.corrupt = true;
    opts.checksum = Some(12345);

    let mut output = Vec::new();
    let result = idb::cli::find::execute(&opts, &mut output);
    assert!(result.is_err());

    let err = result.unwrap_err().to_string();
    assert!(err.contains("--checksum"));
}

// -----------------------------------------------------------------------
// Neither --page nor --corrupt: error
// -----------------------------------------------------------------------

#[test]
fn test_find_neither_page_nor_corrupt() {
    let dir = create_clean_datadir();
    let datadir = dir.path().to_str().unwrap();

    let opts = find_opts(datadir);

    let mut output = Vec::new();
    let result = idb::cli::find::execute(&opts, &mut output);
    assert!(result.is_err());

    let err = result.unwrap_err().to_string();
    assert!(err.contains("--page"));
    assert!(err.contains("--corrupt"));
}

// -----------------------------------------------------------------------
// --corrupt on empty directory: graceful message
// -----------------------------------------------------------------------

#[test]
fn test_find_corrupt_empty_directory() {
    let dir = TempDir::new().unwrap();
    let datadir = dir.path().to_str().unwrap();

    let mut opts = find_opts(datadir);
    opts.corrupt = true;

    let mut output = Vec::new();
    let result = idb::cli::find::execute(&opts, &mut output);
    assert!(result.is_ok());

    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("No .ibd files found"));
}

// -----------------------------------------------------------------------
// Existing --page search still works
// -----------------------------------------------------------------------

#[test]
fn test_find_page_search_still_works() {
    let dir = create_clean_datadir();
    let datadir = dir.path().to_str().unwrap();

    let mut opts = find_opts(datadir);
    opts.page = Some(1);

    let mut output = Vec::new();
    let result = idb::cli::find::execute(&opts, &mut output);
    assert!(result.is_ok());

    let text = String::from_utf8(output).unwrap();
    // Page 1 exists in both orders.ibd and users.ibd
    assert!(text.contains("Found page 1"));
    assert!(text.contains("2 match(es)"));
}

// -----------------------------------------------------------------------
// --depth flag integration tests
// -----------------------------------------------------------------------

/// Create a datadir with files at multiple depths for depth testing.
/// Structure:
///   root/root_table.ibd          (depth 1)
///   root/db1/orders.ibd          (depth 2)
///   root/db1/sub/deep.ibd        (depth 3)
fn create_deep_datadir() -> TempDir {
    let dir = TempDir::new().unwrap();

    // Root-level file
    let p0 = build_fsp_hdr_page(1, 2);
    let p1 = build_index_page(1, 1, 2000);
    write_ibd_file(dir.path(), "root_table.ibd", &[p0, p1]);

    // Depth-2 file
    let db1 = dir.path().join("db1");
    fs::create_dir(&db1).unwrap();
    let p0 = build_fsp_hdr_page(10, 2);
    let p1 = build_index_page(1, 10, 2000);
    write_ibd_file(&db1, "orders.ibd", &[p0, p1]);

    // Depth-3 file
    let sub = db1.join("sub");
    fs::create_dir(&sub).unwrap();
    let p0 = build_fsp_hdr_page(20, 2);
    let p1 = build_index_page(1, 20, 2000);
    write_ibd_file(&sub, "deep.ibd", &[p0, p1]);

    dir
}

#[test]
fn test_find_depth_1_root_only() {
    let dir = create_deep_datadir();
    let datadir = dir.path().to_str().unwrap();

    let mut opts = find_opts(datadir);
    opts.page = Some(1);
    opts.json = true;
    opts.depth = Some(1);

    let mut output = Vec::new();
    idb::cli::find::execute(&opts, &mut output).unwrap();

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    let matches = json["matches"].as_array().unwrap();
    // Only root_table.ibd at depth 1
    assert_eq!(matches.len(), 1);
    assert!(matches[0]["file"]
        .as_str()
        .unwrap()
        .contains("root_table.ibd"));
}

#[test]
fn test_find_depth_2_default() {
    let dir = create_deep_datadir();
    let datadir = dir.path().to_str().unwrap();

    let mut opts = find_opts(datadir);
    opts.page = Some(1);
    opts.json = true;
    // depth: None uses default depth 2

    let mut output = Vec::new();
    idb::cli::find::execute(&opts, &mut output).unwrap();

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    let matches = json["matches"].as_array().unwrap();
    // root_table.ibd + db1/orders.ibd, but NOT db1/sub/deep.ibd
    assert_eq!(matches.len(), 2);
}

#[test]
fn test_find_depth_3_includes_deep() {
    let dir = create_deep_datadir();
    let datadir = dir.path().to_str().unwrap();

    let mut opts = find_opts(datadir);
    opts.page = Some(1);
    opts.json = true;
    opts.depth = Some(3);

    let mut output = Vec::new();
    idb::cli::find::execute(&opts, &mut output).unwrap();

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    let matches = json["matches"].as_array().unwrap();
    // All 3 files
    assert_eq!(matches.len(), 3);
}

#[test]
fn test_find_depth_0_unlimited() {
    let dir = create_deep_datadir();
    let datadir = dir.path().to_str().unwrap();

    let mut opts = find_opts(datadir);
    opts.page = Some(1);
    opts.json = true;
    opts.depth = Some(0);

    let mut output = Vec::new();
    idb::cli::find::execute(&opts, &mut output).unwrap();

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    let matches = json["matches"].as_array().unwrap();
    // All 3 files with unlimited depth
    assert_eq!(matches.len(), 3);
}
