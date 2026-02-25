#![cfg(feature = "cli")]
//! Integration tests for `inno audit`.

use byteorder::{BigEndian, ByteOrder};
use std::fs;
use std::io::Write;
use tempfile::TempDir;

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
    BigEndian::write_u16(&mut page[ph + PAGE_N_HEAP..], 0x8002);
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

fn write_ibd_file(dir: &std::path::Path, name: &str, pages: &[Vec<u8>]) {
    let path = dir.join(name);
    let mut f = fs::File::create(path).unwrap();
    for page in pages {
        f.write_all(page).unwrap();
    }
    f.flush().unwrap();
}

/// Create a test data directory with subdirectories and valid .ibd files.
fn create_test_datadir() -> TempDir {
    let dir = TempDir::new().unwrap();

    // db1/orders.ibd — 3 pages, all valid
    let db1 = dir.path().join("db1");
    fs::create_dir(&db1).unwrap();
    let p0 = build_fsp_hdr_page(10, 3);
    let p1 = build_index_page(1, 10, 2000, 100, 0, 50, 8000, 0);
    let p2 = build_index_page(2, 10, 3000, 100, 0, 50, 8000, 0);
    write_ibd_file(&db1, "orders.ibd", &[p0, p1, p2]);

    // db2/users.ibd — 2 pages, all valid
    let db2 = dir.path().join("db2");
    fs::create_dir(&db2).unwrap();
    let p0 = build_fsp_hdr_page(20, 2);
    let p1 = build_index_page(1, 20, 2000, 200, 0, 30, 4000, 0);
    write_ibd_file(&db2, "users.ibd", &[p0, p1]);

    dir
}

fn audit_opts(datadir: &str) -> idb::cli::audit::AuditOptions {
    idb::cli::audit::AuditOptions {
        datadir: datadir.to_string(),
        health: false,
        checksum_mismatch: false,
        verbose: false,
        json: false,
        csv: false,
        page_size: None,
        keyring: None,
        mmap: false,
        min_fill_factor: None,
        max_fragmentation: None,
        depth: None,
    }
}

// -----------------------------------------------------------------------
// Default integrity mode tests
// -----------------------------------------------------------------------

#[test]
fn test_audit_all_clean() {
    let dir = create_test_datadir();
    let datadir = dir.path().to_str().unwrap();

    let mut output = Vec::new();
    let result = idb::cli::audit::execute(&audit_opts(datadir), &mut output);
    assert!(result.is_ok());

    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("PASS"));
    assert!(text.contains("Summary"));
    assert!(text.contains("Integrity"));
}

#[test]
fn test_audit_with_corruption() {
    let dir = create_test_datadir();
    let datadir = dir.path().to_str().unwrap();

    // Corrupt a page in orders.ibd
    let orders_path = dir.path().join("db1").join("orders.ibd");
    let mut data = fs::read(&orders_path).unwrap();
    // Corrupt page 1's checksum (bytes at offset PAGE_SIZE)
    data[PS] = 0xFF;
    data[PS + 1] = 0xFF;
    data[PS + 2] = 0xFF;
    data[PS + 3] = 0xFF;
    fs::write(&orders_path, &data).unwrap();

    let mut output = Vec::new();
    let result = idb::cli::audit::execute(&audit_opts(datadir), &mut output);
    assert!(result.is_err()); // exit code 1

    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("FAIL"));
    assert!(text.contains("corrupt"));
}

#[test]
fn test_audit_json_output() {
    let dir = create_test_datadir();
    let datadir = dir.path().to_str().unwrap();

    let mut opts = audit_opts(datadir);
    opts.json = true;

    let mut output = Vec::new();
    idb::cli::audit::execute(&opts, &mut output).unwrap();

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert!(json.get("datadir").is_some());
    assert!(json.get("files").is_some());
    assert!(json.get("summary").is_some());

    let files = json["files"].as_array().unwrap();
    assert_eq!(files.len(), 2);

    let summary = &json["summary"];
    assert_eq!(summary["total_files"], 2);
    assert_eq!(summary["files_passed"], 2);
    assert_eq!(summary["files_failed"], 0);
    assert_eq!(summary["corrupt_pages"], 0);
}

#[test]
fn test_audit_csv_output() {
    let dir = create_test_datadir();
    let datadir = dir.path().to_str().unwrap();

    let mut opts = audit_opts(datadir);
    opts.csv = true;

    let mut output = Vec::new();
    idb::cli::audit::execute(&opts, &mut output).unwrap();

    let text = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    // Header + 2 data rows
    assert!(lines.len() >= 3);
    assert!(lines[0].contains("file,status,total_pages"));
    assert!(lines[1].contains("PASS"));
}

#[test]
fn test_audit_empty_directory() {
    let dir = TempDir::new().unwrap();
    let datadir = dir.path().to_str().unwrap();

    let mut output = Vec::new();
    let result = idb::cli::audit::execute(&audit_opts(datadir), &mut output);
    assert!(result.is_ok());

    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("No .ibd files found"));
}

#[test]
fn test_audit_unreadable_file() {
    let dir = create_test_datadir();
    let datadir = dir.path().to_str().unwrap();

    // Create a file too small to be a valid tablespace
    let db1 = dir.path().join("db1");
    fs::write(db1.join("broken.ibd"), b"not a tablespace").unwrap();

    let mut opts = audit_opts(datadir);
    opts.json = true;

    let mut output = Vec::new();
    // Should still succeed overall (error files don't cause exit 1)
    idb::cli::audit::execute(&opts, &mut output).unwrap();

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    let files = json["files"].as_array().unwrap();
    // Should have 3 files: orders.ibd, users.ibd, broken.ibd
    assert_eq!(files.len(), 3);

    let error_file = files.iter().find(|f| f["status"] == "error");
    assert!(error_file.is_some());
    assert!(error_file.unwrap()["error"].is_string());

    // The other 2 should be PASS
    let passed = files.iter().filter(|f| f["status"] == "PASS").count();
    assert_eq!(passed, 2);
}

// -----------------------------------------------------------------------
// Health mode tests
// -----------------------------------------------------------------------

#[test]
fn test_audit_health_mode() {
    let dir = create_test_datadir();
    let datadir = dir.path().to_str().unwrap();

    let mut opts = audit_opts(datadir);
    opts.health = true;

    let mut output = Vec::new();
    idb::cli::audit::execute(&opts, &mut output).unwrap();

    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("Directory Health"));
    assert!(text.contains("Fill%"));
    assert!(text.contains("Frag%"));
    assert!(text.contains("Summary"));
}

#[test]
fn test_audit_health_json() {
    let dir = create_test_datadir();
    let datadir = dir.path().to_str().unwrap();

    let mut opts = audit_opts(datadir);
    opts.health = true;
    opts.json = true;

    let mut output = Vec::new();
    idb::cli::audit::execute(&opts, &mut output).unwrap();

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert!(json.get("datadir").is_some());
    assert!(json.get("tablespaces").is_some());
    assert!(json.get("summary").is_some());

    let tablespaces = json["tablespaces"].as_array().unwrap();
    assert_eq!(tablespaces.len(), 2);

    for ts in tablespaces {
        assert!(ts.get("avg_fill_factor").is_some());
        assert!(ts.get("avg_fragmentation").is_some());
        assert!(ts.get("index_count").is_some());
    }
}

#[test]
fn test_audit_health_thresholds() {
    let dir = create_test_datadir();
    let datadir = dir.path().to_str().unwrap();

    // Set min-fill-factor very high so all tables appear "unhealthy"
    let mut opts = audit_opts(datadir);
    opts.health = true;
    opts.json = true;
    opts.min_fill_factor = Some(99.0);

    let mut output = Vec::new();
    idb::cli::audit::execute(&opts, &mut output).unwrap();

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    let tablespaces = json["tablespaces"].as_array().unwrap();
    // All should be included since fill < 99%
    assert_eq!(tablespaces.len(), 2);

    // Now set min-fill-factor very low so nothing appears
    let mut opts2 = audit_opts(datadir);
    opts2.health = true;
    opts2.json = true;
    opts2.min_fill_factor = Some(1.0);

    let mut output2 = Vec::new();
    idb::cli::audit::execute(&opts2, &mut output2).unwrap();

    let json2: serde_json::Value = serde_json::from_slice(&output2).unwrap();
    let tablespaces2 = json2["tablespaces"].as_array().unwrap();
    // Fill factor is above 1% so nothing should be filtered in
    assert_eq!(tablespaces2.len(), 0);
}

// -----------------------------------------------------------------------
// Mismatch mode tests
// -----------------------------------------------------------------------

#[test]
fn test_audit_checksum_mismatch_clean() {
    let dir = create_test_datadir();
    let datadir = dir.path().to_str().unwrap();

    let mut opts = audit_opts(datadir);
    opts.checksum_mismatch = true;

    let mut output = Vec::new();
    let result = idb::cli::audit::execute(&opts, &mut output);
    assert!(result.is_ok());

    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("No checksum mismatches found"));
}

#[test]
fn test_audit_checksum_mismatch_with_corruption() {
    let dir = create_test_datadir();
    let datadir = dir.path().to_str().unwrap();

    // Corrupt page 1 in orders.ibd
    let orders_path = dir.path().join("db1").join("orders.ibd");
    let mut data = fs::read(&orders_path).unwrap();
    data[PS] = 0xFF;
    data[PS + 1] = 0xFF;
    data[PS + 2] = 0xFF;
    data[PS + 3] = 0xFF;
    fs::write(&orders_path, &data).unwrap();

    let mut opts = audit_opts(datadir);
    opts.checksum_mismatch = true;

    let mut output = Vec::new();
    let result = idb::cli::audit::execute(&opts, &mut output);
    assert!(result.is_err()); // exit code 1

    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("orders.ibd"));
    assert!(text.contains("0x"));
}

#[test]
fn test_audit_checksum_mismatch_json() {
    let dir = create_test_datadir();
    let datadir = dir.path().to_str().unwrap();

    // Corrupt a page
    let orders_path = dir.path().join("db1").join("orders.ibd");
    let mut data = fs::read(&orders_path).unwrap();
    data[PS] = 0xFF;
    data[PS + 1] = 0xFF;
    data[PS + 2] = 0xFF;
    data[PS + 3] = 0xFF;
    fs::write(&orders_path, &data).unwrap();

    let mut opts = audit_opts(datadir);
    opts.checksum_mismatch = true;
    opts.json = true;

    let mut output = Vec::new();
    let _ = idb::cli::audit::execute(&opts, &mut output);

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert!(json.get("datadir").is_some());
    assert!(json.get("mismatches").is_some());
    assert!(json.get("total_files_scanned").is_some());

    let mismatches = json["mismatches"].as_array().unwrap();
    assert!(!mismatches.is_empty());

    let m = &mismatches[0];
    assert!(m.get("file").is_some());
    assert!(m.get("page_number").is_some());
    assert!(m.get("stored_checksum").is_some());
    assert!(m.get("calculated_checksum").is_some());
    assert!(m.get("algorithm").is_some());
}

// -----------------------------------------------------------------------
// Flag validation
// -----------------------------------------------------------------------

#[test]
fn test_audit_mutually_exclusive_flags() {
    let dir = create_test_datadir();
    let datadir = dir.path().to_str().unwrap();

    let mut opts = audit_opts(datadir);
    opts.health = true;
    opts.checksum_mismatch = true;

    let mut output = Vec::new();
    let result = idb::cli::audit::execute(&opts, &mut output);
    assert!(result.is_err());

    let err = result.unwrap_err().to_string();
    assert!(err.contains("mutually exclusive"));
}

// -----------------------------------------------------------------------
// Depth flag tests
// -----------------------------------------------------------------------

#[test]
fn test_audit_depth_limits_discovery() {
    let dir = TempDir::new().unwrap();

    // Depth 1: root-level file
    let p0 = build_fsp_hdr_page(1, 2);
    let p1 = build_index_page(1, 1, 2000, 100, 0, 50, 8000, 0);
    write_ibd_file(dir.path(), "root_table.ibd", &[p0, p1]);

    // Depth 2: one subdir down
    let db1 = dir.path().join("db1");
    fs::create_dir(&db1).unwrap();
    let p0 = build_fsp_hdr_page(10, 2);
    let p1 = build_index_page(1, 10, 2000, 200, 0, 30, 4000, 0);
    write_ibd_file(&db1, "orders.ibd", &[p0, p1]);

    // Depth 3: two subdirs down
    let sub = db1.join("sub");
    fs::create_dir(&sub).unwrap();
    let p0 = build_fsp_hdr_page(20, 2);
    let p1 = build_index_page(1, 20, 2000, 300, 0, 20, 3000, 0);
    write_ibd_file(&sub, "deep.ibd", &[p0, p1]);

    let datadir = dir.path().to_str().unwrap();

    // depth=1: only root-level file
    let mut opts = audit_opts(datadir);
    opts.json = true;
    opts.depth = Some(1);

    let mut output = Vec::new();
    idb::cli::audit::execute(&opts, &mut output).unwrap();

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(json["summary"]["total_files"], 1);

    // depth=None (default 2): root + db1
    let mut opts2 = audit_opts(datadir);
    opts2.json = true;

    let mut output2 = Vec::new();
    idb::cli::audit::execute(&opts2, &mut output2).unwrap();

    let json2: serde_json::Value = serde_json::from_slice(&output2).unwrap();
    assert_eq!(json2["summary"]["total_files"], 2);

    // depth=0 (unlimited): all 3
    let mut opts3 = audit_opts(datadir);
    opts3.json = true;
    opts3.depth = Some(0);

    let mut output3 = Vec::new();
    idb::cli::audit::execute(&opts3, &mut output3).unwrap();

    let json3: serde_json::Value = serde_json::from_slice(&output3).unwrap();
    assert_eq!(json3["summary"]["total_files"], 3);
}
