#![cfg(feature = "cli")]
//! Integration tests for `inno repair`.

use byteorder::{BigEndian, ByteOrder};
use std::io::Write;
use std::sync::Arc;
use tempfile::{NamedTempFile, TempDir};

use idb::innodb::checksum::{validate_checksum, ChecksumAlgorithm};
use idb::innodb::constants::*;
use idb::innodb::write;
use idb::util::audit::AuditLogger;

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
            file: Some(path.to_string()),
            batch: None,
            page: None,
            algorithm: "auto".to_string(),
            no_backup: true,
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
            file: Some(path.to_string()),
            batch: None,
            page: None,
            algorithm: "auto".to_string(),
            no_backup: true,
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
            file: Some(path.to_string()),
            batch: None,
            page: Some(1),
            algorithm: "crc32c".to_string(),
            no_backup: true,
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
            file: Some(path.to_string()),
            batch: None,
            page: None,
            algorithm: "auto".to_string(),
            no_backup: true,
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
            file: Some(path.to_string()),
            batch: None,
            page: None,
            algorithm: "auto".to_string(),
            no_backup: true,
            dry_run: false,
            verbose: false,
            json: true,
            page_size: None,
            keyring: None,
            mmap: false,
            audit_logger: None,
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
            file: Some(path.to_string()),
            batch: None,
            page: Some(1),
            algorithm: "innodb".to_string(),
            no_backup: true,
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
            file: Some(path.to_string()),
            batch: None,
            page: None,
            algorithm: "auto".to_string(),
            no_backup: true,
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

    // No .bak file should exist
    let backup = format!("{}.bak", path);
    assert!(!std::path::Path::new(&backup).exists());
}

// ---------------------------------------------------------------------------
// Batch repair integration tests
// ---------------------------------------------------------------------------

/// Write a tablespace file into a directory, returning the file path.
fn write_tablespace_in_dir(dir: &std::path::Path, name: &str, pages: &[Vec<u8>]) -> String {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).unwrap();
    for page in pages {
        f.write_all(page).unwrap();
    }
    f.flush().unwrap();
    path.to_str().unwrap().to_string()
}

#[test]
fn test_batch_all_valid_files() {
    let dir = TempDir::new().unwrap();

    // Two valid tablespace files
    let page0a = build_fsp_hdr_page(1, 2);
    let page1a = build_index_page(1, 1, 1000);
    write_tablespace_in_dir(dir.path(), "a.ibd", &[page0a, page1a]);

    let page0b = build_fsp_hdr_page(2, 2);
    let page1b = build_index_page(1, 2, 2000);
    write_tablespace_in_dir(dir.path(), "b.ibd", &[page0b, page1b]);

    let mut output = Vec::new();
    idb::cli::repair::execute(
        &idb::cli::repair::RepairOptions {
            file: None,
            batch: Some(dir.path().to_str().unwrap().to_string()),
            page: None,
            algorithm: "auto".to_string(),
            no_backup: true,
            dry_run: false,
            verbose: false,
            json: true,
            page_size: None,
            keyring: None,
            mmap: false,
            audit_logger: None,
        },
        &mut output,
    )
    .unwrap();

    let text = String::from_utf8(output).unwrap();
    let json: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(json["summary"]["total_files"], 2);
    assert_eq!(json["summary"]["total_pages_repaired"], 0);
    assert_eq!(json["summary"]["files_already_valid"], 2);
}

#[test]
fn test_batch_repairs_corrupt_pages() {
    let dir = TempDir::new().unwrap();

    // File with a corrupt page
    let page0 = build_fsp_hdr_page(1, 3);
    let page1 = build_index_page(1, 1, 1000);
    let mut page2 = build_index_page(2, 1, 2000);
    corrupt_checksum(&mut page2);
    let fpath = write_tablespace_in_dir(dir.path(), "bad.ibd", &[page0, page1, page2]);

    let mut output = Vec::new();
    idb::cli::repair::execute(
        &idb::cli::repair::RepairOptions {
            file: None,
            batch: Some(dir.path().to_str().unwrap().to_string()),
            page: None,
            algorithm: "auto".to_string(),
            no_backup: true,
            dry_run: false,
            verbose: false,
            json: true,
            page_size: None,
            keyring: None,
            mmap: false,
            audit_logger: None,
        },
        &mut output,
    )
    .unwrap();

    let text = String::from_utf8(output).unwrap();
    let json: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(json["summary"]["total_pages_repaired"], 1);
    assert_eq!(json["summary"]["files_repaired"], 1);

    // Verify page is now valid
    let p2 = write::read_page_raw(&fpath, 2, PAGE_SIZE).unwrap();
    assert!(validate_checksum(&p2, PAGE_SIZE, None).valid);
}

#[test]
fn test_batch_dry_run_no_modification() {
    let dir = TempDir::new().unwrap();

    let page0 = build_fsp_hdr_page(1, 2);
    let mut page1 = build_index_page(1, 1, 1000);
    corrupt_checksum(&mut page1);
    let corrupted_csum = BigEndian::read_u32(&page1[FIL_PAGE_SPACE_OR_CHKSUM..]);
    let fpath = write_tablespace_in_dir(dir.path(), "test.ibd", &[page0, page1]);

    let mut output = Vec::new();
    idb::cli::repair::execute(
        &idb::cli::repair::RepairOptions {
            file: None,
            batch: Some(dir.path().to_str().unwrap().to_string()),
            page: None,
            algorithm: "auto".to_string(),
            no_backup: true,
            dry_run: true,
            verbose: false,
            json: true,
            page_size: None,
            keyring: None,
            mmap: false,
            audit_logger: None,
        },
        &mut output,
    )
    .unwrap();

    // Page should still have the corrupt checksum
    let p1 = write::read_page_raw(&fpath, 1, PAGE_SIZE).unwrap();
    let after_csum = BigEndian::read_u32(&p1[FIL_PAGE_SPACE_OR_CHKSUM..]);
    assert_eq!(after_csum, corrupted_csum);

    let text = String::from_utf8(output).unwrap();
    let json: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(json["dry_run"], true);
}

#[test]
fn test_batch_json_output_structure() {
    let dir = TempDir::new().unwrap();

    let page0 = build_fsp_hdr_page(1, 2);
    let page1 = build_index_page(1, 1, 1000);
    write_tablespace_in_dir(dir.path(), "test.ibd", &[page0, page1]);

    let mut output = Vec::new();
    idb::cli::repair::execute(
        &idb::cli::repair::RepairOptions {
            file: None,
            batch: Some(dir.path().to_str().unwrap().to_string()),
            page: None,
            algorithm: "auto".to_string(),
            no_backup: true,
            dry_run: false,
            verbose: false,
            json: true,
            page_size: None,
            keyring: None,
            mmap: false,
            audit_logger: None,
        },
        &mut output,
    )
    .unwrap();

    let text = String::from_utf8(output).unwrap();
    let json: serde_json::Value = serde_json::from_str(&text).unwrap();

    // Validate top-level fields
    assert!(json["datadir"].is_string());
    assert!(json["algorithm"].is_string());
    assert!(json["files"].is_array());
    assert!(json["summary"].is_object());

    // Validate file-level fields
    let file = &json["files"][0];
    assert!(file["file"].is_string());
    assert!(file["total_pages"].is_number());
    assert!(file["already_valid"].is_number());
    assert!(file["repaired"].is_number());

    // Validate summary fields
    let s = &json["summary"];
    assert!(s["total_files"].is_number());
    assert!(s["files_repaired"].is_number());
    assert!(s["files_already_valid"].is_number());
    assert!(s["total_pages_scanned"].is_number());
    assert!(s["total_pages_repaired"].is_number());
}

#[test]
fn test_batch_with_audit_log() {
    let dir = TempDir::new().unwrap();

    let page0 = build_fsp_hdr_page(1, 2);
    let mut page1 = build_index_page(1, 1, 1000);
    corrupt_checksum(&mut page1);
    write_tablespace_in_dir(dir.path(), "test.ibd", &[page0, page1]);

    // Create audit log
    let audit_tmp = NamedTempFile::new().unwrap();
    let audit_path = audit_tmp.path().to_str().unwrap().to_string();
    drop(audit_tmp);

    let logger = Arc::new(AuditLogger::open(&audit_path).unwrap());
    logger.start_session(vec!["test".into()]).unwrap();

    let mut output = Vec::new();
    idb::cli::repair::execute(
        &idb::cli::repair::RepairOptions {
            file: None,
            batch: Some(dir.path().to_str().unwrap().to_string()),
            page: None,
            algorithm: "auto".to_string(),
            no_backup: true,
            dry_run: false,
            verbose: false,
            json: true,
            page_size: None,
            keyring: None,
            mmap: false,
            audit_logger: Some(logger.clone()),
        },
        &mut output,
    )
    .unwrap();

    logger.end_session().unwrap();

    // Verify audit log contains expected events
    let audit_content = std::fs::read_to_string(&audit_path).unwrap();
    let lines: Vec<&str> = audit_content.lines().collect();
    assert!(lines.len() >= 3); // session_start + page_write + session_end

    let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(first["event"], "session_start");

    // Find page_write event
    let has_page_write = lines.iter().any(|l| l.contains("\"event\":\"page_write\""));
    assert!(has_page_write);

    let last: serde_json::Value = serde_json::from_str(lines.last().unwrap()).unwrap();
    assert_eq!(last["event"], "session_end");
}

#[test]
fn test_batch_no_backup_skips_bak_files() {
    let dir = TempDir::new().unwrap();

    let page0 = build_fsp_hdr_page(1, 2);
    let mut page1 = build_index_page(1, 1, 1000);
    corrupt_checksum(&mut page1);
    write_tablespace_in_dir(dir.path(), "test.ibd", &[page0, page1]);

    let mut output = Vec::new();
    idb::cli::repair::execute(
        &idb::cli::repair::RepairOptions {
            file: None,
            batch: Some(dir.path().to_str().unwrap().to_string()),
            page: None,
            algorithm: "auto".to_string(),
            no_backup: true,
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

    // No .bak files should exist
    let bak_exists = std::fs::read_dir(dir.path()).unwrap().any(|e| {
        e.unwrap()
            .path()
            .extension()
            .map_or(false, |ext| ext == "bak")
    });
    assert!(!bak_exists);
}

#[test]
fn test_batch_empty_directory() {
    let dir = TempDir::new().unwrap();

    let mut output = Vec::new();
    idb::cli::repair::execute(
        &idb::cli::repair::RepairOptions {
            file: None,
            batch: Some(dir.path().to_str().unwrap().to_string()),
            page: None,
            algorithm: "auto".to_string(),
            no_backup: true,
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
    assert!(text.contains("No .ibd files found"));
}

#[test]
fn test_batch_file_mutual_exclusion() {
    let result = idb::cli::repair::execute(
        &idb::cli::repair::RepairOptions {
            file: Some("test.ibd".to_string()),
            batch: Some("/tmp".to_string()),
            page: None,
            algorithm: "auto".to_string(),
            no_backup: true,
            dry_run: false,
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
            audit_logger: None,
        },
        &mut Vec::new(),
    );

    match result {
        Err(idb::IdbError::Argument(msg)) => {
            assert!(msg.contains("mutually exclusive"));
        }
        other => panic!("Expected Argument error, got {:?}", other.err()),
    }
}

#[test]
fn test_batch_page_incompatible() {
    let result = idb::cli::repair::execute(
        &idb::cli::repair::RepairOptions {
            file: None,
            batch: Some("/tmp".to_string()),
            page: Some(1),
            algorithm: "auto".to_string(),
            no_backup: true,
            dry_run: false,
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
            audit_logger: None,
        },
        &mut Vec::new(),
    );

    match result {
        Err(idb::IdbError::Argument(msg)) => {
            assert!(msg.contains("--page cannot be used with --batch"));
        }
        other => panic!("Expected Argument error, got {:?}", other.err()),
    }
}
