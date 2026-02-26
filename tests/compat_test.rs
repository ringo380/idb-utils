#![cfg(feature = "cli")]
//! Integration tests for `inno compat`.

use byteorder::{BigEndian, ByteOrder};
use std::io::Write;
use tempfile::NamedTempFile;

use idb::innodb::checksum::{recalculate_checksum, ChecksumAlgorithm};
use idb::innodb::compat::{
    build_compat_report, check_compatibility, MysqlVersion, Severity, TablespaceInfo,
};
use idb::innodb::constants::*;
use idb::innodb::vendor::{MariaDbFormat, VendorInfo};
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

    // FIL trailer
    let trailer = PS - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut page[trailer + 4..], (lsn & 0xFFFFFFFF) as u32);

    recalculate_checksum(&mut page, PAGE_SIZE, ChecksumAlgorithm::Crc32c);
    page
}

fn build_mariadb_fsp_page(space_id: u32, total_pages: u32) -> Vec<u8> {
    // MariaDB full_crc32: bit 4 set in FSP flags
    let flags = 0x10;
    write::build_fsp_page(
        space_id,
        total_pages,
        flags,
        1000,
        PAGE_SIZE,
        ChecksumAlgorithm::Crc32c,
    )
}

fn write_tablespace(pages: &[Vec<u8>]) -> NamedTempFile {
    let mut tmp = NamedTempFile::new().unwrap();
    for page in pages {
        tmp.write_all(page).unwrap();
    }
    tmp.flush().unwrap();
    tmp
}

// ---------------------------------------------------------------------------
// MysqlVersion unit tests
// ---------------------------------------------------------------------------

#[test]
fn test_mysql_version_parse_valid() {
    let v = MysqlVersion::parse("8.0.32").unwrap();
    assert_eq!(v.major, 8);
    assert_eq!(v.minor, 0);
    assert_eq!(v.patch, 32);
}

#[test]
fn test_mysql_version_parse_invalid_two_parts() {
    assert!(MysqlVersion::parse("8.0").is_err());
}

#[test]
fn test_mysql_version_parse_invalid_non_numeric() {
    assert!(MysqlVersion::parse("8.0.abc").is_err());
}

#[test]
fn test_mysql_version_parse_invalid_empty() {
    assert!(MysqlVersion::parse("").is_err());
}

#[test]
fn test_mysql_version_from_id() {
    let v = MysqlVersion::from_id(80400);
    assert_eq!(v.major, 8);
    assert_eq!(v.minor, 4);
    assert_eq!(v.patch, 0);
}

#[test]
fn test_mysql_version_to_id() {
    let v = MysqlVersion::parse("9.0.1").unwrap();
    assert_eq!(v.to_id(), 90001);
}

#[test]
fn test_mysql_version_is_at_least() {
    let v57 = MysqlVersion::parse("5.7.0").unwrap();
    let v80 = MysqlVersion::parse("8.0.0").unwrap();
    let v84 = MysqlVersion::parse("8.4.0").unwrap();
    let v90 = MysqlVersion::parse("9.0.0").unwrap();

    assert!(v90.is_at_least(&v84));
    assert!(v84.is_at_least(&v80));
    assert!(v80.is_at_least(&v57));
    assert!(!v57.is_at_least(&v80));
    assert!(v80.is_at_least(&v80)); // equal
}

#[test]
fn test_mysql_version_display() {
    let v = MysqlVersion::parse("8.4.3").unwrap();
    assert_eq!(format!("{}", v), "8.4.3");
}

// ---------------------------------------------------------------------------
// Compatibility check unit tests with synthetic TablespaceInfo
// ---------------------------------------------------------------------------

#[test]
fn test_compat_mariadb_vendor_error() {
    let info = TablespaceInfo {
        page_size: 16384,
        fsp_flags: 0x10,
        space_id: 1,
        row_format: None,
        has_sdi: false,
        is_encrypted: false,
        vendor: VendorInfo::mariadb(MariaDbFormat::FullCrc32),
        mysql_version_id: None,
        has_compressed_pages: false,
        has_instant_columns: false,
    };
    let target = MysqlVersion::parse("8.4.0").unwrap();
    let checks = check_compatibility(&info, &target);

    // Should have vendor error + SDI error (no SDI for 8.0+ target)
    let vendor_errors: Vec<_> = checks
        .iter()
        .filter(|c| c.check == "vendor" && c.severity == Severity::Error)
        .collect();
    assert_eq!(vendor_errors.len(), 1);
    assert!(vendor_errors[0].message.contains("MariaDB"));
}

#[test]
fn test_compat_no_sdi_error_for_80_target() {
    let info = TablespaceInfo {
        page_size: 16384,
        fsp_flags: 0,
        space_id: 1,
        row_format: None,
        has_sdi: false,
        is_encrypted: false,
        vendor: VendorInfo::mysql(),
        mysql_version_id: None,
        has_compressed_pages: false,
        has_instant_columns: false,
    };
    let target = MysqlVersion::parse("8.0.0").unwrap();
    let checks = check_compatibility(&info, &target);

    let sdi_errors: Vec<_> = checks
        .iter()
        .filter(|c| c.check == "sdi" && c.severity == Severity::Error)
        .collect();
    assert_eq!(sdi_errors.len(), 1);
    assert!(sdi_errors[0].message.contains("lacks SDI"));
}

#[test]
fn test_compat_no_sdi_ok_for_57_target() {
    let info = TablespaceInfo {
        page_size: 16384,
        fsp_flags: 0,
        space_id: 1,
        row_format: None,
        has_sdi: false,
        is_encrypted: false,
        vendor: VendorInfo::mysql(),
        mysql_version_id: None,
        has_compressed_pages: false,
        has_instant_columns: false,
    };
    let target = MysqlVersion::parse("5.7.44").unwrap();
    let checks = check_compatibility(&info, &target);

    // No SDI check should trigger for pre-8.0 target with no SDI
    let sdi_checks: Vec<_> = checks.iter().filter(|c| c.check == "sdi").collect();
    assert!(sdi_checks.is_empty());
}

#[test]
fn test_compat_compressed_row_format_warning() {
    let info = TablespaceInfo {
        page_size: 16384,
        fsp_flags: 0,
        space_id: 1,
        row_format: Some("COMPRESSED".to_string()),
        has_sdi: true,
        is_encrypted: false,
        vendor: VendorInfo::mysql(),
        mysql_version_id: Some(80032),
        has_compressed_pages: false,
        has_instant_columns: false,
    };
    let target = MysqlVersion::parse("8.4.0").unwrap();
    let checks = check_compatibility(&info, &target);

    let rf_warnings: Vec<_> = checks
        .iter()
        .filter(|c| c.check == "row_format" && c.severity == Severity::Warning)
        .collect();
    assert_eq!(rf_warnings.len(), 1);
    assert!(rf_warnings[0].message.contains("COMPRESSED"));
}

#[test]
fn test_compat_redundant_row_format_warning_90() {
    let info = TablespaceInfo {
        page_size: 16384,
        fsp_flags: 0,
        space_id: 1,
        row_format: Some("REDUNDANT".to_string()),
        has_sdi: true,
        is_encrypted: false,
        vendor: VendorInfo::mysql(),
        mysql_version_id: Some(80032),
        has_compressed_pages: false,
        has_instant_columns: false,
    };
    let target = MysqlVersion::parse("9.0.0").unwrap();
    let checks = check_compatibility(&info, &target);

    let rf_warnings: Vec<_> = checks
        .iter()
        .filter(|c| c.check == "row_format" && c.severity == Severity::Warning)
        .collect();
    assert_eq!(rf_warnings.len(), 1);
    assert!(rf_warnings[0].message.contains("REDUNDANT"));
}

// ---------------------------------------------------------------------------
// build_compat_report tests
// ---------------------------------------------------------------------------

#[test]
fn test_build_report_compatible() {
    let info = TablespaceInfo {
        page_size: 16384,
        fsp_flags: 0,
        space_id: 1,
        row_format: Some("DYNAMIC".to_string()),
        has_sdi: true,
        is_encrypted: false,
        vendor: VendorInfo::mysql(),
        mysql_version_id: Some(80032),
        has_compressed_pages: false,
        has_instant_columns: false,
    };
    let target = MysqlVersion::parse("8.4.0").unwrap();
    let report = build_compat_report(&info, &target, "test.ibd");

    assert!(report.compatible);
    assert_eq!(report.summary.errors, 0);
    assert_eq!(report.target_version, "8.4.0");
    assert_eq!(report.source_version, Some("8.0.32".to_string()));
    assert_eq!(report.file, "test.ibd");
}

#[test]
fn test_build_report_incompatible() {
    let info = TablespaceInfo {
        page_size: 16384,
        fsp_flags: 0,
        space_id: 1,
        row_format: None,
        has_sdi: false,
        is_encrypted: false,
        vendor: VendorInfo::mariadb(MariaDbFormat::FullCrc32),
        mysql_version_id: None,
        has_compressed_pages: false,
        has_instant_columns: false,
    };
    let target = MysqlVersion::parse("8.4.0").unwrap();
    let report = build_compat_report(&info, &target, "bad.ibd");

    assert!(!report.compatible);
    assert!(report.summary.errors >= 2); // vendor + sdi
}

// ---------------------------------------------------------------------------
// JSON output test
// ---------------------------------------------------------------------------

#[test]
fn test_json_output_parsing() {
    let info = TablespaceInfo {
        page_size: 16384,
        fsp_flags: 0,
        space_id: 42,
        row_format: Some("DYNAMIC".to_string()),
        has_sdi: true,
        is_encrypted: false,
        vendor: VendorInfo::mysql(),
        mysql_version_id: Some(80400),
        has_compressed_pages: false,
        has_instant_columns: false,
    };
    let target = MysqlVersion::parse("9.0.0").unwrap();
    let report = build_compat_report(&info, &target, "table.ibd");

    let json_str = serde_json::to_string_pretty(&report).expect("report should serialize to JSON");

    // Parse it back to verify structure
    let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("JSON should be valid");

    assert_eq!(parsed["file"], "table.ibd");
    assert_eq!(parsed["target_version"], "9.0.0");
    assert_eq!(parsed["source_version"], "8.4.0");
    assert_eq!(parsed["compatible"], true);
    assert!(parsed["checks"].is_array());
    assert!(parsed["summary"].is_object());
    assert_eq!(parsed["summary"]["errors"], 0);
}

// ---------------------------------------------------------------------------
// Integration tests with synthetic tablespace files
// ---------------------------------------------------------------------------

#[test]
fn test_compat_basic_with_synthetic_tablespace() {
    let page0 = build_fsp_hdr_page(42, 3);
    let page1 = build_index_page(1, 42, 2000);
    let page2 = build_index_page(2, 42, 3000);

    let tmp = write_tablespace(&[page0, page1, page2]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::compat::execute(
        &idb::cli::compat::CompatOptions {
            file: Some(path.to_string()),
            scan: None,
            target: "8.4.0".to_string(),
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
            depth: None,
        },
        &mut output,
    )
    .unwrap();

    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("Compatibility Check:"));
    assert!(text.contains("Target version: MySQL 8.4.0"));
    // This synthetic tablespace has no SDI, so 8.4 target should report error
    assert!(text.contains("Result:"));
}

#[test]
fn test_compat_json_with_synthetic_tablespace() {
    let page0 = build_fsp_hdr_page(42, 2);
    let page1 = build_index_page(1, 42, 2000);

    let tmp = write_tablespace(&[page0, page1]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::compat::execute(
        &idb::cli::compat::CompatOptions {
            file: Some(path.to_string()),
            scan: None,
            target: "8.0.0".to_string(),
            verbose: false,
            json: true,
            page_size: None,
            keyring: None,
            mmap: false,
            depth: None,
        },
        &mut output,
    )
    .unwrap();

    let text = String::from_utf8(output).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&text).expect("should be valid JSON");

    assert_eq!(parsed["target_version"], "8.0.0");
    assert!(parsed["checks"].is_array());
    assert!(parsed["summary"].is_object());
}

#[test]
fn test_compat_mariadb_flags_detected() {
    let page0 = build_mariadb_fsp_page(42, 2);
    let page1 = build_index_page(1, 42, 2000);

    let tmp = write_tablespace(&[page0, page1]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::compat::execute(
        &idb::cli::compat::CompatOptions {
            file: Some(path.to_string()),
            scan: None,
            target: "8.4.0".to_string(),
            verbose: true,
            json: true,
            page_size: None,
            keyring: None,
            mmap: false,
            depth: None,
        },
        &mut output,
    )
    .unwrap();

    let text = String::from_utf8(output).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&text).expect("should be valid JSON");

    // MariaDB tablespace should be incompatible with MySQL target
    assert_eq!(parsed["compatible"], false);

    // Should have a vendor error check
    let checks = parsed["checks"].as_array().unwrap();
    let vendor_errors: Vec<_> = checks
        .iter()
        .filter(|c| c["check"] == "vendor" && c["severity"] == "Error")
        .collect();
    assert_eq!(vendor_errors.len(), 1);
}

#[test]
fn test_compat_verbose_text_output() {
    let page0 = build_fsp_hdr_page(42, 2);
    let page1 = build_index_page(1, 42, 2000);

    let tmp = write_tablespace(&[page0, page1]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::compat::execute(
        &idb::cli::compat::CompatOptions {
            file: Some(path.to_string()),
            scan: None,
            target: "8.0.0".to_string(),
            verbose: true,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
            depth: None,
        },
        &mut output,
    )
    .unwrap();

    let text = String::from_utf8(output).unwrap();
    // Verbose mode should include "Current:" and "Expected:" lines
    assert!(text.contains("Current:"));
    assert!(text.contains("Expected:"));
}

#[test]
fn test_compat_57_target_no_sdi_ok() {
    let page0 = build_fsp_hdr_page(42, 2);
    let page1 = build_index_page(1, 42, 2000);

    let tmp = write_tablespace(&[page0, page1]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::compat::execute(
        &idb::cli::compat::CompatOptions {
            file: Some(path.to_string()),
            scan: None,
            target: "5.7.44".to_string(),
            verbose: false,
            json: true,
            page_size: None,
            keyring: None,
            mmap: false,
            depth: None,
        },
        &mut output,
    )
    .unwrap();

    let text = String::from_utf8(output).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&text).expect("should be valid JSON");

    // No SDI is fine for 5.7 target, no vendor issues for MySQL
    assert_eq!(parsed["compatible"], true);
    assert_eq!(parsed["summary"]["errors"], 0);
}

#[test]
fn test_compat_invalid_version_returns_error() {
    let page0 = build_fsp_hdr_page(42, 1);
    let tmp = write_tablespace(&[page0]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    let result = idb::cli::compat::execute(
        &idb::cli::compat::CompatOptions {
            file: Some(path.to_string()),
            scan: None,
            target: "invalid".to_string(),
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
            depth: None,
        },
        &mut output,
    );

    assert!(result.is_err());
}
