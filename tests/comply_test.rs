#![cfg(feature = "cli")]
//! Integration tests for `inno comply` (Epic 10: GDPR / compliance verification).
//!
//! These run against a committed real MySQL 9.0 tablespace fixture whose single
//! table `standard(id INT PK, name VARCHAR(100), data TEXT)` holds three rows;
//! row 1 has `name = "test_row_1"` (verified via a hex dump of leaf page 4).
//!
//! The present/absent assertion PAIRS are the mutation checks: a scanner that
//! always returned empty would fail the "present" case, and one that always
//! returned matches would fail the "absent" case. Both directions must hold.

use idb::innodb::compliance::{encryption_audit, scan_residue, verify_deleted, Pattern, Region};
use idb::innodb::tablespace::Tablespace;

const FIXTURE: &str = "tests/fixtures/mysql9/mysql90_standard.ibd";

fn open() -> Tablespace {
    Tablespace::open(FIXTURE).expect("open fixture")
}

// ---------------------------------------------------------------------------
// Residue scanning
// ---------------------------------------------------------------------------

#[test]
fn scan_residue_finds_known_record_string() {
    let mut ts = open();
    let pat = Pattern::parse("test_row_1").unwrap();
    let matches = scan_residue(&mut ts, &pat, 1000).unwrap();
    assert_eq!(matches.len(), 1, "expected exactly one live record match");
    let m = &matches[0];
    assert_eq!(m.page_number, 4, "leaf page holding the clustered records");
    assert_eq!(m.region, Region::RecordHeap);
    // "test_row_1" == 746573745f726f775f31
    assert!(m.context_hex.starts_with("746573745f726f775f31"));
}

#[test]
fn scan_residue_absent_pattern_finds_nothing() {
    // Mutation pair with the test above: proves matches are content-driven,
    // not a constant non-empty result.
    let mut ts = open();
    let pat = Pattern::parse("zzz_definitely_absent_value").unwrap();
    let matches = scan_residue(&mut ts, &pat, 1000).unwrap();
    assert!(matches.is_empty());
}

#[test]
fn scan_residue_hex_pattern() {
    // hex for "test_row_1"
    let mut ts = open();
    let pat = Pattern::parse("hex:746573745f726f775f31").unwrap();
    let matches = scan_residue(&mut ts, &pat, 1000).unwrap();
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].page_number, 4);
}

#[test]
fn scan_residue_respects_max_hits() {
    // "x" (the data column padding) appears many times; cap should bound it.
    let mut ts = open();
    let pat = Pattern::parse("x").unwrap();
    let matches = scan_residue(&mut ts, &pat, 5).unwrap();
    assert_eq!(matches.len(), 5);
}

// ---------------------------------------------------------------------------
// Deletion verification
// ---------------------------------------------------------------------------

#[test]
fn verify_present_string_is_not_purged() {
    let mut ts = open();
    let report = verify_deleted(&mut ts, "name", "test_row_1", false).unwrap();
    assert!(!report.fully_purged);
    assert!(report
        .residue_sites
        .iter()
        .any(|s| s.region == "live_record"));
    assert_eq!(report.table_name.as_deref(), Some("standard"));
    assert_eq!(report.column, "name");
}

#[test]
fn verify_absent_string_is_purged() {
    // Mutation pair: a broken decode/compare that never matched would make the
    // "present" test above fail; a broken one that always matched fails here.
    let mut ts = open();
    let report = verify_deleted(&mut ts, "name", "absent_zzz_value", false).unwrap();
    assert!(report.fully_purged);
    assert!(report.residue_sites.is_empty());
}

#[test]
fn verify_present_pk_is_not_purged() {
    let mut ts = open();
    let report = verify_deleted(&mut ts, "id", "1", false).unwrap();
    assert!(!report.fully_purged);
}

#[test]
fn verify_absent_pk_is_purged() {
    let mut ts = open();
    let report = verify_deleted(&mut ts, "id", "999999", false).unwrap();
    assert!(report.fully_purged);
}

#[test]
fn verify_pk_column_scans_undo_region_but_non_pk_does_not() {
    // Undo stores PK fields for deletes, so the undo region is only scanned when
    // the target is a PK column.
    let mut ts = open();
    let pk_report = verify_deleted(&mut ts, "id", "1", false).unwrap();
    assert!(pk_report
        .regions_scanned
        .iter()
        .any(|r| r == "undo_del_mark"));

    let mut ts2 = open();
    let non_pk_report = verify_deleted(&mut ts2, "name", "test_row_1", false).unwrap();
    assert!(!non_pk_report
        .regions_scanned
        .iter()
        .any(|r| r == "undo_del_mark"));
}

#[test]
fn verify_thorough_adds_raw_pass() {
    let mut ts = open();
    let report = verify_deleted(&mut ts, "name", "test_row_1", true).unwrap();
    assert!(report.thorough);
    assert!(report.regions_scanned.iter().any(|r| r == "raw_page_bytes"));
    // Logical (live_record) hit + at least one raw byte-pass hit for the utf8 form,
    // tagged with the page region it landed in (e.g. raw_record_heap).
    assert!(report.residue_sites.len() >= 2);
    assert!(report
        .residue_sites
        .iter()
        .any(|s| s.region.starts_with("raw_")));
}

#[test]
fn verify_unknown_column_is_argument_error() {
    let mut ts = open();
    let err = verify_deleted(&mut ts, "no_such_column", "x", false).unwrap_err();
    match err {
        idb::IdbError::Argument(_) => {}
        other => panic!("expected Argument error, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Encryption audit
// ---------------------------------------------------------------------------

#[test]
fn encryption_audit_plaintext_fixture() {
    let mut ts = open();
    let report = encryption_audit(&mut ts).unwrap();
    assert!(!report.tablespace_encrypted);
    assert_eq!(report.encrypted_page_count, 0);
    assert_eq!(report.total_pages, 7);
    assert!(report.algorithm.is_none());
}

// ---------------------------------------------------------------------------
// CLI wiring
// ---------------------------------------------------------------------------

#[test]
fn cli_scan_mode_reports_match() {
    let opts = idb::cli::comply::ComplyOptions {
        file: FIXTURE.to_string(),
        verify_deleted: false,
        scan_residue: true,
        encryption_audit: false,
        table: None,
        where_clause: None,
        pattern: Some("test_row_1".to_string()),
        thorough: false,
        max_hits: 1000,
        page_size: None,
        keyring: None,
        mmap: false,
        json: false,
        csv: false,
        verbose: false,
    };
    let mut out = Vec::new();
    idb::cli::comply::execute(&opts, &mut out).unwrap();
    let text = String::from_utf8(out).unwrap();
    assert!(text.contains("1 match"));
    assert!(text.contains("record_heap"));
}

#[test]
fn cli_verify_mode_rejects_missing_where() {
    let opts = idb::cli::comply::ComplyOptions {
        file: FIXTURE.to_string(),
        verify_deleted: true,
        scan_residue: false,
        encryption_audit: false,
        table: None,
        where_clause: None,
        pattern: None,
        thorough: false,
        max_hits: 1000,
        page_size: None,
        keyring: None,
        mmap: false,
        json: false,
        csv: false,
        verbose: false,
    };
    let mut out = Vec::new();
    let err = idb::cli::comply::execute(&opts, &mut out).unwrap_err();
    assert!(matches!(err, idb::IdbError::Argument(_)));
}

#[test]
fn cli_no_mode_selected_errors() {
    let opts = idb::cli::comply::ComplyOptions {
        file: FIXTURE.to_string(),
        verify_deleted: false,
        scan_residue: false,
        encryption_audit: false,
        table: None,
        where_clause: None,
        pattern: None,
        thorough: false,
        max_hits: 1000,
        page_size: None,
        keyring: None,
        mmap: false,
        json: false,
        csv: false,
        verbose: false,
    };
    let mut out = Vec::new();
    assert!(idb::cli::comply::execute(&opts, &mut out).is_err());
}

// ---------------------------------------------------------------------------
// audit --compliance (#182)
// ---------------------------------------------------------------------------

#[test]
fn audit_compliance_scans_directory() {
    let opts = idb::cli::audit::AuditOptions {
        datadir: "tests/fixtures/mysql9".to_string(),
        health: false,
        checksum_mismatch: false,
        verbose: false,
        json: false,
        csv: false,
        prometheus: false,
        page_size: None,
        keyring: None,
        mmap: false,
        min_fill_factor: None,
        max_fragmentation: None,
        bloat: false,
        max_bloat_grade: None,
        depth: None,
        compliance: true,
        pattern: Some("test_row_1".to_string()),
    };
    let mut out = Vec::new();
    idb::cli::audit::execute(&opts, &mut out).unwrap();
    let text = String::from_utf8(out).unwrap();
    // Both the 9.0 and 9.1 standard fixtures carry the same test data.
    assert!(text.contains("mysql90_standard.ibd"));
    assert!(text.contains("match"));
}

#[test]
fn audit_compliance_requires_pattern() {
    let opts = idb::cli::audit::AuditOptions {
        datadir: "tests/fixtures/mysql9".to_string(),
        health: false,
        checksum_mismatch: false,
        verbose: false,
        json: false,
        csv: false,
        prometheus: false,
        page_size: None,
        keyring: None,
        mmap: false,
        min_fill_factor: None,
        max_fragmentation: None,
        bloat: false,
        max_bloat_grade: None,
        depth: None,
        compliance: true,
        pattern: None,
    };
    let mut out = Vec::new();
    assert!(idb::cli::audit::execute(&opts, &mut out).is_err());
}
