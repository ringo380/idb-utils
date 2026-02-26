#![cfg(feature = "cli")]
//! Integration tests for `inno verify`.

use byteorder::{BigEndian, ByteOrder};
use std::io::Write;
use tempfile::NamedTempFile;

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

fn build_index_page(page_num: u32, space_id: u32, lsn: u64, prev: u32, next: u32) -> Vec<u8> {
    let mut page = vec![0u8; PS];

    // FIL header
    BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], page_num);
    BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], prev);
    BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], next);
    BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], lsn);
    BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855); // INDEX
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);

    // INDEX page header (minimal)
    let ph = FIL_PAGE_DATA;
    BigEndian::write_u16(&mut page[ph + PAGE_N_DIR_SLOTS..], 2);
    BigEndian::write_u16(&mut page[ph + PAGE_HEAP_TOP..], 200);
    BigEndian::write_u16(&mut page[ph + PAGE_N_HEAP..], 0x8002); // compact
    BigEndian::write_u16(&mut page[ph + PAGE_N_RECS..], 1);
    BigEndian::write_u16(&mut page[ph + PAGE_LEVEL..], 0); // leaf
    BigEndian::write_u64(&mut page[ph + PAGE_INDEX_ID..], 100);

    // FIL trailer
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

// ── Test: valid tablespace passes all checks ─────────────────────────

#[test]
fn test_verify_passes_valid_tablespace() {
    let page0 = build_fsp_hdr_page(42, 4);
    let page1 = build_index_page(1, 42, 2000, FIL_NULL, 2);
    let page2 = build_index_page(2, 42, 3000, 1, FIL_NULL);
    let page3 = build_index_page(3, 42, 4000, FIL_NULL, FIL_NULL);

    let tmp = write_tablespace(&[page0, page1, page2, page3]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    let result = idb::cli::verify::execute(
        &idb::cli::verify::VerifyOptions {
            file: path.to_string(),
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
            redo: None,
            chain: vec![],
        },
        &mut output,
    );

    assert!(
        result.is_ok(),
        "Expected verify to pass on valid tablespace"
    );
    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("PASS"));
    assert!(text.contains("Structural Verification"));
}

// ── Test: wrong page_number in header fails PageNumberSequence ───────

#[test]
fn test_verify_fails_wrong_page_number() {
    let page0 = build_fsp_hdr_page(42, 3);

    // Build page at position 1 but with page_number 5 in header
    let mut page1 = build_index_page(5, 42, 2000, FIL_NULL, FIL_NULL);
    // page_number is already set to 5 by build_index_page
    recalculate_checksum(&mut page1, PAGE_SIZE, ChecksumAlgorithm::Crc32c);

    let page2 = build_index_page(2, 42, 3000, FIL_NULL, FIL_NULL);

    let tmp = write_tablespace(&[page0, page1, page2]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    let result = idb::cli::verify::execute(
        &idb::cli::verify::VerifyOptions {
            file: path.to_string(),
            verbose: true,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
            redo: None,
            chain: vec![],
        },
        &mut output,
    );

    assert!(
        result.is_err(),
        "Expected verify to fail on wrong page number"
    );
    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("FAIL"));
    assert!(text.contains("page_number_sequence"));
    assert!(text.contains("Page 1 has page_number 5"));
}

// ── Test: mixed space_ids fails SpaceIdConsistency ───────────────────

#[test]
fn test_verify_fails_mixed_space_ids() {
    let page0 = build_fsp_hdr_page(42, 3);

    // Build page 1 with space_id 99 instead of 42
    let page1 = build_index_page(1, 99, 2000, FIL_NULL, FIL_NULL);
    let page2 = build_index_page(2, 42, 3000, FIL_NULL, FIL_NULL);

    let tmp = write_tablespace(&[page0, page1, page2]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    let result = idb::cli::verify::execute(
        &idb::cli::verify::VerifyOptions {
            file: path.to_string(),
            verbose: true,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
            redo: None,
            chain: vec![],
        },
        &mut output,
    );

    assert!(
        result.is_err(),
        "Expected verify to fail on mixed space IDs"
    );
    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("space_id_consistency"));
    assert!(text.contains("space_id 99"));
}

// ── Test: broken prev/next chain fails PageChainBounds ──────────────

#[test]
fn test_verify_fails_chain_out_of_bounds() {
    let page0 = build_fsp_hdr_page(42, 3);

    // Build page 1 with next pointer pointing beyond file bounds
    let page1 = build_index_page(1, 42, 2000, FIL_NULL, 999);
    let page2 = build_index_page(2, 42, 3000, FIL_NULL, FIL_NULL);

    let tmp = write_tablespace(&[page0, page1, page2]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    let result = idb::cli::verify::execute(
        &idb::cli::verify::VerifyOptions {
            file: path.to_string(),
            verbose: true,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
            redo: None,
            chain: vec![],
        },
        &mut output,
    );

    assert!(
        result.is_err(),
        "Expected verify to fail on out-of-bounds chain pointer"
    );
    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("page_chain_bounds"));
    assert!(text.contains("next pointer 999"));
}

// ── Test: trailer LSN mismatch fails TrailerLsnMatch ────────────────

#[test]
fn test_verify_fails_trailer_lsn_mismatch() {
    let page0 = build_fsp_hdr_page(42, 2);

    // Build page 1 with correct header LSN but broken trailer
    let mut page1 = vec![0u8; PS];
    let lsn: u64 = 5000;
    BigEndian::write_u32(&mut page1[FIL_PAGE_OFFSET..], 1);
    BigEndian::write_u32(&mut page1[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut page1[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut page1[FIL_PAGE_LSN..], lsn);
    BigEndian::write_u16(&mut page1[FIL_PAGE_TYPE..], 17855);
    BigEndian::write_u32(&mut page1[FIL_PAGE_SPACE_ID..], 42);

    // Set trailer LSN low-32 to a WRONG value
    let trailer = PS - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut page1[trailer + 4..], 0xDEADBEEF);

    // Recalculate checksum (checksum won't fix the trailer LSN mismatch)
    recalculate_checksum(&mut page1, PAGE_SIZE, ChecksumAlgorithm::Crc32c);

    let tmp = write_tablespace(&[page0, page1]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    let result = idb::cli::verify::execute(
        &idb::cli::verify::VerifyOptions {
            file: path.to_string(),
            verbose: true,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
            redo: None,
            chain: vec![],
        },
        &mut output,
    );

    assert!(
        result.is_err(),
        "Expected verify to fail on trailer LSN mismatch"
    );
    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("trailer_lsn_match"));
}

// ── Test: JSON output is valid and contains expected fields ─────────

#[test]
fn test_verify_json_output_valid_tablespace() {
    let page0 = build_fsp_hdr_page(42, 3);
    let page1 = build_index_page(1, 42, 2000, FIL_NULL, FIL_NULL);
    let page2 = build_index_page(2, 42, 3000, FIL_NULL, FIL_NULL);

    let tmp = write_tablespace(&[page0, page1, page2]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    let result = idb::cli::verify::execute(
        &idb::cli::verify::VerifyOptions {
            file: path.to_string(),
            verbose: false,
            json: true,
            page_size: None,
            keyring: None,
            mmap: false,
            redo: None,
            chain: vec![],
        },
        &mut output,
    );

    assert!(result.is_ok());

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert!(json.get("file").is_some());
    assert_eq!(json["total_pages"], 3);
    assert_eq!(json["page_size"], 16384);
    assert_eq!(json["passed"], true);
    // findings is omitted from JSON when empty (skip_serializing_if)
    assert!(json.get("findings").is_none());
    assert!(!json["summary"].as_array().unwrap().is_empty());

    // Check that all summary entries have the expected fields
    for s in json["summary"].as_array().unwrap() {
        assert!(s.get("kind").is_some());
        assert!(s.get("pages_checked").is_some());
        assert!(s.get("issues_found").is_some());
        assert!(s.get("passed").is_some());
    }
}

// ── Test: JSON output with failures includes findings ───────────────

#[test]
fn test_verify_json_output_with_failures() {
    let page0 = build_fsp_hdr_page(42, 2);

    // Build page 1 with wrong space_id
    let page1 = build_index_page(1, 99, 2000, FIL_NULL, FIL_NULL);

    let tmp = write_tablespace(&[page0, page1]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    let _result = idb::cli::verify::execute(
        &idb::cli::verify::VerifyOptions {
            file: path.to_string(),
            verbose: false,
            json: true,
            page_size: None,
            keyring: None,
            mmap: false,
            redo: None,
            chain: vec![],
        },
        &mut output,
    );

    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(json["passed"], false);
    assert!(!json["findings"].as_array().unwrap().is_empty());

    // Check that at least one finding has the space_id issue
    let findings = json["findings"].as_array().unwrap();
    let has_space_id_finding = findings.iter().any(|f| f["kind"] == "SpaceIdConsistency");
    assert!(
        has_space_id_finding,
        "Expected to find SpaceIdConsistency finding in JSON"
    );

    // Check that the summary for space_id_consistency shows failure
    let summaries = json["summary"].as_array().unwrap();
    let space_id_summary = summaries
        .iter()
        .find(|s| s["kind"] == "SpaceIdConsistency")
        .unwrap();
    assert_eq!(space_id_summary["passed"], false);
    assert!(space_id_summary["issues_found"].as_u64().unwrap() > 0);
}

// ── Test: valid tablespace with empty pages passes ──────────────────

#[test]
fn test_verify_passes_with_empty_pages() {
    let page0 = build_fsp_hdr_page(42, 3);
    let page1 = vec![0u8; PS]; // all-zero page (should be skipped)
    let page2 = build_index_page(2, 42, 3000, FIL_NULL, FIL_NULL);

    let tmp = write_tablespace(&[page0, page1, page2]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    let result = idb::cli::verify::execute(
        &idb::cli::verify::VerifyOptions {
            file: path.to_string(),
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
            redo: None,
            chain: vec![],
        },
        &mut output,
    );

    assert!(
        result.is_ok(),
        "Expected verify to pass when empty pages are present"
    );
}

// ── Test: prev pointer out of bounds ────────────────────────────────

#[test]
fn test_verify_fails_prev_pointer_out_of_bounds() {
    let page0 = build_fsp_hdr_page(42, 2);

    // Build page 1 with prev pointing beyond file
    let page1 = build_index_page(1, 42, 2000, 500, FIL_NULL);

    let tmp = write_tablespace(&[page0, page1]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    let result = idb::cli::verify::execute(
        &idb::cli::verify::VerifyOptions {
            file: path.to_string(),
            verbose: true,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
            redo: None,
            chain: vec![],
        },
        &mut output,
    );

    assert!(
        result.is_err(),
        "Expected verify to fail on prev pointer out of bounds"
    );
    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("prev pointer 500"));
}

// ── Backup chain tests ──────────────────────────────────────────────

#[test]
fn test_chain_contiguous_passes() {
    // Two tablespaces with overlapping LSN ranges
    let page0a = build_fsp_hdr_page(42, 2);
    let page1a = build_index_page(1, 42, 1000, FIL_NULL, FIL_NULL);
    let file_a = write_tablespace(&[page0a, page1a]);

    let page0b = build_fsp_hdr_page(42, 2);
    let page1b = build_index_page(1, 42, 2000, FIL_NULL, FIL_NULL);
    let file_b = write_tablespace(&[page0b, page1b]);

    let mut output = Vec::new();
    let result = idb::cli::verify::execute(
        &idb::cli::verify::VerifyOptions {
            file: file_a.path().to_str().unwrap().to_string(),
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
            redo: None,
            chain: vec![
                file_a.path().to_str().unwrap().to_string(),
                file_b.path().to_str().unwrap().to_string(),
            ],
        },
        &mut output,
    );

    assert!(result.is_ok(), "Expected chain to pass");
    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("PASS"));
}

#[test]
fn test_chain_json_output() {
    let page0a = build_fsp_hdr_page(42, 2);
    let page1a = build_index_page(1, 42, 1000, FIL_NULL, FIL_NULL);
    let file_a = write_tablespace(&[page0a, page1a]);

    let page0b = build_fsp_hdr_page(42, 2);
    let page1b = build_index_page(1, 42, 2000, FIL_NULL, FIL_NULL);
    let file_b = write_tablespace(&[page0b, page1b]);

    let mut output = Vec::new();
    let result = idb::cli::verify::execute(
        &idb::cli::verify::VerifyOptions {
            file: file_a.path().to_str().unwrap().to_string(),
            verbose: false,
            json: true,
            page_size: None,
            keyring: None,
            mmap: false,
            redo: None,
            chain: vec![
                file_a.path().to_str().unwrap().to_string(),
                file_b.path().to_str().unwrap().to_string(),
            ],
        },
        &mut output,
    );

    assert!(result.is_ok());
    let text = String::from_utf8(output).unwrap();
    let json: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(json["contiguous"], true);
    assert_eq!(json["consistent_space_id"], true);
    assert_eq!(json["files"].as_array().unwrap().len(), 2);
}

#[test]
fn test_chain_mixed_space_ids_fails() {
    let page0a = build_fsp_hdr_page(42, 2);
    let page1a = build_index_page(1, 42, 1000, FIL_NULL, FIL_NULL);
    let file_a = write_tablespace(&[page0a, page1a]);

    // Different space_id
    let page0b = build_fsp_hdr_page(99, 2);
    let page1b = build_index_page(1, 99, 2000, FIL_NULL, FIL_NULL);
    let file_b = write_tablespace(&[page0b, page1b]);

    let mut output = Vec::new();
    let result = idb::cli::verify::execute(
        &idb::cli::verify::VerifyOptions {
            file: file_a.path().to_str().unwrap().to_string(),
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
            redo: None,
            chain: vec![
                file_a.path().to_str().unwrap().to_string(),
                file_b.path().to_str().unwrap().to_string(),
            ],
        },
        &mut output,
    );

    assert!(
        result.is_err(),
        "Expected chain to fail with mixed space IDs"
    );
    let text = String::from_utf8(output).unwrap();
    assert!(text.contains("FAIL"));
}

#[test]
fn test_chain_requires_two_files() {
    let page0 = build_fsp_hdr_page(42, 2);
    let page1 = build_index_page(1, 42, 1000, FIL_NULL, FIL_NULL);
    let file_a = write_tablespace(&[page0, page1]);

    let mut output = Vec::new();
    let result = idb::cli::verify::execute(
        &idb::cli::verify::VerifyOptions {
            file: file_a.path().to_str().unwrap().to_string(),
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
            redo: None,
            chain: vec![file_a.path().to_str().unwrap().to_string()],
        },
        &mut output,
    );

    assert!(result.is_err());
}
