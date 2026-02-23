#![cfg(feature = "cli")]
//! Edge-case integration tests for v3.0 write operations (repair, defrag, transplant, corrupt).

use byteorder::{BigEndian, ByteOrder};
use std::io::Write;
use tempfile::NamedTempFile;

use idb::innodb::checksum::{recalculate_checksum, validate_checksum, ChecksumAlgorithm};
use idb::innodb::constants::*;
use idb::innodb::vendor::{MariaDbFormat, VendorInfo};
use idb::innodb::write;

const PAGE_SIZE: u32 = 16384;
const PS: usize = PAGE_SIZE as usize;

const PAGE_SIZE_8K: u32 = 8192;
const PS_8K: usize = PAGE_SIZE_8K as usize;

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
    BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855); // INDEX
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);
    let trailer = PS - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut page[trailer + 4..], (lsn & 0xFFFFFFFF) as u32);
    recalculate_checksum(&mut page, PAGE_SIZE, ChecksumAlgorithm::Crc32c);
    page
}

fn build_index_page_with_chain(
    page_num: u32,
    space_id: u32,
    lsn: u64,
    prev: u32,
    next: u32,
    index_id: u64,
    level: u16,
) -> Vec<u8> {
    let mut page = vec![0u8; PS];
    BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], page_num);
    BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], prev);
    BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], next);
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

fn build_undo_page(page_num: u32, space_id: u32, lsn: u64) -> Vec<u8> {
    let mut page = vec![0u8; PS];
    BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], page_num);
    BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], lsn);
    BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 2); // UNDO_LOG
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);
    let trailer = PS - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut page[trailer + 4..], (lsn & 0xFFFFFFFF) as u32);
    recalculate_checksum(&mut page, PAGE_SIZE, ChecksumAlgorithm::Crc32c);
    page
}

fn build_index_page_defrag(
    page_num: u32,
    space_id: u32,
    lsn: u64,
    index_id: u64,
    level: u16,
) -> Vec<u8> {
    let mut page = vec![0u8; PS];
    BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], page_num);
    BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], lsn);
    BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855); // INDEX
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);

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

fn build_8k_index_page(page_num: u32, space_id: u32, lsn: u64) -> Vec<u8> {
    let mut page = vec![0u8; PS_8K];
    BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], page_num);
    BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], lsn);
    BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855); // INDEX
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);
    let trailer = PS_8K - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut page[trailer + 4..], (lsn & 0xFFFFFFFF) as u32);
    recalculate_checksum(&mut page, PAGE_SIZE_8K, ChecksumAlgorithm::Crc32c);
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

// ---------------------------------------------------------------------------
// 1. MariaDB full_crc32 repair
// ---------------------------------------------------------------------------

#[test]
fn test_mariadb_full_crc32_repair() {
    let flags = MARIADB_FSP_FLAGS_FCRC32_MARKER_MASK; // bit 4 = 0x10
    let page0 = write::build_fsp_page(
        42,
        3,
        flags,
        1000,
        PAGE_SIZE,
        ChecksumAlgorithm::MariaDbFullCrc32,
    );
    let mut page1 = vec![0u8; PS];
    BigEndian::write_u32(&mut page1[FIL_PAGE_OFFSET..], 1);
    BigEndian::write_u32(&mut page1[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut page1[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut page1[FIL_PAGE_LSN..], 2000);
    BigEndian::write_u16(&mut page1[FIL_PAGE_TYPE..], 17855);
    BigEndian::write_u32(&mut page1[FIL_PAGE_SPACE_ID..], 42);
    let trailer = PS - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut page1[trailer + 4..], 2000);
    recalculate_checksum(&mut page1, PAGE_SIZE, ChecksumAlgorithm::MariaDbFullCrc32);

    let mut page2 = page1.clone();
    BigEndian::write_u32(&mut page2[FIL_PAGE_OFFSET..], 2);
    BigEndian::write_u64(&mut page2[FIL_PAGE_LSN..], 3000);
    let trailer = PS - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut page2[trailer + 4..], 3000);
    recalculate_checksum(&mut page2, PAGE_SIZE, ChecksumAlgorithm::MariaDbFullCrc32);

    // Corrupt page 1 by zeroing the last 4 bytes (where MariaDB stores the checksum)
    BigEndian::write_u32(&mut page1[PS - 4..], 0xBADBAD00);

    let vendor = VendorInfo::mariadb(MariaDbFormat::FullCrc32);
    assert!(!validate_checksum(&page1, PAGE_SIZE, Some(&vendor)).valid);

    let tmp = write_tablespace(&[page0, page1, page2]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::repair::execute(
        &idb::cli::repair::RepairOptions {
            file: path.to_string(),
            page: None,
            algorithm: "full_crc32".to_string(),
            no_backup: true,
            dry_run: false,
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    // All pages should now be valid under MariaDB full_crc32
    for i in 0..3 {
        let page = write::read_page_raw(path, i, PAGE_SIZE).unwrap();
        assert!(
            validate_checksum(&page, PAGE_SIZE, Some(&vendor)).valid,
            "Page {} invalid after MariaDB full_crc32 repair",
            i
        );
    }
}

// ---------------------------------------------------------------------------
// 2. Repair single page leaves other pages untouched
// ---------------------------------------------------------------------------

#[test]
fn test_repair_single_page_leaves_others_untouched() {
    let page0 = build_fsp_hdr_page(42, 4);
    let page1 = build_index_page(1, 42, 2000);
    let mut page2 = build_index_page(2, 42, 3000);
    let page3 = build_index_page(3, 42, 4000);

    // Save originals before corruption
    let original_page1 = page1.clone();
    let original_page3 = page3.clone();

    corrupt_checksum(&mut page2);

    let tmp = write_tablespace(&[page0, page1, page2, page3]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::repair::execute(
        &idb::cli::repair::RepairOptions {
            file: path.to_string(),
            page: Some(2),
            algorithm: "crc32c".to_string(),
            no_backup: true,
            dry_run: false,
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    // Page 2 should now be valid
    let p2 = write::read_page_raw(path, 2, PAGE_SIZE).unwrap();
    assert!(validate_checksum(&p2, PAGE_SIZE, None).valid);

    // Pages 1 and 3 should be byte-identical to originals
    let p1 = write::read_page_raw(path, 1, PAGE_SIZE).unwrap();
    let p3 = write::read_page_raw(path, 3, PAGE_SIZE).unwrap();
    assert_eq!(
        p1, original_page1,
        "Page 1 was modified by single-page repair"
    );
    assert_eq!(
        p3, original_page3,
        "Page 3 was modified by single-page repair"
    );
}

// ---------------------------------------------------------------------------
// 3. Repair dry-run does not modify file
// ---------------------------------------------------------------------------

#[test]
fn test_repair_dry_run_file_unchanged() {
    let page0 = build_fsp_hdr_page(42, 3);
    let page1 = build_index_page(1, 42, 2000);
    let mut page2 = build_index_page(2, 42, 3000);
    corrupt_checksum(&mut page2);

    let tmp = write_tablespace(&[page0, page1, page2]);
    let path = tmp.path().to_str().unwrap();

    // Read entire file before repair
    let before_bytes = std::fs::read(path).unwrap();

    let mut output = Vec::new();
    idb::cli::repair::execute(
        &idb::cli::repair::RepairOptions {
            file: path.to_string(),
            page: None,
            algorithm: "auto".to_string(),
            no_backup: true,
            dry_run: true,
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    // File should be completely unchanged
    let after_bytes = std::fs::read(path).unwrap();
    assert_eq!(before_bytes, after_bytes, "Dry-run modified the file");
}

// ---------------------------------------------------------------------------
// 4. Defrag with mixed page types (INDEX + UNDO)
// ---------------------------------------------------------------------------

#[test]
fn test_defrag_mixed_page_types() {
    let page0 = write::build_fsp_page(42, 5, 0, 1000, PAGE_SIZE, ChecksumAlgorithm::Crc32c);
    let page1 = build_index_page_defrag(1, 42, 2000, 100, 0);
    let page2 = build_index_page_defrag(2, 42, 3000, 100, 0);
    let page3 = build_undo_page(3, 42, 4000);
    let page4 = build_index_page_defrag(4, 42, 5000, 200, 0);

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

    // 5 source pages, all should survive (none empty, none corrupt)
    assert_eq!(json["source_pages"], 5);
    assert_eq!(json["output_pages"], 5);
    assert_eq!(json["index_pages"], 3);
    assert_eq!(json["empty_removed"], 0);

    // All output pages should have valid checksums
    for i in 0..5u64 {
        let page = write::read_page_raw(&output_path, i, PAGE_SIZE).unwrap();
        assert!(
            validate_checksum(&page, PAGE_SIZE, None).valid,
            "Page {} invalid in mixed-type defrag output",
            i
        );
    }

    let _ = std::fs::remove_file(&output_path);
}

// ---------------------------------------------------------------------------
// 5. Defrag removes empty pages
// ---------------------------------------------------------------------------

#[test]
fn test_defrag_removes_empty_pages_edge() {
    let page0 = write::build_fsp_page(42, 5, 0, 1000, PAGE_SIZE, ChecksumAlgorithm::Crc32c);
    let page1 = build_index_page_defrag(1, 42, 2000, 100, 0);
    let empty1 = vec![0u8; PS];
    let page3 = build_index_page_defrag(3, 42, 3000, 100, 0);
    let empty2 = vec![0u8; PS];

    let tmp = write_tablespace(&[page0, page1, empty1, page3, empty2]);
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

    assert_eq!(json["source_pages"], 5);
    assert_eq!(json["output_pages"], 3); // page0 + 2 INDEX pages
    assert_eq!(json["empty_removed"], 2);

    // Verify output is smaller than input
    let output_size = std::fs::metadata(&output_path).unwrap().len();
    assert_eq!(output_size, 3 * PS as u64);

    let _ = std::fs::remove_file(&output_path);
}

// ---------------------------------------------------------------------------
// 6. Defrag preserves prev/next chain for leaf pages
// ---------------------------------------------------------------------------

#[test]
fn test_defrag_preserves_prev_next_chain() {
    let page0 = write::build_fsp_page(42, 4, 0, 1000, PAGE_SIZE, ChecksumAlgorithm::Crc32c);
    // Three leaf pages from the same index, with an initial (broken) chain
    let page1 = build_index_page_with_chain(1, 42, 2000, FIL_NULL, 2, 100, 0);
    let page2 = build_index_page_with_chain(2, 42, 3000, 1, 3, 100, 0);
    let page3 = build_index_page_with_chain(3, 42, 4000, 2, FIL_NULL, 100, 0);

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

    // Verify the prev/next chain in output: 1 <-> 2 <-> 3
    let p1 = write::read_page_raw(&output_path, 1, PAGE_SIZE).unwrap();
    let p2 = write::read_page_raw(&output_path, 2, PAGE_SIZE).unwrap();
    let p3 = write::read_page_raw(&output_path, 3, PAGE_SIZE).unwrap();

    assert_eq!(BigEndian::read_u32(&p1[FIL_PAGE_PREV..]), FIL_NULL);
    assert_eq!(BigEndian::read_u32(&p1[FIL_PAGE_NEXT..]), 2);

    assert_eq!(BigEndian::read_u32(&p2[FIL_PAGE_PREV..]), 1);
    assert_eq!(BigEndian::read_u32(&p2[FIL_PAGE_NEXT..]), 3);

    assert_eq!(BigEndian::read_u32(&p3[FIL_PAGE_PREV..]), 2);
    assert_eq!(BigEndian::read_u32(&p3[FIL_PAGE_NEXT..]), FIL_NULL);

    // All checksums valid
    for i in 0..4u64 {
        let page = write::read_page_raw(&output_path, i, PAGE_SIZE).unwrap();
        assert!(
            validate_checksum(&page, PAGE_SIZE, None).valid,
            "Page {} invalid after defrag chain preservation",
            i
        );
    }

    let _ = std::fs::remove_file(&output_path);
}

// ---------------------------------------------------------------------------
// 7. Transplant single page replaces only the targeted page
// ---------------------------------------------------------------------------

#[test]
fn test_transplant_single_page_only_target_differs() {
    let page0 = write::build_fsp_page(42, 3, 0, 1000, PAGE_SIZE, ChecksumAlgorithm::Crc32c);
    let donor_page1 = build_index_page(1, 42, 9999); // different LSN
    let donor_page2 = build_index_page(2, 42, 8888);
    let donor = write_tablespace(&[page0.clone(), donor_page1, donor_page2]);

    let target_page1 = build_index_page(1, 42, 2000);
    let target_page2 = build_index_page(2, 42, 3000);
    let target = write_tablespace(&[page0, target_page1, target_page2]);

    let donor_path = donor.path().to_str().unwrap();
    let target_path = target.path().to_str().unwrap();

    // Save original target pages
    let original_target_p0 = write::read_page_raw(target_path, 0, PAGE_SIZE).unwrap();
    let original_target_p2 = write::read_page_raw(target_path, 2, PAGE_SIZE).unwrap();

    let mut output = Vec::new();
    idb::cli::transplant::execute(
        &idb::cli::transplant::TransplantOptions {
            donor: donor_path.to_string(),
            target: target_path.to_string(),
            pages: vec![1],
            no_backup: true,
            force: false,
            dry_run: false,
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    // Page 1 should match donor's page 1
    let transplanted_p1 = write::read_page_raw(target_path, 1, PAGE_SIZE).unwrap();
    let donor_p1 = write::read_page_raw(donor_path, 1, PAGE_SIZE).unwrap();
    assert_eq!(
        transplanted_p1, donor_p1,
        "Transplanted page does not match donor"
    );

    // Pages 0 and 2 should be unchanged
    let after_p0 = write::read_page_raw(target_path, 0, PAGE_SIZE).unwrap();
    let after_p2 = write::read_page_raw(target_path, 2, PAGE_SIZE).unwrap();
    assert_eq!(
        after_p0, original_target_p0,
        "Page 0 was modified by transplant"
    );
    assert_eq!(
        after_p2, original_target_p2,
        "Page 2 was modified by transplant"
    );
}

// ---------------------------------------------------------------------------
// 8. Transplant page 0 rejected without --force
// ---------------------------------------------------------------------------

#[test]
fn test_transplant_page0_rejected_without_force() {
    let page0 = write::build_fsp_page(42, 2, 0, 1000, PAGE_SIZE, ChecksumAlgorithm::Crc32c);
    let page1 = build_index_page(1, 42, 2000);
    let donor = write_tablespace(&[page0.clone(), page1.clone()]);
    let target = write_tablespace(&[page0, page1]);

    let target_path = target.path().to_str().unwrap();
    let original_p0 = write::read_page_raw(target_path, 0, PAGE_SIZE).unwrap();

    let mut output = Vec::new();
    idb::cli::transplant::execute(
        &idb::cli::transplant::TransplantOptions {
            donor: donor.path().to_str().unwrap().to_string(),
            target: target_path.to_string(),
            pages: vec![0],
            no_backup: true,
            force: false,
            dry_run: false,
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

    // Page 0 should be skipped, not transplanted
    assert_eq!(json["skipped"], 1);
    assert_eq!(json["transplanted"], 0);

    // Target page 0 should be unchanged
    let after_p0 = write::read_page_raw(target_path, 0, PAGE_SIZE).unwrap();
    assert_eq!(
        after_p0, original_p0,
        "Page 0 was modified despite no --force"
    );
}

// ---------------------------------------------------------------------------
// 9. Transplant with corrupt donor page skipped without --force
// ---------------------------------------------------------------------------

#[test]
fn test_transplant_corrupt_donor_page_skipped() {
    let page0 = write::build_fsp_page(42, 3, 0, 1000, PAGE_SIZE, ChecksumAlgorithm::Crc32c);

    // Donor: page 1 is corrupt, page 2 is valid
    let mut donor_page1 = build_index_page(1, 42, 5000);
    corrupt_checksum(&mut donor_page1);
    let donor_page2 = build_index_page(2, 42, 6000);
    let donor = write_tablespace(&[page0.clone(), donor_page1, donor_page2]);

    let target_page1 = build_index_page(1, 42, 2000);
    let target_page2 = build_index_page(2, 42, 3000);
    let target = write_tablespace(&[page0, target_page1, target_page2]);

    let target_path = target.path().to_str().unwrap();
    let original_target_p1 = write::read_page_raw(target_path, 1, PAGE_SIZE).unwrap();

    let mut output = Vec::new();
    idb::cli::transplant::execute(
        &idb::cli::transplant::TransplantOptions {
            donor: donor.path().to_str().unwrap().to_string(),
            target: target_path.to_string(),
            pages: vec![1, 2],
            no_backup: true,
            force: false,
            dry_run: false,
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

    // Page 1 (corrupt donor) should be skipped; page 2 transplanted
    assert_eq!(json["skipped"], 1);
    assert_eq!(json["transplanted"], 1);

    // Target page 1 should be unchanged (corrupt donor page was skipped)
    let after_p1 = write::read_page_raw(target_path, 1, PAGE_SIZE).unwrap();
    assert_eq!(
        after_p1, original_target_p1,
        "Target page 1 was overwritten by corrupt donor page"
    );

    // Target page 2 should have been transplanted (donor page 2 was valid)
    let after_p2 = write::read_page_raw(target_path, 2, PAGE_SIZE).unwrap();
    let donor_p2 = write::read_page_raw(donor.path().to_str().unwrap(), 2, PAGE_SIZE).unwrap();
    assert_eq!(
        after_p2, donor_p2,
        "Target page 2 was not transplanted from donor"
    );
}

// ---------------------------------------------------------------------------
// 10. Transplant dry-run does not modify target
// ---------------------------------------------------------------------------

#[test]
fn test_transplant_dry_run_no_modification() {
    let page0 = write::build_fsp_page(42, 3, 0, 1000, PAGE_SIZE, ChecksumAlgorithm::Crc32c);
    let donor_page1 = build_index_page(1, 42, 9999);
    let donor = write_tablespace(&[page0.clone(), donor_page1, build_index_page(2, 42, 3000)]);

    let target_page1 = build_index_page(1, 42, 2000);
    let target = write_tablespace(&[page0, target_page1, build_index_page(2, 42, 3000)]);

    let target_path = target.path().to_str().unwrap();
    let before_bytes = std::fs::read(target_path).unwrap();

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
        },
        &mut output,
    )
    .unwrap();

    let after_bytes = std::fs::read(target_path).unwrap();
    assert_eq!(
        before_bytes, after_bytes,
        "Transplant dry-run modified the target file"
    );
}

// ---------------------------------------------------------------------------
// 11. Corrupt with specific byte count
// ---------------------------------------------------------------------------

#[test]
fn test_corrupt_specific_byte_count() {
    let page0 = build_fsp_hdr_page(42, 2);
    let page1 = build_index_page(1, 42, 2000);
    let original_page1 = page1.clone();

    let tmp = write_tablespace(&[page0, page1]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::corrupt::execute(
        &idb::cli::corrupt::CorruptOptions {
            file: path.to_string(),
            page: Some(1),
            bytes: 16,
            header: false,
            records: false,
            offset: None,
            verify: false,
            json: false,
            page_size: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    let corrupted = write::read_page_raw(path, 1, PAGE_SIZE).unwrap();

    // Count how many bytes differ
    let diff_count = original_page1
        .iter()
        .zip(corrupted.iter())
        .filter(|(a, b)| a != b)
        .count();

    // At least 1 byte should differ (could be fewer than 16 if random bytes
    // happen to match originals, but with 16 random bytes this is extremely unlikely).
    // We check that at most 16 bytes differ (the corruption doesn't write more than requested).
    assert!(diff_count > 0, "No bytes were corrupted");
    assert!(
        diff_count <= 16,
        "More than 16 bytes differ: {}",
        diff_count
    );
}

// ---------------------------------------------------------------------------
// 12. Corrupt header mode targets bytes 0-37
// ---------------------------------------------------------------------------

#[test]
fn test_corrupt_header_mode() {
    let page0 = build_fsp_hdr_page(42, 2);
    let page1 = build_index_page(1, 42, 2000);
    let original_page1 = page1.clone();

    let tmp = write_tablespace(&[page0, page1]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::corrupt::execute(
        &idb::cli::corrupt::CorruptOptions {
            file: path.to_string(),
            page: Some(1),
            bytes: 8,
            header: true,
            records: false,
            offset: None,
            verify: false,
            json: false,
            page_size: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    let corrupted = write::read_page_raw(path, 1, PAGE_SIZE).unwrap();

    // Find the range of differing bytes
    let diffs: Vec<usize> = original_page1
        .iter()
        .zip(corrupted.iter())
        .enumerate()
        .filter(|(_, (a, b))| a != b)
        .map(|(i, _)| i)
        .collect();

    assert!(!diffs.is_empty(), "No bytes were corrupted in header mode");

    // All differences should be within the FIL header (bytes 0-37)
    for &pos in &diffs {
        assert!(
            pos < SIZE_FIL_HEAD,
            "Corruption at byte {} is outside FIL header (expected < {})",
            pos,
            SIZE_FIL_HEAD
        );
    }
}

// ---------------------------------------------------------------------------
// 13. Corrupt record area targets bytes 120+
// ---------------------------------------------------------------------------

#[test]
fn test_corrupt_record_area() {
    let page0 = build_fsp_hdr_page(42, 2);
    let page1 = build_index_page(1, 42, 2000);
    let original_page1 = page1.clone();

    let tmp = write_tablespace(&[page0, page1]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::corrupt::execute(
        &idb::cli::corrupt::CorruptOptions {
            file: path.to_string(),
            page: Some(1),
            bytes: 8,
            header: false,
            records: true,
            offset: None,
            verify: false,
            json: false,
            page_size: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    let corrupted = write::read_page_raw(path, 1, PAGE_SIZE).unwrap();

    let diffs: Vec<usize> = original_page1
        .iter()
        .zip(corrupted.iter())
        .enumerate()
        .filter(|(_, (a, b))| a != b)
        .map(|(i, _)| i)
        .collect();

    assert!(!diffs.is_empty(), "No bytes were corrupted in records mode");

    // All differences should be in the user data area (offset 120+, before trailer)
    let user_data_start = 120;
    let trailer_start = PS - SIZE_FIL_TRAILER;
    for &pos in &diffs {
        assert!(
            pos >= user_data_start && pos < trailer_start,
            "Corruption at byte {} is outside record area ({}-{})",
            pos,
            user_data_start,
            trailer_start
        );
    }
}

// ---------------------------------------------------------------------------
// 14. Cross-page-size (8K) repair roundtrip
// ---------------------------------------------------------------------------

#[test]
fn test_8k_page_size_repair_roundtrip() {
    // ssize=4 encodes 8K: 1 << (4+9) = 8192
    let flags_8k = 4 << 6; // ssize=4 in bits 6-9
    let page0 = write::build_fsp_page(
        42,
        3,
        flags_8k,
        1000,
        PAGE_SIZE_8K,
        ChecksumAlgorithm::Crc32c,
    );
    let page1 = build_8k_index_page(1, 42, 2000);
    let mut page2 = build_8k_index_page(2, 42, 3000);

    // Verify page 2 starts valid
    assert!(validate_checksum(&page2, PAGE_SIZE_8K, None).valid);

    // Corrupt page 2
    corrupt_checksum(&mut page2);
    assert!(!validate_checksum(&page2, PAGE_SIZE_8K, None).valid);

    let tmp = write_tablespace(&[page0, page1, page2]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::repair::execute(
        &idb::cli::repair::RepairOptions {
            file: path.to_string(),
            page: None,
            algorithm: "auto".to_string(),
            no_backup: true,
            dry_run: false,
            verbose: false,
            json: false,
            page_size: Some(PAGE_SIZE_8K),
            keyring: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    // All 3 pages should now be valid
    for i in 0..3 {
        let page = write::read_page_raw(path, i, PAGE_SIZE_8K).unwrap();
        assert!(
            validate_checksum(&page, PAGE_SIZE_8K, None).valid,
            "8K page {} invalid after repair",
            i
        );
    }
}

// ---------------------------------------------------------------------------
// 15. Encrypted tablespace flags without keyring
// ---------------------------------------------------------------------------

#[test]
fn test_encrypted_flags_repair_without_keyring() {
    // Bit 13 set in FSP flags indicates encryption
    let flags = 1 << 13;
    let page0 = write::build_fsp_page(42, 3, flags, 1000, PAGE_SIZE, ChecksumAlgorithm::Crc32c);
    let page1 = build_index_page(1, 42, 2000);
    let mut page2 = build_index_page(2, 42, 3000);
    corrupt_checksum(&mut page2);

    let tmp = write_tablespace(&[page0, page1, page2]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    let result = idb::cli::repair::execute(
        &idb::cli::repair::RepairOptions {
            file: path.to_string(),
            page: None,
            algorithm: "crc32c".to_string(),
            no_backup: true,
            dry_run: false,
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
            mmap: false,
        },
        &mut output,
    );

    // Repair should succeed even with encryption flags when no keyring is provided,
    // because repair operates on raw page bytes (checksums are not encrypted).
    // If the implementation chooses to error, the error message should be clear.
    match result {
        Ok(()) => {
            // Repair succeeded: verify the previously-corrupt page is now valid
            let p2 = write::read_page_raw(path, 2, PAGE_SIZE).unwrap();
            assert!(
                validate_checksum(&p2, PAGE_SIZE, None).valid,
                "Page 2 should be valid after repair despite encryption flags"
            );
        }
        Err(e) => {
            let msg = format!("{}", e);
            // If it errors, the message should reference encryption or keyring
            assert!(
                msg.to_lowercase().contains("encrypt") || msg.to_lowercase().contains("keyring"),
                "Error should mention encryption or keyring, got: {}",
                msg
            );
        }
    }
}
