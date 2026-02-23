#![cfg(feature = "cli")]
//! Integration tests for `inno export`.

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

/// Build an INDEX leaf page with compact-format user records.
///
/// Creates a page with a proper infimum/supremum chain and synthetic
/// user records. Each record has a 5-byte compact header followed by
/// `record_data_len` bytes of data.
fn build_index_page_with_records(
    page_num: u32,
    space_id: u32,
    lsn: u64,
    index_id: u64,
    n_recs: u16,
    record_data_len: usize,
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
    let rec_size = REC_N_NEW_EXTRA_BYTES + record_data_len;

    // Infimum: at offset PAGE_NEW_INFIMUM (99), extra header at 94
    // Supremum: at offset PAGE_NEW_SUPREMUM (112), extra header at 107
    // User records start after supremum area: 112 + 8 = 120
    let user_area_start = PAGE_NEW_SUPREMUM + 8; // 120

    // Set heap_top
    let heap_top = user_area_start + (n_recs as usize) * rec_size;
    BigEndian::write_u16(&mut page[ph + PAGE_HEAP_TOP..], heap_top as u16);
    BigEndian::write_u16(
        &mut page[ph + PAGE_N_HEAP..],
        0x8000 | (n_recs + 2), // compact flag + infimum + supremum + n_recs
    );
    BigEndian::write_u16(&mut page[ph + PAGE_N_RECS..], n_recs);
    BigEndian::write_u16(&mut page[ph + PAGE_LEVEL..], 0); // leaf
    BigEndian::write_u64(&mut page[ph + PAGE_INDEX_ID..], index_id);

    if n_recs == 0 {
        // Infimum points to supremum
        let infimum_hdr_start = PAGE_NEW_INFIMUM - REC_N_NEW_EXTRA_BYTES; // 94
        page[infimum_hdr_start] = 0x01; // n_owned=1
        BigEndian::write_u16(&mut page[infimum_hdr_start + 1..], 2 << 3 | 2); // heap_no=2, infimum
        let offset_to_supremum = (PAGE_NEW_SUPREMUM as i16) - (PAGE_NEW_INFIMUM as i16);
        BigEndian::write_i16(&mut page[infimum_hdr_start + 3..], offset_to_supremum);
        // "infimum\0"
        page[PAGE_NEW_INFIMUM..PAGE_NEW_INFIMUM + 8].copy_from_slice(b"infimum\0");

        // Supremum header
        let supremum_hdr_start = PAGE_NEW_SUPREMUM - REC_N_NEW_EXTRA_BYTES; // 107
        page[supremum_hdr_start] = 0x01; // n_owned=1
        BigEndian::write_u16(&mut page[supremum_hdr_start + 1..], 3 << 3 | 3); // heap_no=3, supremum
        BigEndian::write_i16(&mut page[supremum_hdr_start + 3..], 0); // end of chain
        page[PAGE_NEW_SUPREMUM..PAGE_NEW_SUPREMUM + 8].copy_from_slice(b"supremum");
    } else {
        // Build record chain: infimum -> rec0 -> rec1 -> ... -> supremum

        // First user record offset
        let first_rec_origin = user_area_start + REC_N_NEW_EXTRA_BYTES;

        // Infimum header: points to first user record
        let infimum_hdr_start = PAGE_NEW_INFIMUM - REC_N_NEW_EXTRA_BYTES;
        page[infimum_hdr_start] = 0x01;
        BigEndian::write_u16(&mut page[infimum_hdr_start + 1..], 0 << 3 | 2); // heap_no=0, infimum
        let offset_to_first = first_rec_origin as i16 - PAGE_NEW_INFIMUM as i16;
        BigEndian::write_i16(&mut page[infimum_hdr_start + 3..], offset_to_first);
        page[PAGE_NEW_INFIMUM..PAGE_NEW_INFIMUM + 8].copy_from_slice(b"infimum\0");

        // User records
        for i in 0..n_recs {
            let rec_start = user_area_start + (i as usize) * rec_size;
            let rec_origin = rec_start + REC_N_NEW_EXTRA_BYTES;

            // Record extra header (5 bytes before origin)
            let hdr_start = rec_start;
            page[hdr_start] = 0x00; // n_owned=0, no delete_mark
            let heap_no = (i + 2) as u16; // 0=infimum, 1=supremum, 2+=user
            BigEndian::write_u16(&mut page[hdr_start + 1..], heap_no << 3); // Ordinary type

            // Next offset
            if i + 1 < n_recs {
                let next_origin = rec_origin + rec_size;
                let next_offset = next_origin as i16 - rec_origin as i16;
                BigEndian::write_i16(&mut page[hdr_start + 3..], next_offset);
            } else {
                // Last record points to supremum
                let offset_to_supremum = PAGE_NEW_SUPREMUM as i16 - rec_origin as i16;
                BigEndian::write_i16(&mut page[hdr_start + 3..], offset_to_supremum);
            }

            // Record data: fill with some recognizable bytes
            for j in 0..record_data_len {
                page[rec_origin + j] = ((i as u8).wrapping_mul(17)).wrapping_add(j as u8);
            }
        }

        // Supremum header
        let supremum_hdr_start = PAGE_NEW_SUPREMUM - REC_N_NEW_EXTRA_BYTES;
        page[supremum_hdr_start] = (n_recs as u8) + 1; // n_owned
        BigEndian::write_u16(&mut page[supremum_hdr_start + 1..], 1 << 3 | 3); // heap_no=1, supremum
        BigEndian::write_i16(&mut page[supremum_hdr_start + 3..], 0); // end of chain
        page[PAGE_NEW_SUPREMUM..PAGE_NEW_SUPREMUM + 8].copy_from_slice(b"supremum");
    }

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
fn test_export_hex_output() {
    let page0 = build_fsp_hdr_page(42, 3);
    let page1 = build_index_page_with_records(1, 42, 2000, 100, 3, 20);
    let page2 = vec![0u8; PS]; // empty

    let tmp = write_tablespace(&[page0, page1, page2]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::export::execute(
        &idb::cli::export::ExportOptions {
            file: path.to_string(),
            page: None,
            format: "hex".to_string(),
            where_delete_mark: false,
            system_columns: false,
            verbose: false,
            page_size: None,
            keyring: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    let text = String::from_utf8(output).unwrap();
    // Should have a header line and 3 record lines
    let lines: Vec<&str> = text.lines().collect();
    assert!(
        lines.len() >= 4,
        "Expected header + 3 records, got: {}",
        text
    );
    assert!(lines[0].contains("PAGE"));
    assert!(lines[0].contains("OFFSET"));
    assert!(lines[0].contains("HEAP_NO"));
}

#[test]
fn test_export_hex_specific_page() {
    let page0 = build_fsp_hdr_page(42, 3);
    let page1 = build_index_page_with_records(1, 42, 2000, 100, 5, 20);
    let page2 = build_index_page_with_records(2, 42, 3000, 100, 3, 20);

    let tmp = write_tablespace(&[page0, page1, page2]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::export::execute(
        &idb::cli::export::ExportOptions {
            file: path.to_string(),
            page: Some(2),
            format: "hex".to_string(),
            where_delete_mark: false,
            system_columns: false,
            verbose: false,
            page_size: None,
            keyring: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    let text = String::from_utf8(output).unwrap();
    let data_lines: Vec<&str> = text.lines().skip(1).collect();
    assert_eq!(data_lines.len(), 3, "Expected 3 records from page 2");
}

#[test]
fn test_export_empty_page() {
    let page0 = build_fsp_hdr_page(42, 2);
    let page1 = build_index_page_with_records(1, 42, 2000, 100, 0, 0);

    let tmp = write_tablespace(&[page0, page1]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::export::execute(
        &idb::cli::export::ExportOptions {
            file: path.to_string(),
            page: None,
            format: "hex".to_string(),
            where_delete_mark: false,
            system_columns: false,
            verbose: false,
            page_size: None,
            keyring: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    let text = String::from_utf8(output).unwrap();
    // Only header line, no records
    let data_lines: Vec<&str> = text.lines().skip(1).collect();
    assert_eq!(data_lines.len(), 0, "Expected no records from empty page");
}

#[test]
fn test_export_invalid_format() {
    let page0 = build_fsp_hdr_page(42, 2);
    let page1 = vec![0u8; PS];

    let tmp = write_tablespace(&[page0, page1]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    let result = idb::cli::export::execute(
        &idb::cli::export::ExportOptions {
            file: path.to_string(),
            page: None,
            format: "xml".to_string(),
            where_delete_mark: false,
            system_columns: false,
            verbose: false,
            page_size: None,
            keyring: None,
            mmap: false,
        },
        &mut output,
    );

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Unknown format"));
}

#[test]
fn test_export_csv_falls_back_to_hex_without_sdi() {
    // Without SDI, csv/json should fall back to hex
    let page0 = build_fsp_hdr_page(42, 2);
    let page1 = build_index_page_with_records(1, 42, 2000, 100, 2, 20);

    let tmp = write_tablespace(&[page0, page1]);
    let path = tmp.path().to_str().unwrap();

    let mut output = Vec::new();
    idb::cli::export::execute(
        &idb::cli::export::ExportOptions {
            file: path.to_string(),
            page: None,
            format: "csv".to_string(),
            where_delete_mark: false,
            system_columns: false,
            verbose: false,
            page_size: None,
            keyring: None,
            mmap: false,
        },
        &mut output,
    )
    .unwrap();

    let text = String::from_utf8(output).unwrap();
    // Should fall back to hex output
    assert!(
        text.contains("PAGE") && text.contains("HEAP_NO"),
        "Expected hex fallback output, got: {}",
        text
    );
}
