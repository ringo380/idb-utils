#![cfg(feature = "cli")]
//! Integration tests for MariaDB tablespace format support.
//!
//! These tests construct synthetic tablespace files with MariaDB-specific FSP
//! flags and page structures, then verify correct vendor detection, page size
//! parsing, checksum validation, and page type recognition.

use byteorder::{BigEndian, ByteOrder};
use std::io::Write;
use tempfile::NamedTempFile;

use idb::innodb::checksum::{validate_checksum, ChecksumAlgorithm};
use idb::innodb::compression::{
    detect_compression, detect_mariadb_page_compression, CompressionAlgorithm,
};
use idb::innodb::constants::*;
use idb::innodb::encryption::{detect_encryption, EncryptionAlgorithm};
use idb::innodb::page::{FilHeader, FspHeader};
use idb::innodb::page_types::PageType;
use idb::innodb::tablespace::Tablespace;
use idb::innodb::vendor::{
    detect_vendor_from_created_by, detect_vendor_from_flags, InnoDbVendor, MariaDbFormat,
    VendorInfo,
};

const PAGE_SIZE: u32 = 16384;
const PS: usize = PAGE_SIZE as usize;

/// Build a MariaDB full_crc32 FSP_HDR page (page 0).
///
/// FSP flags: bit 4 (full_crc32 marker) + ssize in bits 0-3.
fn build_mariadb_full_crc32_fsp_page(space_id: u32, total_pages: u32, ssize: u32) -> Vec<u8> {
    let mut page = vec![0u8; PS];

    // FIL header
    BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], 0);
    BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], 1000);
    BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 8); // FSP_HDR
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);

    // FSP header at FIL_PAGE_DATA (offset 38)
    let fsp = FIL_PAGE_DATA;
    BigEndian::write_u32(&mut page[fsp + FSP_SPACE_ID..], space_id);
    BigEndian::write_u32(&mut page[fsp + FSP_SIZE..], total_pages);
    BigEndian::write_u32(&mut page[fsp + FSP_FREE_LIMIT..], total_pages);
    // full_crc32 marker (bit 4) + ssize in bits 0-3
    let flags = MARIADB_FSP_FLAGS_FCRC32_MARKER_MASK | (ssize & 0x0F);
    BigEndian::write_u32(&mut page[fsp + FSP_SPACE_FLAGS..], flags);

    // MariaDB full_crc32: checksum = CRC-32C over bytes [0..page_size-4),
    // stored in last 4 bytes
    write_mariadb_full_crc32_checksum(&mut page);

    page
}

/// Build a MariaDB full_crc32 INDEX page.
fn build_mariadb_index_page(page_num: u32, space_id: u32, lsn: u64) -> Vec<u8> {
    let mut page = vec![0u8; PS];

    BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], page_num);
    BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], lsn);
    BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855); // INDEX
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);

    write_mariadb_full_crc32_checksum(&mut page);

    page
}

/// Build a MySQL standard FSP_HDR page (page 0) with CRC-32C checksum.
fn build_mysql_fsp_page(space_id: u32, total_pages: u32) -> Vec<u8> {
    let mut page = vec![0u8; PS];

    BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], 0);
    BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], 1000);
    BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 8);
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);

    let fsp = FIL_PAGE_DATA;
    BigEndian::write_u32(&mut page[fsp + FSP_SPACE_ID..], space_id);
    BigEndian::write_u32(&mut page[fsp + FSP_SIZE..], total_pages);
    BigEndian::write_u32(&mut page[fsp + FSP_FREE_LIMIT..], total_pages);
    BigEndian::write_u32(&mut page[fsp + FSP_SPACE_FLAGS..], 0);

    let trailer = PS - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut page[trailer + 4..], 1000 & 0xFFFFFFFF);

    // MySQL CRC-32C
    let end = PS - SIZE_FIL_TRAILER;
    let crc1 = crc32c::crc32c(&page[FIL_PAGE_OFFSET..FIL_PAGE_FILE_FLUSH_LSN]);
    let crc2 = crc32c::crc32c(&page[FIL_PAGE_DATA..end]);
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_OR_CHKSUM..], crc1 ^ crc2);

    page
}

fn build_mysql_index_page(page_num: u32, space_id: u32, lsn: u64) -> Vec<u8> {
    let mut page = vec![0u8; PS];

    BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], page_num);
    BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], lsn);
    BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855);
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);

    let trailer = PS - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut page[trailer + 4..], (lsn & 0xFFFFFFFF) as u32);

    let end = PS - SIZE_FIL_TRAILER;
    let crc1 = crc32c::crc32c(&page[FIL_PAGE_OFFSET..FIL_PAGE_FILE_FLUSH_LSN]);
    let crc2 = crc32c::crc32c(&page[FIL_PAGE_DATA..end]);
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_OR_CHKSUM..], crc1 ^ crc2);

    page
}

/// Write the MariaDB full_crc32 checksum (last 4 bytes of page).
fn write_mariadb_full_crc32_checksum(page: &mut [u8]) {
    let ps = page.len();
    let crc = crc32c::crc32c(&page[0..ps - 4]);
    BigEndian::write_u32(&mut page[ps - 4..], crc);
}

fn write_pages(pages: &[Vec<u8>]) -> NamedTempFile {
    let mut tmp = NamedTempFile::new().expect("create temp file");
    for page in pages {
        tmp.write_all(page).expect("write page");
    }
    tmp.flush().expect("flush");
    tmp
}

// ── Vendor detection tests ──────────────────────────────────────────

#[test]
fn test_default_flags_detect_mysql() {
    let info = detect_vendor_from_flags(0);
    assert_eq!(info.vendor, InnoDbVendor::MySQL);
    assert_eq!(info.mariadb_format, None);
}

#[test]
fn test_full_crc32_flags_detect_mariadb() {
    let flags = MARIADB_FSP_FLAGS_FCRC32_MARKER_MASK | 5; // marker + ssize=5
    let info = detect_vendor_from_flags(flags);
    assert_eq!(info.vendor, InnoDbVendor::MariaDB);
    assert_eq!(info.mariadb_format, Some(MariaDbFormat::FullCrc32));
}

#[test]
fn test_page_compression_flags_detect_mariadb_original() {
    let flags = MARIADB_FSP_FLAGS_PAGE_COMPRESSION;
    let info = detect_vendor_from_flags(flags);
    assert_eq!(info.vendor, InnoDbVendor::MariaDB);
    assert_eq!(info.mariadb_format, Some(MariaDbFormat::Original));
}

#[test]
fn test_vendor_from_created_by() {
    assert_eq!(
        detect_vendor_from_created_by("MySQL 8.0.32"),
        InnoDbVendor::MySQL
    );
    assert_eq!(
        detect_vendor_from_created_by("MariaDB 10.11.4"),
        InnoDbVendor::MariaDB
    );
    assert_eq!(
        detect_vendor_from_created_by("Percona Server 8.0.32-24"),
        InnoDbVendor::Percona
    );
    assert_eq!(detect_vendor_from_created_by(""), InnoDbVendor::MySQL);
}

// ── Tablespace vendor detection tests ───────────────────────────────

#[test]
fn test_tablespace_detects_mysql_vendor() {
    let tmp = write_pages(&[
        build_mysql_fsp_page(1, 2),
        build_mysql_index_page(1, 1, 2000),
    ]);
    let ts = Tablespace::open(tmp.path()).unwrap();
    assert_eq!(ts.vendor_info().vendor, InnoDbVendor::MySQL);
    assert_eq!(ts.vendor_info().mariadb_format, None);
}

#[test]
fn test_tablespace_detects_mariadb_full_crc32() {
    let tmp = write_pages(&[
        build_mariadb_full_crc32_fsp_page(1, 2, 5), // ssize=5 => 16K
        build_mariadb_index_page(1, 1, 2000),
    ]);
    let ts = Tablespace::open(tmp.path()).unwrap();
    assert_eq!(ts.vendor_info().vendor, InnoDbVendor::MariaDB);
    assert_eq!(
        ts.vendor_info().mariadb_format,
        Some(MariaDbFormat::FullCrc32)
    );
    assert_eq!(ts.page_size(), PAGE_SIZE);
}

// ── MariaDB full_crc32 page size detection tests ────────────────────

#[test]
fn test_mariadb_full_crc32_page_size_16k() {
    let tmp = write_pages(&[build_mariadb_full_crc32_fsp_page(1, 1, 5)]);
    let ts = Tablespace::open(tmp.path()).unwrap();
    assert_eq!(ts.page_size(), 16384);
}

#[test]
fn test_mariadb_full_crc32_page_size_default() {
    // ssize=0 => default 16K
    let tmp = write_pages(&[build_mariadb_full_crc32_fsp_page(1, 1, 0)]);
    let ts = Tablespace::open(tmp.path()).unwrap();
    assert_eq!(ts.page_size(), SIZE_PAGE_DEFAULT);
}

// ── MariaDB full_crc32 checksum validation tests ────────────────────

#[test]
fn test_mariadb_full_crc32_checksum_valid() {
    let page = build_mariadb_full_crc32_fsp_page(1, 2, 5);
    let vendor = VendorInfo::mariadb(MariaDbFormat::FullCrc32);
    let result = validate_checksum(&page, PAGE_SIZE, Some(&vendor));
    assert!(result.valid, "MariaDB full_crc32 checksum should be valid");
    assert_eq!(result.algorithm, ChecksumAlgorithm::MariaDbFullCrc32);
}

#[test]
fn test_mariadb_full_crc32_checksum_detects_corruption() {
    let mut page = build_mariadb_full_crc32_fsp_page(1, 2, 5);
    let vendor = VendorInfo::mariadb(MariaDbFormat::FullCrc32);

    // Valid first
    let result = validate_checksum(&page, PAGE_SIZE, Some(&vendor));
    assert!(result.valid);

    // Corrupt a byte
    page[100] ^= 0xFF;

    // Should fail
    let result = validate_checksum(&page, PAGE_SIZE, Some(&vendor));
    assert!(!result.valid);
    assert_eq!(result.algorithm, ChecksumAlgorithm::MariaDbFullCrc32);
}

#[test]
fn test_mariadb_index_page_checksum() {
    let page = build_mariadb_index_page(1, 1, 5000);
    let vendor = VendorInfo::mariadb(MariaDbFormat::FullCrc32);
    let result = validate_checksum(&page, PAGE_SIZE, Some(&vendor));
    assert!(result.valid);
    assert_eq!(result.algorithm, ChecksumAlgorithm::MariaDbFullCrc32);
}

// ── MariaDB page type recognition tests ─────────────────────────────

#[test]
fn test_mariadb_page_compressed_type() {
    let mut page = vec![0u8; PS];
    BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], FIL_PAGE_PAGE_COMPRESSED);
    let header = FilHeader::parse(&page).unwrap();
    assert_eq!(header.page_type, PageType::PageCompressed);
}

#[test]
fn test_mariadb_page_compressed_encrypted_type() {
    let mut page = vec![0u8; PS];
    BigEndian::write_u16(
        &mut page[FIL_PAGE_TYPE..],
        FIL_PAGE_PAGE_COMPRESSED_ENCRYPTED,
    );
    let header = FilHeader::parse(&page).unwrap();
    assert_eq!(header.page_type, PageType::PageCompressedEncrypted);
}

#[test]
fn test_page_type_18_with_vendor_context() {
    let mysql = VendorInfo::mysql();
    let mariadb = VendorInfo::mariadb(MariaDbFormat::FullCrc32);

    // MySQL: type 18 = SdiBlob
    assert_eq!(
        PageType::from_u16_with_vendor(18, &mysql),
        PageType::SdiBlob
    );

    // MariaDB: type 18 = Instant
    assert_eq!(
        PageType::from_u16_with_vendor(18, &mariadb),
        PageType::Instant
    );
}

// ── MariaDB compression detection tests ─────────────────────────────

#[test]
fn test_mariadb_full_crc32_compression_zlib() {
    let vendor = VendorInfo::mariadb(MariaDbFormat::FullCrc32);
    let flags = MARIADB_FSP_FLAGS_FCRC32_MARKER_MASK | (1 << 5); // algo=1 (zlib)
    assert_eq!(
        detect_compression(flags, Some(&vendor)),
        CompressionAlgorithm::Zlib
    );
}

#[test]
fn test_mariadb_full_crc32_compression_lz4() {
    let vendor = VendorInfo::mariadb(MariaDbFormat::FullCrc32);
    let flags = MARIADB_FSP_FLAGS_FCRC32_MARKER_MASK | (2 << 5); // algo=2 (lz4)
    assert_eq!(
        detect_compression(flags, Some(&vendor)),
        CompressionAlgorithm::Lz4
    );
}

#[test]
fn test_mariadb_full_crc32_compression_lzo() {
    let vendor = VendorInfo::mariadb(MariaDbFormat::FullCrc32);
    let flags = MARIADB_FSP_FLAGS_FCRC32_MARKER_MASK | (3 << 5); // algo=3 (lzo)
    assert_eq!(
        detect_compression(flags, Some(&vendor)),
        CompressionAlgorithm::Lzo
    );
}

#[test]
fn test_mariadb_page_level_compression_detection() {
    let mut page = vec![0u8; 38];
    page[26] = 2; // LZ4
    assert_eq!(
        detect_mariadb_page_compression(&page),
        Some(CompressionAlgorithm::Lz4)
    );

    page[26] = 6; // Snappy
    assert_eq!(
        detect_mariadb_page_compression(&page),
        Some(CompressionAlgorithm::Snappy)
    );
}

// ── MariaDB encryption detection tests ──────────────────────────────

#[test]
fn test_mariadb_no_tablespace_level_encryption() {
    let vendor = VendorInfo::mariadb(MariaDbFormat::FullCrc32);
    // Even with MySQL encryption bit set, MariaDB returns None
    assert_eq!(
        detect_encryption(1 << 13, Some(&vendor)),
        EncryptionAlgorithm::None
    );
}

#[test]
fn test_mysql_encryption_still_works() {
    assert_eq!(detect_encryption(1 << 13, None), EncryptionAlgorithm::Aes);
    assert_eq!(detect_encryption(0, None), EncryptionAlgorithm::None);
}

// ── MySQL backward compatibility tests ──────────────────────────────

#[test]
fn test_mysql_tablespace_still_works() {
    let tmp = write_pages(&[
        build_mysql_fsp_page(1, 2),
        build_mysql_index_page(1, 1, 2000),
    ]);
    let mut ts = Tablespace::open(tmp.path()).unwrap();
    assert_eq!(ts.vendor_info().vendor, InnoDbVendor::MySQL);
    assert_eq!(ts.page_size(), PAGE_SIZE);

    // Read and validate all pages
    let page0 = ts.read_page(0).unwrap();
    let result = validate_checksum(&page0, PAGE_SIZE, Some(ts.vendor_info()));
    assert!(result.valid, "MySQL page 0 should still validate");
    assert_eq!(result.algorithm, ChecksumAlgorithm::Crc32c);

    let page1 = ts.read_page(1).unwrap();
    let result = validate_checksum(&page1, PAGE_SIZE, Some(ts.vendor_info()));
    assert!(result.valid, "MySQL page 1 should still validate");
}

#[test]
fn test_mysql_checksum_with_none_vendor() {
    let page = build_mysql_fsp_page(1, 1);
    let result = validate_checksum(&page, PAGE_SIZE, None);
    assert!(result.valid);
    assert_eq!(result.algorithm, ChecksumAlgorithm::Crc32c);
}

// ── FSP header parsing tests ────────────────────────────────────────

#[test]
fn test_mariadb_fsp_header_parse() {
    let page = build_mariadb_full_crc32_fsp_page(42, 100, 5);
    let fsp = FspHeader::parse(&page).unwrap();
    assert_eq!(fsp.space_id, 42);
    assert_eq!(fsp.size, 100);
    // Verify flags have the full_crc32 marker
    assert_ne!(fsp.flags & MARIADB_FSP_FLAGS_FCRC32_MARKER_MASK, 0);
}

// ── SDI rejection test ──────────────────────────────────────────────

#[test]
fn test_sdi_rejects_mariadb_tablespace() {
    use idb::cli::sdi::{execute, SdiOptions};

    let tmp = write_pages(&[
        build_mariadb_full_crc32_fsp_page(1, 2, 5),
        build_mariadb_index_page(1, 1, 2000),
    ]);

    let opts = SdiOptions {
        file: tmp.path().to_str().unwrap().to_string(),
        pretty: false,
        page_size: None,
        keyring: None,
        mmap: false,
    };

    let mut output = Vec::new();
    let result = execute(&opts, &mut output);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("MariaDB"),
        "Error should mention MariaDB: {}",
        err
    );
}
