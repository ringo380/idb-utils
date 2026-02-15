//! Tablespace compression detection and decompression.
//!
//! Detects the compression algorithm from FSP flags and provides zlib and LZ4
//! decompression helpers for compressed page data.
//!
//! Supports both MySQL (bits 11-12) and MariaDB flag layouts:
//! - MariaDB full_crc32: compression algo in bits 5-7
//! - MariaDB original: PAGE_COMPRESSION flag at bit 16
//! - MariaDB page-level: algorithm ID embedded per-page at offset 26

use flate2::read::ZlibDecoder;
use std::io::Read;

use crate::innodb::vendor::VendorInfo;

/// Compression algorithm detected or used for a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionAlgorithm {
    None,
    Zlib,
    Lz4,
    /// MariaDB LZO compression (detection only — not decompressed).
    Lzo,
    /// MariaDB LZMA compression (detection only — not decompressed).
    Lzma,
    /// MariaDB bzip2 compression (detection only — not decompressed).
    Bzip2,
    /// MariaDB Snappy compression (detection only — not decompressed).
    Snappy,
}

/// Detect the compression algorithm from FSP space flags.
///
/// When `vendor_info` is provided:
/// - MariaDB full_crc32: reads compression algo from bits 5-7
/// - MariaDB original: checks bit 16 for PAGE_COMPRESSION (algo is per-page)
/// - MySQL/Percona: reads bits 11-12
///
/// Without vendor info, defaults to MySQL bit layout.
pub fn detect_compression(
    fsp_flags: u32,
    vendor_info: Option<&VendorInfo>,
) -> CompressionAlgorithm {
    use crate::innodb::constants::*;

    if let Some(vi) = vendor_info {
        if vi.is_full_crc32() {
            // MariaDB full_crc32: compression algo in bits 5-7
            let algo = (fsp_flags & MARIADB_FSP_FLAGS_FCRC32_COMPRESSED_ALGO_MASK) >> 5;
            return mariadb_algo_from_id(algo as u8);
        }
        if vi.vendor == crate::innodb::vendor::InnoDbVendor::MariaDB {
            // MariaDB original: bit 16 indicates page compression is enabled
            // but the algorithm is stored per-page, not in FSP flags
            if fsp_flags & MARIADB_FSP_FLAGS_PAGE_COMPRESSION != 0 {
                // Algorithm is per-page; return Zlib as a default indicator
                // that page compression is enabled. Actual algo is in each page.
                return CompressionAlgorithm::Zlib;
            }
            return CompressionAlgorithm::None;
        }
    }

    // MySQL/Percona: bits 11-12
    let comp_bits = (fsp_flags >> 11) & 0x03;
    match comp_bits {
        1 => CompressionAlgorithm::Zlib,
        2 => CompressionAlgorithm::Lz4,
        _ => CompressionAlgorithm::None,
    }
}

/// Detect the compression algorithm from a MariaDB page-compressed page.
///
/// For page types 34354 (PAGE_COMPRESSED) and 37401 (PAGE_COMPRESSED_ENCRYPTED),
/// the algorithm ID is stored as a u8 at byte offset 26 (FIL_PAGE_FILE_FLUSH_LSN).
pub fn detect_mariadb_page_compression(page_data: &[u8]) -> Option<CompressionAlgorithm> {
    if page_data.len() < 27 {
        return None;
    }
    let algo_id = page_data[26];
    Some(mariadb_algo_from_id(algo_id))
}

/// Convert a MariaDB compression algorithm ID to enum.
///
/// IDs from MariaDB `fil_space_t::comp_algo`:
/// 0 = none, 1 = zlib, 2 = lz4, 3 = lzo, 4 = lzma, 5 = bzip2, 6 = snappy
fn mariadb_algo_from_id(id: u8) -> CompressionAlgorithm {
    match id {
        1 => CompressionAlgorithm::Zlib,
        2 => CompressionAlgorithm::Lz4,
        3 => CompressionAlgorithm::Lzo,
        4 => CompressionAlgorithm::Lzma,
        5 => CompressionAlgorithm::Bzip2,
        6 => CompressionAlgorithm::Snappy,
        _ => CompressionAlgorithm::None,
    }
}

/// Decompress zlib-compressed page data.
///
/// Returns the decompressed data, or None if decompression fails.
pub fn decompress_zlib(compressed: &[u8]) -> Option<Vec<u8>> {
    let mut decoder = ZlibDecoder::new(compressed);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed).ok()?;
    Some(decompressed)
}

/// Decompress LZ4-compressed page data.
///
/// `uncompressed_len` is the expected output size (typically the page size).
/// Returns the decompressed data, or None if decompression fails.
pub fn decompress_lz4(compressed: &[u8], uncompressed_len: usize) -> Option<Vec<u8>> {
    lz4_flex::decompress(compressed, uncompressed_len).ok()
}

/// Check if a page appears to be a hole-punched page.
///
/// Hole-punched pages have their data zeroed out after the compressed content.
/// The FIL header is preserved, and the actual data is followed by trailing zeros.
pub fn is_hole_punched(page_data: &[u8], page_size: u32) -> bool {
    if page_data.len() < page_size as usize {
        return false;
    }

    // A hole-punched page has trailing zeros. Check the last quarter of the page.
    let check_start = (page_size as usize * 3) / 4;
    page_data[check_start..page_size as usize]
        .iter()
        .all(|&b| b == 0)
}

impl std::fmt::Display for CompressionAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionAlgorithm::None => write!(f, "None"),
            CompressionAlgorithm::Zlib => write!(f, "Zlib"),
            CompressionAlgorithm::Lz4 => write!(f, "LZ4"),
            CompressionAlgorithm::Lzo => write!(f, "LZO"),
            CompressionAlgorithm::Lzma => write!(f, "LZMA"),
            CompressionAlgorithm::Bzip2 => write!(f, "bzip2"),
            CompressionAlgorithm::Snappy => write!(f, "Snappy"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::innodb::vendor::MariaDbFormat;

    #[test]
    fn test_detect_compression_mysql() {
        assert_eq!(detect_compression(0, None), CompressionAlgorithm::None);
        assert_eq!(
            detect_compression(1 << 11, None),
            CompressionAlgorithm::Zlib
        );
        assert_eq!(detect_compression(2 << 11, None), CompressionAlgorithm::Lz4);
        assert_eq!(
            detect_compression(3 << 11, None),
            CompressionAlgorithm::None
        );
        // Other bits set shouldn't affect compression detection
        assert_eq!(
            detect_compression(0xFF | (1 << 11), None),
            CompressionAlgorithm::Zlib
        );
    }

    #[test]
    fn test_detect_compression_mariadb_full_crc32() {
        let vendor = VendorInfo::mariadb(MariaDbFormat::FullCrc32);
        // bits 5-7 = 1 (zlib)
        let flags = 0x10 | (1 << 5);
        assert_eq!(
            detect_compression(flags, Some(&vendor)),
            CompressionAlgorithm::Zlib
        );
        // bits 5-7 = 2 (lz4)
        let flags = 0x10 | (2 << 5);
        assert_eq!(
            detect_compression(flags, Some(&vendor)),
            CompressionAlgorithm::Lz4
        );
        // bits 5-7 = 3 (lzo)
        let flags = 0x10 | (3 << 5);
        assert_eq!(
            detect_compression(flags, Some(&vendor)),
            CompressionAlgorithm::Lzo
        );
    }

    #[test]
    fn test_detect_mariadb_page_compression() {
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

    #[test]
    fn test_decompress_zlib() {
        use flate2::write::ZlibEncoder;
        use flate2::Compression;
        use std::io::Write;

        let original = b"Hello, InnoDB compression test data!";
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(original).unwrap();
        let compressed = encoder.finish().unwrap();

        let result = decompress_zlib(&compressed).unwrap();
        assert_eq!(result, original);
    }

    #[test]
    fn test_decompress_lz4() {
        let original = b"Hello, LZ4 compression test data for InnoDB!";
        let compressed = lz4_flex::compress_prepend_size(original);
        // lz4_flex::compress_prepend_size adds 4-byte length prefix,
        // but decompress expects just the compressed data with known length
        let result = lz4_flex::decompress(&compressed[4..], original.len());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), original);
    }

    #[test]
    fn test_is_hole_punched() {
        let page_size = 16384u32;
        let mut page = vec![0u8; page_size as usize];
        // All zeros = hole punched
        assert!(is_hole_punched(&page, page_size));

        // Some data in the first part, zeros in the last quarter
        page[0] = 0xFF;
        page[100] = 0xAB;
        assert!(is_hole_punched(&page, page_size));

        // Non-zero byte in the last quarter = not hole punched
        page[page_size as usize - 10] = 0x01;
        assert!(!is_hole_punched(&page, page_size));
    }
}
