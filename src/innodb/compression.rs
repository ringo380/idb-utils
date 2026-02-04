use flate2::read::ZlibDecoder;
use std::io::Read;

/// Compression algorithm detected or used for a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionAlgorithm {
    None,
    Zlib,
    Lz4,
}

/// Detect the compression algorithm from FSP space flags.
///
/// Compression type is stored in bits 11-12 of FSP flags (MySQL 8.0+).
/// - 0 = none
/// - 1 = zlib
/// - 2 = lz4
pub fn detect_compression(fsp_flags: u32) -> CompressionAlgorithm {
    let comp_bits = (fsp_flags >> 11) & 0x03;
    match comp_bits {
        1 => CompressionAlgorithm::Zlib,
        2 => CompressionAlgorithm::Lz4,
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_compression() {
        assert_eq!(detect_compression(0), CompressionAlgorithm::None);
        assert_eq!(detect_compression(1 << 11), CompressionAlgorithm::Zlib);
        assert_eq!(detect_compression(2 << 11), CompressionAlgorithm::Lz4);
        assert_eq!(detect_compression(3 << 11), CompressionAlgorithm::None);
        // Other bits set shouldn't affect compression detection
        assert_eq!(detect_compression(0xFF | (1 << 11)), CompressionAlgorithm::Zlib);
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
