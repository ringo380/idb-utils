//! Tablespace encryption detection and encryption info parsing.
//!
//! Detects whether a tablespace is encrypted by inspecting FSP flags,
//! and parses the encryption info structure from page 0 that contains
//! the encrypted tablespace key and IV needed for decryption.
//!
//! MySQL uses bit 13 for tablespace-level encryption (AES). MariaDB does
//! not have a tablespace-level encryption flag; encryption is per-page
//! (page type 37401).

use crate::innodb::constants::*;
use crate::innodb::vendor::VendorInfo;
use byteorder::{BigEndian, ByteOrder};
use serde::Serialize;

/// Encryption algorithm detected from FSP flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionAlgorithm {
    None,
    Aes,
}

/// Detect encryption from FSP space flags.
///
/// When `vendor_info` indicates MariaDB, returns `None` because MariaDB
/// does not use a tablespace-level encryption flag. For MySQL/Percona,
/// checks bit 13 of FSP flags.
///
/// # Examples
///
/// ```
/// use idb::innodb::encryption::{detect_encryption, EncryptionAlgorithm};
/// use idb::innodb::vendor::{VendorInfo, MariaDbFormat};
///
/// // No encryption flag set → None
/// assert_eq!(detect_encryption(0, None), EncryptionAlgorithm::None);
///
/// // Bit 13 set → AES encryption detected
/// assert_eq!(detect_encryption(1 << 13, None), EncryptionAlgorithm::Aes);
///
/// // MariaDB always returns None (encryption is per-page, not tablespace-level)
/// let maria = VendorInfo::mariadb(MariaDbFormat::FullCrc32);
/// assert_eq!(detect_encryption(1 << 13, Some(&maria)), EncryptionAlgorithm::None);
/// ```
pub fn detect_encryption(fsp_flags: u32, vendor_info: Option<&VendorInfo>) -> EncryptionAlgorithm {
    // MariaDB: no tablespace-level encryption flag
    if vendor_info.is_some_and(|v| v.vendor == crate::innodb::vendor::InnoDbVendor::MariaDB) {
        return EncryptionAlgorithm::None;
    }

    if (fsp_flags >> 13) & 0x01 != 0 {
        EncryptionAlgorithm::Aes
    } else {
        EncryptionAlgorithm::None
    }
}

/// Check if a tablespace is encrypted based on its FSP flags.
///
/// This is a convenience wrapper around [`detect_encryption`] without
/// vendor-specific handling. It checks whether bit 13 of the FSP flags
/// is set (MySQL/Percona AES encryption).
///
/// # Examples
///
/// ```
/// use idb::innodb::encryption::is_encrypted;
///
/// // No encryption
/// assert!(!is_encrypted(0));
///
/// // Bit 13 set → encrypted
/// assert!(is_encrypted(1 << 13));
///
/// // Other bits do not indicate encryption
/// assert!(!is_encrypted(0xFF));
/// ```
pub fn is_encrypted(fsp_flags: u32) -> bool {
    detect_encryption(fsp_flags, None) != EncryptionAlgorithm::None
}

/// Check if a page type indicates MariaDB page-level encryption.
///
/// MariaDB uses page type 37401 (`FIL_PAGE_PAGE_COMPRESSED_ENCRYPTED`)
/// for pages that are both compressed and encrypted at the page level.
///
/// # Examples
///
/// ```
/// use idb::innodb::encryption::is_mariadb_encrypted_page;
///
/// // MariaDB compressed+encrypted page type
/// assert!(is_mariadb_encrypted_page(37401));
///
/// // Standard INDEX page type
/// assert!(!is_mariadb_encrypted_page(17855));
/// ```
pub fn is_mariadb_encrypted_page(page_type: u16) -> bool {
    page_type == crate::innodb::constants::FIL_PAGE_PAGE_COMPRESSED_ENCRYPTED
}

/// Read the encryption key version from a MariaDB encrypted page.
///
/// For page type 37401 (PAGE_COMPRESSED_ENCRYPTED), the key version
/// is stored as a u32 at byte offset 26.
///
/// # Examples
///
/// ```
/// use idb::innodb::encryption::mariadb_encryption_key_version;
///
/// // Build a minimal page with a key version at offset 26 (big-endian)
/// let mut page = vec![0u8; 38];
/// page[26] = 0x00;
/// page[27] = 0x00;
/// page[28] = 0x00;
/// page[29] = 0x05;
/// assert_eq!(mariadb_encryption_key_version(&page), Some(5));
///
/// // Too-short data returns None
/// let short = vec![0u8; 10];
/// assert_eq!(mariadb_encryption_key_version(&short), None);
/// ```
pub fn mariadb_encryption_key_version(page_data: &[u8]) -> Option<u32> {
    if page_data.len() < 30 {
        return None;
    }
    Some(BigEndian::read_u32(&page_data[26..]))
}

/// Parsed encryption info from page 0 of an encrypted tablespace.
///
/// Located after the XDES array on page 0, this structure contains the
/// master key ID, server UUID, and the encrypted tablespace key+IV needed
/// to decrypt individual pages.
#[derive(Debug, Clone, Serialize)]
pub struct EncryptionInfo {
    /// Encryption info version (1 = `lCA`, 2 = `lCB`, 3 = `lCC`/MySQL 8.0.5+).
    pub magic_version: u8,
    /// Master key ID from the keyring.
    pub master_key_id: u32,
    /// Server UUID string (36 ASCII characters).
    pub server_uuid: String,
    /// Encrypted tablespace key (32 bytes) + IV (32 bytes), AES-256-ECB encrypted.
    #[serde(skip)]
    pub encrypted_key_iv: [u8; 64],
    /// CRC32 checksum of the plaintext key+IV.
    pub checksum: u32,
}

/// Compute the number of pages per extent for a given page size.
fn pages_per_extent(page_size: u32) -> u32 {
    if page_size <= 16384 {
        1048576 / page_size // 1MB extents for page sizes <= 16K
    } else {
        64 // 64 pages per extent for larger page sizes
    }
}

/// Compute the number of XDES entries on page 0 for a given page size.
fn xdes_arr_size(page_size: u32) -> u32 {
    page_size / pages_per_extent(page_size)
}

/// Compute the byte offset of the encryption info on page 0.
///
/// Layout: FIL_PAGE_DATA(38) + FSP_HEADER(112) + XDES_ARRAY(entries * 40)
///
/// # Examples
///
/// ```
/// use idb::innodb::encryption::encryption_info_offset;
///
/// // For 16K pages: 38 + 112 + (256 * 40) = 10390
/// assert_eq!(encryption_info_offset(16384), 10390);
///
/// // For 4K pages: 38 + 112 + (16 * 40) = 790
/// assert_eq!(encryption_info_offset(4096), 790);
/// ```
pub fn encryption_info_offset(page_size: u32) -> usize {
    let xdes_arr_offset = FIL_PAGE_DATA + FSP_HEADER_SIZE;
    let xdes_entries = xdes_arr_size(page_size) as usize;
    xdes_arr_offset + xdes_entries * XDES_SIZE
}

/// Parse encryption info from page 0 of a tablespace.
///
/// Returns `None` if the page does not contain valid encryption info
/// (no magic marker found at the expected offset).
///
/// # Examples
///
/// ```
/// use idb::innodb::encryption::{parse_encryption_info, encryption_info_offset};
///
/// // Build a synthetic 16K page with encryption info V3 (magic "lCC")
/// let page_size = 16384u32;
/// let mut page = vec![0u8; page_size as usize];
/// let offset = encryption_info_offset(page_size);
///
/// // Write magic V3 marker
/// page[offset..offset + 3].copy_from_slice(b"lCC");
/// // Master key ID = 1 (big-endian u32 at offset+3)
/// page[offset + 6] = 1;
/// // Server UUID (36 ASCII bytes at offset+7)
/// let uuid = b"01234567-89ab-cdef-0123-456789abcdef";
/// page[offset + 7..offset + 7 + 36].copy_from_slice(uuid);
///
/// let info = parse_encryption_info(&page, page_size).unwrap();
/// assert_eq!(info.magic_version, 3);
/// assert_eq!(info.master_key_id, 1);
/// assert_eq!(info.server_uuid, "01234567-89ab-cdef-0123-456789abcdef");
///
/// // No magic marker → returns None
/// let empty_page = vec![0u8; page_size as usize];
/// assert!(parse_encryption_info(&empty_page, page_size).is_none());
/// ```
pub fn parse_encryption_info(page0: &[u8], page_size: u32) -> Option<EncryptionInfo> {
    let offset = encryption_info_offset(page_size);

    if page0.len() < offset + ENCRYPTION_INFO_SIZE {
        return None;
    }

    let magic = &page0[offset..offset + ENCRYPTION_MAGIC_SIZE];
    let magic_version = if magic == ENCRYPTION_MAGIC_V1 {
        1
    } else if magic == ENCRYPTION_MAGIC_V2 {
        2
    } else if magic == ENCRYPTION_MAGIC_V3 {
        3
    } else {
        return None;
    };

    let master_key_id = BigEndian::read_u32(&page0[offset + 3..]);
    let uuid_bytes = &page0[offset + 7..offset + 7 + ENCRYPTION_SERVER_UUID_LEN];
    let server_uuid = String::from_utf8_lossy(uuid_bytes).to_string();

    let mut encrypted_key_iv = [0u8; 64];
    encrypted_key_iv.copy_from_slice(&page0[offset + 43..offset + 43 + 64]);

    let checksum = BigEndian::read_u32(&page0[offset + 107..]);

    Some(EncryptionInfo {
        magic_version,
        master_key_id,
        server_uuid,
        encrypted_key_iv,
        checksum,
    })
}

impl std::fmt::Display for EncryptionAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EncryptionAlgorithm::None => write!(f, "None"),
            EncryptionAlgorithm::Aes => write!(f, "AES"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::innodb::vendor::MariaDbFormat;

    #[test]
    fn test_detect_encryption_mysql() {
        assert_eq!(detect_encryption(0, None), EncryptionAlgorithm::None);
        assert_eq!(detect_encryption(1 << 13, None), EncryptionAlgorithm::Aes);
        assert_eq!(detect_encryption(0xFF, None), EncryptionAlgorithm::None);
        assert_eq!(
            detect_encryption(0xFF | (1 << 13), None),
            EncryptionAlgorithm::Aes
        );
    }

    #[test]
    fn test_detect_encryption_mariadb_returns_none() {
        let vendor = VendorInfo::mariadb(MariaDbFormat::FullCrc32);
        // Even with bit 13 set, MariaDB returns None (no TS-level encryption)
        assert_eq!(
            detect_encryption(1 << 13, Some(&vendor)),
            EncryptionAlgorithm::None
        );
    }

    #[test]
    fn test_is_encrypted() {
        assert!(!is_encrypted(0));
        assert!(is_encrypted(1 << 13));
    }

    #[test]
    fn test_is_mariadb_encrypted_page() {
        assert!(is_mariadb_encrypted_page(37401));
        assert!(!is_mariadb_encrypted_page(17855));
    }

    #[test]
    fn test_mariadb_encryption_key_version() {
        let mut page = vec![0u8; 38];
        BigEndian::write_u32(&mut page[26..], 42);
        assert_eq!(mariadb_encryption_key_version(&page), Some(42));
    }

    #[test]
    fn test_encryption_info_offset_16k() {
        // FIL_PAGE_DATA(38) + FSP_HEADER_SIZE(112) + 256 * 40 = 10390
        assert_eq!(encryption_info_offset(16384), 10390);
    }

    #[test]
    fn test_encryption_info_offset_various() {
        assert_eq!(encryption_info_offset(4096), 38 + 112 + 16 * 40); // 790
        assert_eq!(encryption_info_offset(8192), 38 + 112 + 64 * 40); // 2710
        assert_eq!(encryption_info_offset(32768), 38 + 112 + 512 * 40); // 20630
    }

    #[test]
    fn test_parse_encryption_info_v3() {
        let mut page = vec![0u8; 16384];
        let offset = encryption_info_offset(16384);

        // Write magic V3
        page[offset..offset + 3].copy_from_slice(b"lCC");
        // Master key ID
        BigEndian::write_u32(&mut page[offset + 3..], 42);
        // Server UUID (36 bytes)
        let uuid = "12345678-1234-1234-1234-123456789abc";
        page[offset + 7..offset + 7 + 36].copy_from_slice(uuid.as_bytes());
        // Encrypted key+IV (64 bytes) — fill with pattern
        for i in 0..64 {
            page[offset + 43 + i] = i as u8;
        }
        // CRC32 checksum
        BigEndian::write_u32(&mut page[offset + 107..], 0xDEADBEEF);

        let info = parse_encryption_info(&page, 16384).unwrap();
        assert_eq!(info.magic_version, 3);
        assert_eq!(info.master_key_id, 42);
        assert_eq!(info.server_uuid, uuid);
        assert_eq!(info.checksum, 0xDEADBEEF);
        assert_eq!(info.encrypted_key_iv[0], 0);
        assert_eq!(info.encrypted_key_iv[63], 63);
    }

    #[test]
    fn test_parse_encryption_info_v1() {
        let mut page = vec![0u8; 16384];
        let offset = encryption_info_offset(16384);
        page[offset..offset + 3].copy_from_slice(b"lCA");
        BigEndian::write_u32(&mut page[offset + 3..], 1);
        let uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
        page[offset + 7..offset + 7 + 36].copy_from_slice(uuid.as_bytes());
        BigEndian::write_u32(&mut page[offset + 107..], 0x12345678);

        let info = parse_encryption_info(&page, 16384).unwrap();
        assert_eq!(info.magic_version, 1);
        assert_eq!(info.master_key_id, 1);
    }

    #[test]
    fn test_parse_encryption_info_no_magic() {
        let page = vec![0u8; 16384];
        assert!(parse_encryption_info(&page, 16384).is_none());
    }

    #[test]
    fn test_parse_encryption_info_bad_magic() {
        let mut page = vec![0u8; 16384];
        let offset = encryption_info_offset(16384);
        page[offset..offset + 3].copy_from_slice(b"lCD");
        assert!(parse_encryption_info(&page, 16384).is_none());
    }
}
