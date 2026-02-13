//! Tablespace encryption detection.
//!
//! Detects whether a tablespace is encrypted by inspecting FSP flags.
//! MySQL uses bit 13 for tablespace-level encryption (AES). MariaDB does
//! not have a tablespace-level encryption flag; encryption is per-page
//! (page type 37401).

use crate::innodb::vendor::VendorInfo;
use byteorder::{BigEndian, ByteOrder};

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
pub fn detect_encryption(
    fsp_flags: u32,
    vendor_info: Option<&VendorInfo>,
) -> EncryptionAlgorithm {
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
pub fn is_encrypted(fsp_flags: u32) -> bool {
    detect_encryption(fsp_flags, None) != EncryptionAlgorithm::None
}

/// Check if a page type indicates MariaDB page-level encryption.
pub fn is_mariadb_encrypted_page(page_type: u16) -> bool {
    page_type == crate::innodb::constants::FIL_PAGE_PAGE_COMPRESSED_ENCRYPTED
}

/// Read the encryption key version from a MariaDB encrypted page.
///
/// For page type 37401 (PAGE_COMPRESSED_ENCRYPTED), the key version
/// is stored as a u32 at byte offset 26.
pub fn mariadb_encryption_key_version(page_data: &[u8]) -> Option<u32> {
    if page_data.len() < 30 {
        return None;
    }
    Some(BigEndian::read_u32(&page_data[26..]))
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
}
