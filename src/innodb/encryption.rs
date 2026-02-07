//! Tablespace encryption detection.
//!
//! Detects whether a tablespace is encrypted by inspecting bit 13 of the FSP
//! flags (MySQL 5.7.11+). Currently only AES encryption is recognized.

/// Encryption algorithm detected from FSP flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionAlgorithm {
    None,
    Aes,
}

/// Detect encryption from FSP space flags.
///
/// Encryption flag is at bit 13 of FSP flags (MySQL 5.7.11+).
pub fn detect_encryption(fsp_flags: u32) -> EncryptionAlgorithm {
    if (fsp_flags >> 13) & 0x01 != 0 {
        EncryptionAlgorithm::Aes
    } else {
        EncryptionAlgorithm::None
    }
}

/// Check if a tablespace is encrypted based on its FSP flags.
pub fn is_encrypted(fsp_flags: u32) -> bool {
    detect_encryption(fsp_flags) != EncryptionAlgorithm::None
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

    #[test]
    fn test_detect_encryption() {
        assert_eq!(detect_encryption(0), EncryptionAlgorithm::None);
        assert_eq!(detect_encryption(1 << 13), EncryptionAlgorithm::Aes);
        // Other bits shouldn't affect encryption detection
        assert_eq!(detect_encryption(0xFF), EncryptionAlgorithm::None);
        assert_eq!(detect_encryption(0xFF | (1 << 13)), EncryptionAlgorithm::Aes);
    }

    #[test]
    fn test_is_encrypted() {
        assert!(!is_encrypted(0));
        assert!(is_encrypted(1 << 13));
    }
}
