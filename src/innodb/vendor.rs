//! InnoDB vendor detection.
//!
//! Identifies whether a tablespace was created by MySQL, Percona XtraDB, or
//! MariaDB by inspecting FSP flags and redo log creator strings. MariaDB
//! diverges from MySQL's on-disk format starting from 10.1, especially in FSP
//! flags layout, checksum algorithm, and page types. Percona XtraDB is
//! binary-compatible with MySQL at the file level.
//!
//! Use [`detect_vendor_from_flags`] for tablespace files and
//! [`detect_vendor_from_created_by`] for redo log headers.

use serde::Serialize;
use std::fmt;

/// InnoDB implementation vendor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum InnoDbVendor {
    /// Oracle MySQL (and binary-compatible forks like AWS Aurora).
    MySQL,
    /// Percona Server with XtraDB (binary-compatible with MySQL on disk).
    Percona,
    /// MariaDB (divergent on-disk format from 10.1+).
    MariaDB,
}

impl fmt::Display for InnoDbVendor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InnoDbVendor::MySQL => write!(f, "MySQL"),
            InnoDbVendor::Percona => write!(f, "Percona XtraDB"),
            InnoDbVendor::MariaDB => write!(f, "MariaDB"),
        }
    }
}

/// MariaDB tablespace format variant.
///
/// MariaDB 10.5+ introduced `full_crc32`, a simplified format with a single
/// CRC-32C checksum over the entire page (minus the last 4 bytes). Older
/// MariaDB versions use the "original" format, which is closer to MySQL but
/// with different flag bit assignments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum MariaDbFormat {
    /// Original MariaDB format (10.1–10.4). FSP flags layout differs from
    /// MySQL in compression (bit 16) and encryption handling.
    Original,
    /// Full CRC-32C format (MariaDB 10.5+). FSP flags bit 4 is the marker.
    /// Page size in bits 0-3, compression algo in bits 5-7.
    FullCrc32,
}

impl fmt::Display for MariaDbFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MariaDbFormat::Original => write!(f, "original"),
            MariaDbFormat::FullCrc32 => write!(f, "full_crc32"),
        }
    }
}

/// Vendor and format information for a tablespace or redo log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VendorInfo {
    /// The InnoDB implementation vendor.
    pub vendor: InnoDbVendor,
    /// MariaDB-specific format variant, if applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mariadb_format: Option<MariaDbFormat>,
}

impl VendorInfo {
    /// Create a VendorInfo for MySQL (the default).
    pub fn mysql() -> Self {
        VendorInfo {
            vendor: InnoDbVendor::MySQL,
            mariadb_format: None,
        }
    }

    /// Create a VendorInfo for Percona.
    pub fn percona() -> Self {
        VendorInfo {
            vendor: InnoDbVendor::Percona,
            mariadb_format: None,
        }
    }

    /// Create a VendorInfo for MariaDB with a specific format.
    pub fn mariadb(format: MariaDbFormat) -> Self {
        VendorInfo {
            vendor: InnoDbVendor::MariaDB,
            mariadb_format: Some(format),
        }
    }

    /// Returns true if this is a MariaDB full_crc32 tablespace.
    pub fn is_full_crc32(&self) -> bool {
        self.mariadb_format == Some(MariaDbFormat::FullCrc32)
    }
}

impl fmt::Display for VendorInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.mariadb_format {
            Some(format) => write!(f, "{} ({})", self.vendor, format),
            None => write!(f, "{}", self.vendor),
        }
    }
}

/// Detect the InnoDB vendor from FSP flags on page 0.
///
/// Detection logic:
/// - If bit 4 is set → MariaDB full_crc32 (unambiguous marker)
/// - If bit 16 is set and bits 11-14 are zero → likely MariaDB original
///   (PAGE_COMPRESSION flag in a position MySQL doesn't use)
/// - Otherwise → MySQL (Percona is binary-compatible, indistinguishable from
///   flags alone)
pub fn detect_vendor_from_flags(fsp_flags: u32) -> VendorInfo {
    use crate::innodb::constants::*;

    // Check for MariaDB full_crc32 marker (bit 4)
    if fsp_flags & MARIADB_FSP_FLAGS_FCRC32_MARKER_MASK != 0 {
        return VendorInfo::mariadb(MariaDbFormat::FullCrc32);
    }

    // Check for MariaDB original format: bit 16 set (PAGE_COMPRESSION)
    // and MySQL compression bits (11-12) are zero. MySQL never uses bit 16.
    if fsp_flags & MARIADB_FSP_FLAGS_PAGE_COMPRESSION != 0 {
        let mysql_comp_bits = (fsp_flags >> 11) & 0x03;
        if mysql_comp_bits == 0 {
            return VendorInfo::mariadb(MariaDbFormat::Original);
        }
    }

    // Default: MySQL (Percona is indistinguishable from flags)
    VendorInfo::mysql()
}

/// Detect the InnoDB vendor from a redo log `created_by` string.
///
/// Redo log block 0 contains a creator string (e.g., "MySQL 8.0.32",
/// "MariaDB 10.11.4", "Percona Server 8.0.32-24").
pub fn detect_vendor_from_created_by(created_by: &str) -> InnoDbVendor {
    let lower = created_by.to_lowercase();
    if lower.contains("mariadb") {
        InnoDbVendor::MariaDB
    } else if lower.contains("percona") {
        InnoDbVendor::Percona
    } else {
        InnoDbVendor::MySQL
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_flags_are_mysql() {
        let info = detect_vendor_from_flags(0);
        assert_eq!(info.vendor, InnoDbVendor::MySQL);
        assert_eq!(info.mariadb_format, None);
    }

    #[test]
    fn test_mysql_flags_with_page_size() {
        // ssize=5 (16K) at bits 6-9
        let flags = 5 << 6;
        let info = detect_vendor_from_flags(flags);
        assert_eq!(info.vendor, InnoDbVendor::MySQL);
    }

    #[test]
    fn test_mysql_flags_with_compression() {
        // zlib compression at bits 11-12
        let flags = 1 << 11;
        let info = detect_vendor_from_flags(flags);
        assert_eq!(info.vendor, InnoDbVendor::MySQL);
    }

    #[test]
    fn test_mariadb_full_crc32_detected() {
        // Bit 4 set = full_crc32 marker
        let flags = 0x10;
        let info = detect_vendor_from_flags(flags);
        assert_eq!(info.vendor, InnoDbVendor::MariaDB);
        assert_eq!(info.mariadb_format, Some(MariaDbFormat::FullCrc32));
        assert!(info.is_full_crc32());
    }

    #[test]
    fn test_mariadb_full_crc32_with_page_size() {
        // Bit 4 (marker) + ssize=5 in bits 0-3
        let flags = 0x10 | 5;
        let info = detect_vendor_from_flags(flags);
        assert_eq!(info.vendor, InnoDbVendor::MariaDB);
        assert_eq!(info.mariadb_format, Some(MariaDbFormat::FullCrc32));
    }

    #[test]
    fn test_mariadb_original_page_compression() {
        // Bit 16 set, MySQL compression bits (11-12) zero
        let flags = 1 << 16;
        let info = detect_vendor_from_flags(flags);
        assert_eq!(info.vendor, InnoDbVendor::MariaDB);
        assert_eq!(info.mariadb_format, Some(MariaDbFormat::Original));
    }

    #[test]
    fn test_vendor_from_created_by_mysql() {
        assert_eq!(
            detect_vendor_from_created_by("MySQL 8.0.32"),
            InnoDbVendor::MySQL
        );
    }

    #[test]
    fn test_vendor_from_created_by_mariadb() {
        assert_eq!(
            detect_vendor_from_created_by("MariaDB 10.11.4"),
            InnoDbVendor::MariaDB
        );
    }

    #[test]
    fn test_vendor_from_created_by_percona() {
        assert_eq!(
            detect_vendor_from_created_by("Percona Server 8.0.32-24"),
            InnoDbVendor::Percona
        );
    }

    #[test]
    fn test_vendor_from_empty_string() {
        assert_eq!(detect_vendor_from_created_by(""), InnoDbVendor::MySQL);
    }

    #[test]
    fn test_vendor_info_display() {
        assert_eq!(format!("{}", VendorInfo::mysql()), "MySQL");
        assert_eq!(format!("{}", VendorInfo::percona()), "Percona XtraDB");
        assert_eq!(
            format!("{}", VendorInfo::mariadb(MariaDbFormat::FullCrc32)),
            "MariaDB (full_crc32)"
        );
        assert_eq!(
            format!("{}", VendorInfo::mariadb(MariaDbFormat::Original)),
            "MariaDB (original)"
        );
    }
}
