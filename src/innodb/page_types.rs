//! InnoDB page type definitions.
//!
//! Maps the 2-byte page type field (bytes 24-25 of the FIL header) to a
//! [`PageType`] enum. Each variant carries its MySQL source name, a human-readable
//! description, and a usage note via the `PageType::metadata` method.
//!
//! Covers all page types from MySQL 5.7 through 9.x, including INDEX (17855),
//! SDI (17853), UNDO, INODE, BLOB/LOB, compressed LOB (ZLOB), redo log, and
//! encryption key pages. Also includes MariaDB-specific types: page compression
//! (34354), compressed+encrypted (37401), and instant ALTER (18).
//!
//! Verified against MySQL source `fil0fil.h` from versions 8.0, 8.4, 9.0.1,
//! and 9.1.0. Page type constants are identical across all these versions.

use serde::Serialize;
use std::fmt;

use crate::innodb::vendor::VendorInfo;

/// All InnoDB page types from MySQL 5.7 through 9.x.
///
/// Values are from `fil0fil.h` in MySQL source. Verified identical across
/// MySQL 8.0, 8.4, 9.0.1, and 9.1.0.
///
/// Note: Value 18 is ambiguous — it represents `SdiBlob` in MySQL 8.0+ and
/// `Instant` in MariaDB. Use [`PageType::from_u16_with_vendor`] to resolve
/// based on vendor context. Both variants share the same on-disk value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum PageType {
    /// Freshly allocated, type field not initialized (FIL_PAGE_TYPE_ALLOCATED = 0)
    Allocated,
    /// Unused page type marker (FIL_PAGE_TYPE_UNUSED = 1)
    Unused,
    /// Undo log page (FIL_PAGE_UNDO_LOG = 2)
    UndoLog,
    /// File segment inode (FIL_PAGE_INODE = 3)
    Inode,
    /// Insert buffer free list (FIL_PAGE_IBUF_FREE_LIST = 4)
    IbufFreeList,
    /// Insert buffer bitmap (FIL_PAGE_IBUF_BITMAP = 5)
    IbufBitmap,
    /// System internal page (FIL_PAGE_TYPE_SYS = 6)
    Sys,
    /// Transaction system header (FIL_PAGE_TYPE_TRX_SYS = 7)
    TrxSys,
    /// File space header, page 0 of each tablespace (FIL_PAGE_TYPE_FSP_HDR = 8)
    FspHdr,
    /// Extent descriptor (FIL_PAGE_TYPE_XDES = 9)
    Xdes,
    /// Uncompressed BLOB page (FIL_PAGE_TYPE_BLOB = 10)
    Blob,
    /// First compressed BLOB page (FIL_PAGE_TYPE_ZBLOB = 11)
    ZBlob,
    /// Subsequent compressed BLOB page (FIL_PAGE_TYPE_ZBLOB2 = 12)
    ZBlob2,
    /// Unknown/reserved (FIL_PAGE_TYPE_UNKNOWN = 13)
    Unknown,
    /// Compressed page (FIL_PAGE_COMPRESSED = 14)
    Compressed,
    /// Encrypted page (FIL_PAGE_ENCRYPTED = 15)
    Encrypted,
    /// Compressed and encrypted page (FIL_PAGE_COMPRESSED_AND_ENCRYPTED = 16)
    CompressedEncrypted,
    /// Encrypted R-tree page (FIL_PAGE_ENCRYPTED_RTREE = 17)
    EncryptedRtree,
    /// Uncompressed SDI BLOB page (FIL_PAGE_SDI_BLOB = 18, MySQL 8.0+)
    SdiBlob,
    /// Compressed SDI BLOB page (FIL_PAGE_SDI_ZBLOB = 19, MySQL 8.0+)
    SdiZblob,
    /// Legacy doublewrite buffer page (FIL_PAGE_TYPE_LEGACY_DBLWR = 20, MySQL 8.0+)
    LegacyDblwr,
    /// Rollback segment array page (FIL_PAGE_TYPE_RSEG_ARRAY = 21, MySQL 8.0+)
    RsegArray,
    /// LOB index page (FIL_PAGE_TYPE_LOB_INDEX = 22, MySQL 8.0+)
    LobIndex,
    /// LOB data page (FIL_PAGE_TYPE_LOB_DATA = 23, MySQL 8.0+)
    LobData,
    /// LOB first page (FIL_PAGE_TYPE_LOB_FIRST = 24, MySQL 8.0+)
    LobFirst,
    /// First page of compressed LOB (FIL_PAGE_TYPE_ZLOB_FIRST = 25, MySQL 8.0+)
    ZlobFirst,
    /// Data pages of compressed LOB (FIL_PAGE_TYPE_ZLOB_DATA = 26, MySQL 8.0+)
    ZlobData,
    /// Index pages of compressed LOB (FIL_PAGE_TYPE_ZLOB_INDEX = 27, MySQL 8.0+)
    ZlobIndex,
    /// Fragment pages of compressed LOB (FIL_PAGE_TYPE_ZLOB_FRAG = 28, MySQL 8.0+)
    ZlobFrag,
    /// Index of fragment pages for compressed LOB (FIL_PAGE_TYPE_ZLOB_FRAG_ENTRY = 29, MySQL 8.0+)
    ZlobFragEntry,
    /// SDI index page (FIL_PAGE_SDI = 17853, MySQL 8.0+)
    Sdi,
    /// R-tree index page for spatial indexes (FIL_PAGE_RTREE = 17854)
    Rtree,
    /// B+Tree index page for table and index data (FIL_PAGE_INDEX = 17855)
    Index,
    /// MariaDB page-level compression (type 34354)
    PageCompressed,
    /// MariaDB page-level compression + encryption (type 37401)
    PageCompressedEncrypted,
    /// MariaDB instant ALTER TABLE metadata (value 18, same as SdiBlob in MySQL)
    Instant,
}

impl PageType {
    /// Parse a page type from a u16 value read from the FIL header.
    ///
    /// Value 18 defaults to `SdiBlob` (MySQL interpretation). Use
    /// [`from_u16_with_vendor`](PageType::from_u16_with_vendor) for
    /// MariaDB-aware resolution.
    ///
    /// # Examples
    ///
    /// ```
    /// use idb::innodb::page_types::PageType;
    ///
    /// // INDEX pages (B+Tree data) use type code 17855
    /// let page_type = PageType::from_u16(17855);
    /// assert_eq!(page_type, PageType::Index);
    ///
    /// // FSP_HDR (file space header, page 0) uses type code 8
    /// let fsp = PageType::from_u16(8);
    /// assert_eq!(fsp, PageType::FspHdr);
    ///
    /// // Unrecognized values map to Unknown
    /// let unknown = PageType::from_u16(9999);
    /// assert_eq!(unknown, PageType::Unknown);
    /// ```
    pub fn from_u16(value: u16) -> Self {
        match value {
            0 => PageType::Allocated,
            1 => PageType::Unused,
            2 => PageType::UndoLog,
            3 => PageType::Inode,
            4 => PageType::IbufFreeList,
            5 => PageType::IbufBitmap,
            6 => PageType::Sys,
            7 => PageType::TrxSys,
            8 => PageType::FspHdr,
            9 => PageType::Xdes,
            10 => PageType::Blob,
            11 => PageType::ZBlob,
            12 => PageType::ZBlob2,
            13 => PageType::Unknown,
            14 => PageType::Compressed,
            15 => PageType::Encrypted,
            16 => PageType::CompressedEncrypted,
            17 => PageType::EncryptedRtree,
            18 => PageType::SdiBlob,
            19 => PageType::SdiZblob,
            20 => PageType::LegacyDblwr,
            21 => PageType::RsegArray,
            22 => PageType::LobIndex,
            23 => PageType::LobData,
            24 => PageType::LobFirst,
            25 => PageType::ZlobFirst,
            26 => PageType::ZlobData,
            27 => PageType::ZlobIndex,
            28 => PageType::ZlobFrag,
            29 => PageType::ZlobFragEntry,
            17853 => PageType::Sdi,
            17854 => PageType::Rtree,
            17855 => PageType::Index,
            34354 => PageType::PageCompressed,
            37401 => PageType::PageCompressedEncrypted,
            _ => PageType::Unknown,
        }
    }

    /// Parse a page type from a u16 value with vendor context.
    ///
    /// Resolves ambiguous values:
    /// - Value 18: `Instant` for MariaDB, `SdiBlob` for MySQL/Percona
    ///
    /// # Examples
    ///
    /// ```
    /// use idb::innodb::page_types::PageType;
    /// use idb::innodb::vendor::{VendorInfo, MariaDbFormat};
    ///
    /// // For MySQL, value 18 is SDI BLOB
    /// let mysql = VendorInfo::mysql();
    /// assert_eq!(PageType::from_u16_with_vendor(18, &mysql), PageType::SdiBlob);
    ///
    /// // For MariaDB, value 18 is Instant ALTER TABLE metadata
    /// let mariadb = VendorInfo::mariadb(MariaDbFormat::FullCrc32);
    /// assert_eq!(PageType::from_u16_with_vendor(18, &mariadb), PageType::Instant);
    ///
    /// // Non-ambiguous values are unaffected by vendor
    /// assert_eq!(PageType::from_u16_with_vendor(17855, &mysql), PageType::Index);
    /// assert_eq!(PageType::from_u16_with_vendor(17855, &mariadb), PageType::Index);
    /// ```
    pub fn from_u16_with_vendor(value: u16, vendor_info: &VendorInfo) -> Self {
        use crate::innodb::vendor::InnoDbVendor;

        match value {
            18 if vendor_info.vendor == InnoDbVendor::MariaDB => PageType::Instant,
            _ => Self::from_u16(value),
        }
    }

    /// Returns the raw u16 value of this page type.
    ///
    /// Note: Both `SdiBlob` and `Instant` return 18, since they represent the
    /// same on-disk value interpreted differently by MySQL and MariaDB.
    pub fn as_u16(self) -> u16 {
        match self {
            PageType::Allocated => 0,
            PageType::Unused => 1,
            PageType::UndoLog => 2,
            PageType::Inode => 3,
            PageType::IbufFreeList => 4,
            PageType::IbufBitmap => 5,
            PageType::Sys => 6,
            PageType::TrxSys => 7,
            PageType::FspHdr => 8,
            PageType::Xdes => 9,
            PageType::Blob => 10,
            PageType::ZBlob => 11,
            PageType::ZBlob2 => 12,
            PageType::Unknown => 13,
            PageType::Compressed => 14,
            PageType::Encrypted => 15,
            PageType::CompressedEncrypted => 16,
            PageType::EncryptedRtree => 17,
            PageType::SdiBlob | PageType::Instant => 18,
            PageType::SdiZblob => 19,
            PageType::LegacyDblwr => 20,
            PageType::RsegArray => 21,
            PageType::LobIndex => 22,
            PageType::LobData => 23,
            PageType::LobFirst => 24,
            PageType::ZlobFirst => 25,
            PageType::ZlobData => 26,
            PageType::ZlobIndex => 27,
            PageType::ZlobFrag => 28,
            PageType::ZlobFragEntry => 29,
            PageType::Sdi => 17853,
            PageType::Rtree => 17854,
            PageType::Index => 17855,
            PageType::PageCompressed => 34354,
            PageType::PageCompressedEncrypted => 37401,
        }
    }

    /// Returns (name, description, usage) for this page type.
    fn metadata(self) -> (&'static str, &'static str, &'static str) {
        match self {
            PageType::Allocated => (
                "ALLOCATED",
                "Freshly allocated",
                "Page type field not initialized.",
            ),
            PageType::Unused => (
                "UNUSED",
                "Unused page type",
                "Reserved page type marker (not used in practice).",
            ),
            PageType::UndoLog => (
                "UNDO_LOG",
                "Undo log",
                "Stores previous values of modified records.",
            ),
            PageType::Inode => (
                "INODE",
                "File segment inode",
                "Bookkeeping for file segments.",
            ),
            PageType::IbufFreeList => (
                "IBUF_FREE_LIST",
                "Insert buffer free list",
                "Bookkeeping for insert buffer free space management.",
            ),
            PageType::IbufBitmap => (
                "IBUF_BITMAP",
                "Insert buffer bitmap",
                "Bookkeeping for insert buffer writes to be merged.",
            ),
            PageType::Sys => (
                "SYS",
                "System internal",
                "Used for various purposes in the system tablespace.",
            ),
            PageType::TrxSys => (
                "TRX_SYS",
                "Transaction system header",
                "Bookkeeping for the transaction system in system tablespace.",
            ),
            PageType::FspHdr => (
                "FSP_HDR",
                "File space header",
                "Header page (page 0) for each tablespace file.",
            ),
            PageType::Xdes => (
                "XDES",
                "Extent descriptor",
                "Header page for subsequent blocks of 16,384 pages.",
            ),
            PageType::Blob => (
                "BLOB",
                "Uncompressed BLOB",
                "Externally-stored uncompressed BLOB column data.",
            ),
            PageType::ZBlob => (
                "ZBLOB",
                "First compressed BLOB",
                "Externally-stored compressed BLOB column data, first page.",
            ),
            PageType::ZBlob2 => (
                "ZBLOB2",
                "Subsequent compressed BLOB",
                "Externally-stored compressed BLOB column data, subsequent page.",
            ),
            PageType::Unknown => (
                "UNKNOWN",
                "Unknown page type",
                "Unknown or unrecognized page type.",
            ),
            PageType::Compressed => (
                "COMPRESSED",
                "Compressed page",
                "Page stored in compressed format.",
            ),
            PageType::Encrypted => (
                "ENCRYPTED",
                "Encrypted page",
                "Page stored in encrypted format.",
            ),
            PageType::CompressedEncrypted => (
                "COMPRESSED_ENCRYPTED",
                "Compressed and encrypted page",
                "Page stored in compressed and encrypted format.",
            ),
            PageType::EncryptedRtree => (
                "ENCRYPTED_RTREE",
                "Encrypted R-tree page",
                "Encrypted R-tree spatial index page.",
            ),
            PageType::SdiBlob => (
                "SDI_BLOB",
                "SDI BLOB",
                "Uncompressed SDI BLOB overflow data (MySQL 8.0+).",
            ),
            PageType::SdiZblob => (
                "SDI_ZBLOB",
                "Compressed SDI BLOB",
                "Compressed SDI BLOB overflow data (MySQL 8.0+).",
            ),
            PageType::LegacyDblwr => (
                "LEGACY_DBLWR",
                "Legacy doublewrite buffer",
                "Legacy doublewrite buffer page (MySQL 8.0+).",
            ),
            PageType::RsegArray => (
                "RSEG_ARRAY",
                "Rollback segment array",
                "Rollback segment array page (MySQL 8.0+).",
            ),
            PageType::LobIndex => (
                "LOB_INDEX",
                "LOB index",
                "Index page for uncompressed large objects (MySQL 8.0+).",
            ),
            PageType::LobData => (
                "LOB_DATA",
                "LOB data",
                "Data page for uncompressed large objects (MySQL 8.0+).",
            ),
            PageType::LobFirst => (
                "LOB_FIRST",
                "LOB first page",
                "First page of an uncompressed large object (MySQL 8.0+).",
            ),
            PageType::ZlobFirst => (
                "ZLOB_FIRST",
                "Compressed LOB first page",
                "First page of a compressed large object (MySQL 8.0+).",
            ),
            PageType::ZlobData => (
                "ZLOB_DATA",
                "Compressed LOB data",
                "Data page for compressed large objects (MySQL 8.0+).",
            ),
            PageType::ZlobIndex => (
                "ZLOB_INDEX",
                "Compressed LOB index",
                "Index page for compressed large objects (MySQL 8.0+).",
            ),
            PageType::ZlobFrag => (
                "ZLOB_FRAG",
                "Compressed LOB fragment",
                "Fragment page for compressed large objects (MySQL 8.0+).",
            ),
            PageType::ZlobFragEntry => (
                "ZLOB_FRAG_ENTRY",
                "Compressed LOB fragment index",
                "Index of fragment pages for compressed large objects (MySQL 8.0+).",
            ),
            PageType::Sdi => (
                "SDI",
                "Serialized Dictionary Information",
                "Serialized Dictionary Information metadata (MySQL 8.0+).",
            ),
            PageType::Index => (
                "INDEX",
                "B+Tree index",
                "Table and index data stored in B+Tree structure.",
            ),
            PageType::Rtree => (
                "RTREE",
                "R-tree index",
                "Spatial index data stored in R-tree structure.",
            ),
            PageType::PageCompressed => (
                "PAGE_COMPRESSED",
                "MariaDB page compression",
                "Page-level compression (MariaDB). Algorithm ID at offset 26.",
            ),
            PageType::PageCompressedEncrypted => (
                "PAGE_COMPRESSED_ENCRYPTED",
                "MariaDB compressed + encrypted",
                "Page-level compression with encryption (MariaDB).",
            ),
            PageType::Instant => (
                "INSTANT",
                "MariaDB instant ALTER",
                "Instant ALTER TABLE metadata page (MariaDB).",
            ),
        }
    }

    /// Returns the name of this page type as used in MySQL source.
    ///
    /// # Examples
    ///
    /// ```
    /// use idb::innodb::page_types::PageType;
    ///
    /// assert_eq!(PageType::Index.name(), "INDEX");
    /// assert_eq!(PageType::FspHdr.name(), "FSP_HDR");
    /// assert_eq!(PageType::Sdi.name(), "SDI");
    /// assert_eq!(PageType::PageCompressed.name(), "PAGE_COMPRESSED");
    /// ```
    pub fn name(self) -> &'static str {
        self.metadata().0
    }

    /// Returns a human-readable description of this page type.
    pub fn description(self) -> &'static str {
        self.metadata().1
    }

    /// Returns usage information for this page type.
    pub fn usage(self) -> &'static str {
        self.metadata().2
    }
}

impl fmt::Display for PageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::innodb::vendor::{InnoDbVendor, MariaDbFormat};

    #[test]
    fn test_page_type_from_u16() {
        assert_eq!(PageType::from_u16(0), PageType::Allocated);
        assert_eq!(PageType::from_u16(1), PageType::Unused);
        assert_eq!(PageType::from_u16(2), PageType::UndoLog);
        assert_eq!(PageType::from_u16(8), PageType::FspHdr);
        assert_eq!(PageType::from_u16(17855), PageType::Index);
        assert_eq!(PageType::from_u16(17853), PageType::Sdi);
        assert_eq!(PageType::from_u16(9999), PageType::Unknown);
    }

    #[test]
    fn test_page_type_mariadb_types() {
        assert_eq!(PageType::from_u16(34354), PageType::PageCompressed);
        assert_eq!(PageType::from_u16(37401), PageType::PageCompressedEncrypted);
    }

    #[test]
    fn test_page_type_18_default_is_sdi_blob() {
        assert_eq!(PageType::from_u16(18), PageType::SdiBlob);
    }

    #[test]
    fn test_page_type_18_mariadb_is_instant() {
        let mariadb = VendorInfo {
            vendor: InnoDbVendor::MariaDB,
            mariadb_format: Some(MariaDbFormat::FullCrc32),
        };
        assert_eq!(
            PageType::from_u16_with_vendor(18, &mariadb),
            PageType::Instant
        );
    }

    #[test]
    fn test_page_type_18_mysql_is_sdi_blob() {
        let mysql = VendorInfo::mysql();
        assert_eq!(
            PageType::from_u16_with_vendor(18, &mysql),
            PageType::SdiBlob
        );
    }

    /// Verify as_u16() roundtrips correctly for all non-ambiguous types.
    #[test]
    fn test_page_type_roundtrip() {
        let types = [
            PageType::Allocated,
            PageType::Unused,
            PageType::UndoLog,
            PageType::Inode,
            PageType::IbufFreeList,
            PageType::IbufBitmap,
            PageType::Sys,
            PageType::TrxSys,
            PageType::FspHdr,
            PageType::Xdes,
            PageType::Blob,
            PageType::ZBlob,
            PageType::ZBlob2,
            PageType::Unknown,
            PageType::Compressed,
            PageType::Encrypted,
            PageType::CompressedEncrypted,
            PageType::EncryptedRtree,
            PageType::SdiBlob,
            PageType::SdiZblob,
            PageType::LegacyDblwr,
            PageType::RsegArray,
            PageType::LobIndex,
            PageType::LobData,
            PageType::LobFirst,
            PageType::ZlobFirst,
            PageType::ZlobData,
            PageType::ZlobIndex,
            PageType::ZlobFrag,
            PageType::ZlobFragEntry,
            PageType::Sdi,
            PageType::Rtree,
            PageType::Index,
            PageType::PageCompressed,
            PageType::PageCompressedEncrypted,
        ];
        for pt in &types {
            assert_eq!(
                PageType::from_u16(pt.as_u16()),
                *pt,
                "roundtrip failed for {:?} (value {})",
                pt,
                pt.as_u16()
            );
        }
    }

    /// Verify SdiBlob and Instant both map to value 18.
    #[test]
    fn test_sdi_blob_and_instant_share_value_18() {
        assert_eq!(PageType::SdiBlob.as_u16(), 18);
        assert_eq!(PageType::Instant.as_u16(), 18);
    }

    #[test]
    fn test_page_type_display() {
        assert_eq!(format!("{}", PageType::Index), "INDEX");
        assert_eq!(format!("{}", PageType::FspHdr), "FSP_HDR");
        assert_eq!(format!("{}", PageType::PageCompressed), "PAGE_COMPRESSED");
    }

    /// Verify all MySQL source page type values map correctly.
    /// Values from fil0fil.h (identical in MySQL 8.0, 8.4, 9.0.1, 9.1.0).
    #[test]
    fn test_mysql_source_page_type_values() {
        // fil0fil.h constants
        assert_eq!(PageType::from_u16(0), PageType::Allocated);       // FIL_PAGE_TYPE_ALLOCATED
        assert_eq!(PageType::from_u16(1), PageType::Unused);          // FIL_PAGE_TYPE_UNUSED
        assert_eq!(PageType::from_u16(2), PageType::UndoLog);         // FIL_PAGE_UNDO_LOG
        assert_eq!(PageType::from_u16(3), PageType::Inode);           // FIL_PAGE_INODE
        assert_eq!(PageType::from_u16(4), PageType::IbufFreeList);    // FIL_PAGE_IBUF_FREE_LIST
        assert_eq!(PageType::from_u16(5), PageType::IbufBitmap);      // FIL_PAGE_IBUF_BITMAP
        assert_eq!(PageType::from_u16(6), PageType::Sys);             // FIL_PAGE_TYPE_SYS
        assert_eq!(PageType::from_u16(7), PageType::TrxSys);          // FIL_PAGE_TYPE_TRX_SYS
        assert_eq!(PageType::from_u16(8), PageType::FspHdr);          // FIL_PAGE_TYPE_FSP_HDR
        assert_eq!(PageType::from_u16(9), PageType::Xdes);            // FIL_PAGE_TYPE_XDES
        assert_eq!(PageType::from_u16(10), PageType::Blob);           // FIL_PAGE_TYPE_BLOB
        assert_eq!(PageType::from_u16(11), PageType::ZBlob);          // FIL_PAGE_TYPE_ZBLOB
        assert_eq!(PageType::from_u16(12), PageType::ZBlob2);         // FIL_PAGE_TYPE_ZBLOB2
        assert_eq!(PageType::from_u16(13), PageType::Unknown);        // FIL_PAGE_TYPE_UNKNOWN
        assert_eq!(PageType::from_u16(14), PageType::Compressed);     // FIL_PAGE_COMPRESSED
        assert_eq!(PageType::from_u16(15), PageType::Encrypted);      // FIL_PAGE_ENCRYPTED
        assert_eq!(PageType::from_u16(16), PageType::CompressedEncrypted); // FIL_PAGE_COMPRESSED_AND_ENCRYPTED
        assert_eq!(PageType::from_u16(17), PageType::EncryptedRtree); // FIL_PAGE_ENCRYPTED_RTREE
        assert_eq!(PageType::from_u16(18), PageType::SdiBlob);        // FIL_PAGE_SDI_BLOB
        assert_eq!(PageType::from_u16(19), PageType::SdiZblob);       // FIL_PAGE_SDI_ZBLOB
        assert_eq!(PageType::from_u16(20), PageType::LegacyDblwr);    // FIL_PAGE_TYPE_LEGACY_DBLWR
        assert_eq!(PageType::from_u16(21), PageType::RsegArray);      // FIL_PAGE_TYPE_RSEG_ARRAY
        assert_eq!(PageType::from_u16(22), PageType::LobIndex);       // FIL_PAGE_TYPE_LOB_INDEX
        assert_eq!(PageType::from_u16(23), PageType::LobData);        // FIL_PAGE_TYPE_LOB_DATA
        assert_eq!(PageType::from_u16(24), PageType::LobFirst);       // FIL_PAGE_TYPE_LOB_FIRST
        assert_eq!(PageType::from_u16(25), PageType::ZlobFirst);      // FIL_PAGE_TYPE_ZLOB_FIRST
        assert_eq!(PageType::from_u16(26), PageType::ZlobData);       // FIL_PAGE_TYPE_ZLOB_DATA
        assert_eq!(PageType::from_u16(27), PageType::ZlobIndex);      // FIL_PAGE_TYPE_ZLOB_INDEX
        assert_eq!(PageType::from_u16(28), PageType::ZlobFrag);       // FIL_PAGE_TYPE_ZLOB_FRAG
        assert_eq!(PageType::from_u16(29), PageType::ZlobFragEntry);  // FIL_PAGE_TYPE_ZLOB_FRAG_ENTRY
        assert_eq!(PageType::from_u16(17853), PageType::Sdi);         // FIL_PAGE_SDI
        assert_eq!(PageType::from_u16(17854), PageType::Rtree);       // FIL_PAGE_RTREE
        assert_eq!(PageType::from_u16(17855), PageType::Index);       // FIL_PAGE_INDEX
    }

    /// Verify as_u16() values match MySQL source constants.
    #[test]
    fn test_as_u16_matches_mysql_source() {
        assert_eq!(PageType::Allocated.as_u16(), 0);
        assert_eq!(PageType::Unused.as_u16(), 1);
        assert_eq!(PageType::UndoLog.as_u16(), 2);
        assert_eq!(PageType::Inode.as_u16(), 3);
        assert_eq!(PageType::IbufFreeList.as_u16(), 4);
        assert_eq!(PageType::IbufBitmap.as_u16(), 5);
        assert_eq!(PageType::Sys.as_u16(), 6);
        assert_eq!(PageType::TrxSys.as_u16(), 7);
        assert_eq!(PageType::FspHdr.as_u16(), 8);
        assert_eq!(PageType::Xdes.as_u16(), 9);
        assert_eq!(PageType::Blob.as_u16(), 10);
        assert_eq!(PageType::ZBlob.as_u16(), 11);
        assert_eq!(PageType::ZBlob2.as_u16(), 12);
        assert_eq!(PageType::Unknown.as_u16(), 13);
        assert_eq!(PageType::Compressed.as_u16(), 14);
        assert_eq!(PageType::Encrypted.as_u16(), 15);
        assert_eq!(PageType::CompressedEncrypted.as_u16(), 16);
        assert_eq!(PageType::EncryptedRtree.as_u16(), 17);
        assert_eq!(PageType::SdiBlob.as_u16(), 18);
        assert_eq!(PageType::SdiZblob.as_u16(), 19);
        assert_eq!(PageType::LegacyDblwr.as_u16(), 20);
        assert_eq!(PageType::RsegArray.as_u16(), 21);
        assert_eq!(PageType::LobIndex.as_u16(), 22);
        assert_eq!(PageType::LobData.as_u16(), 23);
        assert_eq!(PageType::LobFirst.as_u16(), 24);
        assert_eq!(PageType::ZlobFirst.as_u16(), 25);
        assert_eq!(PageType::ZlobData.as_u16(), 26);
        assert_eq!(PageType::ZlobIndex.as_u16(), 27);
        assert_eq!(PageType::ZlobFrag.as_u16(), 28);
        assert_eq!(PageType::ZlobFragEntry.as_u16(), 29);
        assert_eq!(PageType::Sdi.as_u16(), 17853);
        assert_eq!(PageType::Rtree.as_u16(), 17854);
        assert_eq!(PageType::Index.as_u16(), 17855);
        assert_eq!(PageType::PageCompressed.as_u16(), 34354);
        assert_eq!(PageType::PageCompressedEncrypted.as_u16(), 37401);
        assert_eq!(PageType::Instant.as_u16(), 18);
    }

    /// Verify FIL_PAGE_TYPE_LAST = 29 (ZLOB_FRAG_ENTRY) is the highest
    /// non-index page type, matching MySQL source.
    #[test]
    fn test_last_non_index_page_type() {
        // FIL_PAGE_TYPE_LAST = FIL_PAGE_TYPE_ZLOB_FRAG_ENTRY = 29
        assert_eq!(PageType::ZlobFragEntry.as_u16(), 29);
        // Values 30 and above (below index range) should be Unknown
        assert_eq!(PageType::from_u16(30), PageType::Unknown);
        assert_eq!(PageType::from_u16(100), PageType::Unknown);
    }
}
