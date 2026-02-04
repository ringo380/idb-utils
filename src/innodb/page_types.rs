use serde::Serialize;
use std::fmt;

/// All InnoDB page types from MySQL 5.7 through 9.x.
///
/// Values are from fil0fil.h in MySQL source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[repr(u16)]
pub enum PageType {
    /// Freshly allocated, type field not initialized
    Allocated = 0,
    /// Undo log page (stores previous values of modified records)
    UndoLog = 2,
    /// File segment inode (bookkeeping for file segments)
    Inode = 3,
    /// Insert buffer free list (bookkeeping for insert buffer free space)
    IbufFreeList = 4,
    /// Insert buffer bitmap (bookkeeping for insert buffer writes)
    IbufBitmap = 5,
    /// System internal page (various purposes in system tablespace)
    Sys = 6,
    /// Transaction system header (bookkeeping in system tablespace)
    TrxSys = 7,
    /// File space header (page 0 of each tablespace)
    FspHdr = 8,
    /// Extent descriptor (header for subsequent blocks of 16,384 pages)
    Xdes = 9,
    /// Uncompressed BLOB (externally-stored uncompressed BLOB data)
    Blob = 10,
    /// First compressed BLOB page
    ZBlob = 11,
    /// Subsequent compressed BLOB page
    ZBlob2 = 12,
    /// Unknown/reserved (13)
    Unknown = 13,
    /// Compressed page
    Compressed = 14,
    /// Encrypted page
    Encrypted = 15,
    /// Compressed and encrypted page
    CompressedEncrypted = 16,
    /// Encrypted R-tree page
    EncryptedRtree = 17,
    /// SDI BLOB page (MySQL 8.0+)
    SdiBlob = 17854,
    /// SDI index page (MySQL 8.0+, Serialized Dictionary Information)
    Sdi = 17853,
    /// B+Tree index page (table and index data)
    Index = 17855,
    /// R-tree index page (spatial indexes)
    Rtree = 17856,
    /// LOB index page (MySQL 8.0+ large object index)
    LobIndex = 20,
    /// LOB data page (MySQL 8.0+ large object data)
    LobData = 21,
    /// LOB first page (MySQL 8.0+ large object first page)
    LobFirst = 22,
    /// Rollback segment array page (MySQL 8.0+)
    RsegArray = 23,
}

impl PageType {
    /// Parse a page type from a u16 value read from the FIL header.
    pub fn from_u16(value: u16) -> Self {
        match value {
            0 => PageType::Allocated,
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
            20 => PageType::LobIndex,
            21 => PageType::LobData,
            22 => PageType::LobFirst,
            23 => PageType::RsegArray,
            17853 => PageType::Sdi,
            17854 => PageType::SdiBlob,
            17855 => PageType::Index,
            17856 => PageType::Rtree,
            _ => PageType::Unknown,
        }
    }

    /// Returns the raw u16 value of this page type.
    pub fn as_u16(self) -> u16 {
        self as u16
    }

    /// Returns the name of this page type as used in MySQL source.
    pub fn name(self) -> &'static str {
        match self {
            PageType::Allocated => "ALLOCATED",
            PageType::UndoLog => "UNDO_LOG",
            PageType::Inode => "INODE",
            PageType::IbufFreeList => "IBUF_FREE_LIST",
            PageType::IbufBitmap => "IBUF_BITMAP",
            PageType::Sys => "SYS",
            PageType::TrxSys => "TRX_SYS",
            PageType::FspHdr => "FSP_HDR",
            PageType::Xdes => "XDES",
            PageType::Blob => "BLOB",
            PageType::ZBlob => "ZBLOB",
            PageType::ZBlob2 => "ZBLOB2",
            PageType::Unknown => "UNKNOWN",
            PageType::Compressed => "COMPRESSED",
            PageType::Encrypted => "ENCRYPTED",
            PageType::CompressedEncrypted => "COMPRESSED_ENCRYPTED",
            PageType::EncryptedRtree => "ENCRYPTED_RTREE",
            PageType::SdiBlob => "SDI_BLOB",
            PageType::Sdi => "SDI",
            PageType::Index => "INDEX",
            PageType::Rtree => "RTREE",
            PageType::LobIndex => "LOB_INDEX",
            PageType::LobData => "LOB_DATA",
            PageType::LobFirst => "LOB_FIRST",
            PageType::RsegArray => "RSEG_ARRAY",
        }
    }

    /// Returns a human-readable description of this page type.
    pub fn description(self) -> &'static str {
        match self {
            PageType::Allocated => "Freshly allocated",
            PageType::UndoLog => "Undo log",
            PageType::Inode => "File segment inode",
            PageType::IbufFreeList => "Insert buffer free list",
            PageType::IbufBitmap => "Insert buffer bitmap",
            PageType::Sys => "System internal",
            PageType::TrxSys => "Transaction system header",
            PageType::FspHdr => "File space header",
            PageType::Xdes => "Extent descriptor",
            PageType::Blob => "Uncompressed BLOB",
            PageType::ZBlob => "First compressed BLOB",
            PageType::ZBlob2 => "Subsequent compressed BLOB",
            PageType::Unknown => "Unknown page type",
            PageType::Compressed => "Compressed page",
            PageType::Encrypted => "Encrypted page",
            PageType::CompressedEncrypted => "Compressed and encrypted page",
            PageType::EncryptedRtree => "Encrypted R-tree page",
            PageType::SdiBlob => "SDI BLOB",
            PageType::Sdi => "Serialized Dictionary Information",
            PageType::Index => "B+Tree index",
            PageType::Rtree => "R-tree index",
            PageType::LobIndex => "LOB index",
            PageType::LobData => "LOB data",
            PageType::LobFirst => "LOB first page",
            PageType::RsegArray => "Rollback segment array",
        }
    }

    /// Returns usage information for this page type.
    pub fn usage(self) -> &'static str {
        match self {
            PageType::Allocated => "Page type field not initialized.",
            PageType::UndoLog => "Stores previous values of modified records.",
            PageType::Inode => "Bookkeeping for file segments.",
            PageType::IbufFreeList => "Bookkeeping for insert buffer free space management.",
            PageType::IbufBitmap => "Bookkeeping for insert buffer writes to be merged.",
            PageType::Sys => "Used for various purposes in the system tablespace.",
            PageType::TrxSys => "Bookkeeping for the transaction system in system tablespace.",
            PageType::FspHdr => "Header page (page 0) for each tablespace file.",
            PageType::Xdes => "Header page for subsequent blocks of 16,384 pages.",
            PageType::Blob => "Externally-stored uncompressed BLOB column data.",
            PageType::ZBlob => "Externally-stored compressed BLOB column data, first page.",
            PageType::ZBlob2 => "Externally-stored compressed BLOB column data, subsequent page.",
            PageType::Unknown => "Unknown or unrecognized page type.",
            PageType::Compressed => "Page stored in compressed format.",
            PageType::Encrypted => "Page stored in encrypted format.",
            PageType::CompressedEncrypted => "Page stored in compressed and encrypted format.",
            PageType::EncryptedRtree => "Encrypted R-tree spatial index page.",
            PageType::SdiBlob => "SDI BLOB overflow data (MySQL 8.0+).",
            PageType::Sdi => "Serialized Dictionary Information metadata (MySQL 8.0+).",
            PageType::Index => "Table and index data stored in B+Tree structure.",
            PageType::Rtree => "Spatial index data stored in R-tree structure.",
            PageType::LobIndex => "Large object index page (MySQL 8.0+).",
            PageType::LobData => "Large object data page (MySQL 8.0+).",
            PageType::LobFirst => "Large object first page (MySQL 8.0+).",
            PageType::RsegArray => "Rollback segment array page (MySQL 8.0+).",
        }
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

    #[test]
    fn test_page_type_from_u16() {
        assert_eq!(PageType::from_u16(0), PageType::Allocated);
        assert_eq!(PageType::from_u16(2), PageType::UndoLog);
        assert_eq!(PageType::from_u16(8), PageType::FspHdr);
        assert_eq!(PageType::from_u16(17855), PageType::Index);
        assert_eq!(PageType::from_u16(17853), PageType::Sdi);
        assert_eq!(PageType::from_u16(9999), PageType::Unknown);
    }

    #[test]
    fn test_page_type_roundtrip() {
        let types = [
            PageType::Allocated,
            PageType::UndoLog,
            PageType::Inode,
            PageType::FspHdr,
            PageType::Index,
            PageType::Sdi,
        ];
        for pt in &types {
            assert_eq!(PageType::from_u16(pt.as_u16()), *pt);
        }
    }

    #[test]
    fn test_page_type_display() {
        assert_eq!(format!("{}", PageType::Index), "INDEX");
        assert_eq!(format!("{}", PageType::FspHdr), "FSP_HDR");
    }
}
