//! Large object (BLOB/LOB) page header parsing.
//!
//! InnoDB stores large column values on dedicated overflow pages. Old-style BLOB
//! pages (types 10-12) use a simple 8-byte header ([`BlobPageHeader`]) with the data
//! length and next-page pointer. MySQL 8.0+ introduces structured LOB first pages
//! (type 22) with a richer header ([`LobFirstPageHeader`]) containing version,
//! flags, total data length, transaction ID, and index entry pointers.

use byteorder::{BigEndian, ByteOrder};
use serde::Serialize;

use crate::innodb::constants::{FIL_NULL, FIL_PAGE_DATA};

/// Old-style BLOB page header offsets (relative to FIL_PAGE_DATA).
///
/// From btr0types.h / fil0fil.h in MySQL source.
/// These apply to page types BLOB (10), ZBLOB (11), ZBLOB2 (12).
const LOB_HDR_PART_LEN: usize = 0; // 4 bytes - data length on this page
const LOB_HDR_NEXT_PAGE_NO: usize = 4; // 4 bytes - next BLOB page number
const LOB_HDR_SIZE: usize = 8;

/// New-style LOB first page header offsets (MySQL 8.0+, page type LOB_FIRST = 22).
///
/// From lob0lob.h in MySQL source.
const LOB_FIRST_VERSION: usize = 0; // 1 byte
const LOB_FIRST_FLAGS: usize = 1; // 1 byte
const LOB_FIRST_DATA_LEN: usize = 2; // 4 bytes - total LOB data length
const LOB_FIRST_TRX_ID: usize = 6; // 6 bytes
const LOB_FIRST_HDR_SIZE: usize = 12;

/// New-style LOB index entry size (lob0lob.h: LOB_INDEX_ENTRY_SIZE).
const LOB_INDEX_ENTRY_SIZE: usize = 60;

/// LOB data page header offsets (page type LOB_DATA = 23, relative to FIL_PAGE_DATA).
const LOB_DATA_VERSION: usize = 0; // 1 byte
const LOB_DATA_DATA_LEN: usize = 1; // 4 bytes
const LOB_DATA_TRX_ID: usize = 5; // 6 bytes
const LOB_DATA_HDR_SIZE: usize = 11;

/// ZLOB first page header offsets (page type ZLOB_FIRST = 25, relative to FIL_PAGE_DATA).
const ZLOB_FIRST_VERSION: usize = 0; // 1 byte
const ZLOB_FIRST_FLAGS: usize = 1; // 1 byte
const ZLOB_FIRST_DATA_LEN: usize = 2; // 4 bytes - total uncompressed LOB length
const ZLOB_FIRST_TRX_ID: usize = 6; // 6 bytes
const ZLOB_FIRST_HDR_SIZE: usize = 12;

/// ZLOB data page header offsets (page type ZLOB_DATA = 26, relative to FIL_PAGE_DATA).
const ZLOB_DATA_VERSION: usize = 0; // 1 byte
const ZLOB_DATA_DATA_LEN: usize = 1; // 4 bytes - compressed data length on this page
const ZLOB_DATA_TRX_ID: usize = 5; // 6 bytes
const ZLOB_DATA_HDR_SIZE: usize = 11;

/// Parsed old-style BLOB page header.
#[derive(Debug, Clone, Serialize)]
pub struct BlobPageHeader {
    /// Number of data bytes stored on this page.
    pub part_len: u32,
    /// Page number of the next BLOB page (FIL_NULL if last).
    pub next_page_no: u32,
}

impl BlobPageHeader {
    /// Parse an old-style BLOB page header from a full page buffer.
    ///
    /// # Examples
    ///
    /// ```
    /// use idb::innodb::lob::BlobPageHeader;
    /// use byteorder::{BigEndian, ByteOrder};
    ///
    /// // Build a minimal page buffer (at least 38 + 8 = 46 bytes).
    /// let mut page = vec![0u8; 48];
    /// let base = 38; // FIL_PAGE_DATA
    ///
    /// // part_len (4 bytes) at base+0
    /// BigEndian::write_u32(&mut page[base..], 16000);
    /// // next_page_no (4 bytes) at base+4
    /// BigEndian::write_u32(&mut page[base + 4..], 7);
    ///
    /// let hdr = BlobPageHeader::parse(&page).unwrap();
    /// assert_eq!(hdr.part_len, 16000);
    /// assert_eq!(hdr.next_page_no, 7);
    /// assert!(hdr.has_next());
    ///
    /// // Last page in chain uses FIL_NULL (0xFFFFFFFF).
    /// BigEndian::write_u32(&mut page[base + 4..], 0xFFFFFFFF);
    /// let last = BlobPageHeader::parse(&page).unwrap();
    /// assert!(!last.has_next());
    /// ```
    pub fn parse(page_data: &[u8]) -> Option<Self> {
        let base = FIL_PAGE_DATA;
        if page_data.len() < base + LOB_HDR_SIZE {
            return None;
        }

        let d = &page_data[base..];
        Some(BlobPageHeader {
            part_len: BigEndian::read_u32(&d[LOB_HDR_PART_LEN..]),
            next_page_no: BigEndian::read_u32(&d[LOB_HDR_NEXT_PAGE_NO..]),
        })
    }

    /// Returns true if there is a next page in the chain.
    pub fn has_next(&self) -> bool {
        self.next_page_no != FIL_NULL && self.next_page_no != 0
    }
}

/// Parsed new-style LOB first page header (MySQL 8.0+).
#[derive(Debug, Clone, Serialize)]
pub struct LobFirstPageHeader {
    /// LOB version.
    pub version: u8,
    /// LOB flags.
    pub flags: u8,
    /// Total uncompressed data length of the LOB.
    pub data_len: u32,
    /// Transaction ID that created the LOB.
    pub trx_id: u64,
}

impl LobFirstPageHeader {
    /// Parse a LOB first page header from a full page buffer.
    ///
    /// # Examples
    ///
    /// ```
    /// use idb::innodb::lob::LobFirstPageHeader;
    /// use byteorder::{BigEndian, ByteOrder};
    ///
    /// // Build a minimal page buffer (at least 38 + 12 = 50 bytes).
    /// let mut page = vec![0u8; 52];
    /// let base = 38; // FIL_PAGE_DATA
    ///
    /// // version (1 byte) at base+0
    /// page[base] = 1;
    /// // flags (1 byte) at base+1
    /// page[base + 1] = 0;
    /// // data_len (4 bytes) at base+2
    /// BigEndian::write_u32(&mut page[base + 2..], 250_000);
    /// // trx_id (6 bytes, big-endian) at base+6
    /// let trx_bytes = 9999u64.to_be_bytes();
    /// page[base + 6..base + 12].copy_from_slice(&trx_bytes[2..8]);
    ///
    /// let hdr = LobFirstPageHeader::parse(&page).unwrap();
    /// assert_eq!(hdr.version, 1);
    /// assert_eq!(hdr.flags, 0);
    /// assert_eq!(hdr.data_len, 250_000);
    /// assert_eq!(hdr.trx_id, 9999);
    /// ```
    pub fn parse(page_data: &[u8]) -> Option<Self> {
        let base = FIL_PAGE_DATA;
        if page_data.len() < base + LOB_FIRST_HDR_SIZE {
            return None;
        }

        let d = &page_data[base..];

        // Read 6-byte transaction ID (big-endian, padded to u64)
        let trx_id = if d.len() >= LOB_FIRST_TRX_ID + 6 {
            let mut buf = [0u8; 8];
            buf[2..8].copy_from_slice(&d[LOB_FIRST_TRX_ID..LOB_FIRST_TRX_ID + 6]);
            BigEndian::read_u64(&buf)
        } else {
            0
        };

        Some(LobFirstPageHeader {
            version: d[LOB_FIRST_VERSION],
            flags: d[LOB_FIRST_FLAGS],
            data_len: BigEndian::read_u32(&d[LOB_FIRST_DATA_LEN..]),
            trx_id,
        })
    }
}

/// Parsed LOB index entry (60 bytes, from LOB_FIRST or LOB_INDEX pages).
///
/// Index entries link LOB data pages in the new-style LOB format (MySQL 8.0+).
/// Each entry describes one chunk of LOB data stored on a separate page.
///
/// # Examples
///
/// ```
/// use idb::innodb::lob::LobIndexEntry;
/// use byteorder::{BigEndian, ByteOrder};
///
/// let mut entry = vec![0u8; 60];
/// // prev node (page_no at 0, offset at 4)
/// BigEndian::write_u32(&mut entry[0..], 10);
/// BigEndian::write_u16(&mut entry[4..], 100);
/// // next node (page_no at 6, offset at 10)
/// BigEndian::write_u32(&mut entry[6..], 20);
/// BigEndian::write_u16(&mut entry[10..], 200);
/// // versions = 1 at byte 12
/// entry[12] = 1;
/// // trx_id (6 bytes) at byte 14
/// let trx_bytes = 5000u64.to_be_bytes();
/// entry[14..20].copy_from_slice(&trx_bytes[2..8]);
/// // trx_undo_no (4 bytes) at byte 20
/// BigEndian::write_u32(&mut entry[20..], 42);
/// // page_no (4 bytes) at byte 24
/// BigEndian::write_u32(&mut entry[24..], 7);
/// // data_len (4 bytes) at byte 28
/// BigEndian::write_u32(&mut entry[28..], 16000);
/// // lob_version (4 bytes) at byte 32
/// BigEndian::write_u32(&mut entry[32..], 1);
///
/// let parsed = LobIndexEntry::parse(&entry).unwrap();
/// assert_eq!(parsed.page_no, 7);
/// assert_eq!(parsed.data_len, 16000);
/// assert_eq!(parsed.trx_id, 5000);
/// assert_eq!(parsed.lob_version, 1);
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct LobIndexEntry {
    /// Previous entry page number in the index list.
    pub prev_page_no: u32,
    /// Previous entry offset.
    pub prev_offset: u16,
    /// Next entry page number in the index list.
    pub next_page_no: u32,
    /// Next entry offset.
    pub next_offset: u16,
    /// Number of versions of this entry.
    pub versions: u8,
    /// Transaction ID that created this entry.
    pub trx_id: u64,
    /// Undo log record number for this modification.
    pub trx_undo_no: u32,
    /// Page number containing the LOB data for this chunk.
    pub page_no: u32,
    /// Length of the data stored on the referenced page.
    pub data_len: u32,
    /// LOB version number.
    pub lob_version: u32,
}

impl LobIndexEntry {
    /// Parse a single LOB index entry from a 60-byte buffer.
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < LOB_INDEX_ENTRY_SIZE {
            return None;
        }

        // 6-byte trx_id at offset 14
        let mut trx_buf = [0u8; 8];
        trx_buf[2..8].copy_from_slice(&data[14..20]);

        Some(LobIndexEntry {
            prev_page_no: BigEndian::read_u32(&data[0..]),
            prev_offset: BigEndian::read_u16(&data[4..]),
            next_page_no: BigEndian::read_u32(&data[6..]),
            next_offset: BigEndian::read_u16(&data[10..]),
            versions: data[12],
            trx_id: BigEndian::read_u64(&trx_buf),
            trx_undo_no: BigEndian::read_u32(&data[20..]),
            page_no: BigEndian::read_u32(&data[24..]),
            data_len: BigEndian::read_u32(&data[28..]),
            lob_version: BigEndian::read_u32(&data[32..]),
        })
    }

    /// Parse all LOB index entries from a page, starting at the given offset.
    ///
    /// Reads entries sequentially until the page boundary is reached.
    pub fn parse_all(page_data: &[u8], start_offset: usize) -> Vec<Self> {
        let mut entries = Vec::new();
        let mut offset = start_offset;

        while offset + LOB_INDEX_ENTRY_SIZE <= page_data.len() {
            if let Some(entry) = LobIndexEntry::parse(&page_data[offset..]) {
                // Skip entries with FIL_NULL page_no (empty slots)
                if entry.page_no != FIL_NULL {
                    entries.push(entry);
                }
                offset += LOB_INDEX_ENTRY_SIZE;
            } else {
                break;
            }
        }

        entries
    }
}

/// Parsed LOB data page header (page type LOB_DATA = 23, MySQL 8.0+).
///
/// # Examples
///
/// ```
/// use idb::innodb::lob::LobDataPageHeader;
/// use byteorder::{BigEndian, ByteOrder};
///
/// let mut page = vec![0u8; 60];
/// let base = 38; // FIL_PAGE_DATA
/// page[base] = 1; // version
/// BigEndian::write_u32(&mut page[base + 1..], 8000); // data_len
/// let trx_bytes = 777u64.to_be_bytes();
/// page[base + 5..base + 11].copy_from_slice(&trx_bytes[2..8]);
///
/// let hdr = LobDataPageHeader::parse(&page).unwrap();
/// assert_eq!(hdr.version, 1);
/// assert_eq!(hdr.data_len, 8000);
/// assert_eq!(hdr.trx_id, 777);
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct LobDataPageHeader {
    /// LOB data page version.
    pub version: u8,
    /// Length of data stored on this page.
    pub data_len: u32,
    /// Transaction ID.
    pub trx_id: u64,
}

impl LobDataPageHeader {
    /// Parse a LOB data page header from a full page buffer.
    pub fn parse(page_data: &[u8]) -> Option<Self> {
        let base = FIL_PAGE_DATA;
        if page_data.len() < base + LOB_DATA_HDR_SIZE {
            return None;
        }

        let d = &page_data[base..];
        let mut trx_buf = [0u8; 8];
        trx_buf[2..8].copy_from_slice(&d[LOB_DATA_TRX_ID..LOB_DATA_TRX_ID + 6]);

        Some(LobDataPageHeader {
            version: d[LOB_DATA_VERSION],
            data_len: BigEndian::read_u32(&d[LOB_DATA_DATA_LEN..]),
            trx_id: BigEndian::read_u64(&trx_buf),
        })
    }
}

/// Parsed ZLOB first page header (page type ZLOB_FIRST = 25, MySQL 8.0+).
///
/// The compressed LOB first page mirrors the uncompressed LOB first page layout
/// but stores the total uncompressed data length rather than per-page lengths.
///
/// # Examples
///
/// ```
/// use idb::innodb::lob::ZlobFirstPageHeader;
/// use byteorder::{BigEndian, ByteOrder};
///
/// let mut page = vec![0u8; 60];
/// let base = 38;
/// page[base] = 1; // version
/// page[base + 1] = 0; // flags
/// BigEndian::write_u32(&mut page[base + 2..], 500_000); // total uncompressed len
/// let trx_bytes = 1234u64.to_be_bytes();
/// page[base + 6..base + 12].copy_from_slice(&trx_bytes[2..8]);
///
/// let hdr = ZlobFirstPageHeader::parse(&page).unwrap();
/// assert_eq!(hdr.version, 1);
/// assert_eq!(hdr.data_len, 500_000);
/// assert_eq!(hdr.trx_id, 1234);
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct ZlobFirstPageHeader {
    /// ZLOB version.
    pub version: u8,
    /// ZLOB flags.
    pub flags: u8,
    /// Total uncompressed data length of the LOB.
    pub data_len: u32,
    /// Transaction ID that created the LOB.
    pub trx_id: u64,
}

impl ZlobFirstPageHeader {
    /// Parse a ZLOB first page header from a full page buffer.
    pub fn parse(page_data: &[u8]) -> Option<Self> {
        let base = FIL_PAGE_DATA;
        if page_data.len() < base + ZLOB_FIRST_HDR_SIZE {
            return None;
        }

        let d = &page_data[base..];
        let mut trx_buf = [0u8; 8];
        trx_buf[2..8].copy_from_slice(&d[ZLOB_FIRST_TRX_ID..ZLOB_FIRST_TRX_ID + 6]);

        Some(ZlobFirstPageHeader {
            version: d[ZLOB_FIRST_VERSION],
            flags: d[ZLOB_FIRST_FLAGS],
            data_len: BigEndian::read_u32(&d[ZLOB_FIRST_DATA_LEN..]),
            trx_id: BigEndian::read_u64(&trx_buf),
        })
    }
}

/// Parsed ZLOB data page header (page type ZLOB_DATA = 26, MySQL 8.0+).
///
/// # Examples
///
/// ```
/// use idb::innodb::lob::ZlobDataPageHeader;
/// use byteorder::{BigEndian, ByteOrder};
///
/// let mut page = vec![0u8; 60];
/// let base = 38;
/// page[base] = 1; // version
/// BigEndian::write_u32(&mut page[base + 1..], 4096); // compressed data length
/// let trx_bytes = 999u64.to_be_bytes();
/// page[base + 5..base + 11].copy_from_slice(&trx_bytes[2..8]);
///
/// let hdr = ZlobDataPageHeader::parse(&page).unwrap();
/// assert_eq!(hdr.version, 1);
/// assert_eq!(hdr.data_len, 4096);
/// assert_eq!(hdr.trx_id, 999);
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct ZlobDataPageHeader {
    /// ZLOB data page version.
    pub version: u8,
    /// Compressed data length on this page.
    pub data_len: u32,
    /// Transaction ID.
    pub trx_id: u64,
}

impl ZlobDataPageHeader {
    /// Parse a ZLOB data page header from a full page buffer.
    pub fn parse(page_data: &[u8]) -> Option<Self> {
        let base = FIL_PAGE_DATA;
        if page_data.len() < base + ZLOB_DATA_HDR_SIZE {
            return None;
        }

        let d = &page_data[base..];
        let mut trx_buf = [0u8; 8];
        trx_buf[2..8].copy_from_slice(&d[ZLOB_DATA_TRX_ID..ZLOB_DATA_TRX_ID + 6]);

        Some(ZlobDataPageHeader {
            version: d[ZLOB_DATA_VERSION],
            data_len: BigEndian::read_u32(&d[ZLOB_DATA_DATA_LEN..]),
            trx_id: BigEndian::read_u64(&trx_buf),
        })
    }
}

/// Walk old-style BLOB page chain starting from a given page.
///
/// Returns the list of (page_number, part_len) for each page in the chain.
/// Stops at FIL_NULL or when max_pages is reached.
///
/// # Examples
///
/// ```no_run
/// use idb::innodb::tablespace::Tablespace;
/// use idb::innodb::lob::walk_blob_chain;
///
/// let mut ts = Tablespace::open("table.ibd").unwrap();
/// // Walk up to 100 BLOB pages starting from page 5.
/// let chain = walk_blob_chain(&mut ts, 5, 100).unwrap();
/// for (page_no, part_len) in &chain {
///     println!("Page {}: {} bytes", page_no, part_len);
/// }
/// ```
pub fn walk_blob_chain(
    ts: &mut crate::innodb::tablespace::Tablespace,
    start_page: u64,
    max_pages: usize,
) -> Result<Vec<(u64, u32)>, crate::IdbError> {
    let mut chain = Vec::new();
    let mut current = start_page;

    for _ in 0..max_pages {
        if current == FIL_NULL as u64 || current == 0 {
            break;
        }

        let page_data = ts.read_page(current)?;
        let hdr = match BlobPageHeader::parse(&page_data) {
            Some(h) => h,
            None => break,
        };

        chain.push((current, hdr.part_len));

        if !hdr.has_next() {
            break;
        }
        current = hdr.next_page_no as u64;
    }

    Ok(chain)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blob_page_header_parse() {
        let mut page = vec![0u8; 256];
        let base = FIL_PAGE_DATA;

        // Set part_len = 8000
        BigEndian::write_u32(&mut page[base + LOB_HDR_PART_LEN..], 8000);
        // Set next_page = 42
        BigEndian::write_u32(&mut page[base + LOB_HDR_NEXT_PAGE_NO..], 42);

        let hdr = BlobPageHeader::parse(&page).unwrap();
        assert_eq!(hdr.part_len, 8000);
        assert_eq!(hdr.next_page_no, 42);
        assert!(hdr.has_next());
    }

    #[test]
    fn test_blob_page_header_no_next() {
        let mut page = vec![0u8; 256];
        let base = FIL_PAGE_DATA;

        BigEndian::write_u32(&mut page[base + LOB_HDR_PART_LEN..], 5000);
        BigEndian::write_u32(&mut page[base + LOB_HDR_NEXT_PAGE_NO..], FIL_NULL);

        let hdr = BlobPageHeader::parse(&page).unwrap();
        assert_eq!(hdr.part_len, 5000);
        assert!(!hdr.has_next());
    }

    #[test]
    fn test_lob_first_page_header_parse() {
        let mut page = vec![0u8; 256];
        let base = FIL_PAGE_DATA;

        page[base + LOB_FIRST_VERSION] = 1;
        page[base + LOB_FIRST_FLAGS] = 0;
        BigEndian::write_u32(&mut page[base + LOB_FIRST_DATA_LEN..], 100_000);
        // 6-byte trx_id = 12345
        let trx_bytes = 12345u64.to_be_bytes();
        page[base + LOB_FIRST_TRX_ID..base + LOB_FIRST_TRX_ID + 6]
            .copy_from_slice(&trx_bytes[2..8]);

        let hdr = LobFirstPageHeader::parse(&page).unwrap();
        assert_eq!(hdr.version, 1);
        assert_eq!(hdr.flags, 0);
        assert_eq!(hdr.data_len, 100_000);
        assert_eq!(hdr.trx_id, 12345);
    }

    #[test]
    fn test_lob_index_entry_parse() {
        let mut entry = vec![0u8; 60];
        // prev node
        BigEndian::write_u32(&mut entry[0..], 10);
        BigEndian::write_u16(&mut entry[4..], 100);
        // next node
        BigEndian::write_u32(&mut entry[6..], 20);
        BigEndian::write_u16(&mut entry[10..], 200);
        // versions
        entry[12] = 2;
        // trx_id (6 bytes)
        let trx_bytes = 5000u64.to_be_bytes();
        entry[14..20].copy_from_slice(&trx_bytes[2..8]);
        // trx_undo_no
        BigEndian::write_u32(&mut entry[20..], 42);
        // page_no
        BigEndian::write_u32(&mut entry[24..], 7);
        // data_len
        BigEndian::write_u32(&mut entry[28..], 16000);
        // lob_version
        BigEndian::write_u32(&mut entry[32..], 1);

        let parsed = LobIndexEntry::parse(&entry).unwrap();
        assert_eq!(parsed.prev_page_no, 10);
        assert_eq!(parsed.prev_offset, 100);
        assert_eq!(parsed.next_page_no, 20);
        assert_eq!(parsed.next_offset, 200);
        assert_eq!(parsed.versions, 2);
        assert_eq!(parsed.trx_id, 5000);
        assert_eq!(parsed.trx_undo_no, 42);
        assert_eq!(parsed.page_no, 7);
        assert_eq!(parsed.data_len, 16000);
        assert_eq!(parsed.lob_version, 1);
    }

    #[test]
    fn test_lob_index_entry_too_short() {
        let entry = vec![0u8; 30]; // too short
        assert!(LobIndexEntry::parse(&entry).is_none());
    }

    #[test]
    fn test_lob_index_entry_parse_all() {
        // Two entries followed by a FIL_NULL entry
        let mut data = vec![0u8; 60 * 3];
        // Entry 1
        BigEndian::write_u32(&mut data[24..], 5); // page_no = 5
        BigEndian::write_u32(&mut data[28..], 8000); // data_len
        // Entry 2
        BigEndian::write_u32(&mut data[60 + 24..], 6); // page_no = 6
        BigEndian::write_u32(&mut data[60 + 28..], 4000);
        // Entry 3: FIL_NULL page_no (should be skipped)
        BigEndian::write_u32(&mut data[120 + 24..], FIL_NULL);

        let entries = LobIndexEntry::parse_all(&data, 0);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].page_no, 5);
        assert_eq!(entries[1].page_no, 6);
    }

    #[test]
    fn test_lob_data_page_header_parse() {
        let mut page = vec![0u8; 60];
        let base = FIL_PAGE_DATA;
        page[base] = 1; // version
        BigEndian::write_u32(&mut page[base + 1..], 8000);
        let trx_bytes = 777u64.to_be_bytes();
        page[base + 5..base + 11].copy_from_slice(&trx_bytes[2..8]);

        let hdr = LobDataPageHeader::parse(&page).unwrap();
        assert_eq!(hdr.version, 1);
        assert_eq!(hdr.data_len, 8000);
        assert_eq!(hdr.trx_id, 777);
    }

    #[test]
    fn test_zlob_first_page_header_parse() {
        let mut page = vec![0u8; 60];
        let base = FIL_PAGE_DATA;
        page[base] = 1;
        page[base + 1] = 0;
        BigEndian::write_u32(&mut page[base + 2..], 500_000);
        let trx_bytes = 1234u64.to_be_bytes();
        page[base + 6..base + 12].copy_from_slice(&trx_bytes[2..8]);

        let hdr = ZlobFirstPageHeader::parse(&page).unwrap();
        assert_eq!(hdr.version, 1);
        assert_eq!(hdr.flags, 0);
        assert_eq!(hdr.data_len, 500_000);
        assert_eq!(hdr.trx_id, 1234);
    }

    #[test]
    fn test_zlob_data_page_header_parse() {
        let mut page = vec![0u8; 60];
        let base = FIL_PAGE_DATA;
        page[base] = 1;
        BigEndian::write_u32(&mut page[base + 1..], 4096);
        let trx_bytes = 999u64.to_be_bytes();
        page[base + 5..base + 11].copy_from_slice(&trx_bytes[2..8]);

        let hdr = ZlobDataPageHeader::parse(&page).unwrap();
        assert_eq!(hdr.version, 1);
        assert_eq!(hdr.data_len, 4096);
        assert_eq!(hdr.trx_id, 999);
    }

    #[test]
    fn test_lob_data_page_header_too_short() {
        let page = vec![0u8; 40]; // base=38, need 38+11=49
        assert!(LobDataPageHeader::parse(&page).is_none());
    }
}
