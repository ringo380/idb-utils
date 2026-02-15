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

/// New-style LOB index entry size.
#[allow(dead_code)]
const LOB_INDEX_ENTRY_SIZE: usize = 60;

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
}
