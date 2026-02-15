//! INDEX page internal structure parsing.
//!
//! INDEX pages (page type 17855 / `FIL_PAGE_INDEX`) are the B+Tree nodes that
//! store table data and secondary index entries. Each INDEX page contains a
//! 36-byte [`IndexHeader`] at `FIL_PAGE_DATA` (byte 38), followed by two
//! 10-byte FSEG inode pointers ([`FsegHeader`]) for the leaf and non-leaf
//! segments, and the infimum/supremum system records.
//!
//! Use [`IndexHeader::parse`] and [`FsegHeader::parse`] to decode these
//! structures from raw page bytes.

use byteorder::{BigEndian, ByteOrder};
use serde::Serialize;

use crate::innodb::constants::*;

/// Parsed INDEX page header (36 bytes, at FIL_PAGE_DATA offset within an INDEX page).
///
/// This header is present on all B+Tree INDEX pages (page type 17855).
#[derive(Debug, Clone, Serialize)]
pub struct IndexHeader {
    /// Number of directory slots in the page directory.
    pub n_dir_slots: u16,
    /// Pointer to record heap top.
    pub heap_top: u16,
    /// Number of records in the heap. Bit 15 is the compact format flag.
    pub n_heap_raw: u16,
    /// Pointer to start of free record list (0 if none).
    pub free: u16,
    /// Number of bytes in deleted records (garbage).
    pub garbage: u16,
    /// Pointer to the last inserted record (0 if reset).
    pub last_insert: u16,
    /// Last insert direction.
    pub direction: u16,
    /// Number of consecutive inserts in the same direction.
    pub n_direction: u16,
    /// Number of user records on the page.
    pub n_recs: u16,
    /// Highest trx id that may have modified a record (secondary indexes only).
    pub max_trx_id: u64,
    /// Level in the B+Tree (0 = leaf).
    pub level: u16,
    /// Index ID where the page belongs.
    pub index_id: u64,
}

impl IndexHeader {
    /// Parse an INDEX page header from a full page buffer.
    ///
    /// The INDEX header starts at FIL_PAGE_DATA (byte 38).
    ///
    /// # Examples
    ///
    /// ```
    /// use idb::innodb::index::IndexHeader;
    /// use idb::innodb::constants::*;
    /// use byteorder::{BigEndian, ByteOrder};
    ///
    /// // Build a minimal page buffer (at least 38 + 36 = 74 bytes)
    /// let mut page = vec![0u8; 256];
    /// let base = FIL_PAGE_DATA; // byte 38
    ///
    /// // Set some fields in the INDEX header
    /// BigEndian::write_u16(&mut page[base + PAGE_N_DIR_SLOTS..], 4);
    /// BigEndian::write_u16(&mut page[base + PAGE_N_HEAP..], 0x8003); // compact + 3 records
    /// BigEndian::write_u16(&mut page[base + PAGE_N_RECS..], 1);
    /// BigEndian::write_u16(&mut page[base + PAGE_LEVEL..], 0);       // leaf page
    /// BigEndian::write_u64(&mut page[base + PAGE_INDEX_ID..], 100);
    /// BigEndian::write_u16(&mut page[base + PAGE_DIRECTION..], PAGE_RIGHT);
    ///
    /// let hdr = IndexHeader::parse(&page).unwrap();
    /// assert_eq!(hdr.n_dir_slots, 4);
    /// assert!(hdr.is_compact());
    /// assert_eq!(hdr.n_heap(), 3);
    /// assert_eq!(hdr.n_recs, 1);
    /// assert!(hdr.is_leaf());
    /// assert_eq!(hdr.index_id, 100);
    /// assert_eq!(hdr.direction_name(), "Right");
    /// ```
    pub fn parse(page_data: &[u8]) -> Option<Self> {
        let base = FIL_PAGE_DATA;
        if page_data.len() < base + 36 {
            return None;
        }
        let d = &page_data[base..];

        Some(IndexHeader {
            n_dir_slots: BigEndian::read_u16(&d[PAGE_N_DIR_SLOTS..]),
            heap_top: BigEndian::read_u16(&d[PAGE_HEAP_TOP..]),
            n_heap_raw: BigEndian::read_u16(&d[PAGE_N_HEAP..]),
            free: BigEndian::read_u16(&d[PAGE_FREE..]),
            garbage: BigEndian::read_u16(&d[PAGE_GARBAGE..]),
            last_insert: BigEndian::read_u16(&d[PAGE_LAST_INSERT..]),
            direction: BigEndian::read_u16(&d[PAGE_DIRECTION..]),
            n_direction: BigEndian::read_u16(&d[PAGE_N_DIRECTION..]),
            n_recs: BigEndian::read_u16(&d[PAGE_N_RECS..]),
            max_trx_id: BigEndian::read_u64(&d[PAGE_MAX_TRX_ID..]),
            level: BigEndian::read_u16(&d[PAGE_LEVEL..]),
            index_id: BigEndian::read_u64(&d[PAGE_INDEX_ID..]),
        })
    }

    /// Returns the actual number of records in the heap (masking out the compact flag).
    pub fn n_heap(&self) -> u16 {
        self.n_heap_raw & 0x7FFF
    }

    /// Returns true if this page uses the new compact row format.
    pub fn is_compact(&self) -> bool {
        (self.n_heap_raw & 0x8000) != 0
    }

    /// Returns a human-readable description of the insert direction.
    pub fn direction_name(&self) -> &'static str {
        match self.direction {
            PAGE_LEFT => "Left",
            PAGE_RIGHT => "Right",
            PAGE_SAME_REC => "Same Record",
            PAGE_SAME_PAGE => "Same Page",
            PAGE_NO_DIRECTION => "No Direction",
            _ => "Unknown",
        }
    }

    /// Returns true if this is a leaf-level page.
    pub fn is_leaf(&self) -> bool {
        self.level == 0
    }
}

/// FSEG (File Segment) header pointer (10 bytes each).
///
/// There are two FSEG headers per INDEX page: one for the leaf segment
/// and one for the non-leaf (internal) segment. These follow the INDEX header.
#[derive(Debug, Clone)]
pub struct FsegHeader {
    /// Space ID of the inode.
    pub space_id: u32,
    /// Page number of the inode.
    pub page_no: u32,
    /// Byte offset of the inode within the page.
    pub offset: u16,
}

impl FsegHeader {
    /// Parse an FSEG header from a byte slice (must be at least 10 bytes).
    ///
    /// # Examples
    ///
    /// ```
    /// use idb::innodb::index::FsegHeader;
    /// use byteorder::{BigEndian, ByteOrder};
    ///
    /// let mut data = vec![0u8; 10];
    /// BigEndian::write_u32(&mut data[0..], 3);   // space_id
    /// BigEndian::write_u32(&mut data[4..], 7);   // page_no
    /// BigEndian::write_u16(&mut data[8..], 50);  // offset
    ///
    /// let fseg = FsegHeader::parse(&data).unwrap();
    /// assert_eq!(fseg.space_id, 3);
    /// assert_eq!(fseg.page_no, 7);
    /// assert_eq!(fseg.offset, 50);
    /// ```
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < FSEG_HEADER_SIZE {
            return None;
        }
        Some(FsegHeader {
            space_id: BigEndian::read_u32(&data[0..]),
            page_no: BigEndian::read_u32(&data[4..]),
            offset: BigEndian::read_u16(&data[8..]),
        })
    }

    /// Parse the leaf FSEG header from a full page buffer.
    ///
    /// Leaf FSEG header is at FIL_PAGE_DATA + 36 (after the INDEX header).
    ///
    /// # Examples
    ///
    /// ```
    /// use idb::innodb::index::FsegHeader;
    /// use idb::innodb::constants::{FIL_PAGE_DATA, FSEG_HEADER_SIZE};
    /// use byteorder::{BigEndian, ByteOrder};
    ///
    /// let mut page = vec![0u8; 256];
    /// let leaf_base = FIL_PAGE_DATA + 36; // byte 74
    /// BigEndian::write_u32(&mut page[leaf_base..], 1);       // space_id
    /// BigEndian::write_u32(&mut page[leaf_base + 4..], 10);  // page_no
    /// BigEndian::write_u16(&mut page[leaf_base + 8..], 38);  // offset
    ///
    /// let leaf = FsegHeader::parse_leaf(&page).unwrap();
    /// assert_eq!(leaf.space_id, 1);
    /// assert_eq!(leaf.page_no, 10);
    /// assert_eq!(leaf.offset, 38);
    /// ```
    pub fn parse_leaf(page_data: &[u8]) -> Option<Self> {
        let base = FIL_PAGE_DATA + 36; // IDX_HDR_SIZE = 36
        if page_data.len() < base + FSEG_HEADER_SIZE {
            return None;
        }
        Self::parse(&page_data[base..])
    }

    /// Parse the non-leaf (internal) FSEG header from a full page buffer.
    ///
    /// Internal FSEG header is at FIL_PAGE_DATA + 36 + 10 (after leaf FSEG).
    ///
    /// # Examples
    ///
    /// ```
    /// use idb::innodb::index::FsegHeader;
    /// use idb::innodb::constants::{FIL_PAGE_DATA, FSEG_HEADER_SIZE};
    /// use byteorder::{BigEndian, ByteOrder};
    ///
    /// let mut page = vec![0u8; 256];
    /// let internal_base = FIL_PAGE_DATA + 36 + FSEG_HEADER_SIZE; // byte 84
    /// BigEndian::write_u32(&mut page[internal_base..], 2);       // space_id
    /// BigEndian::write_u32(&mut page[internal_base + 4..], 20);  // page_no
    /// BigEndian::write_u16(&mut page[internal_base + 8..], 38);  // offset
    ///
    /// let internal = FsegHeader::parse_internal(&page).unwrap();
    /// assert_eq!(internal.space_id, 2);
    /// assert_eq!(internal.page_no, 20);
    /// assert_eq!(internal.offset, 38);
    /// ```
    pub fn parse_internal(page_data: &[u8]) -> Option<Self> {
        let base = FIL_PAGE_DATA + 36 + FSEG_HEADER_SIZE;
        if page_data.len() < base + FSEG_HEADER_SIZE {
            return None;
        }
        Self::parse(&page_data[base..])
    }
}

/// System record info extracted from the infimum/supremum area.
#[derive(Debug, Clone)]
pub struct SystemRecords {
    /// Record status (3 lowest bits at SYS_REC_START+1).
    pub rec_status: u8,
    /// Number of records owned (4 lowest bits at SYS_REC_START).
    pub n_owned: u8,
    /// Deleted flag (bit 2 at SYS_REC_START).
    pub deleted: bool,
    /// Heap number (bits 0-12 at SYS_REC_START+1).
    pub heap_no: u16,
    /// Min-rec flag (bit 3 at SYS_REC_START, left-most node on non-leaf).
    pub min_rec: bool,
    /// Infimum next record offset.
    pub infimum_next: u16,
    /// Supremum next record offset.
    pub supremum_next: u16,
}

impl SystemRecords {
    /// Parse system record information from a full page buffer.
    ///
    /// System records start at PAGE_DATA_OFFSET (94) within the page.
    pub fn parse(page_data: &[u8]) -> Option<Self> {
        let base = PAGE_DATA_OFFSET;
        if page_data.len() < base + 18 {
            return None;
        }

        let byte0 = page_data[base];

        // n_owned: bits 4-7 of byte 0 (4 lowest bits of upper nibble)
        let n_owned = (byte0 >> 4) & 0x0F;

        // min_rec: bit 3 of byte 0
        let min_rec = (byte0 & 0x08) != 0;

        // deleted: bit 2 of byte 0
        let deleted = (byte0 & 0x04) != 0;

        // rec_status: bits 0-2 of byte1 (3 lowest bits) combined with byte2
        // Actually, it's the 3 lowest bits from the 2-byte field at base+1
        let two_bytes = BigEndian::read_u16(&page_data[base + 1..]);
        let rec_status = (two_bytes & 0x07) as u8;

        // heap_no: bits 3-15 of the 2-byte field at base+1
        let heap_no = (two_bytes >> 3) & 0x1FFF;

        // Infimum next: 2 bytes at base+3
        let infimum_next = BigEndian::read_u16(&page_data[base + 3..]);

        // Supremum next: 2 bytes at base+16 (offset from infimum to supremum area)
        let supremum_next = BigEndian::read_u16(&page_data[base + 16..]);

        Some(SystemRecords {
            rec_status,
            n_owned,
            deleted,
            heap_no,
            min_rec,
            infimum_next,
            supremum_next,
        })
    }

    /// Returns the record status type name.
    pub fn rec_status_name(&self) -> &'static str {
        match self.rec_status {
            0 => "REC_STATUS_ORDINARY",
            1 => "REC_STATUS_NODE_PTR",
            2 => "REC_STATUS_INFIMUM",
            3 => "REC_STATUS_SUPREMUM",
            _ => "UNKNOWN",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_header_compact_flag() {
        // Create a minimal page buffer with INDEX header
        let mut page = vec![0u8; 256];
        let base = FIL_PAGE_DATA;

        // Set n_heap with compact flag (bit 15 set)
        BigEndian::write_u16(&mut page[base + PAGE_N_HEAP..], 0x8005); // compact, 5 records
        BigEndian::write_u16(&mut page[base + PAGE_LEVEL..], 0); // leaf level
        BigEndian::write_u64(&mut page[base + PAGE_INDEX_ID..], 42);
        BigEndian::write_u16(&mut page[base + PAGE_DIRECTION..], PAGE_RIGHT);

        let hdr = IndexHeader::parse(&page).unwrap();
        assert!(hdr.is_compact());
        assert_eq!(hdr.n_heap(), 5);
        assert!(hdr.is_leaf());
        assert_eq!(hdr.index_id, 42);
        assert_eq!(hdr.direction_name(), "Right");
    }

    #[test]
    fn test_fseg_header_parse() {
        let mut data = vec![0u8; 10];
        BigEndian::write_u32(&mut data[0..], 5); // space_id
        BigEndian::write_u32(&mut data[4..], 2); // page_no
        BigEndian::write_u16(&mut data[8..], 50); // offset

        let fseg = FsegHeader::parse(&data).unwrap();
        assert_eq!(fseg.space_id, 5);
        assert_eq!(fseg.page_no, 2);
        assert_eq!(fseg.offset, 50);
    }
}
