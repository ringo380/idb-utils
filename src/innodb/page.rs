//! InnoDB page header and trailer parsing.
//!
//! Every InnoDB page begins with a 38-byte FIL header ([`FilHeader`]) containing
//! the checksum, page number, prev/next pointers, LSN, page type, flush LSN, and
//! space ID. The last 8 bytes form the FIL trailer ([`FilTrailer`]) with the
//! old-style checksum and low 32 bits of the LSN.
//!
//! Page 0 of every tablespace also contains the FSP header ([`FspHeader`]) at
//! byte offset 38, which stores the space ID, tablespace size, and feature flags
//! (page size, compression, encryption).

use byteorder::{BigEndian, ByteOrder};
use serde::Serialize;

use crate::innodb::constants::*;
use crate::innodb::page_types::PageType;

/// Parsed FIL header (38 bytes, present at the start of every InnoDB page).
#[derive(Debug, Clone, Serialize)]
pub struct FilHeader {
    /// Checksum (or space id in older formats). Bytes 0-3.
    pub checksum: u32,
    /// Page number within the tablespace. Bytes 4-7.
    pub page_number: u32,
    /// Previous page in the doubly-linked list. Bytes 8-11.
    /// FIL_NULL (0xFFFFFFFF) if not used.
    pub prev_page: u32,
    /// Next page in the doubly-linked list. Bytes 12-15.
    /// FIL_NULL (0xFFFFFFFF) if not used.
    pub next_page: u32,
    /// LSN of newest modification to this page. Bytes 16-23.
    pub lsn: u64,
    /// Page type. Bytes 24-25.
    pub page_type: PageType,
    /// Flush LSN (only meaningful for page 0 of system tablespace). Bytes 26-33.
    pub flush_lsn: u64,
    /// Space ID this page belongs to. Bytes 34-37.
    pub space_id: u32,
}

impl FilHeader {
    /// Parse a FIL header from a byte slice.
    ///
    /// The slice must be at least SIZE_FIL_HEAD (38) bytes.
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < SIZE_FIL_HEAD {
            return None;
        }

        Some(FilHeader {
            checksum: BigEndian::read_u32(&data[FIL_PAGE_SPACE_OR_CHKSUM..]),
            page_number: BigEndian::read_u32(&data[FIL_PAGE_OFFSET..]),
            prev_page: BigEndian::read_u32(&data[FIL_PAGE_PREV..]),
            next_page: BigEndian::read_u32(&data[FIL_PAGE_NEXT..]),
            lsn: BigEndian::read_u64(&data[FIL_PAGE_LSN..]),
            page_type: PageType::from_u16(BigEndian::read_u16(&data[FIL_PAGE_TYPE..])),
            flush_lsn: BigEndian::read_u64(&data[FIL_PAGE_FILE_FLUSH_LSN..]),
            space_id: BigEndian::read_u32(&data[FIL_PAGE_SPACE_ID..]),
        })
    }

    /// Returns true if prev_page is FIL_NULL (not used).
    pub fn has_prev(&self) -> bool {
        self.prev_page != FIL_NULL && self.prev_page != 0
    }

    /// Returns true if next_page is FIL_NULL (not used).
    pub fn has_next(&self) -> bool {
        self.next_page != FIL_NULL && self.next_page != 0
    }
}

/// Parsed FIL trailer (8 bytes, present at the end of every InnoDB page).
#[derive(Debug, Clone, Serialize)]
pub struct FilTrailer {
    /// Old-style checksum (or low 32 bits of LSN, depending on version). Bytes 0-3 of trailer.
    pub checksum: u32,
    /// Low 32 bits of the LSN. Bytes 4-7 of trailer.
    pub lsn_low32: u32,
}

impl FilTrailer {
    /// Parse a FIL trailer from a byte slice.
    ///
    /// The slice should be the last 8 bytes of the page, or at least 8 bytes
    /// starting from the trailer position.
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < SIZE_FIL_TRAILER {
            return None;
        }

        Some(FilTrailer {
            checksum: BigEndian::read_u32(&data[0..]),
            lsn_low32: BigEndian::read_u32(&data[4..]),
        })
    }
}

/// Parsed FSP header (from page 0 of a tablespace, starts at FIL_PAGE_DATA).
#[derive(Debug, Clone, Serialize)]
pub struct FspHeader {
    /// Space ID.
    pub space_id: u32,
    /// Size of the tablespace in pages.
    pub size: u32,
    /// Minimum page number not yet initialized.
    pub free_limit: u32,
    /// Space flags (contains page size, compression, encryption info).
    pub flags: u32,
    /// Number of used pages in the FSP_FREE_FRAG list.
    pub frag_n_used: u32,
}

impl FspHeader {
    /// Parse the FSP header from page 0's data area.
    ///
    /// `data` should be the full page buffer. FSP header starts at FIL_PAGE_DATA (byte 38).
    pub fn parse(page_data: &[u8]) -> Option<Self> {
        let offset = FIL_PAGE_DATA;
        if page_data.len() < offset + FSP_HEADER_SIZE {
            return None;
        }
        let data = &page_data[offset..];

        Some(FspHeader {
            space_id: BigEndian::read_u32(&data[FSP_SPACE_ID..]),
            size: BigEndian::read_u32(&data[FSP_SIZE..]),
            free_limit: BigEndian::read_u32(&data[FSP_FREE_LIMIT..]),
            flags: BigEndian::read_u32(&data[FSP_SPACE_FLAGS..]),
            frag_n_used: BigEndian::read_u32(&data[FSP_FRAG_N_USED..]),
        })
    }

    /// Extract the page size from FSP flags.
    ///
    /// Returns the page size in bytes, or None if the flags indicate the default (16K).
    pub fn page_size_from_flags(&self) -> u32 {
        let ssize = (self.flags & FSP_FLAGS_MASK_PAGE_SSIZE) >> FSP_FLAGS_POS_PAGE_SSIZE;
        if ssize == 0 {
            // Default/uncompressed: 16K
            SIZE_PAGE_DEFAULT
        } else {
            // ssize encodes page size as: 512 << ssize for values 1-7
            // In practice: ssize=3 => 4K, ssize=4 => 8K, ssize=5 => 16K, etc.
            // MySQL source: page_size = (512 << ssize) for ssize 1-7
            // But there's a special case: if ssize >= 1, page_size = 1 << (ssize + 9)
            // ssize=1 => 1024, ssize=2 => 2048, ssize=3 => 4096, ssize=4 => 8192,
            // ssize=5 => 16384, ssize=6 => 32768, ssize=7 => 65536
            1u32 << (ssize + 9)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_fil_header_bytes(
        checksum: u32,
        page_num: u32,
        prev: u32,
        next: u32,
        lsn: u64,
        page_type: u16,
        flush_lsn: u64,
        space_id: u32,
    ) -> Vec<u8> {
        let mut buf = vec![0u8; SIZE_FIL_HEAD];
        BigEndian::write_u32(&mut buf[FIL_PAGE_SPACE_OR_CHKSUM..], checksum);
        BigEndian::write_u32(&mut buf[FIL_PAGE_OFFSET..], page_num);
        BigEndian::write_u32(&mut buf[FIL_PAGE_PREV..], prev);
        BigEndian::write_u32(&mut buf[FIL_PAGE_NEXT..], next);
        BigEndian::write_u64(&mut buf[FIL_PAGE_LSN..], lsn);
        BigEndian::write_u16(&mut buf[FIL_PAGE_TYPE..], page_type);
        BigEndian::write_u64(&mut buf[FIL_PAGE_FILE_FLUSH_LSN..], flush_lsn);
        BigEndian::write_u32(&mut buf[FIL_PAGE_SPACE_ID..], space_id);
        buf
    }

    #[test]
    fn test_fil_header_parse() {
        let data = make_fil_header_bytes(
            0x12345678, // checksum
            42,         // page number
            41,         // prev page
            43,         // next page
            1000,       // lsn
            17855,      // INDEX page type
            2000,       // flush lsn
            5,          // space id
        );
        let hdr = FilHeader::parse(&data).unwrap();
        assert_eq!(hdr.checksum, 0x12345678);
        assert_eq!(hdr.page_number, 42);
        assert_eq!(hdr.prev_page, 41);
        assert_eq!(hdr.next_page, 43);
        assert_eq!(hdr.lsn, 1000);
        assert_eq!(hdr.page_type, PageType::Index);
        assert_eq!(hdr.flush_lsn, 2000);
        assert_eq!(hdr.space_id, 5);
        assert!(hdr.has_prev());
        assert!(hdr.has_next());
    }

    #[test]
    fn test_fil_header_null_pages() {
        let data = make_fil_header_bytes(0, 0, FIL_NULL, FIL_NULL, 0, 0, 0, 0);
        let hdr = FilHeader::parse(&data).unwrap();
        assert!(!hdr.has_prev());
        assert!(!hdr.has_next());
    }

    #[test]
    fn test_fil_header_too_short() {
        let data = vec![0u8; 10];
        assert!(FilHeader::parse(&data).is_none());
    }

    #[test]
    fn test_fil_trailer_parse() {
        let mut data = vec![0u8; 8];
        BigEndian::write_u32(&mut data[0..], 0xAABBCCDD);
        BigEndian::write_u32(&mut data[4..], 0x11223344);
        let trl = FilTrailer::parse(&data).unwrap();
        assert_eq!(trl.checksum, 0xAABBCCDD);
        assert_eq!(trl.lsn_low32, 0x11223344);
    }

    #[test]
    fn test_fsp_header_page_size() {
        let fsp = FspHeader {
            space_id: 0,
            size: 100,
            free_limit: 64,
            flags: 0, // ssize=0 => default 16K
            frag_n_used: 0,
        };
        assert_eq!(fsp.page_size_from_flags(), SIZE_PAGE_DEFAULT);

        // ssize=5 => 16384
        let fsp_16k = FspHeader {
            flags: 5 << FSP_FLAGS_POS_PAGE_SSIZE,
            ..fsp
        };
        assert_eq!(fsp_16k.page_size_from_flags(), 16384);

        // ssize=3 => 4096
        let fsp_4k = FspHeader {
            flags: 3 << FSP_FLAGS_POS_PAGE_SSIZE,
            ..fsp
        };
        assert_eq!(fsp_4k.page_size_from_flags(), 4096);
    }
}
