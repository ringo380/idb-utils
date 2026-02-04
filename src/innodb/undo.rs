use byteorder::{BigEndian, ByteOrder};
use serde::Serialize;

use crate::innodb::constants::FIL_PAGE_DATA;

/// Undo log page header offsets (relative to FIL_PAGE_DATA).
///
/// From trx0undo.h in MySQL source.
const TRX_UNDO_PAGE_TYPE: usize = 0; // 2 bytes
const TRX_UNDO_PAGE_START: usize = 2; // 2 bytes
const TRX_UNDO_PAGE_FREE: usize = 4; // 2 bytes
#[allow(dead_code)]
const TRX_UNDO_PAGE_NODE: usize = 6; // 12 bytes (FLST_NODE)
const TRX_UNDO_PAGE_HDR_SIZE: usize = 18;

/// Undo segment header offsets (relative to FIL_PAGE_DATA + TRX_UNDO_PAGE_HDR_SIZE).
const TRX_UNDO_STATE: usize = 0; // 2 bytes
const TRX_UNDO_LAST_LOG: usize = 2; // 2 bytes
#[allow(dead_code)]
const TRX_UNDO_FSEG_HEADER: usize = 4; // 10 bytes (FSEG_HEADER)
#[allow(dead_code)]
const TRX_UNDO_PAGE_LIST: usize = 14; // 16 bytes (FLST_BASE_NODE)
const TRX_UNDO_SEG_HDR_SIZE: usize = 30;

/// Undo log header offsets (at the start of the undo log within the page).
const TRX_UNDO_TRX_ID: usize = 0; // 8 bytes
const TRX_UNDO_TRX_NO: usize = 8; // 8 bytes
const TRX_UNDO_DEL_MARKS: usize = 16; // 2 bytes
const TRX_UNDO_LOG_START: usize = 18; // 2 bytes
const TRX_UNDO_XID_EXISTS: usize = 20; // 1 byte
const TRX_UNDO_DICT_TRANS: usize = 21; // 1 byte
const TRX_UNDO_TABLE_ID: usize = 22; // 8 bytes
const TRX_UNDO_NEXT_LOG: usize = 30; // 2 bytes
const TRX_UNDO_PREV_LOG: usize = 32; // 2 bytes

/// Undo page types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum UndoPageType {
    /// Insert undo log (INSERT operations only)
    Insert,
    /// Update undo log (UPDATE and DELETE operations)
    Update,
    /// Unknown type
    Unknown(u16),
}

impl UndoPageType {
    pub fn from_u16(value: u16) -> Self {
        match value {
            1 => UndoPageType::Insert,
            2 => UndoPageType::Update,
            v => UndoPageType::Unknown(v),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            UndoPageType::Insert => "INSERT",
            UndoPageType::Update => "UPDATE",
            UndoPageType::Unknown(_) => "UNKNOWN",
        }
    }
}

/// Undo segment states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum UndoState {
    /// Active transaction is using this segment
    Active,
    /// Cached for reuse
    Cached,
    /// Insert undo segment can be freed
    ToFree,
    /// Update undo segment will not be freed (has delete marks)
    ToPurge,
    /// Prepared transaction undo
    Prepared,
    /// Unknown state
    Unknown(u16),
}

impl UndoState {
    pub fn from_u16(value: u16) -> Self {
        match value {
            1 => UndoState::Active,
            2 => UndoState::Cached,
            3 => UndoState::ToFree,
            4 => UndoState::ToPurge,
            5 => UndoState::Prepared,
            v => UndoState::Unknown(v),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            UndoState::Active => "ACTIVE",
            UndoState::Cached => "CACHED",
            UndoState::ToFree => "TO_FREE",
            UndoState::ToPurge => "TO_PURGE",
            UndoState::Prepared => "PREPARED",
            UndoState::Unknown(_) => "UNKNOWN",
        }
    }
}

/// Parsed undo log page header.
#[derive(Debug, Clone, Serialize)]
pub struct UndoPageHeader {
    /// Type of undo log (INSERT or UPDATE).
    pub page_type: UndoPageType,
    /// Offset of the start of undo log records on this page.
    pub start: u16,
    /// Offset of the first free byte on this page.
    pub free: u16,
}

/// Parsed undo segment header (only on first page of undo segment).
#[derive(Debug, Clone, Serialize)]
pub struct UndoSegmentHeader {
    /// State of the undo segment.
    pub state: UndoState,
    /// Offset of the last undo log header on the segment.
    pub last_log: u16,
}

impl UndoPageHeader {
    /// Parse an undo page header from a full page buffer.
    ///
    /// The undo page header starts at FIL_PAGE_DATA (byte 38).
    pub fn parse(page_data: &[u8]) -> Option<Self> {
        let base = FIL_PAGE_DATA;
        if page_data.len() < base + TRX_UNDO_PAGE_HDR_SIZE {
            return None;
        }

        let d = &page_data[base..];
        Some(UndoPageHeader {
            page_type: UndoPageType::from_u16(BigEndian::read_u16(&d[TRX_UNDO_PAGE_TYPE..])),
            start: BigEndian::read_u16(&d[TRX_UNDO_PAGE_START..]),
            free: BigEndian::read_u16(&d[TRX_UNDO_PAGE_FREE..]),
        })
    }
}

impl UndoSegmentHeader {
    /// Parse an undo segment header from a full page buffer.
    ///
    /// The segment header follows the page header at FIL_PAGE_DATA + TRX_UNDO_PAGE_HDR_SIZE.
    pub fn parse(page_data: &[u8]) -> Option<Self> {
        let base = FIL_PAGE_DATA + TRX_UNDO_PAGE_HDR_SIZE;
        if page_data.len() < base + TRX_UNDO_SEG_HDR_SIZE {
            return None;
        }

        let d = &page_data[base..];
        Some(UndoSegmentHeader {
            state: UndoState::from_u16(BigEndian::read_u16(&d[TRX_UNDO_STATE..])),
            last_log: BigEndian::read_u16(&d[TRX_UNDO_LAST_LOG..]),
        })
    }
}

/// Parsed undo log record header (at the start of an undo log within the page).
#[derive(Debug, Clone, Serialize)]
pub struct UndoLogHeader {
    /// Transaction ID that created this undo log.
    pub trx_id: u64,
    /// Transaction serial number.
    pub trx_no: u64,
    /// Whether delete marks exist in this undo log.
    pub del_marks: bool,
    /// Offset of the first undo log record.
    pub log_start: u16,
    /// Whether XID info exists (distributed transactions).
    pub xid_exists: bool,
    /// Whether this is a DDL transaction.
    pub dict_trans: bool,
    /// Table ID (for insert undo logs).
    pub table_id: u64,
    /// Offset of the next undo log header (0 if last).
    pub next_log: u16,
    /// Offset of the previous undo log header (0 if first).
    pub prev_log: u16,
}

impl UndoLogHeader {
    /// Parse an undo log header from a page at the given offset.
    ///
    /// The `log_offset` is typically obtained from UndoSegmentHeader::last_log
    /// or UndoPageHeader::start.
    pub fn parse(page_data: &[u8], log_offset: usize) -> Option<Self> {
        if page_data.len() < log_offset + 34 {
            return None;
        }

        let d = &page_data[log_offset..];
        Some(UndoLogHeader {
            trx_id: BigEndian::read_u64(&d[TRX_UNDO_TRX_ID..]),
            trx_no: BigEndian::read_u64(&d[TRX_UNDO_TRX_NO..]),
            del_marks: BigEndian::read_u16(&d[TRX_UNDO_DEL_MARKS..]) != 0,
            log_start: BigEndian::read_u16(&d[TRX_UNDO_LOG_START..]),
            xid_exists: d[TRX_UNDO_XID_EXISTS] != 0,
            dict_trans: d[TRX_UNDO_DICT_TRANS] != 0,
            table_id: BigEndian::read_u64(&d[TRX_UNDO_TABLE_ID..]),
            next_log: BigEndian::read_u16(&d[TRX_UNDO_NEXT_LOG..]),
            prev_log: BigEndian::read_u16(&d[TRX_UNDO_PREV_LOG..]),
        })
    }
}

/// Rollback segment array page header (page type FIL_PAGE_RSEG_ARRAY, MySQL 8.0+).
///
/// This page is the first page of an undo tablespace (.ibu) and contains
/// an array of rollback segment page numbers.
#[derive(Debug, Clone, Serialize)]
pub struct RsegArrayHeader {
    /// Number of rollback segment slots.
    pub size: u32,
}

impl RsegArrayHeader {
    /// Parse a rollback segment array header from a full page buffer.
    ///
    /// RSEG array header starts at FIL_PAGE_DATA.
    pub fn parse(page_data: &[u8]) -> Option<Self> {
        let base = FIL_PAGE_DATA;
        if page_data.len() < base + 4 {
            return None;
        }

        Some(RsegArrayHeader {
            size: BigEndian::read_u32(&page_data[base..]),
        })
    }

    /// Read rollback segment page numbers from the array.
    ///
    /// Each slot is a 4-byte page number. Returns up to `max_slots` entries.
    pub fn read_slots(page_data: &[u8], max_slots: usize) -> Vec<u32> {
        let base = FIL_PAGE_DATA + 4; // After the size field
        let mut slots = Vec::new();

        for i in 0..max_slots {
            let offset = base + i * 4;
            if offset + 4 > page_data.len() {
                break;
            }
            let page_no = BigEndian::read_u32(&page_data[offset..]);
            if page_no != 0 && page_no != crate::innodb::constants::FIL_NULL {
                slots.push(page_no);
            }
        }

        slots
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_undo_page_type() {
        assert_eq!(UndoPageType::from_u16(1), UndoPageType::Insert);
        assert_eq!(UndoPageType::from_u16(2), UndoPageType::Update);
        assert_eq!(UndoPageType::from_u16(1).name(), "INSERT");
        assert_eq!(UndoPageType::from_u16(2).name(), "UPDATE");
    }

    #[test]
    fn test_undo_state() {
        assert_eq!(UndoState::from_u16(1), UndoState::Active);
        assert_eq!(UndoState::from_u16(2), UndoState::Cached);
        assert_eq!(UndoState::from_u16(3), UndoState::ToFree);
        assert_eq!(UndoState::from_u16(4), UndoState::ToPurge);
        assert_eq!(UndoState::from_u16(5), UndoState::Prepared);
        assert_eq!(UndoState::from_u16(1).name(), "ACTIVE");
    }

    #[test]
    fn test_undo_page_header_parse() {
        let mut page = vec![0u8; 256];
        let base = FIL_PAGE_DATA;

        // Set page type = INSERT (1)
        BigEndian::write_u16(&mut page[base + TRX_UNDO_PAGE_TYPE..], 1);
        // Set start offset
        BigEndian::write_u16(&mut page[base + TRX_UNDO_PAGE_START..], 100);
        // Set free offset
        BigEndian::write_u16(&mut page[base + TRX_UNDO_PAGE_FREE..], 200);

        let hdr = UndoPageHeader::parse(&page).unwrap();
        assert_eq!(hdr.page_type, UndoPageType::Insert);
        assert_eq!(hdr.start, 100);
        assert_eq!(hdr.free, 200);
    }

    #[test]
    fn test_undo_segment_header_parse() {
        let mut page = vec![0u8; 256];
        let base = FIL_PAGE_DATA + TRX_UNDO_PAGE_HDR_SIZE;

        // Set state = ACTIVE (1)
        BigEndian::write_u16(&mut page[base + TRX_UNDO_STATE..], 1);
        // Set last log offset
        BigEndian::write_u16(&mut page[base + TRX_UNDO_LAST_LOG..], 150);

        let hdr = UndoSegmentHeader::parse(&page).unwrap();
        assert_eq!(hdr.state, UndoState::Active);
        assert_eq!(hdr.last_log, 150);
    }
}
