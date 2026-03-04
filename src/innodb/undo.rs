//! UNDO log page parsing.
//!
//! UNDO log pages (page type 2 / `FIL_PAGE_UNDO_LOG`) store previous versions
//! of modified records for MVCC and rollback. Each undo page has an
//! [`UndoPageHeader`] at `FIL_PAGE_DATA` (byte 38) describing the undo type
//! and free space pointers, followed by an [`UndoSegmentHeader`] with the
//! segment state and transaction metadata.

use byteorder::{BigEndian, ByteOrder};
use serde::Serialize;

use crate::innodb::constants::{FIL_NULL, FIL_PAGE_DATA};
use crate::innodb::page::FilHeader;
use crate::innodb::page_types::PageType;
use crate::innodb::tablespace::Tablespace;
use crate::IdbError;

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
///
/// # Examples
///
/// ```
/// use idb::innodb::undo::UndoPageType;
///
/// let insert = UndoPageType::from_u16(1);
/// assert_eq!(insert, UndoPageType::Insert);
/// assert_eq!(insert.name(), "INSERT");
///
/// let update = UndoPageType::from_u16(2);
/// assert_eq!(update, UndoPageType::Update);
/// assert_eq!(update.name(), "UPDATE");
///
/// let unknown = UndoPageType::from_u16(99);
/// assert_eq!(unknown, UndoPageType::Unknown(99));
/// assert_eq!(unknown.name(), "UNKNOWN");
/// ```
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
    /// Convert a raw u16 value from the undo page header to an `UndoPageType`.
    pub fn from_u16(value: u16) -> Self {
        match value {
            1 => UndoPageType::Insert,
            2 => UndoPageType::Update,
            v => UndoPageType::Unknown(v),
        }
    }

    /// Returns the MySQL source-style name for this undo page type.
    pub fn name(&self) -> &'static str {
        match self {
            UndoPageType::Insert => "INSERT",
            UndoPageType::Update => "UPDATE",
            UndoPageType::Unknown(_) => "UNKNOWN",
        }
    }
}

/// Undo segment states.
///
/// # Examples
///
/// ```
/// use idb::innodb::undo::UndoState;
///
/// assert_eq!(UndoState::from_u16(1), UndoState::Active);
/// assert_eq!(UndoState::from_u16(2), UndoState::Cached);
/// assert_eq!(UndoState::from_u16(3), UndoState::ToFree);
/// assert_eq!(UndoState::from_u16(4), UndoState::ToPurge);
/// assert_eq!(UndoState::from_u16(5), UndoState::Prepared);
///
/// assert_eq!(UndoState::Active.name(), "ACTIVE");
/// assert_eq!(UndoState::ToPurge.name(), "TO_PURGE");
///
/// let unknown = UndoState::from_u16(0);
/// assert_eq!(unknown, UndoState::Unknown(0));
/// assert_eq!(unknown.name(), "UNKNOWN");
/// ```
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
    /// Convert a raw u16 value from the undo segment header to an `UndoState`.
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

    /// Returns the MySQL source-style name for this undo state.
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
    ///
    /// # Examples
    ///
    /// ```
    /// use idb::innodb::undo::{UndoPageHeader, UndoPageType};
    /// use byteorder::{BigEndian, ByteOrder};
    ///
    /// // Build a minimal page buffer (at least 38 + 18 = 56 bytes).
    /// let mut page = vec![0u8; 64];
    /// let base = 38; // FIL_PAGE_DATA
    ///
    /// // Undo page type = UPDATE (2) at offset base+0
    /// BigEndian::write_u16(&mut page[base..], 2);
    /// // Start offset at base+2
    /// BigEndian::write_u16(&mut page[base + 2..], 80);
    /// // Free offset at base+4
    /// BigEndian::write_u16(&mut page[base + 4..], 160);
    ///
    /// let hdr = UndoPageHeader::parse(&page).unwrap();
    /// assert_eq!(hdr.page_type, UndoPageType::Update);
    /// assert_eq!(hdr.start, 80);
    /// assert_eq!(hdr.free, 160);
    /// ```
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
    ///
    /// # Examples
    ///
    /// ```
    /// use idb::innodb::undo::{UndoSegmentHeader, UndoState};
    /// use byteorder::{BigEndian, ByteOrder};
    ///
    /// // Need at least 38 (FIL header) + 18 (page header) + 30 (seg header) = 86 bytes.
    /// let mut page = vec![0u8; 96];
    /// let base = 38 + 18; // FIL_PAGE_DATA + TRX_UNDO_PAGE_HDR_SIZE
    ///
    /// // State = CACHED (2) at base+0
    /// BigEndian::write_u16(&mut page[base..], 2);
    /// // Last log offset at base+2
    /// BigEndian::write_u16(&mut page[base + 2..], 200);
    ///
    /// let hdr = UndoSegmentHeader::parse(&page).unwrap();
    /// assert_eq!(hdr.state, UndoState::Cached);
    /// assert_eq!(hdr.last_log, 200);
    /// ```
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
    ///
    /// # Examples
    ///
    /// ```
    /// use idb::innodb::undo::UndoLogHeader;
    /// use byteorder::{BigEndian, ByteOrder};
    ///
    /// // The undo log header is 34 bytes starting at log_offset.
    /// let log_offset = 100;
    /// let mut page = vec![0u8; log_offset + 34];
    ///
    /// // trx_id (8 bytes) at offset 0
    /// BigEndian::write_u64(&mut page[log_offset..], 1001);
    /// // trx_no (8 bytes) at offset 8
    /// BigEndian::write_u64(&mut page[log_offset + 8..], 500);
    /// // del_marks (2 bytes) at offset 16
    /// BigEndian::write_u16(&mut page[log_offset + 16..], 1);
    /// // log_start (2 bytes) at offset 18
    /// BigEndian::write_u16(&mut page[log_offset + 18..], 120);
    /// // xid_exists (1 byte) at offset 20
    /// page[log_offset + 20] = 1;
    /// // dict_trans (1 byte) at offset 21
    /// page[log_offset + 21] = 0;
    /// // table_id (8 bytes) at offset 22
    /// BigEndian::write_u64(&mut page[log_offset + 22..], 42);
    /// // next_log (2 bytes) at offset 30
    /// BigEndian::write_u16(&mut page[log_offset + 30..], 0);
    /// // prev_log (2 bytes) at offset 32
    /// BigEndian::write_u16(&mut page[log_offset + 32..], 0);
    ///
    /// let hdr = UndoLogHeader::parse(&page, log_offset).unwrap();
    /// assert_eq!(hdr.trx_id, 1001);
    /// assert_eq!(hdr.trx_no, 500);
    /// assert!(hdr.del_marks);
    /// assert_eq!(hdr.log_start, 120);
    /// assert!(hdr.xid_exists);
    /// assert!(!hdr.dict_trans);
    /// assert_eq!(hdr.table_id, 42);
    /// assert_eq!(hdr.next_log, 0);
    /// assert_eq!(hdr.prev_log, 0);
    /// ```
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

// ---------------------------------------------------------------------------
// Rollback segment header (RSEG header page pointed to by RSEG array slots)
// ---------------------------------------------------------------------------

/// Offsets within the rollback segment header page (at FIL_PAGE_DATA).
const TRX_RSEG_MAX_SIZE: usize = 0; // 4 bytes
const TRX_RSEG_HISTORY_SIZE: usize = 4; // 4 bytes
#[allow(dead_code)]
const TRX_RSEG_HISTORY: usize = 8; // 16 bytes (FLST_BASE_NODE)
const TRX_RSEG_SLOTS_OFFSET: usize = 24; // 1024 * 4 bytes of page numbers

/// Maximum number of undo segment slots per rollback segment.
const TRX_RSEG_N_SLOTS: usize = 1024;

/// Parsed rollback segment header (the page pointed to by RSEG array slots).
///
/// Contains the maximum size, history list length, and an array of up to 1024
/// undo segment page number slots.
#[derive(Debug, Clone, Serialize)]
pub struct RollbackSegmentHeader {
    /// Maximum number of undo pages this RSEG can use.
    pub max_size: u32,
    /// Number of committed transactions in the history list.
    pub history_size: u32,
    /// Undo segment page numbers (FIL_NULL = empty slot).
    pub slots: Vec<u32>,
}

impl RollbackSegmentHeader {
    /// Parse a rollback segment header from a full page buffer.
    ///
    /// The RSEG header starts at FIL_PAGE_DATA on the page pointed to by an
    /// RSEG array slot.
    pub fn parse(page_data: &[u8]) -> Option<Self> {
        let base = FIL_PAGE_DATA;
        let min_size = base + TRX_RSEG_SLOTS_OFFSET + TRX_RSEG_N_SLOTS * 4;
        if page_data.len() < min_size {
            return None;
        }

        let d = &page_data[base..];
        let max_size = BigEndian::read_u32(&d[TRX_RSEG_MAX_SIZE..]);
        let history_size = BigEndian::read_u32(&d[TRX_RSEG_HISTORY_SIZE..]);

        let mut slots = Vec::new();
        for i in 0..TRX_RSEG_N_SLOTS {
            let offset = TRX_RSEG_SLOTS_OFFSET + i * 4;
            let page_no = BigEndian::read_u32(&d[offset..]);
            slots.push(page_no);
        }

        Some(RollbackSegmentHeader {
            max_size,
            history_size,
            slots,
        })
    }

    /// Return only the active (non-FIL_NULL, non-zero) slot page numbers.
    pub fn active_slots(&self) -> Vec<u32> {
        self.slots
            .iter()
            .copied()
            .filter(|&s| s != FIL_NULL && s != 0)
            .collect()
    }
}

// ---------------------------------------------------------------------------
// InnoDB compressed integer reader (mach0data.h encoding)
// ---------------------------------------------------------------------------

/// Read an InnoDB compressed integer from `data` at the given `offset`.
///
/// Returns `(value, bytes_consumed)` or `None` if insufficient data.
///
/// Encoding rules (from `mach0data.h`):
/// - `byte < 0x80`: 1 byte, value = byte
/// - `byte < 0xC0`: 2 bytes, value = `(byte-0x80)<<8 | next`
/// - `byte < 0xE0`: 3 bytes, value = `(byte-0xC0)<<16 | next_2`
/// - `byte < 0xF0`: 4 bytes, value = `(byte-0xE0)<<24 | next_3`
/// - `byte == 0xF0`: 5 bytes, value = `next_4_bytes`
///
/// # Examples
///
/// ```
/// use idb::innodb::undo::read_compressed;
///
/// // 1-byte: value 0x7F = 127
/// assert_eq!(read_compressed(&[0x7F], 0), Some((127, 1)));
///
/// // 2-byte: (0x80 - 0x80) << 8 | 0x01 = 1
/// assert_eq!(read_compressed(&[0x80, 0x01], 0), Some((1, 2)));
///
/// // Insufficient data
/// assert_eq!(read_compressed(&[0x80], 0), None);
/// ```
pub fn read_compressed(data: &[u8], offset: usize) -> Option<(u64, usize)> {
    if offset >= data.len() {
        return None;
    }
    let b = data[offset];
    if b < 0x80 {
        Some((b as u64, 1))
    } else if b < 0xC0 {
        if offset + 2 > data.len() {
            return None;
        }
        let val = ((b as u64 - 0x80) << 8) | data[offset + 1] as u64;
        Some((val, 2))
    } else if b < 0xE0 {
        if offset + 3 > data.len() {
            return None;
        }
        let val =
            ((b as u64 - 0xC0) << 16) | (data[offset + 1] as u64) << 8 | data[offset + 2] as u64;
        Some((val, 3))
    } else if b < 0xF0 {
        if offset + 4 > data.len() {
            return None;
        }
        let val = ((b as u64 - 0xE0) << 24)
            | (data[offset + 1] as u64) << 16
            | (data[offset + 2] as u64) << 8
            | data[offset + 3] as u64;
        Some((val, 4))
    } else if b == 0xF0 {
        if offset + 5 > data.len() {
            return None;
        }
        let val = BigEndian::read_u32(&data[offset + 1..]) as u64;
        Some((val, 5))
    } else {
        // Invalid leading byte (0xF1..0xFF not defined)
        None
    }
}

// ---------------------------------------------------------------------------
// Undo log header chain traversal
// ---------------------------------------------------------------------------

/// Walk the chain of undo log headers within a single page.
///
/// Starts from `start_offset` (typically `UndoSegmentHeader::last_log`) and
/// follows `prev_log` pointers backwards. Returns headers in reverse
/// chronological order (newest first).
pub fn walk_undo_log_headers(page_data: &[u8], start_offset: u16) -> Vec<UndoLogHeader> {
    let mut headers = Vec::new();
    let mut offset = start_offset as usize;
    let max_iterations = 1000; // safety limit

    for _ in 0..max_iterations {
        if offset == 0 || offset >= page_data.len() {
            break;
        }

        match UndoLogHeader::parse(page_data, offset) {
            Some(hdr) => {
                let prev = hdr.prev_log;
                headers.push(hdr);
                if prev == 0 {
                    break;
                }
                offset = prev as usize;
            }
            None => break,
        }
    }

    headers
}

// ---------------------------------------------------------------------------
// Undo record type classification and chain traversal
// ---------------------------------------------------------------------------

/// Undo record operation types from trx0undo.h.
///
/// # Examples
///
/// ```
/// use idb::innodb::undo::UndoRecordType;
///
/// assert_eq!(UndoRecordType::from_type_byte(11), UndoRecordType::InsertRec);
/// assert_eq!(UndoRecordType::from_type_byte(14), UndoRecordType::DelMarkRec);
/// assert_eq!(UndoRecordType::from_u8(11), UndoRecordType::InsertRec);
/// assert_eq!(UndoRecordType::InsertRec.name(), "INSERT");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum UndoRecordType {
    /// TRX_UNDO_INSERT_REC (11) — fresh insert record
    InsertRec,
    /// TRX_UNDO_UPD_EXIST_REC (12) — update of a non-delete-marked record
    UpdExistRec,
    /// TRX_UNDO_UPD_DEL_REC (13) — update of a previously delete-marked record (undelete)
    UpdDelRec,
    /// TRX_UNDO_DEL_MARK_REC (14) — delete marking of a record
    DelMarkRec,
    /// Unrecognized type code
    Unknown(u8),
}

impl UndoRecordType {
    /// Classify an undo record from the type/compilation info byte.
    ///
    /// The type code is stored in the lower 4 bits of the first byte of
    /// each undo record (the upper bits contain compilation info flags).
    pub fn from_type_byte(byte: u8) -> Self {
        match byte & 0x0F {
            11 => UndoRecordType::InsertRec,
            12 => UndoRecordType::UpdExistRec,
            13 => UndoRecordType::UpdDelRec,
            14 => UndoRecordType::DelMarkRec,
            other => UndoRecordType::Unknown(other),
        }
    }

    /// Convert a raw type code to an `UndoRecordType`.
    ///
    /// Unlike `from_type_byte`, this does NOT mask the upper bits.
    /// Caller is responsible for extracting the lower 4 bits if needed.
    pub fn from_u8(val: u8) -> Self {
        match val {
            11 => UndoRecordType::InsertRec,
            12 => UndoRecordType::UpdExistRec,
            13 => UndoRecordType::UpdDelRec,
            14 => UndoRecordType::DelMarkRec,
            v => UndoRecordType::Unknown(v),
        }
    }

    /// Returns the MySQL source-style name for this record type.
    pub fn name(&self) -> &'static str {
        match self {
            UndoRecordType::InsertRec => "INSERT",
            UndoRecordType::UpdExistRec => "UPD_EXIST",
            UndoRecordType::UpdDelRec => "UPD_DEL",
            UndoRecordType::DelMarkRec => "DEL_MARK",
            UndoRecordType::Unknown(_) => "UNKNOWN",
        }
    }
}

impl std::fmt::Display for UndoRecordType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// A single field update from an undo record's update vector.
#[derive(Debug, Clone, Serialize)]
pub struct UndoUpdateField {
    /// Field number within the row (0-based).
    pub field_no: u64,
    /// Raw field data bytes.
    pub data: Vec<u8>,
}

/// A parsed undo record within an undo log page.
#[derive(Debug, Clone, Serialize)]
pub struct UndoRecord {
    /// Byte offset of this record within the page.
    pub offset: usize,
    /// Operation type.
    pub record_type: UndoRecordType,
    /// Info bits (compilation info flags from upper bits of type byte).
    pub info_bits: u8,
    /// Offset of the next undo record (2-byte pointer), 0 if last.
    pub next_offset: u16,
    /// Approximate data length of this record (bytes until next record or free pointer).
    pub data_len: usize,
}

/// Walk the chain of undo records within a single page.
///
/// Starts at `start_offset` (typically `UndoPageHeader::start` or
/// `UndoLogHeader::log_start`) and follows 2-byte "next record" pointers.
/// Terminates at offset 0 or when reaching `free_offset` (from `UndoPageHeader::free`).
///
/// Returns records in forward order (oldest first).
pub fn walk_undo_records(
    page_data: &[u8],
    start_offset: u16,
    free_offset: u16,
    max_records: usize,
) -> Vec<UndoRecord> {
    let mut records = Vec::new();
    let mut offset = start_offset as usize;

    for _ in 0..max_records {
        if offset == 0 || offset >= page_data.len() || offset as u16 >= free_offset {
            break;
        }

        // Each undo record starts with a 2-byte "next record" pointer
        // followed by a type/compilation_info byte
        if offset + 3 > page_data.len() {
            break;
        }

        let next_offset = BigEndian::read_u16(&page_data[offset..]);
        let type_byte = page_data[offset + 2];
        let record_type = UndoRecordType::from_type_byte(type_byte);
        let info_bits = type_byte >> 4;

        // Estimate data length: from current offset to next record or free pointer
        let end = if next_offset > 0 && (next_offset as usize) < page_data.len() {
            next_offset as usize
        } else {
            free_offset as usize
        };
        let data_len = if end > offset + 3 {
            end - offset - 3
        } else {
            0
        };

        records.push(UndoRecord {
            offset,
            record_type,
            info_bits,
            next_offset,
            data_len,
        });

        if next_offset == 0 {
            break;
        }
        offset = next_offset as usize;
    }

    records
}

// ---------------------------------------------------------------------------
// Detailed undo record parsing (with compressed field decoding)
// ---------------------------------------------------------------------------

/// A fully-parsed undo record with decoded fields.
///
/// Unlike [`UndoRecord`] which only captures offsets and type info, this struct
/// decodes compressed integers (undo_no, table_id), primary key fields,
/// transaction IDs, roll pointers, and update vector fields.
#[derive(Debug, Clone, Serialize)]
pub struct DetailedUndoRecord {
    /// Byte offset of this record within the page.
    pub offset: usize,
    /// Undo operation type.
    pub record_type: UndoRecordType,
    /// Undo record sequence number within the transaction.
    pub undo_no: u64,
    /// Table ID this record belongs to.
    pub table_id: u64,
    /// Raw primary key field bytes.
    pub pk_fields: Vec<Vec<u8>>,
    /// Transaction ID (for DEL_MARK and UPD_EXIST records).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trx_id: Option<u64>,
    /// Roll pointer (7 bytes, for DEL_MARK and UPD_EXIST records).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub roll_ptr: Option<[u8; 7]>,
    /// Update vector fields (for UPD_EXIST/UPD_DEL/DEL_MARK).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub update_fields: Vec<UndoUpdateField>,
}

/// Parse undo records on a single UNDO_LOG page with full field decoding.
///
/// Iterates from `UndoPageHeader::start` to `free`, parsing each record
/// including compressed undo_no, table_id, PK fields, trx_id, roll_ptr,
/// and update vector. Returns all successfully parsed records; stops on
/// parse failure or when reaching the free pointer.
///
/// # Format (from `trx0rec.cc`)
///
/// Each undo record starts with:
/// 1. 2 bytes: next record offset
/// 2. 1 byte: `type_cmpl` (lower 4 bits = type code)
/// 3. Compressed: `undo_no`
/// 4. Compressed: `table_id`
/// 5. For each PK column: compressed length + raw bytes
///    (we read 1 PK field; multi-column PKs need schema info)
/// 6. For UPD_EXIST/DEL_MARK: 6-byte compressed `trx_id` + 7-byte `roll_ptr`
/// 7. Update vector: compressed field count, then per-field
///    (compressed field_no, compressed len, raw data)
pub fn parse_undo_records(page_data: &[u8]) -> Vec<DetailedUndoRecord> {
    let mut records = Vec::new();

    let page_hdr = match UndoPageHeader::parse(page_data) {
        Some(h) => h,
        None => return records,
    };

    let start = page_hdr.start as usize;
    let free = page_hdr.free as usize;

    if start == 0 || start >= page_data.len() || free == 0 || start >= free {
        return records;
    }

    let mut pos = start;
    let mut visited = std::collections::HashSet::new();

    while pos >= start && pos < free && pos + 3 <= page_data.len() {
        if !visited.insert(pos) {
            break; // cycle detection
        }

        let rec_offset = pos;

        // 2 bytes: next record offset
        if pos + 2 > page_data.len() {
            break;
        }
        let next = BigEndian::read_u16(&page_data[pos..]) as usize;
        pos += 2;

        // 1 byte: type_cmpl
        if pos >= page_data.len() {
            break;
        }
        let type_cmpl = page_data[pos];
        let rec_type = UndoRecordType::from_u8(type_cmpl & 0x0F);
        pos += 1;

        // Compressed: undo_no
        let (undo_no, consumed) = match read_compressed(page_data, pos) {
            Some(v) => v,
            None => break,
        };
        pos += consumed;

        // Compressed: table_id
        let (table_id, consumed) = match read_compressed(page_data, pos) {
            Some(v) => v,
            None => break,
        };
        pos += consumed;

        // Read one PK field: compressed length + raw bytes
        let mut pk_fields = Vec::new();
        if let Some((pk_len, consumed)) = read_compressed(page_data, pos) {
            pos += consumed;
            let pk_len = pk_len as usize;
            if pk_len > 0 && pos + pk_len <= page_data.len() && pk_len < 8192 {
                pk_fields.push(page_data[pos..pos + pk_len].to_vec());
                pos += pk_len;
            }
        }

        let mut trx_id = None;
        let mut roll_ptr = None;
        let mut update_fields = Vec::new();

        // For UPD_EXIST/UPD_DEL/DEL_MARK: read trx_id + roll_ptr + update vector
        if matches!(
            rec_type,
            UndoRecordType::UpdExistRec | UndoRecordType::UpdDelRec | UndoRecordType::DelMarkRec
        ) {
            // Compressed trx_id (stored as 6-byte big-endian in undo, but encoded compressed)
            if let Some((tid, consumed)) = read_compressed(page_data, pos) {
                trx_id = Some(tid);
                pos += consumed;
            }

            // 7-byte roll_ptr
            if pos + 7 <= page_data.len() {
                let mut rp = [0u8; 7];
                rp.copy_from_slice(&page_data[pos..pos + 7]);
                roll_ptr = Some(rp);
                pos += 7;
            }

            // Update vector: compressed field count
            if let Some((n_fields, consumed)) = read_compressed(page_data, pos) {
                pos += consumed;
                for _ in 0..n_fields.min(256) {
                    // compressed field_no
                    let (field_no, c1) = match read_compressed(page_data, pos) {
                        Some(v) => v,
                        None => break,
                    };
                    pos += c1;

                    // compressed length
                    let (flen, c2) = match read_compressed(page_data, pos) {
                        Some(v) => v,
                        None => break,
                    };
                    pos += c2;

                    let flen = flen as usize;
                    if flen > 0 && pos + flen <= page_data.len() && flen < 65536 {
                        update_fields.push(UndoUpdateField {
                            field_no,
                            data: page_data[pos..pos + flen].to_vec(),
                        });
                        pos += flen;
                    } else if flen == 0 {
                        update_fields.push(UndoUpdateField {
                            field_no,
                            data: Vec::new(),
                        });
                    } else {
                        break;
                    }
                }
            }
        }

        records.push(DetailedUndoRecord {
            offset: rec_offset,
            record_type: rec_type,
            undo_no,
            table_id,
            pk_fields,
            trx_id,
            roll_ptr,
            update_fields,
        });

        // Advance to next record
        if next == 0 || next >= free || next <= rec_offset {
            break;
        }
        pos = next;
    }

    records
}

// ---------------------------------------------------------------------------
// Undo segment and tablespace analysis
// ---------------------------------------------------------------------------

/// Aggregated information about a single undo segment within a tablespace.
#[derive(Debug, Clone, Serialize)]
pub struct UndoSegmentInfo {
    /// Page number of the undo segment header page.
    pub page_no: u64,
    /// Parsed undo page header.
    pub page_header: UndoPageHeader,
    /// Parsed undo segment header.
    pub segment_header: UndoSegmentHeader,
    /// Undo log headers found by walking the chain.
    pub log_headers: Vec<UndoLogHeader>,
    /// Number of undo records found on this page.
    pub record_count: usize,
}

/// Top-level analysis result for an undo tablespace.
#[derive(Debug, Clone, Serialize)]
pub struct UndoAnalysis {
    /// RSEG array slot page numbers (from page 0 of MySQL 8.0+ undo tablespace).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub rseg_slots: Vec<u32>,
    /// Per-rollback-segment details.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub rseg_headers: Vec<RsegInfo>,
    /// Per-undo-segment details (from RSEG slot traversal or direct scan).
    pub segments: Vec<UndoSegmentInfo>,
    /// Total undo log headers found.
    pub total_transactions: usize,
    /// Count of segments in ACTIVE state.
    pub active_transactions: usize,
}

/// Rollback segment summary within an undo tablespace.
#[derive(Debug, Clone, Serialize)]
pub struct RsegInfo {
    /// Page number of the RSEG header page.
    pub page_no: u32,
    /// Maximum undo pages this RSEG can use.
    pub max_size: u32,
    /// History list length.
    pub history_size: u32,
    /// Number of active (non-empty) undo segment slots.
    pub active_slot_count: usize,
}

/// Analyze an undo tablespace (MySQL 8.0+ `.ibu` file).
///
/// Reads the RSEG array from page 0 (if it's an RSEG_ARRAY page), then
/// follows the RSEG slots to find rollback segment header pages, and finally
/// reads undo segment pages to collect log headers.
///
/// For non-RSEG-array tablespaces, falls back to scanning all pages for
/// undo log pages (FIL_PAGE_UNDO_LOG).
pub fn analyze_undo_tablespace(ts: &mut Tablespace) -> Result<UndoAnalysis, IdbError> {
    let page0 = ts.read_page(0)?;
    let page0_type = FilHeader::parse(&page0).map(|h| h.page_type);

    if page0_type == Some(PageType::RsegArray) {
        analyze_via_rseg_array(ts)
    } else {
        analyze_via_scan(ts)
    }
}

/// Analyze using RSEG array structure (MySQL 8.0+ undo tablespaces).
fn analyze_via_rseg_array(ts: &mut Tablespace) -> Result<UndoAnalysis, IdbError> {
    let page0 = ts.read_page(0)?;
    let rseg_array = RsegArrayHeader::parse(&page0);
    let rseg_slots = rseg_array
        .map(|a| {
            let max = a.size.min(128) as usize;
            RsegArrayHeader::read_slots(&page0, max)
        })
        .unwrap_or_default();

    let mut rseg_headers = Vec::new();
    let mut segments = Vec::new();

    for &rseg_page_no in &rseg_slots {
        let rseg_page = match ts.read_page(rseg_page_no as u64) {
            Ok(p) => p,
            Err(_) => continue,
        };

        if let Some(rseg_hdr) = RollbackSegmentHeader::parse(&rseg_page) {
            let active_slots = rseg_hdr.active_slots();
            rseg_headers.push(RsegInfo {
                page_no: rseg_page_no,
                max_size: rseg_hdr.max_size,
                history_size: rseg_hdr.history_size,
                active_slot_count: active_slots.len(),
            });

            for &undo_page_no in &active_slots {
                if let Ok(info) = read_undo_segment(ts, undo_page_no as u64) {
                    segments.push(info);
                }
            }
        }
    }

    let total_transactions: usize = segments.iter().map(|s| s.log_headers.len()).sum();
    let active_transactions = segments
        .iter()
        .filter(|s| s.segment_header.state == UndoState::Active)
        .count();

    Ok(UndoAnalysis {
        rseg_slots,
        rseg_headers,
        segments,
        total_transactions,
        active_transactions,
    })
}

/// Fallback: scan all pages for undo log pages.
fn analyze_via_scan(ts: &mut Tablespace) -> Result<UndoAnalysis, IdbError> {
    let page_count = ts.page_count();
    let mut segments = Vec::new();

    for page_num in 0..page_count {
        let page_data = match ts.read_page(page_num) {
            Ok(d) => d,
            Err(_) => continue,
        };

        let fil_hdr = match FilHeader::parse(&page_data) {
            Some(h) => h,
            None => continue,
        };

        if fil_hdr.page_type != PageType::UndoLog {
            continue;
        }

        // Only process segment header pages (those with a valid segment header)
        let page_hdr = match UndoPageHeader::parse(&page_data) {
            Some(h) => h,
            None => continue,
        };

        let seg_hdr = match UndoSegmentHeader::parse(&page_data) {
            Some(h) => h,
            None => continue,
        };

        // Only walk log headers if this appears to be a segment's first page
        // (indicated by having a non-zero last_log offset)
        if seg_hdr.last_log == 0 {
            continue;
        }

        let log_headers = walk_undo_log_headers(&page_data, seg_hdr.last_log);
        let record_count =
            walk_undo_records(&page_data, page_hdr.start, page_hdr.free, 10000).len();

        segments.push(UndoSegmentInfo {
            page_no: page_num,
            page_header: page_hdr,
            segment_header: seg_hdr,
            log_headers,
            record_count,
        });
    }

    let total_transactions: usize = segments.iter().map(|s| s.log_headers.len()).sum();
    let active_transactions = segments
        .iter()
        .filter(|s| s.segment_header.state == UndoState::Active)
        .count();

    Ok(UndoAnalysis {
        rseg_slots: Vec::new(),
        rseg_headers: Vec::new(),
        segments,
        total_transactions,
        active_transactions,
    })
}

/// Read a single undo segment page and extract its headers.
fn read_undo_segment(ts: &mut Tablespace, page_no: u64) -> Result<UndoSegmentInfo, IdbError> {
    let page_data = ts.read_page(page_no)?;

    let page_header = UndoPageHeader::parse(&page_data)
        .ok_or_else(|| IdbError::Parse("Cannot parse undo page header".to_string()))?;

    let segment_header = UndoSegmentHeader::parse(&page_data)
        .ok_or_else(|| IdbError::Parse("Cannot parse undo segment header".to_string()))?;

    let log_headers = walk_undo_log_headers(&page_data, segment_header.last_log);

    let record_count =
        walk_undo_records(&page_data, page_header.start, page_header.free, 10000).len();

    Ok(UndoSegmentInfo {
        page_no,
        page_header,
        segment_header,
        log_headers,
        record_count,
    })
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
    fn test_walk_undo_log_headers_single() {
        // Build a page with one undo log header at offset 86 (after page hdr + seg hdr)
        let mut page = vec![0u8; 256];
        let seg_base = FIL_PAGE_DATA + TRX_UNDO_PAGE_HDR_SIZE;
        // last_log = 86 (= FIL_PAGE_DATA + TRX_UNDO_PAGE_HDR_SIZE + TRX_UNDO_SEG_HDR_SIZE)
        let log_offset = seg_base + TRX_UNDO_SEG_HDR_SIZE;
        BigEndian::write_u16(&mut page[seg_base + TRX_UNDO_LAST_LOG..], log_offset as u16);

        // Write undo log header at log_offset
        BigEndian::write_u64(&mut page[log_offset..], 1001); // trx_id
        BigEndian::write_u64(&mut page[log_offset + 8..], 500); // trx_no
        BigEndian::write_u16(&mut page[log_offset + 16..], 1); // del_marks
        BigEndian::write_u16(&mut page[log_offset + 18..], 120); // log_start
        BigEndian::write_u16(&mut page[log_offset + 30..], 0); // next_log
        BigEndian::write_u16(&mut page[log_offset + 32..], 0); // prev_log

        let headers = walk_undo_log_headers(&page, log_offset as u16);
        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0].trx_id, 1001);
        assert_eq!(headers[0].trx_no, 500);
        assert!(headers[0].del_marks);
    }

    #[test]
    fn test_walk_undo_log_headers_chain() {
        // Two undo log headers chained via prev_log
        let mut page = vec![0u8; 512];
        let offset1 = 100usize;
        let offset2 = 200usize;

        // Header at offset2 (newer, start of chain)
        BigEndian::write_u64(&mut page[offset2..], 2002); // trx_id
        BigEndian::write_u64(&mut page[offset2 + 8..], 600);
        BigEndian::write_u16(&mut page[offset2 + 30..], 0); // next_log
        BigEndian::write_u16(&mut page[offset2 + 32..], offset1 as u16); // prev_log → offset1

        // Header at offset1 (older)
        BigEndian::write_u64(&mut page[offset1..], 1001); // trx_id
        BigEndian::write_u64(&mut page[offset1 + 8..], 500);
        BigEndian::write_u16(&mut page[offset1 + 30..], offset2 as u16); // next_log → offset2
        BigEndian::write_u16(&mut page[offset1 + 32..], 0); // prev_log (end)

        let headers = walk_undo_log_headers(&page, offset2 as u16);
        assert_eq!(headers.len(), 2);
        assert_eq!(headers[0].trx_id, 2002); // newest first
        assert_eq!(headers[1].trx_id, 1001);
    }

    #[test]
    fn test_rollback_segment_header_parse() {
        let page_size = 16384;
        let mut page = vec![0u8; page_size];
        let base = FIL_PAGE_DATA;

        BigEndian::write_u32(&mut page[base + TRX_RSEG_MAX_SIZE..], 1000);
        BigEndian::write_u32(&mut page[base + TRX_RSEG_HISTORY_SIZE..], 42);

        // Write slot 0 with a page number, slot 1 with FIL_NULL
        BigEndian::write_u32(&mut page[base + TRX_RSEG_SLOTS_OFFSET..], 5);
        BigEndian::write_u32(&mut page[base + TRX_RSEG_SLOTS_OFFSET + 4..], FIL_NULL);

        let hdr = RollbackSegmentHeader::parse(&page).unwrap();
        assert_eq!(hdr.max_size, 1000);
        assert_eq!(hdr.history_size, 42);
        let active = hdr.active_slots();
        assert_eq!(active, vec![5]);
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

    #[test]
    fn test_undo_record_type_classification() {
        assert_eq!(
            UndoRecordType::from_type_byte(11),
            UndoRecordType::InsertRec
        );
        assert_eq!(
            UndoRecordType::from_type_byte(12),
            UndoRecordType::UpdExistRec
        );
        assert_eq!(
            UndoRecordType::from_type_byte(13),
            UndoRecordType::UpdDelRec
        );
        assert_eq!(
            UndoRecordType::from_type_byte(14),
            UndoRecordType::DelMarkRec
        );
        assert_eq!(
            UndoRecordType::from_type_byte(0),
            UndoRecordType::Unknown(0)
        );
    }

    // -----------------------------------------------------------------------
    // read_compressed tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_read_compressed_1byte() {
        // Values 0..0x7F are encoded as a single byte
        assert_eq!(read_compressed(&[0x00], 0), Some((0, 1)));
        assert_eq!(read_compressed(&[0x7F], 0), Some((127, 1)));
        assert_eq!(read_compressed(&[0x42], 0), Some((0x42, 1)));
    }

    #[test]
    fn test_read_compressed_2byte() {
        // 0x80..0xBF: (b-0x80)<<8 | next
        assert_eq!(read_compressed(&[0x80, 0x01], 0), Some((1, 2)));
        assert_eq!(read_compressed(&[0xBF, 0xFF], 0), Some((0x3FFF, 2)));
    }

    #[test]
    fn test_read_compressed_3byte() {
        // 0xC0..0xDF: (b-0xC0)<<16 | next_2
        assert_eq!(read_compressed(&[0xC0, 0x00, 0x01], 0), Some((1, 3)));
        assert_eq!(read_compressed(&[0xDF, 0xFF, 0xFF], 0), Some((0x1FFFFF, 3)));
    }

    #[test]
    fn test_read_compressed_4byte() {
        // 0xE0..0xEF: (b-0xE0)<<24 | next_3
        assert_eq!(read_compressed(&[0xE0, 0x00, 0x00, 0x01], 0), Some((1, 4)));
        assert_eq!(
            read_compressed(&[0xEF, 0xFF, 0xFF, 0xFF], 0),
            Some((0x0FFFFFFF, 4))
        );
    }

    #[test]
    fn test_undo_record_type_masks_upper_bits() {
        // Upper 4 bits are compilation info flags — should be masked off
        assert_eq!(
            UndoRecordType::from_type_byte(0xFB), // 0xF0 | 11
            UndoRecordType::InsertRec
        );
        assert_eq!(
            UndoRecordType::from_type_byte(0x2E), // 0x20 | 14
            UndoRecordType::DelMarkRec
        );
    }

    #[test]
    fn test_read_compressed_5byte() {
        // 0xF0: next 4 bytes as u32
        assert_eq!(
            read_compressed(&[0xF0, 0x00, 0x00, 0x00, 0x42], 0),
            Some((0x42, 5))
        );
        assert_eq!(
            read_compressed(&[0xF0, 0xFF, 0xFF, 0xFF, 0xFF], 0),
            Some((0xFFFFFFFF, 5))
        );
    }

    #[test]
    fn test_undo_record_type_names() {
        assert_eq!(UndoRecordType::InsertRec.name(), "INSERT");
        assert_eq!(UndoRecordType::UpdExistRec.name(), "UPD_EXIST");
        assert_eq!(UndoRecordType::UpdDelRec.name(), "UPD_DEL");
        assert_eq!(UndoRecordType::DelMarkRec.name(), "DEL_MARK");
        assert_eq!(UndoRecordType::Unknown(0).name(), "UNKNOWN");
    }

    #[test]
    fn test_walk_undo_records_single() {
        // Build a page with one undo record at offset 100
        let mut page = vec![0u8; 256];
        let offset = 100usize;

        // next_offset = 0 (last record)
        BigEndian::write_u16(&mut page[offset..], 0);
        // type byte = 11 (INSERT)
        page[offset + 2] = 11;

        let records = walk_undo_records(&page, offset as u16, 200, 100);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].offset, 100);
        assert_eq!(records[0].record_type, UndoRecordType::InsertRec);
        assert_eq!(records[0].next_offset, 0);
    }

    #[test]
    fn test_walk_undo_records_chain() {
        // Build a page with 3 chained undo records
        let mut page = vec![0u8; 512];
        let o1 = 100usize;
        let o2 = 150usize;
        let o3 = 200usize;

        // Record 1: next -> o2, type INSERT
        BigEndian::write_u16(&mut page[o1..], o2 as u16);
        page[o1 + 2] = 11;

        // Record 2: next -> o3, type UPD_EXIST
        BigEndian::write_u16(&mut page[o2..], o3 as u16);
        page[o2 + 2] = 12;

        // Record 3: next -> 0 (end), type DEL_MARK
        BigEndian::write_u16(&mut page[o3..], 0);
        page[o3 + 2] = 14;

        let records = walk_undo_records(&page, o1 as u16, 300, 100);
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].record_type, UndoRecordType::InsertRec);
        assert_eq!(records[1].record_type, UndoRecordType::UpdExistRec);
        assert_eq!(records[2].record_type, UndoRecordType::DelMarkRec);
        assert_eq!(records[0].data_len, 47); // 150 - 100 - 3
        assert_eq!(records[1].data_len, 47); // 200 - 150 - 3
    }

    #[test]
    fn test_walk_undo_records_respects_free_offset() {
        // Record chain continues past free_offset -- should stop
        let mut page = vec![0u8; 512];
        let o1 = 100usize;
        let o2 = 200usize;

        BigEndian::write_u16(&mut page[o1..], o2 as u16);
        page[o1 + 2] = 11;
        BigEndian::write_u16(&mut page[o2..], 0);
        page[o2 + 2] = 12;

        // free_offset = 150, so o2 (200) is past free -- should get only 1 record
        let records = walk_undo_records(&page, o1 as u16, 150, 100);
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn test_walk_undo_records_respects_max() {
        // Chain of 3 but max_records = 2
        let mut page = vec![0u8; 512];
        let o1 = 100usize;
        let o2 = 150usize;
        let o3 = 200usize;

        BigEndian::write_u16(&mut page[o1..], o2 as u16);
        page[o1 + 2] = 11;
        BigEndian::write_u16(&mut page[o2..], o3 as u16);
        page[o2 + 2] = 12;
        BigEndian::write_u16(&mut page[o3..], 0);
        page[o3 + 2] = 14;

        let records = walk_undo_records(&page, o1 as u16, 300, 2);
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn test_walk_undo_records_empty() {
        let page = vec![0u8; 256];
        // start_offset = 0 means no records
        let records = walk_undo_records(&page, 0, 200, 100);
        assert_eq!(records.len(), 0);
    }

    #[test]
    fn test_read_compressed_insufficient_data() {
        assert_eq!(read_compressed(&[], 0), None);
        assert_eq!(read_compressed(&[0x80], 0), None); // needs 2 bytes
        assert_eq!(read_compressed(&[0xC0, 0x00], 0), None); // needs 3 bytes
        assert_eq!(read_compressed(&[0xE0, 0x00, 0x00], 0), None); // needs 4 bytes
        assert_eq!(read_compressed(&[0xF0, 0x00, 0x00, 0x00], 0), None); // needs 5 bytes
    }

    #[test]
    fn test_read_compressed_with_offset() {
        let data = [0x00, 0x00, 0x42];
        assert_eq!(read_compressed(&data, 2), Some((0x42, 1)));
    }

    #[test]
    fn test_read_compressed_invalid_leading_byte() {
        // 0xF1..0xFF are not valid leading bytes
        assert_eq!(read_compressed(&[0xF1], 0), None);
        assert_eq!(read_compressed(&[0xFF], 0), None);
    }

    // -----------------------------------------------------------------------
    // UndoRecordType from_u8 tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_undo_record_type_from_u8() {
        assert_eq!(UndoRecordType::from_u8(11), UndoRecordType::InsertRec);
        assert_eq!(UndoRecordType::from_u8(12), UndoRecordType::UpdExistRec);
        assert_eq!(UndoRecordType::from_u8(13), UndoRecordType::UpdDelRec);
        assert_eq!(UndoRecordType::from_u8(14), UndoRecordType::DelMarkRec);
        assert_eq!(UndoRecordType::from_u8(99), UndoRecordType::Unknown(99));
    }

    // -----------------------------------------------------------------------
    // parse_undo_records tests (detailed undo record parsing)
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_undo_records_empty_page() {
        // Page where start == free (no records)
        let mut page = vec![0u8; 256];
        let base = FIL_PAGE_DATA;
        BigEndian::write_u16(&mut page[base + TRX_UNDO_PAGE_TYPE..], 2); // UPDATE
        BigEndian::write_u16(&mut page[base + TRX_UNDO_PAGE_START..], 100);
        BigEndian::write_u16(&mut page[base + TRX_UNDO_PAGE_FREE..], 100); // start == free
        assert!(parse_undo_records(&page).is_empty());
    }

    #[test]
    fn test_parse_undo_records_single_insert() {
        let mut page = vec![0u8; 512];
        let base = FIL_PAGE_DATA;

        let start_offset: u16 = 100;
        let free_offset: u16 = 120;

        BigEndian::write_u16(&mut page[base + TRX_UNDO_PAGE_TYPE..], 1); // INSERT
        BigEndian::write_u16(&mut page[base + TRX_UNDO_PAGE_START..], start_offset);
        BigEndian::write_u16(&mut page[base + TRX_UNDO_PAGE_FREE..], free_offset);

        let mut pos = start_offset as usize;

        // next record offset = 0 (last record)
        BigEndian::write_u16(&mut page[pos..], 0);
        pos += 2;

        // type_cmpl = 11 (InsertRec)
        page[pos] = 11;
        pos += 1;

        // undo_no = 5 (single byte compressed)
        page[pos] = 5;
        pos += 1;

        // table_id = 42 (single byte compressed)
        page[pos] = 42;
        pos += 1;

        // PK field: length=4, data=[0,0,0,1]
        page[pos] = 4; // compressed length
        pos += 1;
        BigEndian::write_u32(&mut page[pos..], 1);

        let records = parse_undo_records(&page);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record_type, UndoRecordType::InsertRec);
        assert_eq!(records[0].undo_no, 5);
        assert_eq!(records[0].table_id, 42);
        assert_eq!(records[0].pk_fields.len(), 1);
        assert_eq!(records[0].pk_fields[0], vec![0, 0, 0, 1]);
        assert!(records[0].trx_id.is_none());
    }

    #[test]
    fn test_parse_undo_records_del_mark() {
        let mut page = vec![0u8; 512];
        let base = FIL_PAGE_DATA;

        let start_offset: u16 = 100;
        let free_offset: u16 = 200;

        BigEndian::write_u16(&mut page[base + TRX_UNDO_PAGE_TYPE..], 2); // UPDATE
        BigEndian::write_u16(&mut page[base + TRX_UNDO_PAGE_START..], start_offset);
        BigEndian::write_u16(&mut page[base + TRX_UNDO_PAGE_FREE..], free_offset);

        let mut pos = start_offset as usize;

        // next = 0 (last)
        BigEndian::write_u16(&mut page[pos..], 0);
        pos += 2;

        // type_cmpl = 14 (DelMarkRec)
        page[pos] = 14;
        pos += 1;

        // undo_no = 10
        page[pos] = 10;
        pos += 1;

        // table_id = 7
        page[pos] = 7;
        pos += 1;

        // PK field: length=2, data=[0x00, 0x05]
        page[pos] = 2;
        pos += 1;
        page[pos] = 0x00;
        page[pos + 1] = 0x05;
        pos += 2;

        // trx_id = 100 (compressed)
        page[pos] = 100;
        pos += 1;

        // roll_ptr = 7 bytes
        for i in 0..7 {
            page[pos + i] = (i + 1) as u8;
        }
        pos += 7;

        // update vector: 0 fields
        page[pos] = 0;

        let records = parse_undo_records(&page);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record_type, UndoRecordType::DelMarkRec);
        assert_eq!(records[0].table_id, 7);
        assert_eq!(records[0].trx_id, Some(100));
        assert_eq!(records[0].roll_ptr, Some([1, 2, 3, 4, 5, 6, 7]));
    }

    #[test]
    fn test_parse_undo_records_bounds_safety() {
        // Page too small to parse even the header
        let page = vec![0u8; 30];
        assert!(parse_undo_records(&page).is_empty());
    }

    #[test]
    fn test_detailed_undo_record_serialization() {
        let rec = DetailedUndoRecord {
            offset: 100,
            record_type: UndoRecordType::DelMarkRec,
            undo_no: 5,
            table_id: 42,
            pk_fields: vec![vec![0, 0, 0, 1]],
            trx_id: Some(100),
            roll_ptr: Some([1, 2, 3, 4, 5, 6, 7]),
            update_fields: vec![],
        };
        let json = serde_json::to_string(&rec).unwrap();
        assert!(json.contains("\"record_type\":\"DelMarkRec\""));
        assert!(json.contains("\"table_id\":42"));
        assert!(json.contains("\"trx_id\":100"));
    }
}
