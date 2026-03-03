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

        segments.push(UndoSegmentInfo {
            page_no: page_num,
            page_header: page_hdr,
            segment_header: seg_hdr,
            log_headers,
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

    Ok(UndoSegmentInfo {
        page_no,
        page_header,
        segment_header,
        log_headers,
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
        BigEndian::write_u32(
            &mut page[base + TRX_RSEG_SLOTS_OFFSET..],
            5,
        );
        BigEndian::write_u32(
            &mut page[base + TRX_RSEG_SLOTS_OFFSET + 4..],
            FIL_NULL,
        );

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
}
