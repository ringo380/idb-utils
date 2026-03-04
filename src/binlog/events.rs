//! Binlog event types and row-based event parsing.
//!
//! Covers the standard MySQL binlog event type codes and provides parsing
//! for TABLE_MAP and row-based events (WRITE/UPDATE/DELETE ROWS v2).

use byteorder::{ByteOrder, LittleEndian};
use serde::Serialize;

/// MySQL binlog event type codes.
///
/// # Examples
///
/// ```
/// use idb::binlog::events::BinlogEventType;
///
/// let et = BinlogEventType::from_code(15);
/// assert_eq!(et, BinlogEventType::FormatDescription);
/// assert_eq!(et.name(), "FORMAT_DESCRIPTION");
///
/// let et = BinlogEventType::from_code(19);
/// assert_eq!(et, BinlogEventType::TableMap);
///
/// let et = BinlogEventType::from_code(255);
/// assert_eq!(et, BinlogEventType::Unknown(255));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum BinlogEventType {
    Unknown(u8),
    StartV3,
    Query,
    Stop,
    Rotate,
    IntVar,
    Load,
    Slave,
    CreateFile,
    AppendBlock,
    ExecLoad,
    DeleteFile,
    NewLoad,
    Rand,
    UserVar,
    FormatDescription,
    Xid,
    BeginLoadQuery,
    ExecuteLoadQuery,
    TableMap,
    PreGaWriteRows,
    PreGaUpdateRows,
    PreGaDeleteRows,
    WriteRowsV1,
    UpdateRowsV1,
    DeleteRowsV1,
    Incident,
    Heartbeat,
    IgnorableLogEvent,
    RowsQuery,
    WriteRowsV2,
    UpdateRowsV2,
    DeleteRowsV2,
    GtidLogEvent,
    AnonymousGtidLogEvent,
    PreviousGtids,
    TransactionContext,
    ViewChange,
    XaPrepareLogEvent,
    PartialUpdateRowsEvent,
    TransactionPayload,
    HeartbeatV2,
}

impl BinlogEventType {
    /// Classify a binlog event from its type code byte.
    pub fn from_code(code: u8) -> Self {
        match code {
            1 => BinlogEventType::StartV3,
            2 => BinlogEventType::Query,
            3 => BinlogEventType::Stop,
            4 => BinlogEventType::Rotate,
            5 => BinlogEventType::IntVar,
            6 => BinlogEventType::Load,
            7 => BinlogEventType::Slave,
            8 => BinlogEventType::CreateFile,
            9 => BinlogEventType::AppendBlock,
            10 => BinlogEventType::ExecLoad,
            11 => BinlogEventType::DeleteFile,
            12 => BinlogEventType::NewLoad,
            13 => BinlogEventType::Rand,
            14 => BinlogEventType::UserVar,
            15 => BinlogEventType::FormatDescription,
            16 => BinlogEventType::Xid,
            17 => BinlogEventType::BeginLoadQuery,
            18 => BinlogEventType::ExecuteLoadQuery,
            19 => BinlogEventType::TableMap,
            20 => BinlogEventType::PreGaWriteRows,
            21 => BinlogEventType::PreGaUpdateRows,
            22 => BinlogEventType::PreGaDeleteRows,
            23 => BinlogEventType::WriteRowsV1,
            24 => BinlogEventType::UpdateRowsV1,
            25 => BinlogEventType::DeleteRowsV1,
            26 => BinlogEventType::Incident,
            27 => BinlogEventType::Heartbeat,
            28 => BinlogEventType::IgnorableLogEvent,
            29 => BinlogEventType::RowsQuery,
            30 => BinlogEventType::WriteRowsV2,
            31 => BinlogEventType::UpdateRowsV2,
            32 => BinlogEventType::DeleteRowsV2,
            33 => BinlogEventType::GtidLogEvent,
            34 => BinlogEventType::AnonymousGtidLogEvent,
            35 => BinlogEventType::PreviousGtids,
            36 => BinlogEventType::TransactionContext,
            37 => BinlogEventType::ViewChange,
            38 => BinlogEventType::XaPrepareLogEvent,
            39 => BinlogEventType::PartialUpdateRowsEvent,
            40 => BinlogEventType::TransactionPayload,
            41 => BinlogEventType::HeartbeatV2,
            other => BinlogEventType::Unknown(other),
        }
    }

    /// Returns the MySQL source-style name for this event type.
    pub fn name(&self) -> &'static str {
        match self {
            BinlogEventType::Unknown(_) => "UNKNOWN",
            BinlogEventType::StartV3 => "START_V3",
            BinlogEventType::Query => "QUERY",
            BinlogEventType::Stop => "STOP",
            BinlogEventType::Rotate => "ROTATE",
            BinlogEventType::IntVar => "INTVAR",
            BinlogEventType::Load => "LOAD",
            BinlogEventType::Slave => "SLAVE",
            BinlogEventType::CreateFile => "CREATE_FILE",
            BinlogEventType::AppendBlock => "APPEND_BLOCK",
            BinlogEventType::ExecLoad => "EXEC_LOAD",
            BinlogEventType::DeleteFile => "DELETE_FILE",
            BinlogEventType::NewLoad => "NEW_LOAD",
            BinlogEventType::Rand => "RAND",
            BinlogEventType::UserVar => "USER_VAR",
            BinlogEventType::FormatDescription => "FORMAT_DESCRIPTION",
            BinlogEventType::Xid => "XID",
            BinlogEventType::BeginLoadQuery => "BEGIN_LOAD_QUERY",
            BinlogEventType::ExecuteLoadQuery => "EXECUTE_LOAD_QUERY",
            BinlogEventType::TableMap => "TABLE_MAP",
            BinlogEventType::PreGaWriteRows => "PRE_GA_WRITE_ROWS",
            BinlogEventType::PreGaUpdateRows => "PRE_GA_UPDATE_ROWS",
            BinlogEventType::PreGaDeleteRows => "PRE_GA_DELETE_ROWS",
            BinlogEventType::WriteRowsV1 => "WRITE_ROWS_V1",
            BinlogEventType::UpdateRowsV1 => "UPDATE_ROWS_V1",
            BinlogEventType::DeleteRowsV1 => "DELETE_ROWS_V1",
            BinlogEventType::Incident => "INCIDENT",
            BinlogEventType::Heartbeat => "HEARTBEAT",
            BinlogEventType::IgnorableLogEvent => "IGNORABLE",
            BinlogEventType::RowsQuery => "ROWS_QUERY",
            BinlogEventType::WriteRowsV2 => "WRITE_ROWS_V2",
            BinlogEventType::UpdateRowsV2 => "UPDATE_ROWS_V2",
            BinlogEventType::DeleteRowsV2 => "DELETE_ROWS_V2",
            BinlogEventType::GtidLogEvent => "GTID",
            BinlogEventType::AnonymousGtidLogEvent => "ANONYMOUS_GTID",
            BinlogEventType::PreviousGtids => "PREVIOUS_GTIDS",
            BinlogEventType::TransactionContext => "TRANSACTION_CONTEXT",
            BinlogEventType::ViewChange => "VIEW_CHANGE",
            BinlogEventType::XaPrepareLogEvent => "XA_PREPARE",
            BinlogEventType::PartialUpdateRowsEvent => "PARTIAL_UPDATE_ROWS",
            BinlogEventType::TransactionPayload => "TRANSACTION_PAYLOAD",
            BinlogEventType::HeartbeatV2 => "HEARTBEAT_V2",
        }
    }
}

impl std::fmt::Display for BinlogEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Parsed TABLE_MAP event (type 19).
///
/// Maps a table_id to a database.table name and column type information.
/// This event precedes row-based events to provide schema context.
///
/// # Examples
///
/// ```
/// use idb::binlog::events::TableMapEvent;
/// use byteorder::{LittleEndian, ByteOrder};
///
/// let mut data = vec![0u8; 50];
/// // table_id (6 bytes LE)
/// LittleEndian::write_u32(&mut data[0..], 42);
/// data[4] = 0; data[5] = 0;
/// // flags (2 bytes)
/// LittleEndian::write_u16(&mut data[6..], 0);
/// // database name length (1 byte) + name + NUL
/// data[8] = 4; // "test"
/// data[9..13].copy_from_slice(b"test");
/// data[13] = 0; // NUL
/// // table name length (1 byte) + name + NUL
/// data[14] = 5; // "users"
/// data[15..20].copy_from_slice(b"users");
/// data[20] = 0; // NUL
/// // column_count (packed integer)
/// data[21] = 3;
/// // column_types
/// data[22] = 3; // LONG
/// data[23] = 15; // VARCHAR
/// data[24] = 12; // DATETIME
///
/// let tme = TableMapEvent::parse(&data).unwrap();
/// assert_eq!(tme.table_id, 42);
/// assert_eq!(tme.database_name, "test");
/// assert_eq!(tme.table_name, "users");
/// assert_eq!(tme.column_count, 3);
/// assert_eq!(tme.column_types, vec![3, 15, 12]);
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct TableMapEvent {
    /// Internal table ID.
    pub table_id: u64,
    /// Database (schema) name.
    pub database_name: String,
    /// Table name.
    pub table_name: String,
    /// Number of columns.
    pub column_count: u64,
    /// Column type codes.
    pub column_types: Vec<u8>,
}

impl TableMapEvent {
    /// Parse a TABLE_MAP event from the event data (after the common header).
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 10 {
            return None;
        }

        // table_id: 6 bytes LE
        let table_id = LittleEndian::read_u32(&data[0..]) as u64
            | ((data[4] as u64) << 32)
            | ((data[5] as u64) << 40);

        // flags: 2 bytes (skip)
        let mut offset = 8;

        // database name: 1-byte length + string + NUL
        if offset >= data.len() {
            return None;
        }
        let db_len = data[offset] as usize;
        offset += 1;
        if offset + db_len + 1 > data.len() {
            return None;
        }
        let database_name = std::str::from_utf8(&data[offset..offset + db_len])
            .unwrap_or("")
            .to_string();
        offset += db_len + 1; // skip NUL

        // table name: 1-byte length + string + NUL
        if offset >= data.len() {
            return None;
        }
        let tbl_len = data[offset] as usize;
        offset += 1;
        if offset + tbl_len + 1 > data.len() {
            return None;
        }
        let table_name = std::str::from_utf8(&data[offset..offset + tbl_len])
            .unwrap_or("")
            .to_string();
        offset += tbl_len + 1; // skip NUL

        // column_count: packed integer (lenenc)
        if offset >= data.len() {
            return None;
        }
        let (column_count, bytes_read) = read_lenenc_int(&data[offset..]);
        offset += bytes_read;

        // column_types: column_count bytes
        let end = offset + column_count as usize;
        if end > data.len() {
            return None;
        }
        let column_types = data[offset..end].to_vec();

        Some(TableMapEvent {
            table_id,
            database_name,
            table_name,
            column_count,
            column_types,
        })
    }
}

/// Parsed row-based event summary (types 30-32).
///
/// Contains metadata about row changes. Full row data decoding is not
/// performed — only the event structure and row count are extracted.
#[derive(Debug, Clone, Serialize)]
pub struct RowsEvent {
    /// Internal table ID (matches TABLE_MAP event).
    pub table_id: u64,
    /// Event type.
    pub event_type: BinlogEventType,
    /// Event flags.
    pub flags: u16,
    /// Number of columns involved.
    pub column_count: u64,
    /// Approximate row count (estimated from data size).
    pub row_count: usize,
}

impl RowsEvent {
    /// Parse a row-based event from the event data (after the common header).
    ///
    /// Supports WRITE_ROWS_V2 (30), UPDATE_ROWS_V2 (31), DELETE_ROWS_V2 (32).
    pub fn parse(data: &[u8], type_code: u8) -> Option<Self> {
        if data.len() < 10 {
            return None;
        }

        let event_type = BinlogEventType::from_code(type_code);

        // table_id: 6 bytes LE
        let table_id = LittleEndian::read_u32(&data[0..]) as u64
            | ((data[4] as u64) << 32)
            | ((data[5] as u64) << 40);

        // flags: 2 bytes
        let flags = LittleEndian::read_u16(&data[6..]);

        // extra_data_length: 2 bytes (v2 events)
        let extra_len = LittleEndian::read_u16(&data[8..]) as usize;
        let mut offset = 10 + extra_len.saturating_sub(2); // extra_len includes itself

        // column_count: packed integer
        if offset >= data.len() {
            return None;
        }
        let (column_count, bytes_read) = read_lenenc_int(&data[offset..]);
        offset += bytes_read;

        // Skip column bitmaps to estimate row count from remaining data
        let bitmap_len = (column_count as usize).div_ceil(8);
        offset += bitmap_len; // columns_before_image
        if type_code == 31 {
            offset += bitmap_len; // columns_after_image for UPDATE
        }

        // Rough row count estimate: we can't parse row data without column metadata,
        // so count is estimated as 1 if there's remaining data
        let row_count = if offset < data.len() { 1 } else { 0 };

        Some(RowsEvent {
            table_id,
            event_type,
            flags,
            column_count,
            row_count,
        })
    }
}

/// Read a MySQL packed (length-encoded) integer.
///
/// Returns (value, bytes_consumed).
fn read_lenenc_int(data: &[u8]) -> (u64, usize) {
    if data.is_empty() {
        return (0, 0);
    }
    match data[0] {
        0..=250 => (data[0] as u64, 1),
        252 => {
            if data.len() < 3 {
                return (0, 1);
            }
            (LittleEndian::read_u16(&data[1..]) as u64, 3)
        }
        253 => {
            if data.len() < 4 {
                return (0, 1);
            }
            let v = data[1] as u64 | (data[2] as u64) << 8 | (data[3] as u64) << 16;
            (v, 4)
        }
        254 => {
            if data.len() < 9 {
                return (0, 1);
            }
            (LittleEndian::read_u64(&data[1..]), 9)
        }
        _ => (0, 1), // 251 = NULL, 255 = undefined
    }
}

/// Summary of a single binlog event for analysis output.
#[derive(Debug, Clone, Serialize)]
pub struct BinlogEventSummary {
    /// File offset of the event.
    pub offset: u64,
    /// Event type name.
    pub event_type: String,
    /// Event type code.
    pub type_code: u8,
    /// Unix timestamp.
    pub timestamp: u32,
    /// Server ID.
    pub server_id: u32,
    /// Total event size in bytes.
    pub event_length: u32,
}

/// Top-level analysis result for a binary log file.
#[derive(Debug, Clone, Serialize)]
pub struct BinlogAnalysis {
    /// Format description from the first event.
    pub format_description: FormatDescriptionEvent,
    /// Total number of events.
    pub event_count: usize,
    /// Count of events by type name.
    pub event_type_counts: std::collections::HashMap<String, usize>,
    /// TABLE_MAP events found.
    pub table_maps: Vec<TableMapEvent>,
    /// Individual event summaries.
    pub events: Vec<BinlogEventSummary>,
}

use crate::binlog::header::{
    validate_binlog_magic, BinlogEventHeader, FormatDescriptionEvent, BINLOG_EVENT_HEADER_SIZE,
};
use std::io::{Read, Seek, SeekFrom};

/// Analyze a binary log file from a reader.
///
/// Reads all events, collecting summaries, TABLE_MAP events, and type counts.
pub fn analyze_binlog<R: Read + Seek>(mut reader: R) -> Result<BinlogAnalysis, crate::IdbError> {
    // Validate magic
    let mut magic = [0u8; 4];
    reader
        .read_exact(&mut magic)
        .map_err(|e| crate::IdbError::Io(format!("Failed to read binlog magic: {e}")))?;

    if !validate_binlog_magic(&magic) {
        return Err(crate::IdbError::Parse(
            "Not a valid MySQL binary log file (bad magic)".to_string(),
        ));
    }

    let file_size = reader
        .seek(SeekFrom::End(0))
        .map_err(|e| crate::IdbError::Io(format!("Failed to seek: {e}")))?;
    reader
        .seek(SeekFrom::Start(4))
        .map_err(|e| crate::IdbError::Io(format!("Failed to seek: {e}")))?;

    let mut events = Vec::new();
    let mut event_type_counts = std::collections::HashMap::new();
    let mut table_maps = Vec::new();
    let mut format_desc = None;

    let mut position = 4u64;
    let mut header_buf = vec![0u8; BINLOG_EVENT_HEADER_SIZE];

    while position + BINLOG_EVENT_HEADER_SIZE as u64 <= file_size {
        if reader.read_exact(&mut header_buf).is_err() {
            break;
        }

        let hdr = match BinlogEventHeader::parse(&header_buf) {
            Some(h) => h,
            None => break,
        };

        if hdr.event_length < BINLOG_EVENT_HEADER_SIZE as u32 {
            break;
        }

        let data_len = hdr.event_length as usize - BINLOG_EVENT_HEADER_SIZE;
        let mut event_data = vec![0u8; data_len];
        if reader.read_exact(&mut event_data).is_err() {
            break;
        }

        let event_type = BinlogEventType::from_code(hdr.type_code);

        // Parse specific event types
        if hdr.type_code == 15 && format_desc.is_none() {
            format_desc = FormatDescriptionEvent::parse(&event_data);
        } else if hdr.type_code == 19 {
            if let Some(tme) = TableMapEvent::parse(&event_data) {
                table_maps.push(tme);
            }
        }

        *event_type_counts
            .entry(event_type.name().to_string())
            .or_insert(0) += 1;

        events.push(BinlogEventSummary {
            offset: position,
            event_type: event_type.name().to_string(),
            type_code: hdr.type_code,
            timestamp: hdr.timestamp,
            server_id: hdr.server_id,
            event_length: hdr.event_length,
        });

        position = if hdr.next_position > 0 {
            hdr.next_position as u64
        } else {
            position + hdr.event_length as u64
        };

        // Seek to next event position (in case of padding or checksum)
        if reader.seek(SeekFrom::Start(position)).is_err() {
            break;
        }
    }

    let format_description = format_desc.unwrap_or(FormatDescriptionEvent {
        binlog_version: 0,
        server_version: "unknown".to_string(),
        create_timestamp: 0,
        header_length: 19,
        checksum_alg: 0,
    });

    Ok(BinlogAnalysis {
        format_description,
        event_count: events.len(),
        event_type_counts,
        table_maps,
        events,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_type_from_code() {
        assert_eq!(BinlogEventType::from_code(2), BinlogEventType::Query);
        assert_eq!(
            BinlogEventType::from_code(15),
            BinlogEventType::FormatDescription
        );
        assert_eq!(BinlogEventType::from_code(19), BinlogEventType::TableMap);
        assert_eq!(BinlogEventType::from_code(30), BinlogEventType::WriteRowsV2);
        assert_eq!(
            BinlogEventType::from_code(31),
            BinlogEventType::UpdateRowsV2
        );
        assert_eq!(
            BinlogEventType::from_code(32),
            BinlogEventType::DeleteRowsV2
        );
        assert_eq!(
            BinlogEventType::from_code(255),
            BinlogEventType::Unknown(255)
        );
    }

    #[test]
    fn test_event_type_names() {
        assert_eq!(
            BinlogEventType::FormatDescription.name(),
            "FORMAT_DESCRIPTION"
        );
        assert_eq!(BinlogEventType::TableMap.name(), "TABLE_MAP");
        assert_eq!(BinlogEventType::WriteRowsV2.name(), "WRITE_ROWS_V2");
        assert_eq!(BinlogEventType::GtidLogEvent.name(), "GTID");
    }

    #[test]
    fn test_event_type_display() {
        assert_eq!(format!("{}", BinlogEventType::Query), "QUERY");
        assert_eq!(format!("{}", BinlogEventType::Unknown(99)), "UNKNOWN");
    }

    #[test]
    fn test_table_map_event_parse() {
        let mut data = vec![0u8; 50];
        // table_id = 42
        LittleEndian::write_u32(&mut data[0..], 42);
        data[4] = 0;
        data[5] = 0;
        // flags
        LittleEndian::write_u16(&mut data[6..], 0);
        // db name: "test"
        data[8] = 4;
        data[9..13].copy_from_slice(b"test");
        data[13] = 0;
        // table name: "users"
        data[14] = 5;
        data[15..20].copy_from_slice(b"users");
        data[20] = 0;
        // column_count = 3
        data[21] = 3;
        // column types
        data[22] = 3; // LONG
        data[23] = 15; // VARCHAR
        data[24] = 12; // DATETIME

        let tme = TableMapEvent::parse(&data).unwrap();
        assert_eq!(tme.table_id, 42);
        assert_eq!(tme.database_name, "test");
        assert_eq!(tme.table_name, "users");
        assert_eq!(tme.column_count, 3);
        assert_eq!(tme.column_types, vec![3, 15, 12]);
    }

    #[test]
    fn test_table_map_event_too_short() {
        let data = vec![0u8; 5];
        assert!(TableMapEvent::parse(&data).is_none());
    }

    #[test]
    fn test_rows_event_parse() {
        let mut data = vec![0u8; 30];
        // table_id = 42
        LittleEndian::write_u32(&mut data[0..], 42);
        data[4] = 0;
        data[5] = 0;
        // flags
        LittleEndian::write_u16(&mut data[6..], 1);
        // extra_data_length = 2 (minimum, self-inclusive)
        LittleEndian::write_u16(&mut data[8..], 2);
        // column_count = 3
        data[10] = 3;
        // bitmap (1 byte for 3 columns)
        data[11] = 0x07;
        // some row data
        data[12] = 0x01;

        let re = RowsEvent::parse(&data, 30).unwrap();
        assert_eq!(re.table_id, 42);
        assert_eq!(re.event_type, BinlogEventType::WriteRowsV2);
        assert_eq!(re.flags, 1);
        assert_eq!(re.column_count, 3);
    }

    #[test]
    fn test_lenenc_int() {
        assert_eq!(read_lenenc_int(&[5]), (5, 1));
        assert_eq!(read_lenenc_int(&[250]), (250, 1));
        assert_eq!(read_lenenc_int(&[252, 0x01, 0x00]), (1, 3));
        assert_eq!(read_lenenc_int(&[253, 0x01, 0x00, 0x00]), (1, 4));
    }

    #[test]
    fn test_analyze_binlog_synthetic() {
        use std::io::Cursor;

        // Build a minimal synthetic binlog:
        // 4-byte magic + FDE event (19-byte header + FDE data)
        let mut binlog = Vec::new();
        binlog.extend_from_slice(&[0xfe, 0x62, 0x69, 0x6e]); // magic

        // Build FDE event
        let fde_data_len = 100usize;
        let fde_event_len = (BINLOG_EVENT_HEADER_SIZE + fde_data_len) as u32;

        let mut fde_header = vec![0u8; 19];
        LittleEndian::write_u32(&mut fde_header[0..], 1700000000); // timestamp
        fde_header[4] = 15; // FORMAT_DESCRIPTION_EVENT
        LittleEndian::write_u32(&mut fde_header[5..], 1); // server_id
        LittleEndian::write_u32(&mut fde_header[9..], fde_event_len); // event_length
        LittleEndian::write_u32(&mut fde_header[13..], 4 + fde_event_len); // next_position
        binlog.extend_from_slice(&fde_header);

        let mut fde_data = vec![0u8; fde_data_len];
        LittleEndian::write_u16(&mut fde_data[0..], 4); // binlog_version
        let ver = b"8.0.35";
        fde_data[2..2 + ver.len()].copy_from_slice(ver);
        LittleEndian::write_u32(&mut fde_data[52..], 1700000000);
        fde_data[56] = 19;
        fde_data[95] = 1; // checksum_alg = CRC32
        binlog.extend_from_slice(&fde_data);

        let cursor = Cursor::new(binlog);
        let analysis = analyze_binlog(cursor).unwrap();

        assert_eq!(analysis.event_count, 1);
        assert_eq!(analysis.format_description.binlog_version, 4);
        assert_eq!(analysis.format_description.server_version, "8.0.35");
        assert_eq!(
            analysis.event_type_counts.get("FORMAT_DESCRIPTION"),
            Some(&1)
        );
    }

    #[test]
    fn test_analyze_binlog_bad_magic() {
        use std::io::Cursor;
        let data = vec![0u8; 100];
        let cursor = Cursor::new(data);
        assert!(analyze_binlog(cursor).is_err());
    }
}
