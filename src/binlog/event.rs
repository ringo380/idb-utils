//! Binlog event types and common event header.
//!
//! Every binlog event begins with a 19-byte common header containing the
//! timestamp, event type, server ID, total length, next-event position, and
//! flags. The [`CommonEventHeader`] struct parses this header, and the
//! [`BinlogEventType`] enum maps the type code byte to a named variant.
//!
//! The [`BinlogEvent`] enum wraps the parsed payload for recognized event
//! types (FORMAT_DESCRIPTION, ROTATE, STOP, QUERY, XID) with an `Unknown`
//! fallback for everything else.

use byteorder::{ByteOrder, LittleEndian};
use serde::Serialize;
use std::fmt;

use super::constants::*;
use super::header::{FormatDescriptionEvent, RotateEvent};

/// MySQL binary log event type codes.
///
/// Covers all event types from MySQL 5.0 through 9.x (42 named variants).
/// Unrecognized type codes are preserved in the `Unknown(u8)` variant
/// for forward compatibility.
///
/// # Examples
///
/// ```
/// use idb::binlog::BinlogEventType;
///
/// let t = BinlogEventType::from_u8(15);
/// assert_eq!(t, BinlogEventType::FormatDescription);
/// assert_eq!(t.name(), "FORMAT_DESCRIPTION");
///
/// let u = BinlogEventType::from_u8(200);
/// assert!(matches!(u, BinlogEventType::Unknown(200)));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum BinlogEventType {
    /// Unknown or invalid event (type code 0).
    UnknownEvent,
    /// Start event v3 (pre-v4 format).
    StartEventV3,
    /// SQL query event.
    QueryEvent,
    /// Server shutdown.
    StopEvent,
    /// Binlog file rotation.
    RotateEvent,
    /// Integer session variable.
    IntvarEvent,
    /// LOAD DATA INFILE event (deprecated).
    LoadEvent,
    /// Slave event (internal replication, deprecated).
    SlaveEvent,
    /// Create file for LOAD DATA (deprecated).
    CreateFileEvent,
    /// Append block for LOAD DATA (deprecated).
    AppendBlockEvent,
    /// Execute LOAD DATA (deprecated).
    ExecLoadEvent,
    /// Delete file for LOAD DATA (deprecated).
    DeleteFileEvent,
    /// New LOAD DATA INFILE event (deprecated).
    NewLoadEvent,
    /// Random seed for RAND().
    RandEvent,
    /// User-defined variable.
    UserVarEvent,
    /// Format description event (binlog v4 header).
    FormatDescription,
    /// XA transaction commit.
    XidEvent,
    /// Begin LOAD QUERY event.
    BeginLoadQueryEvent,
    /// Execute LOAD QUERY event.
    ExecuteLoadQueryEvent,
    /// Table map (row-based replication).
    TableMapEvent,
    /// Pre-GA write rows event.
    PreGaWriteRowsEvent,
    /// Pre-GA update rows event.
    PreGaUpdateRowsEvent,
    /// Pre-GA delete rows event.
    PreGaDeleteRowsEvent,
    /// Write rows v1.
    WriteRowsEventV1,
    /// Update rows v1.
    UpdateRowsEventV1,
    /// Delete rows v1.
    DeleteRowsEventV1,
    /// Incident event.
    IncidentEvent,
    /// Heartbeat event.
    HeartbeatEvent,
    /// Ignorable log event.
    IgnorableLogEvent,
    /// Rows query event.
    RowsQueryEvent,
    /// Write rows v2.
    WriteRowsEvent,
    /// Update rows v2.
    UpdateRowsEvent,
    /// Delete rows v2.
    DeleteRowsEvent,
    /// GTID event.
    GtidLogEvent,
    /// Anonymous GTID event.
    AnonymousGtidLogEvent,
    /// Previous GTIDs event.
    PreviousGtidsLogEvent,
    /// Transaction context event (Group Replication).
    TransactionContextEvent,
    /// View change event (Group Replication).
    ViewChangeEvent,
    /// XA prepare log event.
    XaPrepareLogEvent,
    /// Partial update rows event (MySQL 8.0+).
    PartialUpdateRowsEvent,
    /// Transaction payload event (MySQL 8.0.20+).
    TransactionPayloadEvent,
    /// Heartbeat v2 event (MySQL 8.0.26+).
    HeartbeatEventV2,
    /// Unrecognized event type code (forward compatibility).
    Unknown(u8),
}

impl BinlogEventType {
    /// Map a raw type code byte to a [`BinlogEventType`] variant.
    pub fn from_u8(code: u8) -> Self {
        match code {
            UNKNOWN_EVENT => Self::UnknownEvent,
            START_EVENT_V3 => Self::StartEventV3,
            QUERY_EVENT => Self::QueryEvent,
            STOP_EVENT => Self::StopEvent,
            ROTATE_EVENT => Self::RotateEvent,
            INTVAR_EVENT => Self::IntvarEvent,
            LOAD_EVENT => Self::LoadEvent,
            SLAVE_EVENT => Self::SlaveEvent,
            CREATE_FILE_EVENT => Self::CreateFileEvent,
            APPEND_BLOCK_EVENT => Self::AppendBlockEvent,
            EXEC_LOAD_EVENT => Self::ExecLoadEvent,
            DELETE_FILE_EVENT => Self::DeleteFileEvent,
            NEW_LOAD_EVENT => Self::NewLoadEvent,
            RAND_EVENT => Self::RandEvent,
            USER_VAR_EVENT => Self::UserVarEvent,
            FORMAT_DESCRIPTION_EVENT => Self::FormatDescription,
            XID_EVENT => Self::XidEvent,
            BEGIN_LOAD_QUERY_EVENT => Self::BeginLoadQueryEvent,
            EXECUTE_LOAD_QUERY_EVENT => Self::ExecuteLoadQueryEvent,
            TABLE_MAP_EVENT => Self::TableMapEvent,
            PRE_GA_WRITE_ROWS_EVENT => Self::PreGaWriteRowsEvent,
            PRE_GA_UPDATE_ROWS_EVENT => Self::PreGaUpdateRowsEvent,
            PRE_GA_DELETE_ROWS_EVENT => Self::PreGaDeleteRowsEvent,
            WRITE_ROWS_EVENT_V1 => Self::WriteRowsEventV1,
            UPDATE_ROWS_EVENT_V1 => Self::UpdateRowsEventV1,
            DELETE_ROWS_EVENT_V1 => Self::DeleteRowsEventV1,
            INCIDENT_EVENT => Self::IncidentEvent,
            HEARTBEAT_LOG_EVENT => Self::HeartbeatEvent,
            IGNORABLE_LOG_EVENT => Self::IgnorableLogEvent,
            ROWS_QUERY_LOG_EVENT => Self::RowsQueryEvent,
            WRITE_ROWS_EVENT => Self::WriteRowsEvent,
            UPDATE_ROWS_EVENT => Self::UpdateRowsEvent,
            DELETE_ROWS_EVENT => Self::DeleteRowsEvent,
            GTID_LOG_EVENT => Self::GtidLogEvent,
            ANONYMOUS_GTID_LOG_EVENT => Self::AnonymousGtidLogEvent,
            PREVIOUS_GTIDS_LOG_EVENT => Self::PreviousGtidsLogEvent,
            TRANSACTION_CONTEXT_EVENT => Self::TransactionContextEvent,
            VIEW_CHANGE_EVENT => Self::ViewChangeEvent,
            XA_PREPARE_LOG_EVENT => Self::XaPrepareLogEvent,
            PARTIAL_UPDATE_ROWS_EVENT => Self::PartialUpdateRowsEvent,
            TRANSACTION_PAYLOAD_EVENT => Self::TransactionPayloadEvent,
            HEARTBEAT_LOG_EVENT_V2 => Self::HeartbeatEventV2,
            other => Self::Unknown(other),
        }
    }

    /// Return the raw type code byte.
    pub fn type_code(&self) -> u8 {
        match self {
            Self::UnknownEvent => UNKNOWN_EVENT,
            Self::StartEventV3 => START_EVENT_V3,
            Self::QueryEvent => QUERY_EVENT,
            Self::StopEvent => STOP_EVENT,
            Self::RotateEvent => ROTATE_EVENT,
            Self::IntvarEvent => INTVAR_EVENT,
            Self::LoadEvent => LOAD_EVENT,
            Self::SlaveEvent => SLAVE_EVENT,
            Self::CreateFileEvent => CREATE_FILE_EVENT,
            Self::AppendBlockEvent => APPEND_BLOCK_EVENT,
            Self::ExecLoadEvent => EXEC_LOAD_EVENT,
            Self::DeleteFileEvent => DELETE_FILE_EVENT,
            Self::NewLoadEvent => NEW_LOAD_EVENT,
            Self::RandEvent => RAND_EVENT,
            Self::UserVarEvent => USER_VAR_EVENT,
            Self::FormatDescription => FORMAT_DESCRIPTION_EVENT,
            Self::XidEvent => XID_EVENT,
            Self::BeginLoadQueryEvent => BEGIN_LOAD_QUERY_EVENT,
            Self::ExecuteLoadQueryEvent => EXECUTE_LOAD_QUERY_EVENT,
            Self::TableMapEvent => TABLE_MAP_EVENT,
            Self::PreGaWriteRowsEvent => PRE_GA_WRITE_ROWS_EVENT,
            Self::PreGaUpdateRowsEvent => PRE_GA_UPDATE_ROWS_EVENT,
            Self::PreGaDeleteRowsEvent => PRE_GA_DELETE_ROWS_EVENT,
            Self::WriteRowsEventV1 => WRITE_ROWS_EVENT_V1,
            Self::UpdateRowsEventV1 => UPDATE_ROWS_EVENT_V1,
            Self::DeleteRowsEventV1 => DELETE_ROWS_EVENT_V1,
            Self::IncidentEvent => INCIDENT_EVENT,
            Self::HeartbeatEvent => HEARTBEAT_LOG_EVENT,
            Self::IgnorableLogEvent => IGNORABLE_LOG_EVENT,
            Self::RowsQueryEvent => ROWS_QUERY_LOG_EVENT,
            Self::WriteRowsEvent => WRITE_ROWS_EVENT,
            Self::UpdateRowsEvent => UPDATE_ROWS_EVENT,
            Self::DeleteRowsEvent => DELETE_ROWS_EVENT,
            Self::GtidLogEvent => GTID_LOG_EVENT,
            Self::AnonymousGtidLogEvent => ANONYMOUS_GTID_LOG_EVENT,
            Self::PreviousGtidsLogEvent => PREVIOUS_GTIDS_LOG_EVENT,
            Self::TransactionContextEvent => TRANSACTION_CONTEXT_EVENT,
            Self::ViewChangeEvent => VIEW_CHANGE_EVENT,
            Self::XaPrepareLogEvent => XA_PREPARE_LOG_EVENT,
            Self::PartialUpdateRowsEvent => PARTIAL_UPDATE_ROWS_EVENT,
            Self::TransactionPayloadEvent => TRANSACTION_PAYLOAD_EVENT,
            Self::HeartbeatEventV2 => HEARTBEAT_LOG_EVENT_V2,
            Self::Unknown(c) => *c,
        }
    }

    /// Returns the MySQL source-style name for this event type.
    pub fn name(&self) -> &'static str {
        match self {
            Self::UnknownEvent => "UNKNOWN",
            Self::StartEventV3 => "START_V3",
            Self::QueryEvent => "QUERY",
            Self::StopEvent => "STOP",
            Self::RotateEvent => "ROTATE",
            Self::IntvarEvent => "INTVAR",
            Self::LoadEvent => "LOAD",
            Self::SlaveEvent => "SLAVE",
            Self::CreateFileEvent => "CREATE_FILE",
            Self::AppendBlockEvent => "APPEND_BLOCK",
            Self::ExecLoadEvent => "EXEC_LOAD",
            Self::DeleteFileEvent => "DELETE_FILE",
            Self::NewLoadEvent => "NEW_LOAD",
            Self::RandEvent => "RAND",
            Self::UserVarEvent => "USER_VAR",
            Self::FormatDescription => "FORMAT_DESCRIPTION",
            Self::XidEvent => "XID",
            Self::BeginLoadQueryEvent => "BEGIN_LOAD_QUERY",
            Self::ExecuteLoadQueryEvent => "EXECUTE_LOAD_QUERY",
            Self::TableMapEvent => "TABLE_MAP",
            Self::PreGaWriteRowsEvent => "PRE_GA_WRITE_ROWS",
            Self::PreGaUpdateRowsEvent => "PRE_GA_UPDATE_ROWS",
            Self::PreGaDeleteRowsEvent => "PRE_GA_DELETE_ROWS",
            Self::WriteRowsEventV1 => "WRITE_ROWS_V1",
            Self::UpdateRowsEventV1 => "UPDATE_ROWS_V1",
            Self::DeleteRowsEventV1 => "DELETE_ROWS_V1",
            Self::IncidentEvent => "INCIDENT",
            Self::HeartbeatEvent => "HEARTBEAT",
            Self::IgnorableLogEvent => "IGNORABLE",
            Self::RowsQueryEvent => "ROWS_QUERY",
            Self::WriteRowsEvent => "WRITE_ROWS_V2",
            Self::UpdateRowsEvent => "UPDATE_ROWS_V2",
            Self::DeleteRowsEvent => "DELETE_ROWS_V2",
            Self::GtidLogEvent => "GTID",
            Self::AnonymousGtidLogEvent => "ANONYMOUS_GTID",
            Self::PreviousGtidsLogEvent => "PREVIOUS_GTIDS",
            Self::TransactionContextEvent => "TRANSACTION_CONTEXT",
            Self::ViewChangeEvent => "VIEW_CHANGE",
            Self::XaPrepareLogEvent => "XA_PREPARE",
            Self::PartialUpdateRowsEvent => "PARTIAL_UPDATE_ROWS",
            Self::TransactionPayloadEvent => "TRANSACTION_PAYLOAD",
            Self::HeartbeatEventV2 => "HEARTBEAT_V2",
            Self::Unknown(_) => "UNKNOWN",
        }
    }
}

impl fmt::Display for BinlogEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown(c) => write!(f, "UNKNOWN({c})"),
            _ => write!(f, "{}", self.name()),
        }
    }
}

/// The 19-byte common header present at the start of every binlog event.
///
/// All integer fields are stored in little-endian byte order.
///
/// # Examples
///
/// ```
/// use idb::binlog::CommonEventHeader;
/// use byteorder::{LittleEndian, ByteOrder};
///
/// let mut data = vec![0u8; 19];
/// LittleEndian::write_u32(&mut data[0..], 1_700_000_000);  // timestamp
/// data[4] = 15;                                             // FORMAT_DESCRIPTION_EVENT
/// LittleEndian::write_u32(&mut data[5..], 1);               // server_id
/// LittleEndian::write_u32(&mut data[9..], 119);             // event_length
/// LittleEndian::write_u32(&mut data[13..], 123);            // next_position
/// LittleEndian::write_u16(&mut data[17..], 0);              // flags
///
/// let hdr = CommonEventHeader::parse(&data).unwrap();
/// assert_eq!(hdr.timestamp, 1_700_000_000);
/// assert_eq!(hdr.event_length, 119);
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct CommonEventHeader {
    /// Seconds since Unix epoch when the event was created.
    pub timestamp: u32,
    /// Event type.
    pub type_code: BinlogEventType,
    /// Server ID of the originating MySQL server.
    pub server_id: u32,
    /// Total event size in bytes (header + payload + optional checksum).
    pub event_length: u32,
    /// Absolute file position of the next event.
    pub next_position: u32,
    /// Event flags (e.g. `LOG_EVENT_BINLOG_IN_USE_F`).
    pub flags: u16,
}

impl CommonEventHeader {
    /// Parse a common event header from a byte slice.
    ///
    /// Returns `None` if the slice is shorter than [`COMMON_HEADER_SIZE`] (19 bytes).
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < COMMON_HEADER_SIZE {
            return None;
        }

        Some(Self {
            timestamp: LittleEndian::read_u32(&data[EVENT_TIMESTAMP_OFFSET..]),
            type_code: BinlogEventType::from_u8(data[EVENT_TYPE_OFFSET]),
            server_id: LittleEndian::read_u32(&data[EVENT_SERVER_ID_OFFSET..]),
            event_length: LittleEndian::read_u32(&data[EVENT_LENGTH_OFFSET..]),
            next_position: LittleEndian::read_u32(&data[EVENT_NEXT_POSITION_OFFSET..]),
            flags: LittleEndian::read_u16(&data[EVENT_FLAGS_OFFSET..]),
        })
    }

    /// Byte offset where the event payload begins (always 19).
    pub fn payload_offset(&self) -> usize {
        COMMON_HEADER_SIZE
    }

    /// Size of the event payload in bytes, excluding header and optional checksum.
    pub fn payload_length(&self, checksum_enabled: bool) -> usize {
        let total = self.event_length as usize;
        let overhead = COMMON_HEADER_SIZE
            + if checksum_enabled {
                BINLOG_CHECKSUM_LEN
            } else {
                0
            };
        total.saturating_sub(overhead)
    }
}

/// Parsed binlog event payload.
///
/// Wraps the typed payload for recognized event types. Events not yet
/// supported by full parsing are stored as `Unknown` with their raw bytes.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum BinlogEvent {
    /// FORMAT_DESCRIPTION_EVENT — always the first event after the magic bytes.
    FormatDescription(FormatDescriptionEvent),
    /// ROTATE_EVENT — signals rotation to the next binlog file.
    Rotate(RotateEvent),
    /// STOP_EVENT — server shutdown, no payload.
    Stop,
    /// QUERY_EVENT — SQL query (full parsing deferred to #135).
    Query {
        /// Raw payload bytes (query text + metadata).
        #[serde(skip)]
        payload: Vec<u8>,
    },
    /// XID_EVENT — transaction commit with XA transaction ID.
    Xid {
        /// The XID value.
        xid: u64,
    },
    /// Unrecognized or not-yet-parsed event type.
    Unknown {
        /// Raw type code.
        type_code: u8,
        /// Raw payload bytes.
        #[serde(skip)]
        payload: Vec<u8>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_type_roundtrip() {
        for code in 0..=41 {
            let t = BinlogEventType::from_u8(code);
            assert_eq!(t.type_code(), code);
        }
        // Unknown codes
        let t = BinlogEventType::from_u8(200);
        assert_eq!(t.type_code(), 200);
        assert!(matches!(t, BinlogEventType::Unknown(200)));
    }

    #[test]
    fn event_type_display() {
        assert_eq!(
            BinlogEventType::FormatDescription.to_string(),
            "FORMAT_DESCRIPTION"
        );
        assert_eq!(BinlogEventType::StopEvent.to_string(), "STOP");
        assert_eq!(BinlogEventType::Unknown(99).to_string(), "UNKNOWN(99)");
    }

    #[test]
    fn event_type_name() {
        assert_eq!(BinlogEventType::QueryEvent.name(), "QUERY");
        assert_eq!(BinlogEventType::TableMapEvent.name(), "TABLE_MAP");
        assert_eq!(BinlogEventType::WriteRowsEvent.name(), "WRITE_ROWS_V2");
        assert_eq!(BinlogEventType::GtidLogEvent.name(), "GTID");
        assert_eq!(BinlogEventType::HeartbeatEventV2.name(), "HEARTBEAT_V2");
    }

    #[test]
    fn parse_common_header() {
        let mut data = vec![0u8; 19];
        LittleEndian::write_u32(&mut data[0..], 1_700_000_000);
        data[4] = FORMAT_DESCRIPTION_EVENT;
        LittleEndian::write_u32(&mut data[5..], 42);
        LittleEndian::write_u32(&mut data[9..], 119);
        LittleEndian::write_u32(&mut data[13..], 123);
        LittleEndian::write_u16(&mut data[17..], 0x0001);

        let hdr = CommonEventHeader::parse(&data).unwrap();
        assert_eq!(hdr.timestamp, 1_700_000_000);
        assert_eq!(hdr.type_code, BinlogEventType::FormatDescription);
        assert_eq!(hdr.server_id, 42);
        assert_eq!(hdr.event_length, 119);
        assert_eq!(hdr.next_position, 123);
        assert_eq!(hdr.flags, 0x0001);
    }

    #[test]
    fn parse_common_header_too_short() {
        let data = vec![0u8; 18];
        assert!(CommonEventHeader::parse(&data).is_none());
    }

    #[test]
    fn payload_length_with_checksum() {
        let mut data = vec![0u8; 19];
        LittleEndian::write_u32(&mut data[9..], 100); // event_length = 100
        let hdr = CommonEventHeader::parse(&data).unwrap();

        // Without checksum: 100 - 19 = 81
        assert_eq!(hdr.payload_length(false), 81);
        // With checksum: 100 - 19 - 4 = 77
        assert_eq!(hdr.payload_length(true), 77);
    }
}
