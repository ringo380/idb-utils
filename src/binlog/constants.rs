//! MySQL binary log format constants.
//!
//! Defines magic bytes, field offsets, event type codes, flags, and checksum
//! algorithm identifiers for MySQL binary log files. All integer fields in
//! binlog events use **little-endian** byte order (unlike InnoDB pages which
//! use big-endian).
//!
//! Constants are derived from MySQL source `libbinlogevents/include/binlog_event.h`
//! and verified across MySQL 5.7, 8.0, 8.4, and 9.x.

// ---------------------------------------------------------------------------
// File header
// ---------------------------------------------------------------------------

/// Magic bytes at the start of every binlog file: `\xfebin`.
pub const BINLOG_MAGIC: [u8; 4] = [0xfe, 0x62, 0x69, 0x6e];

/// Size of the magic byte prefix in bytes.
pub const BINLOG_MAGIC_SIZE: usize = 4;

// ---------------------------------------------------------------------------
// Common event header (19 bytes, all fields little-endian)
// ---------------------------------------------------------------------------

/// Offset of the timestamp field (u32, seconds since Unix epoch).
pub const EVENT_TIMESTAMP_OFFSET: usize = 0;

/// Offset of the event type code (u8).
pub const EVENT_TYPE_OFFSET: usize = 4;

/// Offset of the originating server ID (u32).
pub const EVENT_SERVER_ID_OFFSET: usize = 5;

/// Offset of the total event length (u32, includes header + payload + checksum).
pub const EVENT_LENGTH_OFFSET: usize = 9;

/// Offset of the next event position (u32, absolute file offset).
pub const EVENT_NEXT_POSITION_OFFSET: usize = 13;

/// Offset of the event flags (u16).
pub const EVENT_FLAGS_OFFSET: usize = 17;

/// Size of the common event header in bytes.
pub const COMMON_HEADER_SIZE: usize = 19;

// ---------------------------------------------------------------------------
// FORMAT_DESCRIPTION_EVENT payload offsets (relative to payload start)
// ---------------------------------------------------------------------------

/// Offset of the binlog format version (u16) within the FDE payload.
pub const FDE_BINLOG_VERSION_OFFSET: usize = 0;

/// Offset of the server version string (50 bytes, null-padded ASCII).
pub const FDE_SERVER_VERSION_OFFSET: usize = 2;

/// Length of the server version string field.
pub const FDE_SERVER_VERSION_LEN: usize = 50;

/// Offset of the creation timestamp (u32) within the FDE payload.
pub const FDE_CREATE_TIMESTAMP_OFFSET: usize = 52;

/// Offset of the common header length byte (u8, should be 19 for v4).
pub const FDE_HEADER_LENGTH_OFFSET: usize = 56;

/// Offset of the post-header-lengths array within the FDE payload.
pub const FDE_POST_HEADER_LENGTHS_OFFSET: usize = 57;

// ---------------------------------------------------------------------------
// ROTATE_EVENT payload offsets
// ---------------------------------------------------------------------------

/// Offset of the next binlog start position (u64) within ROTATE_EVENT payload.
pub const ROTATE_POSITION_OFFSET: usize = 0;

/// Offset of the next binlog filename within ROTATE_EVENT payload.
pub const ROTATE_FILENAME_OFFSET: usize = 8;

// ---------------------------------------------------------------------------
// Event type codes (u8) — from `enum Log_event_type` in binlog_event.h
// ---------------------------------------------------------------------------

/// Unknown or invalid event type.
pub const UNKNOWN_EVENT: u8 = 0;
/// Start event (binlog format v1-v3, replaced by FORMAT_DESCRIPTION_EVENT in v4).
pub const START_EVENT_V3: u8 = 1;
/// SQL query execution event.
pub const QUERY_EVENT: u8 = 2;
/// Server shutdown event.
pub const STOP_EVENT: u8 = 3;
/// Binlog file rotation event.
pub const ROTATE_EVENT: u8 = 4;
/// Integer session variable.
pub const INTVAR_EVENT: u8 = 5;
/// Random seed event for RAND().
pub const RAND_EVENT: u8 = 13;
/// User-defined variable event.
pub const USER_VAR_EVENT: u8 = 14;
/// Format description event (binlog v4 header, always first real event).
pub const FORMAT_DESCRIPTION_EVENT: u8 = 15;
/// XA transaction commit event (contains XID).
pub const XID_EVENT: u8 = 16;
/// Table map event (row-based replication).
pub const TABLE_MAP_EVENT: u8 = 19;
/// Write (INSERT) rows event, v1 format.
pub const WRITE_ROWS_EVENT_V1: u8 = 23;
/// Update rows event, v1 format.
pub const UPDATE_ROWS_EVENT_V1: u8 = 24;
/// Delete rows event, v1 format.
pub const DELETE_ROWS_EVENT_V1: u8 = 25;
/// Write (INSERT) rows event, v2 format (MySQL 5.6+).
pub const WRITE_ROWS_EVENT: u8 = 30;
/// Update rows event, v2 format (MySQL 5.6+).
pub const UPDATE_ROWS_EVENT: u8 = 31;
/// Delete rows event, v2 format (MySQL 5.6+).
pub const DELETE_ROWS_EVENT: u8 = 32;
/// GTID event (MySQL 5.6+).
pub const GTID_LOG_EVENT: u8 = 33;
/// Anonymous GTID event (MySQL 5.6+).
pub const ANONYMOUS_GTID_LOG_EVENT: u8 = 34;
/// Previous GTIDs event (MySQL 5.6+).
pub const PREVIOUS_GTIDS_LOG_EVENT: u8 = 35;

// ---------------------------------------------------------------------------
// Event flags
// ---------------------------------------------------------------------------

/// Flag indicating the binlog file is in use (not cleanly closed).
pub const LOG_EVENT_BINLOG_IN_USE_F: u16 = 0x0001;

// ---------------------------------------------------------------------------
// Checksum algorithm identifiers
// ---------------------------------------------------------------------------

/// No event checksums.
pub const BINLOG_CHECKSUM_ALG_OFF: u8 = 0;

/// CRC-32 event checksums (default since MySQL 5.6.6).
pub const BINLOG_CHECKSUM_ALG_CRC32: u8 = 1;

/// Size of the CRC-32 checksum appended to each event (when enabled).
pub const BINLOG_CHECKSUM_LEN: usize = 4;
