//! Binary log file header and format description event parsing.
//!
//! MySQL binary logs start with a 4-byte magic number (`\xfe\x62\x69\x6e`)
//! followed by a Format Description Event that describes the binlog version,
//! server version, and event header length.

use byteorder::{ByteOrder, LittleEndian};
use serde::Serialize;

/// Binary log magic number bytes.
pub const BINLOG_MAGIC: [u8; 4] = [0xfe, 0x62, 0x69, 0x6e];

/// Standard binlog event header size (v4 format).
pub const BINLOG_EVENT_HEADER_SIZE: usize = 19;

/// Validate that the first 4 bytes match the binlog magic number.
///
/// # Examples
///
/// ```
/// use idb::binlog::header::validate_binlog_magic;
///
/// assert!(validate_binlog_magic(&[0xfe, 0x62, 0x69, 0x6e]));
/// assert!(!validate_binlog_magic(&[0x00, 0x00, 0x00, 0x00]));
/// assert!(!validate_binlog_magic(&[0xfe, 0x62])); // too short
/// ```
pub fn validate_binlog_magic(data: &[u8]) -> bool {
    data.len() >= 4 && data[..4] == BINLOG_MAGIC
}

/// Parsed binlog event header (19 bytes, common to all events).
///
/// # Examples
///
/// ```
/// use idb::binlog::header::BinlogEventHeader;
/// use byteorder::{LittleEndian, ByteOrder};
///
/// let mut buf = vec![0u8; 19];
/// LittleEndian::write_u32(&mut buf[0..], 1700000000); // timestamp
/// buf[4] = 15; // FORMAT_DESCRIPTION_EVENT
/// LittleEndian::write_u32(&mut buf[5..], 1); // server_id
/// LittleEndian::write_u32(&mut buf[9..], 100); // event_length
/// LittleEndian::write_u32(&mut buf[13..], 119); // next_position
/// LittleEndian::write_u16(&mut buf[17..], 0); // flags
///
/// let hdr = BinlogEventHeader::parse(&buf).unwrap();
/// assert_eq!(hdr.timestamp, 1700000000);
/// assert_eq!(hdr.type_code, 15);
/// assert_eq!(hdr.server_id, 1);
/// assert_eq!(hdr.event_length, 100);
/// assert_eq!(hdr.next_position, 119);
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct BinlogEventHeader {
    /// Unix timestamp of the event.
    pub timestamp: u32,
    /// Event type code.
    pub type_code: u8,
    /// Server ID that produced the event.
    pub server_id: u32,
    /// Total length of the event (header + data + checksum).
    pub event_length: u32,
    /// Position of the next event in the binlog file.
    pub next_position: u32,
    /// Event flags.
    pub flags: u16,
}

impl BinlogEventHeader {
    /// Parse a binlog event header from at least 19 bytes.
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < BINLOG_EVENT_HEADER_SIZE {
            return None;
        }

        Some(BinlogEventHeader {
            timestamp: LittleEndian::read_u32(&data[0..]),
            type_code: data[4],
            server_id: LittleEndian::read_u32(&data[5..]),
            event_length: LittleEndian::read_u32(&data[9..]),
            next_position: LittleEndian::read_u32(&data[13..]),
            flags: LittleEndian::read_u16(&data[17..]),
        })
    }
}

/// Parsed Format Description Event (type 15).
///
/// The first real event in any v4 binlog file. Describes the binlog format
/// version, server version string, creation timestamp, and event header length.
///
/// # Examples
///
/// ```
/// use idb::binlog::header::FormatDescriptionEvent;
/// use byteorder::{LittleEndian, ByteOrder};
///
/// let mut buf = vec![0u8; 100];
/// // binlog_version = 4
/// LittleEndian::write_u16(&mut buf[0..], 4);
/// // server_version (50 bytes, null-padded)
/// let ver = b"8.0.35";
/// buf[2..2 + ver.len()].copy_from_slice(ver);
/// // create_timestamp
/// LittleEndian::write_u32(&mut buf[52..], 1700000000);
/// // header_length
/// buf[56] = 19;
///
/// let fde = FormatDescriptionEvent::parse(&buf).unwrap();
/// assert_eq!(fde.binlog_version, 4);
/// assert_eq!(fde.server_version, "8.0.35");
/// assert_eq!(fde.create_timestamp, 1700000000);
/// assert_eq!(fde.header_length, 19);
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct FormatDescriptionEvent {
    /// Binlog format version (always 4 for MySQL 5.0+).
    pub binlog_version: u16,
    /// Server version string (e.g. "8.0.35").
    pub server_version: String,
    /// Timestamp when the binlog was created.
    pub create_timestamp: u32,
    /// Length of each event header (19 for v4).
    pub header_length: u8,
    /// Checksum algorithm (0 = none, 1 = CRC32).
    pub checksum_alg: u8,
}

impl FormatDescriptionEvent {
    /// Parse a Format Description Event from the event data (after the 19-byte common header).
    pub fn parse(data: &[u8]) -> Option<Self> {
        // Minimum: 2 (version) + 50 (server_version) + 4 (timestamp) + 1 (header_length) = 57
        if data.len() < 57 {
            return None;
        }

        let binlog_version = LittleEndian::read_u16(&data[0..]);

        // Server version is 50 bytes, null-terminated
        let ver_bytes = &data[2..52];
        let server_version = std::str::from_utf8(ver_bytes)
            .unwrap_or("")
            .trim_end_matches('\0')
            .to_string();

        let create_timestamp = LittleEndian::read_u32(&data[52..]);
        let header_length = data[56];

        // Checksum algorithm is the last byte before the 4-byte checksum
        // In FDE, it's at a variable position based on the post-header lengths array.
        // For simplicity, try to read it from the end of the data.
        let checksum_alg = if data.len() >= 58 {
            // After header_length byte, there's a variable-length array of post-header
            // lengths. The checksum_alg byte is at the end of this array.
            // For FDE v4, it's typically at offset 57 + (number_of_event_types)
            // A reasonable heuristic: last 5 bytes are [checksum_alg, crc32_checksum(4)]
            if data.len() >= 5 {
                data[data.len() - 5]
            } else {
                0
            }
        } else {
            0
        };

        Some(FormatDescriptionEvent {
            binlog_version,
            server_version,
            create_timestamp,
            header_length,
            checksum_alg,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binlog_magic_valid() {
        assert!(validate_binlog_magic(&BINLOG_MAGIC));
        assert!(validate_binlog_magic(&[0xfe, 0x62, 0x69, 0x6e, 0x00]));
    }

    #[test]
    fn test_binlog_magic_invalid() {
        assert!(!validate_binlog_magic(&[0x00, 0x00, 0x00, 0x00]));
        assert!(!validate_binlog_magic(&[0xfe, 0x62])); // too short
        assert!(!validate_binlog_magic(&[]));
    }

    #[test]
    fn test_binlog_event_header_parse() {
        let mut buf = vec![0u8; 19];
        LittleEndian::write_u32(&mut buf[0..], 1700000000);
        buf[4] = 15; // FORMAT_DESCRIPTION_EVENT
        LittleEndian::write_u32(&mut buf[5..], 1);
        LittleEndian::write_u32(&mut buf[9..], 100);
        LittleEndian::write_u32(&mut buf[13..], 119);
        LittleEndian::write_u16(&mut buf[17..], 0);

        let hdr = BinlogEventHeader::parse(&buf).unwrap();
        assert_eq!(hdr.timestamp, 1700000000);
        assert_eq!(hdr.type_code, 15);
        assert_eq!(hdr.server_id, 1);
        assert_eq!(hdr.event_length, 100);
        assert_eq!(hdr.next_position, 119);
        assert_eq!(hdr.flags, 0);
    }

    #[test]
    fn test_binlog_event_header_too_short() {
        let buf = vec![0u8; 10];
        assert!(BinlogEventHeader::parse(&buf).is_none());
    }

    #[test]
    fn test_format_description_event_parse() {
        let mut buf = vec![0u8; 100];
        LittleEndian::write_u16(&mut buf[0..], 4);
        let ver = b"8.0.35";
        buf[2..2 + ver.len()].copy_from_slice(ver);
        LittleEndian::write_u32(&mut buf[52..], 1700000000);
        buf[56] = 19;
        // checksum_alg at buf[95] (100 - 5)
        buf[95] = 1; // CRC32

        let fde = FormatDescriptionEvent::parse(&buf).unwrap();
        assert_eq!(fde.binlog_version, 4);
        assert_eq!(fde.server_version, "8.0.35");
        assert_eq!(fde.create_timestamp, 1700000000);
        assert_eq!(fde.header_length, 19);
        assert_eq!(fde.checksum_alg, 1);
    }

    #[test]
    fn test_format_description_event_too_short() {
        let buf = vec![0u8; 30];
        assert!(FormatDescriptionEvent::parse(&buf).is_none());
    }
}
