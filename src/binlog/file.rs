//! Binlog file reader with event iteration.
//!
//! [`BinlogFile`] opens a MySQL binary log file, validates the 4-byte magic
//! header, reads the FORMAT_DESCRIPTION_EVENT, and provides random-access
//! event reading and sequential iteration.
//!
//! # Examples
//!
//! ```rust,ignore
//! use idb::binlog::BinlogFile;
//!
//! let mut binlog = BinlogFile::open("mysql-bin.000001")?;
//! println!("Server: {}", binlog.format_description().unwrap().server_version);
//!
//! for result in binlog.events() {
//!     let (offset, header, event) = result?;
//!     println!("{offset}: {}", header.type_code);
//! }
//! ```

use byteorder::{ByteOrder, LittleEndian};
use std::io::{Cursor, Read, Seek, SeekFrom};

use crate::IdbError;

use super::checksum::validate_event_checksum;
use super::constants::*;
use super::event::{BinlogEvent, BinlogEventType, CommonEventHeader};
use super::header::{FormatDescriptionEvent, RotateEvent};

/// Supertrait combining `Read + Seek` for type-erased readers.
trait ReadSeek: Read + Seek {}
impl<T: Read + Seek> ReadSeek for T {}

/// A MySQL binary log file reader.
///
/// Validates the magic bytes on open, parses the FORMAT_DESCRIPTION_EVENT,
/// and provides methods to read individual events or iterate through all of them.
pub struct BinlogFile {
    reader: Box<dyn ReadSeek>,
    file_size: u64,
    fde: Option<FormatDescriptionEvent>,
}

impl BinlogFile {
    /// Open a binlog file from the filesystem.
    ///
    /// Validates the 4-byte magic header and reads the FORMAT_DESCRIPTION_EVENT.
    pub fn open(path: &str) -> Result<Self, IdbError> {
        let file = std::fs::File::open(path)
            .map_err(|e| IdbError::Io(format!("cannot open {path}: {e}")))?;
        let metadata = file
            .metadata()
            .map_err(|e| IdbError::Io(format!("cannot stat {path}: {e}")))?;
        let file_size = metadata.len();

        let mut binlog = Self {
            reader: Box::new(file),
            file_size,
            fde: None,
        };
        binlog.read_header()?;
        Ok(binlog)
    }

    /// Create a binlog reader from in-memory bytes (useful for WASM).
    pub fn from_bytes(data: Vec<u8>) -> Result<Self, IdbError> {
        let file_size = data.len() as u64;
        let mut binlog = Self {
            reader: Box::new(Cursor::new(data)),
            file_size,
            fde: None,
        };
        binlog.read_header()?;
        Ok(binlog)
    }

    /// Total file size in bytes.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// The FORMAT_DESCRIPTION_EVENT parsed from the file header, if available.
    pub fn format_description(&self) -> Option<&FormatDescriptionEvent> {
        self.fde.as_ref()
    }

    /// Whether CRC-32 checksums are enabled for events in this file.
    pub fn has_checksum(&self) -> bool {
        self.fde.as_ref().is_some_and(|f| f.has_checksum())
    }

    /// Read and parse the event at the given absolute file offset.
    ///
    /// Returns `Ok(None)` if the offset is at or beyond EOF. Returns the
    /// common header and parsed event payload on success.
    pub fn read_event_at(
        &mut self,
        offset: u64,
    ) -> Result<Option<(CommonEventHeader, BinlogEvent)>, IdbError> {
        if offset >= self.file_size {
            return Ok(None);
        }

        self.reader
            .seek(SeekFrom::Start(offset))
            .map_err(|e| IdbError::Io(format!("seek to offset {offset}: {e}")))?;

        // Read common header
        let mut hdr_buf = [0u8; COMMON_HEADER_SIZE];
        if let Err(e) = self.reader.read_exact(&mut hdr_buf) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
            return Err(IdbError::Io(format!("read event header at {offset}: {e}")));
        }

        let header = CommonEventHeader::parse(&hdr_buf)
            .ok_or_else(|| IdbError::Parse("invalid event header".into()))?;

        let event_len = header.event_length as usize;
        if event_len < COMMON_HEADER_SIZE {
            return Err(IdbError::Parse(format!(
                "event at offset {offset} has invalid length {event_len}"
            )));
        }

        // Read full event data (including header) for checksum validation
        let mut event_data = vec![0u8; event_len];
        event_data[..COMMON_HEADER_SIZE].copy_from_slice(&hdr_buf);
        if event_len > COMMON_HEADER_SIZE {
            self.reader
                .read_exact(&mut event_data[COMMON_HEADER_SIZE..])
                .map_err(|e| IdbError::Io(format!("read event body at {offset}: {e}")))?;
        }

        let checksum_enabled = self.has_checksum();

        // Extract payload (between header and checksum)
        let payload_end = if checksum_enabled {
            event_len.saturating_sub(BINLOG_CHECKSUM_LEN)
        } else {
            event_len
        };
        let payload = &event_data[COMMON_HEADER_SIZE..payload_end];

        let event = match header.type_code {
            BinlogEventType::FormatDescription => {
                // Bootstrap: when reading the FDE, self.fde is None so
                // checksum_enabled is false and the CRC-32 bytes (if present)
                // are still in `payload`. Try parsing with CRC stripped first
                // (most common case since MySQL 5.6.6+), then fall back to
                // parsing the full payload.
                let fde_with_crc = if !checksum_enabled
                    && event_len > COMMON_HEADER_SIZE + BINLOG_CHECKSUM_LEN
                {
                    let stripped = &event_data[COMMON_HEADER_SIZE..event_len - BINLOG_CHECKSUM_LEN];
                    FormatDescriptionEvent::parse(stripped).filter(|fde| fde.has_checksum())
                } else {
                    None
                };

                let parsed = fde_with_crc.or_else(|| FormatDescriptionEvent::parse(payload));

                match parsed {
                    Some(fde) => BinlogEvent::FormatDescription(fde),
                    None => BinlogEvent::Unknown {
                        type_code: header.type_code.type_code(),
                        payload: payload.to_vec(),
                    },
                }
            }
            BinlogEventType::RotateEvent => match RotateEvent::parse(payload) {
                Some(re) => BinlogEvent::Rotate(re),
                None => BinlogEvent::Unknown {
                    type_code: header.type_code.type_code(),
                    payload: payload.to_vec(),
                },
            },
            BinlogEventType::StopEvent => BinlogEvent::Stop,
            BinlogEventType::QueryEvent => BinlogEvent::Query {
                payload: payload.to_vec(),
            },
            BinlogEventType::XidEvent => {
                if payload.len() >= 8 {
                    let xid = LittleEndian::read_u64(payload);
                    BinlogEvent::Xid { xid }
                } else {
                    BinlogEvent::Unknown {
                        type_code: header.type_code.type_code(),
                        payload: payload.to_vec(),
                    }
                }
            }
            _ => BinlogEvent::Unknown {
                type_code: header.type_code.type_code(),
                payload: payload.to_vec(),
            },
        };

        Ok(Some((header, event)))
    }

    /// Return an iterator over all events in the file, starting after the magic bytes.
    ///
    /// Each item is `(file_offset, CommonEventHeader, BinlogEvent)`.
    pub fn events(&mut self) -> BinlogEventIterator<'_> {
        BinlogEventIterator {
            binlog: self,
            offset: BINLOG_MAGIC_SIZE as u64,
            done: false,
        }
    }

    /// Validate the CRC-32C checksum of the event at the given offset.
    ///
    /// Returns `None` if the event cannot be read, `Some(true)` if valid,
    /// `Some(false)` if the checksum does not match.
    pub fn validate_checksum_at(&mut self, offset: u64) -> Result<Option<bool>, IdbError> {
        if !self.has_checksum() {
            return Ok(None);
        }

        self.reader
            .seek(SeekFrom::Start(offset))
            .map_err(|e| IdbError::Io(format!("seek to {offset}: {e}")))?;

        // Read just the header to get event length
        let mut hdr_buf = [0u8; COMMON_HEADER_SIZE];
        self.reader
            .read_exact(&mut hdr_buf)
            .map_err(|e| IdbError::Io(format!("read header at {offset}: {e}")))?;

        let header = CommonEventHeader::parse(&hdr_buf)
            .ok_or_else(|| IdbError::Parse("invalid event header".into()))?;

        let event_len = header.event_length as usize;
        let mut event_data = vec![0u8; event_len];
        event_data[..COMMON_HEADER_SIZE].copy_from_slice(&hdr_buf);
        if event_len > COMMON_HEADER_SIZE {
            self.reader
                .read_exact(&mut event_data[COMMON_HEADER_SIZE..])
                .map_err(|e| IdbError::Io(format!("read event at {offset}: {e}")))?;
        }

        Ok(Some(validate_event_checksum(&event_data)))
    }

    /// Read and validate the magic bytes and FORMAT_DESCRIPTION_EVENT.
    fn read_header(&mut self) -> Result<(), IdbError> {
        self.reader
            .seek(SeekFrom::Start(0))
            .map_err(|e| IdbError::Io(format!("seek to start: {e}")))?;

        // Validate magic bytes
        let mut magic = [0u8; BINLOG_MAGIC_SIZE];
        self.reader
            .read_exact(&mut magic)
            .map_err(|e| IdbError::Io(format!("read magic bytes: {e}")))?;

        if magic != BINLOG_MAGIC {
            return Err(IdbError::Parse(format!(
                "invalid binlog magic bytes: expected {:02x?}, got {:02x?}",
                BINLOG_MAGIC, magic
            )));
        }

        // Read the first event (should be FORMAT_DESCRIPTION_EVENT)
        match self.read_event_at(BINLOG_MAGIC_SIZE as u64)? {
            Some((header, BinlogEvent::FormatDescription(fde))) => {
                if header.type_code != BinlogEventType::FormatDescription {
                    return Err(IdbError::Parse(
                        "first event is not FORMAT_DESCRIPTION_EVENT".into(),
                    ));
                }
                self.fde = Some(fde);
                Ok(())
            }
            Some((header, _)) => Err(IdbError::Parse(format!(
                "first event is {} (expected FORMAT_DESCRIPTION_EVENT)",
                header.type_code
            ))),
            None => Err(IdbError::Parse("no events after magic bytes".into())),
        }
    }
}

/// Iterator over binlog events.
///
/// Yields `(offset, CommonEventHeader, BinlogEvent)` for each event in the file.
pub struct BinlogEventIterator<'a> {
    binlog: &'a mut BinlogFile,
    offset: u64,
    done: bool,
}

impl<'a> Iterator for BinlogEventIterator<'a> {
    type Item = Result<(u64, CommonEventHeader, BinlogEvent), IdbError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let current_offset = self.offset;
        match self.binlog.read_event_at(current_offset) {
            Ok(Some((header, event))) => {
                // Advance to next event
                let next = header.next_position;
                if next == 0 || next as u64 <= current_offset {
                    // next_position == 0 means this is the last event (e.g. in relay logs)
                    // or next_position didn't advance — stop to avoid infinite loop
                    self.done = true;
                } else {
                    self.offset = next as u64;
                }
                Some(Ok((current_offset, header, event)))
            }
            Ok(None) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a synthetic binlog file with magic bytes and a FORMAT_DESCRIPTION_EVENT.
    ///
    /// The FDE has CRC-32 checksums enabled and contains a minimal post-header-lengths array.
    fn build_synthetic_binlog() -> Vec<u8> {
        let mut buf = Vec::new();

        // Magic bytes
        buf.extend_from_slice(&BINLOG_MAGIC);

        // Build FDE payload
        let mut fde_payload = vec![0u8; 59]; // 57 fixed + 1 post-header-length + 1 checksum algo
        LittleEndian::write_u16(&mut fde_payload[0..], 4); // binlog_version
        let ver = b"8.0.35";
        fde_payload[2..2 + ver.len()].copy_from_slice(ver);
        LittleEndian::write_u32(&mut fde_payload[52..], 1_700_000_000); // create_timestamp
        fde_payload[56] = 19; // header_length
        fde_payload[57] = 0; // one post-header-length entry
        fde_payload[58] = BINLOG_CHECKSUM_ALG_CRC32; // checksum algo

        let event_len = COMMON_HEADER_SIZE + fde_payload.len() + BINLOG_CHECKSUM_LEN;
        let next_pos = BINLOG_MAGIC_SIZE + event_len;

        // Build common header for FDE
        let mut hdr = vec![0u8; COMMON_HEADER_SIZE];
        LittleEndian::write_u32(&mut hdr[0..], 1_700_000_000); // timestamp
        hdr[4] = FORMAT_DESCRIPTION_EVENT; // type
        LittleEndian::write_u32(&mut hdr[5..], 1); // server_id
        LittleEndian::write_u32(&mut hdr[9..], event_len as u32); // event_length
        LittleEndian::write_u32(&mut hdr[13..], next_pos as u32); // next_position
        LittleEndian::write_u16(&mut hdr[17..], LOG_EVENT_BINLOG_IN_USE_F); // flags

        // Assemble event: header + payload
        let mut event = Vec::new();
        event.extend_from_slice(&hdr);
        event.extend_from_slice(&fde_payload);

        // Compute and append CRC-32C
        let crc = crc32c::crc32c(&event);
        let mut crc_bytes = [0u8; 4];
        LittleEndian::write_u32(&mut crc_bytes, crc);
        event.extend_from_slice(&crc_bytes);

        buf.extend_from_slice(&event);
        buf
    }

    /// Build a STOP_EVENT (no payload, just header + checksum).
    fn build_stop_event(offset: usize) -> Vec<u8> {
        let event_len = COMMON_HEADER_SIZE + BINLOG_CHECKSUM_LEN;
        let mut hdr = vec![0u8; COMMON_HEADER_SIZE];
        LittleEndian::write_u32(&mut hdr[0..], 1_700_000_001);
        hdr[4] = STOP_EVENT;
        LittleEndian::write_u32(&mut hdr[5..], 1);
        LittleEndian::write_u32(&mut hdr[9..], event_len as u32);
        LittleEndian::write_u32(&mut hdr[13..], 0); // 0 = last event

        let crc = crc32c::crc32c(&hdr);
        let mut crc_bytes = [0u8; 4];
        LittleEndian::write_u32(&mut crc_bytes, crc);

        let mut event = hdr;
        event.extend_from_slice(&crc_bytes);
        let _ = offset; // used for context only
        event
    }

    /// Build a ROTATE_EVENT.
    fn build_rotate_event(offset: usize, next_filename: &str) -> Vec<u8> {
        let mut payload = vec![0u8; 8 + next_filename.len()];
        LittleEndian::write_u64(&mut payload[0..], 4); // position
        payload[8..].copy_from_slice(next_filename.as_bytes());

        let event_len = COMMON_HEADER_SIZE + payload.len() + BINLOG_CHECKSUM_LEN;
        let mut hdr = vec![0u8; COMMON_HEADER_SIZE];
        LittleEndian::write_u32(&mut hdr[0..], 1_700_000_002);
        hdr[4] = ROTATE_EVENT;
        LittleEndian::write_u32(&mut hdr[5..], 1);
        LittleEndian::write_u32(&mut hdr[9..], event_len as u32);
        LittleEndian::write_u32(&mut hdr[13..], (offset + event_len) as u32);

        let mut event = Vec::new();
        event.extend_from_slice(&hdr);
        event.extend_from_slice(&payload);

        let crc = crc32c::crc32c(&event);
        let mut crc_bytes = [0u8; 4];
        LittleEndian::write_u32(&mut crc_bytes, crc);
        event.extend_from_slice(&crc_bytes);

        event
    }

    #[test]
    fn open_synthetic_binlog() {
        let data = build_synthetic_binlog();
        let binlog = BinlogFile::from_bytes(data).unwrap();

        assert!(binlog.has_checksum());
        let fde = binlog.format_description().unwrap();
        assert_eq!(fde.binlog_version, 4);
        assert_eq!(fde.server_version, "8.0.35");
        assert_eq!(fde.header_length, 19);
    }

    #[test]
    fn invalid_magic_bytes() {
        let data = vec![0u8; 100];
        match BinlogFile::from_bytes(data) {
            Err(IdbError::Parse(msg)) => assert!(msg.contains("magic bytes")),
            Ok(_) => panic!("expected Parse error, got Ok"),
            Err(e) => panic!("expected Parse error, got: {e}"),
        }
    }

    #[test]
    fn iterate_events() {
        let mut data = build_synthetic_binlog();

        // Append a ROTATE_EVENT
        let rotate_offset = data.len();
        let rotate = build_rotate_event(rotate_offset, "mysql-bin.000002");

        // Fix the FDE's next_position to point to the ROTATE_EVENT
        // (it's currently set, but let's verify by also updating)
        // Actually the synthetic builder already sets it correctly to the end of FDE,
        // but we appended the rotate after. We need to update the FDE next_pos.
        let fde_next_pos_offset = BINLOG_MAGIC_SIZE + EVENT_NEXT_POSITION_OFFSET;
        LittleEndian::write_u32(&mut data[fde_next_pos_offset..], rotate_offset as u32);
        // Recompute FDE checksum
        let fde_event_start = BINLOG_MAGIC_SIZE;
        let fde_event_len =
            LittleEndian::read_u32(&data[fde_event_start + EVENT_LENGTH_OFFSET..]) as usize;
        let fde_crc_offset = fde_event_start + fde_event_len - BINLOG_CHECKSUM_LEN;
        let crc = crc32c::crc32c(&data[fde_event_start..fde_crc_offset]);
        LittleEndian::write_u32(&mut data[fde_crc_offset..], crc);

        data.extend_from_slice(&rotate);

        // Append a STOP_EVENT
        let stop_offset = data.len();
        let stop = build_stop_event(stop_offset);

        // Fix rotate's next_position to point to stop
        let rotate_next_pos_offset = rotate_offset + EVENT_NEXT_POSITION_OFFSET;
        LittleEndian::write_u32(&mut data[rotate_next_pos_offset..], stop_offset as u32);
        // Recompute rotate checksum
        let rotate_event_len =
            LittleEndian::read_u32(&data[rotate_offset + EVENT_LENGTH_OFFSET..]) as usize;
        let rotate_crc_offset = rotate_offset + rotate_event_len - BINLOG_CHECKSUM_LEN;
        let crc = crc32c::crc32c(&data[rotate_offset..rotate_crc_offset]);
        LittleEndian::write_u32(&mut data[rotate_crc_offset..], crc);

        data.extend_from_slice(&stop);

        let mut binlog = BinlogFile::from_bytes(data).unwrap();

        let events: Vec<_> = binlog.events().collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(events.len(), 3);

        // First: FDE
        assert_eq!(events[0].1.type_code, BinlogEventType::FormatDescription);
        assert!(matches!(events[0].2, BinlogEvent::FormatDescription(_)));

        // Second: ROTATE
        assert_eq!(events[1].1.type_code, BinlogEventType::RotateEvent);
        if let BinlogEvent::Rotate(ref re) = events[1].2 {
            assert_eq!(re.next_filename, "mysql-bin.000002");
            assert_eq!(re.position, 4);
        } else {
            panic!("expected Rotate event");
        }

        // Third: STOP
        assert_eq!(events[2].1.type_code, BinlogEventType::StopEvent);
        assert!(matches!(events[2].2, BinlogEvent::Stop));
    }

    #[test]
    fn validate_checksum_at_offset() {
        let data = build_synthetic_binlog();
        let mut binlog = BinlogFile::from_bytes(data).unwrap();

        let result = binlog
            .validate_checksum_at(BINLOG_MAGIC_SIZE as u64)
            .unwrap();
        assert_eq!(result, Some(true));
    }

    #[test]
    fn xid_event_parsing() {
        let mut data = build_synthetic_binlog();

        // Append a XID_EVENT
        let xid_offset = data.len();
        let xid_value: u64 = 42;
        let mut xid_payload = [0u8; 8];
        LittleEndian::write_u64(&mut xid_payload, xid_value);

        let event_len = COMMON_HEADER_SIZE + 8 + BINLOG_CHECKSUM_LEN;
        let mut hdr = vec![0u8; COMMON_HEADER_SIZE];
        LittleEndian::write_u32(&mut hdr[0..], 1_700_000_003);
        hdr[4] = XID_EVENT;
        LittleEndian::write_u32(&mut hdr[5..], 1);
        LittleEndian::write_u32(&mut hdr[9..], event_len as u32);
        LittleEndian::write_u32(&mut hdr[13..], 0); // last event

        let mut event = Vec::new();
        event.extend_from_slice(&hdr);
        event.extend_from_slice(&xid_payload);
        let crc = crc32c::crc32c(&event);
        let mut crc_bytes = [0u8; 4];
        LittleEndian::write_u32(&mut crc_bytes, crc);
        event.extend_from_slice(&crc_bytes);

        // Fix FDE's next_position
        let fde_next_pos_offset = BINLOG_MAGIC_SIZE + EVENT_NEXT_POSITION_OFFSET;
        LittleEndian::write_u32(&mut data[fde_next_pos_offset..], xid_offset as u32);
        let fde_event_start = BINLOG_MAGIC_SIZE;
        let fde_event_len =
            LittleEndian::read_u32(&data[fde_event_start + EVENT_LENGTH_OFFSET..]) as usize;
        let fde_crc_offset = fde_event_start + fde_event_len - BINLOG_CHECKSUM_LEN;
        let crc = crc32c::crc32c(&data[fde_event_start..fde_crc_offset]);
        LittleEndian::write_u32(&mut data[fde_crc_offset..], crc);

        data.extend_from_slice(&event);

        let mut binlog = BinlogFile::from_bytes(data).unwrap();
        let events: Vec<_> = binlog.events().collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(events.len(), 2);
        if let BinlogEvent::Xid { xid } = &events[1].2 {
            assert_eq!(*xid, 42);
        } else {
            panic!("expected Xid event");
        }
    }
}
