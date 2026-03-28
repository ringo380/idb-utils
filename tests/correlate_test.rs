//! Integration tests for the binlog-to-page correlation API.

use byteorder::{BigEndian, ByteOrder, LittleEndian};

use idb::binlog::correlate::{correlate_events, RowEventType};
use idb::binlog::BinlogFile;
use idb::innodb::tablespace::Tablespace;

// ── Helpers ─────────────────────────────────────────────────────────────

/// Build a minimal valid .ibd tablespace (page 0 only, no SDI).
///
/// `correlate_events` should return `Ok(vec![])` for a tablespace without SDI.
fn make_minimal_tablespace() -> Vec<u8> {
    let page_size = 16384usize;
    let mut buf = vec![0u8; page_size];

    // FIL header: page_number = 0
    BigEndian::write_u32(&mut buf[4..], 0); // page_number
    BigEndian::write_u16(&mut buf[24..], 8); // page_type = FSP_HDR
    BigEndian::write_u32(&mut buf[34..], 1); // space_id = 1

    // FSP header flags at offset 54 (FIL_PAGE_DATA=38 + 16): default page size
    BigEndian::write_u32(&mut buf[54..], 0); // flags = 0 (default 16K)

    // FIL trailer: set LSN low 32 bits
    BigEndian::write_u32(&mut buf[page_size - 4..], 0);

    // Write CRC-32C checksum
    let crc1 = crc32c::crc32c(&buf[4..26]);
    let crc2 = crc32c::crc32c(&buf[38..page_size - 8]);
    let checksum = crc1 ^ crc2;
    BigEndian::write_u32(&mut buf[0..], checksum);

    buf
}

/// Build a minimal valid binlog file (magic + FORMAT_DESCRIPTION_EVENT only).
fn make_minimal_binlog() -> Vec<u8> {
    let mut buf = Vec::new();

    // Magic bytes
    buf.extend_from_slice(&[0xfe, 0x62, 0x69, 0x6e]);

    // FORMAT_DESCRIPTION_EVENT (type 15)
    let fde_data_len = 57; // minimum FDE payload
    let event_len = 19 + fde_data_len; // header + payload (no checksum for simplicity)

    // Common header (19 bytes)
    let mut header = vec![0u8; 19];
    LittleEndian::write_u32(&mut header[0..], 1_700_000_000); // timestamp
    header[4] = 15; // type_code = FORMAT_DESCRIPTION_EVENT
    LittleEndian::write_u32(&mut header[5..], 1); // server_id
    LittleEndian::write_u32(&mut header[9..], event_len as u32); // event_length
    LittleEndian::write_u32(&mut header[13..], (4 + event_len) as u32); // next_position
    LittleEndian::write_u16(&mut header[17..], 0); // flags
    buf.extend_from_slice(&header);

    // FDE payload
    let mut fde = vec![0u8; fde_data_len];
    LittleEndian::write_u16(&mut fde[0..], 4); // binlog_version = 4
    // server_version: "8.0.35" (50 bytes, null-padded)
    fde[2..8].copy_from_slice(b"8.0.35");
    LittleEndian::write_u32(&mut fde[52..], 1_700_000_000); // create_timestamp
    fde[56] = 19; // header_length = 19
    buf.extend_from_slice(&fde);

    buf
}

// ── Tests ───────────────────────────────────────────────────────────────

#[test]
fn correlate_events_returns_empty_for_no_sdi() {
    let ts_data = make_minimal_tablespace();
    let binlog_data = make_minimal_binlog();

    let mut ts = Tablespace::from_bytes(ts_data).unwrap();
    let mut binlog = BinlogFile::from_bytes(binlog_data).unwrap();

    let result = correlate_events(&mut binlog, &mut ts).unwrap();
    assert!(result.is_empty(), "Expected empty results for tablespace without SDI");
}

#[test]
fn row_event_type_variants() {
    assert_eq!(RowEventType::from_type_code(30), Some(RowEventType::Insert));
    assert_eq!(RowEventType::from_type_code(31), Some(RowEventType::Update));
    assert_eq!(RowEventType::from_type_code(32), Some(RowEventType::Delete));
    assert_eq!(RowEventType::from_type_code(15), None);

    assert_eq!(format!("{}", RowEventType::Insert), "INSERT");
    assert_eq!(format!("{}", RowEventType::Update), "UPDATE");
    assert_eq!(format!("{}", RowEventType::Delete), "DELETE");
}
