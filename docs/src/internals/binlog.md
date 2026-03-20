# Binary Log Format

MySQL binary logs record all data-modifying events for replication and point-in-time recovery. Unlike InnoDB tablespace files which use big-endian byte order, **all integer fields in binary logs are little-endian**.

## File Structure

Every binlog file begins with a 4-byte magic number followed by a sequence of events:

```text
+-------------------+  byte 0
| Magic: 0xFE 'bin' |  4 bytes
+-------------------+  byte 4
| FORMAT_DESCRIPTION |  First event (always)
+-------------------+
| Event 2            |
+-------------------+
| Event 3            |
+-------------------+
| ...                |
+-------------------+
| ROTATE / STOP      |  Last event (usually)
+-------------------+
```

The magic bytes `\xfebin` (`[0xfe, 0x62, 0x69, 0x6e]`) identify the file as a MySQL binary log.

## Common Event Header (19 bytes)

Every event starts with a 19-byte header. All fields are little-endian:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | timestamp | Seconds since Unix epoch when the event was created |
| 4 | 1 | type_code | Event type identifier (see table below) |
| 5 | 4 | server_id | Originating server's `server_id` value |
| 9 | 4 | event_length | Total event size including header, payload, and checksum |
| 13 | 4 | next_position | Absolute file offset of the next event |
| 17 | 2 | flags | Event flags (bit 0 = binlog in use / not cleanly closed) |

The `next_position` field enables sequential scanning: read the header, then seek to `next_position` for the next event.

## Event Types

The type code byte identifies the event kind. Key types for analysis:

| Code | Name | Description |
|------|------|-------------|
| 2 | QUERY_EVENT | SQL statement execution |
| 4 | ROTATE_EVENT | Points to the next binlog file |
| 15 | FORMAT_DESCRIPTION_EVENT | Binlog metadata (always first real event) |
| 16 | XID_EVENT | Transaction commit (XA transaction ID) |
| 19 | TABLE_MAP_EVENT | Maps table ID to schema/table name |
| 30 | WRITE_ROWS_EVENT | Row insert (row-based replication) |
| 31 | UPDATE_ROWS_EVENT | Row update (row-based replication) |
| 32 | DELETE_ROWS_EVENT | Row delete (row-based replication) |
| 33 | GTID_LOG_EVENT | Global Transaction ID |
| 39 | PARTIAL_UPDATE_ROWS_EVENT | Partial JSON update (MySQL 8.0+) |
| 40 | TRANSACTION_PAYLOAD_EVENT | Compressed transaction (MySQL 8.0.20+) |

A complete list of all 41 named event types is defined in `src/binlog/constants.rs`, derived from MySQL's `binlog_event.h`.

## FORMAT_DESCRIPTION_EVENT

The first event after the magic bytes is always a FORMAT_DESCRIPTION_EVENT (FDE). It describes the binlog format version and the server that created the file:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 2 | binlog_version | Format version (4 for all modern MySQL) |
| 2 | 50 | server_version | Null-padded ASCII server version string |
| 52 | 4 | create_timestamp | Timestamp when the binlog was created |
| 56 | 1 | header_length | Common header length (always 19 for v4) |
| 57 | N | post_header_lengths | Array of per-event-type post-header sizes |

The FDE also implicitly tells the parser whether CRC-32C checksums are present. Since `binlog_checksum=CRC32` is the default from MySQL 5.6.6 onward, the parser must handle both cases. `inno binlog` auto-detects checksum presence by attempting to parse the FDE with and without the trailing 4-byte CRC.

## ROTATE_EVENT

A ROTATE_EVENT appears at the end of a binlog file (or during live rotation) and points to the next file in the sequence:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | position | Start position in the next binlog file |
| 8 | N | filename | Name of the next binlog file (variable length) |

## Row-Based Replication Events

Row-based replication (the default since MySQL 5.7.7) records actual row data rather than SQL statements.

### TABLE_MAP_EVENT

Before any row events, a TABLE_MAP_EVENT maps a numeric table ID to a database and table name. It includes the column count and column type descriptors, enabling the row events that follow to be decoded.

### Row Events (WRITE/UPDATE/DELETE)

Row events reference the table ID from the preceding TABLE_MAP_EVENT and contain:

- A bitmap of columns present in the row image
- For UPDATE events: a "before" bitmap and an "after" bitmap
- The actual row data encoded according to InnoDB's column type rules

Row events use v2 format (types 30-32) in MySQL 5.6+, which supports extra data fields for partial row images.

## Event Checksums

Since MySQL 5.6.6, each event ends with a 4-byte CRC-32C checksum (when `binlog_checksum=CRC32`, which is the default):

```text
[common header: 19 bytes][payload: N bytes][CRC-32C: 4 bytes]
```

The checksum covers all bytes from the start of the event through the end of the payload (everything except the checksum itself). The `event_length` field in the header **includes** the 4-byte checksum.

## Byte Order Difference

This is worth emphasizing: binary logs use **little-endian** byte order for all integer fields. This is the opposite of InnoDB tablespace files, which use big-endian. The `src/binlog/` module uses `byteorder::LittleEndian` throughout, while `src/innodb/` uses `byteorder::BigEndian`.

## Inspecting Binary Logs with inno

```bash
# Parse a binary log file
inno binlog -f mysql-bin.000001

# Verbose output with event details
inno binlog -f mysql-bin.000001 -v

# JSON output
inno binlog -f mysql-bin.000001 --json
```

## Source Reference

- Constants and event codes: `src/binlog/constants.rs`
- Event types and common header: `src/binlog/event.rs` -- `BinlogEventType`, `CommonEventHeader`
- FDE and ROTATE parsing: `src/binlog/header.rs` -- `FormatDescriptionEvent`, `RotateEvent`
- Checksum validation: `src/binlog/checksum.rs`
- File reader and iteration: `src/binlog/file.rs` -- `BinlogFile`
- MySQL source: `libbinlogevents/include/binlog_event.h`, `sql/binlog/event/event_reader.cpp`
