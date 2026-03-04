# inno binlog

Parse and analyze MySQL binary log files.

## Usage

```bash
# Text summary
inno binlog -f mysql-bin.000001

# JSON output
inno binlog -f mysql-bin.000001 --json

# Limit event listing
inno binlog -f mysql-bin.000001 -l 100

# Filter by event type
inno binlog -f mysql-bin.000001 --filter-type TABLE_MAP

# Verbose (show column types)
inno binlog -f mysql-bin.000001 -v
```

## Options

| Option | Description |
|--------|-------------|
| `-f, --file` | Path to MySQL binary log file |
| `-l, --limit` | Maximum number of events to display |
| `--filter-type` | Filter events by type name (e.g. `TABLE_MAP`, `WRITE_ROWS`) |
| `-v, --verbose` | Show additional detail (column types for TABLE_MAP events) |
| `--json` | Output in JSON format |

## Output

### Text Mode

Text output shows:

1. **Format Description** — server version, binlog version, creation timestamp, checksum algorithm.

2. **Event Type Distribution** — counts per event type, sorted by frequency.

3. **Table Maps** — databases and tables referenced in TABLE_MAP events with column counts.

4. **Event Listing** — chronological event table with offset, type, timestamp, server ID, and event length.

### JSON Mode

Returns a structured `BinlogAnalysis` object:

```json
{
  "format_description": {
    "binlog_version": 4,
    "server_version": "8.0.35",
    "create_timestamp": 1700000000,
    "header_length": 19,
    "checksum_alg": 1
  },
  "event_count": 1542,
  "event_type_counts": {
    "TABLE_MAP_EVENT": 200,
    "WRITE_ROWS_EVENT_V2": 180,
    "QUERY_EVENT": 150
  },
  "table_maps": [
    {
      "table_id": 108,
      "database_name": "mydb",
      "table_name": "users",
      "column_count": 5,
      "column_types": [3, 15, 15, 12, 3]
    }
  ],
  "events": [...]
}
```

## Event Types

The parser recognizes all standard MySQL binary log event types (0-40+). Key event types:

| Type Code | Name | Description |
|-----------|------|-------------|
| 2 | QUERY_EVENT | SQL statement (DDL or statement-based DML) |
| 15 | FORMAT_DESCRIPTION_EVENT | Binlog file header metadata |
| 16 | XID_EVENT | Transaction commit marker |
| 19 | TABLE_MAP_EVENT | Table schema mapping for row events |
| 30 | WRITE_ROWS_EVENT_V2 | Row-based INSERT data |
| 31 | UPDATE_ROWS_EVENT_V2 | Row-based UPDATE data (before/after) |
| 32 | DELETE_ROWS_EVENT_V2 | Row-based DELETE data |
| 33 | GTID_LOG_EVENT | Global Transaction ID |

## Background

MySQL binary logs record all data-modifying operations for replication and point-in-time recovery. Binary log files start with a 4-byte magic number (`\xfe\x62\x69\x6e`) followed by a `FORMAT_DESCRIPTION_EVENT`, then a sequence of events.

The `inno binlog` subcommand reads the file header, validates the magic bytes, parses the format description, then streams through all events to produce type distribution statistics and a detailed event listing. Row-based events are parsed to extract table mappings and row counts.

Binary log files use **little-endian** byte order, unlike InnoDB tablespace files which use big-endian.
