# Binary Log Parsing

This guide explains how to use `inno binlog` to analyze MySQL binary log files for replication debugging, forensic analysis, and data change auditing.

## When to Use

- **Replication debugging** — inspect binary log events to diagnose replication lag or errors
- **Change auditing** — review which tables were modified and how many rows were affected
- **Forensic analysis** — trace data changes by timestamp, server ID, and transaction
- **Format validation** — verify binary log integrity and event structure

## Binary Log Structure

MySQL binary log files have a simple structure:

```text
┌──────────────────────────────────────┐
│ Magic bytes: 0xfe 0x62 0x69 0x6e     │  4 bytes
├──────────────────────────────────────┤
│ FORMAT_DESCRIPTION_EVENT             │  Variable length
├──────────────────────────────────────┤
│ Event 1                              │
├──────────────────────────────────────┤
│ Event 2                              │
├──────────────────────────────────────┤
│ ...                                  │
└──────────────────────────────────────┘
```

Each event starts with a 19-byte common header containing timestamp, type code, server ID, event length, and next position.

## Basic Analysis

```bash
inno binlog -f mysql-bin.000001
```

This shows the format description, event type distribution, table maps, and a chronological event listing.

## Filtering Events

### By Type

```bash
# Show only TABLE_MAP events
inno binlog -f mysql-bin.000001 --filter-type TABLE_MAP

# Show only row insert events
inno binlog -f mysql-bin.000001 --filter-type WRITE_ROWS
```

Type names are matched as substrings, so `ROWS` matches all row event types.

### By Count

```bash
# Show first 50 events
inno binlog -f mysql-bin.000001 -l 50
```

## JSON Analysis

```bash
# Event type distribution
inno binlog -f mysql-bin.000001 --json | jq '.event_type_counts'

# Tables with most row events
inno binlog -f mysql-bin.000001 --json | jq '[.table_maps[] | {db: .database_name, table: .table_name, cols: .column_count}]'

# Server version
inno binlog -f mysql-bin.000001 --json | jq '.format_description.server_version'
```

## Verbose Mode

```bash
inno binlog -f mysql-bin.000001 -v
```

Verbose mode adds column type codes for TABLE_MAP events, which helps identify the column layout used in row events.

## Key Event Types

### Transaction Flow

A typical row-based transaction produces this sequence:

```text
GTID_LOG_EVENT          → Transaction identifier
QUERY_EVENT             → BEGIN
TABLE_MAP_EVENT         → Table schema for following row events
WRITE_ROWS_EVENT_V2     → INSERT data
TABLE_MAP_EVENT         → Table schema (may differ for UPDATE)
UPDATE_ROWS_EVENT_V2    → UPDATE before/after images
XID_EVENT               → COMMIT
```

### DDL Events

DDL statements appear as QUERY_EVENT with the full SQL statement. They are not wrapped in BEGIN/COMMIT.

## Web UI

The web analyzer can also parse binary log files. Drop a binary log file onto the dropzone and it will auto-detect the file type from the magic bytes, showing a dedicated Binary Log tab with event listing, type distribution, and table maps.

## Technical Notes

- Binary logs use **little-endian** byte order (unlike InnoDB tablespace pages which use big-endian)
- The `--filter-type` flag matches against the string representation of event types
- Event timestamps are Unix timestamps (seconds since epoch)
- Row events reference TABLE_MAP events by `table_id` — the parser maintains an internal mapping
