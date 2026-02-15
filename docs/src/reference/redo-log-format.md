# Redo Log Format

InnoDB redo logs record all modifications to tablespace pages, enabling crash recovery. The logs are organized as a sequence of 512-byte blocks.

## File Layout

| Block | Purpose |
|-------|---------|
| 0 | File header |
| 1 | Checkpoint 1 |
| 2 | Reserved |
| 3 | Checkpoint 2 |
| 4+ | Data blocks containing log records |

## File Header (Block 0)

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | Group ID | Redo log group identifier |
| 4 | 8 | Start LSN | LSN of the first log record in this file |
| 12 | 4 | File number | Sequence number within the redo log group |
| 16 | 32 | Creator string | Server version and vendor (e.g., "MySQL 8.0.32", "Percona XtraDB 8.0.35") |

The creator string is particularly useful for [vendor detection](../guides/vendor-support.md) -- it identifies whether the redo log was created by MySQL, Percona XtraDB, or MariaDB.

## Checkpoint Blocks (Blocks 1 and 3)

Two checkpoint blocks provide redundancy. InnoDB alternates between them, so at least one is always valid after a crash.

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | Checkpoint number | Monotonically increasing checkpoint counter |
| 8 | 8 | LSN | LSN up to which all changes are flushed to tablespace files |
| 16 | 4 | Byte offset | Offset within the log file for this checkpoint |
| 20 | 4 | Log buffer size | Size of the log buffer in bytes |
| 24 | 8 | Archived LSN | LSN up to which logs have been archived (0 if archiving disabled) |

The checkpoint with the higher checkpoint number is the most recent. During crash recovery, InnoDB replays all log records from the checkpoint LSN forward.

## Data Block Header (14 bytes)

Each data block (block 4 and beyond) starts with a 14-byte header:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | Block number | Sequential block number; bit 31 is the flush flag (1 = first block in a flush batch) |
| 4 | 2 | Data length | Number of bytes of log data in this block (max 494) |
| 6 | 2 | First record group offset | Offset to the first complete log record group starting in this block |
| 8 | 4 | Checkpoint number | Checkpoint number when this block was written |
| 12 | 2 | Padding | Reserved bytes |

## Data Block Checksum

Each block ends with a 4-byte CRC-32C checksum:

- Computed over bytes `[0..508)` of the block
- Stored at bytes `[508..512)`

`inno log` validates block checksums and reports any corruption.

## Block Capacity

Each 512-byte block carries at most 494 bytes of log record data:

```text
[header: 14 bytes][log data: up to 494 bytes][checksum: 4 bytes]
 0             13  14                    507  508           511
```

Log records can span multiple blocks. The data length field indicates how many bytes of the 494-byte payload contain actual log data (the rest is padding).

## File Formats

### Legacy Format (MySQL < 8.0.30)

- Files: `ib_logfile0`, `ib_logfile1` (or more, controlled by `innodb_log_files_in_group`)
- Fixed-size files, pre-allocated at server startup
- Location: MySQL data directory root

### New Format (MySQL 8.0.30+)

- Files: `#innodb_redo/#ib_redo*` (numbered sequentially)
- Dynamic file creation and removal
- Location: `#innodb_redo/` subdirectory within the data directory
- `inno log` supports both formats

## MLOG Record Types

Redo log records use type codes to identify the operation. Common types include:

| Type | Value | Description |
|------|-------|-------------|
| `MLOG_1BYTE` | 1 | Write 1 byte to a page |
| `MLOG_2BYTES` | 2 | Write 2 bytes to a page |
| `MLOG_4BYTES` | 4 | Write 4 bytes to a page |
| `MLOG_8BYTES` | 8 | Write 8 bytes to a page |
| `MLOG_REC_INSERT` | 9 | Insert a record (non-compact format) |
| `MLOG_REC_UPDATE_IN_PLACE` | 13 | Update a record in place |
| `MLOG_REC_DELETE` | 14 | Delete a record |
| `MLOG_PAGE_CREATE` | 16 | Create a page |
| `MLOG_UNDO_INSERT` | 20 | Insert an undo log record |
| `MLOG_INIT_FILE_PAGE` | 24 | Initialize a file page |
| `MLOG_COMP_REC_INSERT` | 38 | Insert a record (compact format) |
| `MLOG_COMP_REC_UPDATE_IN_PLACE` | 42 | Update a record in place (compact format) |
| `MLOG_COMP_REC_DELETE` | 43 | Delete a record (compact format) |
| `MLOG_COMP_PAGE_CREATE` | 44 | Create a page (compact format) |

The `MLOG_COMP_*` variants are used by the compact row format (MySQL 5.0+), while the non-compact versions correspond to the older redundant row format.

## Usage with inno

```bash
# Parse a legacy redo log
inno log -f /var/lib/mysql/ib_logfile0

# Parse a new-format redo log
inno log -f /var/lib/mysql/#innodb_redo/#ib_redo10

# Show specific blocks
inno log -f ib_logfile0 -b 0-10

# Skip empty blocks
inno log -f ib_logfile0 --no-empty

# Verbose output with block checksums
inno log -f ib_logfile0 -v

# JSON output
inno log -f ib_logfile0 --json
```
