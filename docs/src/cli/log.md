# inno log

Analyze InnoDB redo log files.

## Synopsis

```text
inno log -f <file> [-b <blocks>] [--no-empty] [-v] [--json]
```

## Description

Opens an InnoDB redo log file and displays its internal structure. Supports both legacy and modern redo log formats:

- **Legacy format** (MySQL < 8.0.30): `ib_logfile0` and `ib_logfile1` files in the data directory.
- **Modern format** (MySQL 8.0.30+): `#ib_redo*` files in the `#innodb_redo/` subdirectory.

InnoDB redo logs are organized as a sequence of 512-byte blocks:

- **Block 0**: Log file header (group ID, start LSN, file number, creator string).
- **Block 1**: Checkpoint record 1 (checkpoint number, LSN, offset, buffer size, archived LSN).
- **Block 2**: Reserved/unused.
- **Block 3**: Checkpoint record 2.
- **Blocks 4+**: Data blocks containing the actual redo log records.

For each data block, the header is decoded to show the block number, data length, first-record-group offset, checkpoint number, flush flag, and CRC-32C checksum validation status.

With `--verbose`, the payload bytes of each non-empty data block are scanned for MLOG record type bytes (e.g., `MLOG_REC_INSERT`, `MLOG_UNDO_INSERT`, `MLOG_WRITE_STRING`) and a frequency summary is printed. MLOG record type decoding is skipped for MariaDB redo logs due to incompatible format.

Vendor detection is performed automatically from the log file header's creator string.

## Flags

| Flag | Short | Required | Default | Description |
|------|-------|----------|---------|-------------|
| `--file <path>` | `-f` | Yes | -- | Path to redo log file (`ib_logfile0`, `ib_logfile1`, or `#ib_redo*`). |
| `--blocks <count>` | `-b` | No | All data blocks | Limit output to the first N data blocks. |
| `--no-empty` | -- | No | Off | Skip blocks that contain no redo log data. |
| `--verbose` | `-v` | No | Off | Decode and display MLOG record types within each data block. |
| `--json` | -- | No | Off | Output in JSON format. |

## Examples

### Analyze a legacy redo log

```bash
inno log -f /var/lib/mysql/ib_logfile0
```

### Analyze a MySQL 8.0.30+ redo log

```bash
inno log -f '/var/lib/mysql/#innodb_redo/#ib_redo10'
```

### Show only the first 10 data blocks

```bash
inno log -f ib_logfile0 -b 10
```

### Skip empty blocks

```bash
inno log -f ib_logfile0 --no-empty
```

### Verbose output with MLOG record types

```bash
inno log -f ib_logfile0 -v
```

### JSON output

```bash
inno log -f ib_logfile0 --json | jq '.header'
```

## Output

### Text Mode

```text
InnoDB Redo Log File
  File:       ib_logfile0
  Size:       50331648 bytes
  Blocks:     98304 total (98300 data)

Log File Header (block 0)
  Group ID:   0
  Start LSN:  19217920
  File No:    0
  Created by: MySQL 8.0.40
  Vendor:     MySQL

Checkpoint 1 (block 1)
  Number:       42
  LSN:          19218432
  Offset:       1024
  Buffer size:  8388608

Checkpoint 2 (block 3)
  Number:       41
  LSN:          19218000
  Offset:       512
  Buffer size:  8388608

Data Blocks
  Block      4  no=4          len=492   first_rec=12    chk_no=42         csum=OK
  Block      5  no=5          len=512   first_rec=0     chk_no=42         csum=OK FLUSH
  Block      6  no=6          len=200   first_rec=12    chk_no=42         csum=OK

Displayed 3 data blocks (of 98300)
```

With `--verbose`, record type summaries appear under each data block:

```text
  Block      4  no=4          len=492   first_rec=12    chk_no=42         csum=OK
    record types: MLOG_COMP_REC_INSERT(12), MLOG_WRITE_STRING(8), MLOG_COMP_PAGE_CREATE(2)
```

### JSON Mode

```json
{
  "file": "ib_logfile0",
  "file_size": 50331648,
  "total_blocks": 98304,
  "data_blocks": 98300,
  "header": {
    "group_id": 0,
    "start_lsn": 19217920,
    "file_no": 0,
    "created_by": "MySQL 8.0.40"
  },
  "checkpoint_1": {
    "number": 42,
    "lsn": 19218432,
    "offset": 1024,
    "buf_size": 8388608,
    "archived_lsn": 0
  },
  "checkpoint_2": { "..." : "..." },
  "blocks": [
    {
      "block_index": 4,
      "block_no": 4,
      "flush_flag": false,
      "data_len": 492,
      "first_rec_group": 12,
      "checkpoint_no": 42,
      "checksum_valid": true,
      "record_types": ["MLOG_COMP_REC_INSERT", "MLOG_WRITE_STRING"]
    }
  ]
}
```
