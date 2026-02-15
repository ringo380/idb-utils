# Redo Logs

The `idb::innodb::log` module provides types and functions for reading InnoDB redo log files. These are `ib_logfile0`/`ib_logfile1` for MySQL versions before 8.0.30, or `#ib_redo*` numbered files for MySQL 8.0.30 and later.

## File Layout

An InnoDB redo log file consists of 512-byte blocks:

- **Block 0**: Log file header (group ID, start LSN, file number, creator string)
- **Block 1**: Checkpoint slot 0
- **Block 2**: Reserved
- **Block 3**: Checkpoint slot 1
- **Blocks 4+**: Data blocks containing redo log records

Each block has a 14-byte header and a 4-byte trailer (CRC-32C checksum at bytes 508-511).

## Opening a Redo Log

### From a file path

```rust,no_run
use idb::innodb::log::LogFile;

let mut log = LogFile::open("/var/lib/mysql/ib_logfile0").unwrap();
println!("File size: {} bytes", log.file_size());
println!("Total blocks: {}", log.block_count());
println!("Data blocks: {}", log.data_block_count());
```

`LogFile::open` is not available on `wasm32` targets.

### From an in-memory buffer

```rust,no_run
use idb::innodb::log::LogFile;

let data: Vec<u8> = std::fs::read("/var/lib/mysql/ib_logfile0").unwrap();
let mut log = LogFile::from_bytes(data).unwrap();
```

The file must be at least 2048 bytes (4 blocks of 512 bytes for the header and checkpoint areas).

## Reading the File Header

```rust,no_run
use idb::innodb::log::LogFile;

let mut log = LogFile::open("/var/lib/mysql/ib_logfile0").unwrap();
let header = log.read_header().unwrap();

println!("Group ID: {}", header.group_id);
println!("Start LSN: {}", header.start_lsn);
println!("File number: {}", header.file_no);
println!("Created by: {}", header.created_by);
```

### LogFileHeader Fields

| Field | Type | Description |
|-------|------|-------------|
| `group_id` | `u32` | Log group ID |
| `start_lsn` | `u64` | Start LSN of this log file |
| `file_no` | `u32` | File number within the log group |
| `created_by` | `String` | MySQL version string (e.g., "MySQL 8.0.32", "MariaDB 10.11.4") |

The `created_by` string can be used for vendor detection:

```rust,no_run
use idb::innodb::log::LogFile;
use idb::innodb::vendor::detect_vendor_from_created_by;

let mut log = LogFile::open("/var/lib/mysql/ib_logfile0").unwrap();
let header = log.read_header().unwrap();
let vendor = detect_vendor_from_created_by(&header.created_by);
println!("Vendor: {}", vendor);
```

## Reading Checkpoints

InnoDB maintains two checkpoint slots for crash recovery. Slot 0 is in block 1, slot 1 is in block 3.

```rust,no_run
use idb::innodb::log::LogFile;

let mut log = LogFile::open("/var/lib/mysql/ib_logfile0").unwrap();

let cp0 = log.read_checkpoint(0).unwrap();
println!("Checkpoint 0: number={}, LSN={}, offset={}, buf_size={}",
    cp0.number, cp0.lsn, cp0.offset, cp0.buf_size);

let cp1 = log.read_checkpoint(1).unwrap();
println!("Checkpoint 1: number={}, LSN={}, offset={}, buf_size={}",
    cp1.number, cp1.lsn, cp1.offset, cp1.buf_size);
```

### LogCheckpoint Fields

| Field | Type | Description |
|-------|------|-------------|
| `number` | `u64` | Checkpoint sequence number |
| `lsn` | `u64` | LSN at the time of this checkpoint |
| `offset` | `u32` | Byte offset of the checkpoint within the log file |
| `buf_size` | `u32` | Log buffer size at checkpoint time |
| `archived_lsn` | `u64` | LSN up to which log has been archived |

## Reading Data Blocks

```rust,no_run
use idb::innodb::log::{LogFile, LogBlockHeader, LogBlockTrailer, LOG_FILE_HDR_BLOCKS};

let mut log = LogFile::open("/var/lib/mysql/ib_logfile0").unwrap();

// Read data blocks (starting at block 4, after header/checkpoint blocks)
for block_no in LOG_FILE_HDR_BLOCKS..log.block_count() {
    let block = log.read_block(block_no).unwrap();
    let header = LogBlockHeader::parse(&block).unwrap();

    if header.has_data() {
        println!("Block {}: data_len={}, flush={}, checkpoint_no={}",
            header.block_no, header.data_len, header.flush_flag, header.checkpoint_no);
    }
}
```

### LogBlockHeader Fields

| Field | Type | Description |
|-------|------|-------------|
| `block_no` | `u32` | Block number (with flush bit masked out) |
| `flush_flag` | `bool` | Whether this block was the first in a flush batch (bit 31 of raw block number) |
| `data_len` | `u16` | Number of bytes of log data in this block (including the 14-byte header) |
| `first_rec_group` | `u16` | Offset of the first log record group starting in this block |
| `checkpoint_no` | `u32` | Checkpoint number when this block was written |

The `has_data()` method returns `true` when `data_len` exceeds the header size (14 bytes), indicating the block contains log record data.

## Block Checksum Validation

Each 512-byte block has a CRC-32C checksum stored in the last 4 bytes (offset 508-511), covering bytes 0-507.

```rust,no_run
use idb::innodb::log::{LogFile, validate_log_block_checksum, LOG_FILE_HDR_BLOCKS};

let mut log = LogFile::open("/var/lib/mysql/ib_logfile0").unwrap();

for block_no in LOG_FILE_HDR_BLOCKS..log.block_count() {
    let block = log.read_block(block_no).unwrap();
    let valid = validate_log_block_checksum(&block);
    if !valid {
        println!("Block {} has invalid checksum", block_no);
    }
}
```

## MLOG Record Types

The `MlogRecordType` enum classifies redo log record types from MySQL's `mtr0types.h`. This is useful for analyzing what operations are recorded in the redo log.

```rust,no_run
use idb::innodb::log::MlogRecordType;

let rec_type = MlogRecordType::from_u8(9);
println!("{}", rec_type);        // "MLOG_REC_INSERT"
println!("{}", rec_type.name()); // "MLOG_REC_INSERT"

let unknown = MlogRecordType::from_u8(200);
println!("{}", unknown);         // "UNKNOWN(200)"
```

### Common MLOG Record Types

| Value | Variant | Name |
|-------|---------|------|
| 1 | `Mlog1Byte` | `MLOG_1BYTE` |
| 2 | `Mlog2Bytes` | `MLOG_2BYTES` |
| 4 | `Mlog4Bytes` | `MLOG_4BYTES` |
| 8 | `Mlog8Bytes` | `MLOG_8BYTES` |
| 9 | `MlogRecInsert` | `MLOG_REC_INSERT` |
| 13 | `MlogRecUpdate` | `MLOG_REC_UPDATE_IN_PLACE` |
| 14 | `MlogRecDelete` | `MLOG_REC_DELETE` |
| 19 | `MlogPageCreate` | `MLOG_PAGE_CREATE` |
| 22 | `MlogUndoInit` | `MLOG_UNDO_INIT` |
| 30 | `MlogInitFilePage` | `MLOG_INIT_FILE_PAGE` |
| 31 | `MlogWriteString` | `MLOG_WRITE_STRING` |
| 32 | `MlogMultiRecEnd` | `MLOG_MULTI_REC_END` |
| 36 | `MlogCompRecInsert` | `MLOG_COMP_REC_INSERT` |
| 44 | `MlogCompPageReorganize` | `MLOG_COMP_PAGE_REORGANIZE` |
| 52 | `MlogFileCreate` | `MLOG_FILE_CREATE` |

Unknown type codes are represented as `MlogRecordType::Unknown(value)`.

## Block Count Methods

| Method | Description |
|--------|-------------|
| `block_count()` | Total number of 512-byte blocks in the file |
| `data_block_count()` | Number of data blocks (total minus 4 header/checkpoint blocks) |
| `file_size()` | File size in bytes |

## Constants

The module exports several constants matching MySQL's `log0log.h`:

| Constant | Value | Description |
|----------|-------|-------------|
| `LOG_BLOCK_SIZE` | 512 | Size of a redo log block in bytes |
| `LOG_BLOCK_HDR_SIZE` | 14 | Size of the block header |
| `LOG_BLOCK_TRL_SIZE` | 4 | Size of the block trailer |
| `LOG_BLOCK_FLUSH_BIT_MASK` | 0x80000000 | Bitmask for the flush flag in block number |
| `LOG_BLOCK_CHECKSUM_OFFSET` | 508 | Byte offset of CRC-32C checksum within a block |
| `LOG_FILE_HDR_BLOCKS` | 4 | Number of reserved header/checkpoint blocks |
