//! InnoDB redo log file parsing.
//!
//! Reads InnoDB redo log files (`ib_logfile0`/`ib_logfile1` for MySQL < 8.0.30,
//! or `#ib_redo*` files for 8.0.30+). The file layout consists of a 2048-byte
//! header (4 blocks of 512 bytes each) containing the log file header and two
//! checkpoint records, followed by 512-byte data blocks.
//!
//! ## Log format versions
//!
//! The redo log format has evolved across MySQL versions:
//!
//! | Version | Introduced | Key changes |
//! |---------|-----------|-------------|
//! | 1 | MySQL 5.7.9 | Initial versioned format |
//! | 2 | MySQL 8.0.1 | Removed MLOG_FILE_NAME, added MLOG_FILE_OPEN |
//! | 3 | MySQL 8.0.3 | checkpoint_lsn can point to any byte |
//! | 4 | MySQL 8.0.19 | Expanded compressed ulint form |
//! | 5 | MySQL 8.0.28 | Row versioning header; new MLOG record types (67-76) |
//! | 6 | MySQL 8.0.30 | innodb_redo_log_capacity; `#ib_redo*` file naming |
//!
//! MySQL 9.0 and 9.1 continue to use format version 6.
//!
//! Use [`LogFile::open`] to read and parse the header and checkpoint records,
//! then [`LogFile::read_block`] to read individual data blocks. Each block's
//! [`LogBlockHeader`] provides the block number, data length, first record
//! group offset, epoch number, and CRC-32C checksum validation status.

use byteorder::{BigEndian, ByteOrder};
use serde::Serialize;
use std::io::{Cursor, Read, Seek, SeekFrom};

use crate::IdbError;

/// Supertrait combining `Read + Seek` for type-erased readers.
trait ReadSeek: Read + Seek {}
impl<T: Read + Seek> ReadSeek for T {}

/// Size of a redo log block in bytes (from MySQL `log0constants.h`).
pub const LOG_BLOCK_SIZE: usize = 512;
/// Size of the log block header in bytes (MySQL 8.0.30+).
///
/// Prior to MySQL 8.0.30, the header was 14 bytes. In 8.0.30+ (format
/// version 6), the header is 12 bytes with the epoch number at offset 8.
pub const LOG_BLOCK_HDR_SIZE: usize = 12;
/// Size of the log block trailer in bytes.
pub const LOG_BLOCK_TRL_SIZE: usize = 4;
/// Bitmask for the flush flag in the block number field (bit 31).
pub const LOG_BLOCK_FLUSH_BIT_MASK: u32 = 0x80000000;
/// Byte offset of the CRC-32C checksum within a block (bytes 508-511).
pub const LOG_BLOCK_CHECKSUM_OFFSET: usize = 508;
/// Number of reserved header/checkpoint blocks at the start of the file.
pub const LOG_FILE_HDR_BLOCKS: u64 = 4;

/// Offset of the format version within the log file header (block 0).
///
/// This field was called "group ID" in MySQL < 8.0.30. In 8.0.30+ (format
/// version 6), it stores the log format version number (e.g. 6 for MySQL 9.x).
pub const LOG_HEADER_FORMAT: usize = 0;
/// Offset of the group ID within the log file header (block 0).
///
/// Alias for [`LOG_HEADER_FORMAT`] for backward compatibility. In MySQL < 8.0.30,
/// this field stored the log group ID. In 8.0.30+ it stores the format version.
pub const LOG_HEADER_GROUP_ID: usize = 0;
/// Offset of the log UUID within the log file header (bytes 4-7).
///
/// In MySQL 8.0.30+, this is a 4-byte UUID identifying the data directory.
/// In older formats, this was part of the start LSN field.
pub const LOG_HEADER_LOG_UUID: usize = 4;
/// Offset of the start LSN within the log file header (bytes 8-15).
pub const LOG_HEADER_START_LSN: usize = 8;
/// Offset of the file number within the log file header (bytes 12-15).
///
/// Only meaningful for MySQL < 8.0.30 where the start LSN was at offset 4.
/// Overlaps with the lower 4 bytes of `LOG_HEADER_START_LSN` in the 8.0.30+ layout.
#[deprecated(note = "Use LOG_HEADER_START_LSN instead; file_no is not a separate field in 8.0.30+")]
pub const LOG_HEADER_FILE_NO: usize = 12;
/// Offset of the creator string within the log file header (bytes 16-47).
pub const LOG_HEADER_CREATED_BY: usize = 16;
/// Maximum length of the creator string.
pub const LOG_HEADER_CREATED_BY_LEN: usize = 32;

/// Offset of the checkpoint number within a checkpoint block.
///
/// Only meaningful for MySQL < 8.0.30. In 8.0.30+ the checkpoint block
/// only contains the checkpoint LSN at offset 8.
pub const LOG_CHECKPOINT_NO: usize = 0;
/// Offset of the checkpoint LSN within a checkpoint block.
pub const LOG_CHECKPOINT_LSN: usize = 8;
/// Offset of the checkpoint byte-offset within a checkpoint block.
///
/// Only meaningful for MySQL < 8.0.30.
pub const LOG_CHECKPOINT_OFFSET: usize = 16;
/// Offset of the log buffer size within a checkpoint block.
///
/// Only meaningful for MySQL < 8.0.30.
pub const LOG_CHECKPOINT_BUF_SIZE: usize = 20;
/// Offset of the archived LSN within a checkpoint block.
///
/// Only meaningful for MySQL < 8.0.30.
pub const LOG_CHECKPOINT_ARCHIVED_LSN: usize = 24;

/// Log file header (block 0 of the redo log file).
///
/// The header layout changed in MySQL 8.0.30 (format version 6):
///
/// | Offset | Pre-8.0.30 | 8.0.30+ (incl. 9.x) |
/// |--------|------------|---------------------|
/// | 0 | group_id (u32) | format_version (u32) |
/// | 4 | start_lsn (u64) | log_uuid (u32) |
/// | 8 | (start_lsn cont.) | start_lsn (u64) |
/// | 12 | file_no (u32) | (start_lsn cont.) |
/// | 16 | created_by (32 bytes) | created_by (32 bytes) |
///
/// The `format_version` and `group_id` fields share offset 0; in practice,
/// format version values 1-6 are distinguishable from typical group IDs.
#[derive(Debug, Clone, Serialize)]
pub struct LogFileHeader {
    /// Log format version (offset 0).
    ///
    /// In MySQL < 8.0.30 this was the log group ID. In 8.0.30+ this is the
    /// format version number (e.g. 6 for MySQL 8.0.30+, 9.0, 9.1).
    /// Aliased as `group_id` for backward compatibility.
    pub format_version: u32,
    /// Start LSN of this log file.
    pub start_lsn: u64,
    /// Log UUID (MySQL 8.0.30+) or 0 for older formats.
    ///
    /// A 4-byte identifier for the data directory, stored at offset 4.
    #[serde(skip_serializing_if = "is_zero_u32")]
    pub log_uuid: u32,
    /// MySQL version string that created this log file (e.g. "MySQL 9.0.1").
    pub created_by: String,
}

fn is_zero_u32(v: &u32) -> bool {
    *v == 0
}

impl LogFileHeader {
    /// Backward-compatible accessor: returns `format_version` as the group ID.
    ///
    /// In MySQL < 8.0.30, offset 0 stored the log group ID. In 8.0.30+
    /// it stores the format version. This accessor preserves API compatibility.
    pub fn group_id(&self) -> u32 {
        self.format_version
    }

    /// Parse a log file header from the first 512-byte block.
    ///
    /// Handles both pre-8.0.30 and 8.0.30+ header layouts. The format is
    /// auto-detected: format versions 1-6 indicate the 8.0.30+ layout
    /// where offset 4 is the log UUID and offset 8 is the start LSN.
    ///
    /// # Examples
    ///
    /// ```
    /// use idb::innodb::log::{LogFileHeader, LOG_BLOCK_SIZE,
    ///     LOG_HEADER_FORMAT, LOG_HEADER_START_LSN,
    ///     LOG_HEADER_LOG_UUID, LOG_HEADER_CREATED_BY};
    /// use byteorder::{BigEndian, ByteOrder};
    ///
    /// // MySQL 8.0.30+ / 9.x format (format_version = 6)
    /// let mut block = vec![0u8; LOG_BLOCK_SIZE];
    /// BigEndian::write_u32(&mut block[LOG_HEADER_FORMAT..], 6);
    /// BigEndian::write_u32(&mut block[LOG_HEADER_LOG_UUID..], 0x12345678);
    /// BigEndian::write_u64(&mut block[LOG_HEADER_START_LSN..], 0x1000);
    /// block[LOG_HEADER_CREATED_BY..LOG_HEADER_CREATED_BY + 12]
    ///     .copy_from_slice(b"MySQL 9.0.1\0");
    ///
    /// let hdr = LogFileHeader::parse(&block).unwrap();
    /// assert_eq!(hdr.format_version, 6);
    /// assert_eq!(hdr.start_lsn, 0x1000);
    /// assert_eq!(hdr.log_uuid, 0x12345678);
    /// assert_eq!(hdr.created_by, "MySQL 9.0.1");
    /// ```
    pub fn parse(block: &[u8]) -> Option<Self> {
        if block.len() < LOG_BLOCK_SIZE {
            return None;
        }

        let format_version = BigEndian::read_u32(&block[LOG_HEADER_FORMAT..]);
        let log_uuid = BigEndian::read_u32(&block[LOG_HEADER_LOG_UUID..]);
        let start_lsn = BigEndian::read_u64(&block[LOG_HEADER_START_LSN..]);

        let created_bytes =
            &block[LOG_HEADER_CREATED_BY..LOG_HEADER_CREATED_BY + LOG_HEADER_CREATED_BY_LEN];
        let created_by = created_bytes
            .iter()
            .take_while(|&&b| b != 0)
            .map(|&b| b as char)
            .collect::<String>();

        Some(LogFileHeader {
            format_version,
            start_lsn,
            log_uuid,
            created_by,
        })
    }
}

/// Checkpoint record (blocks 1 and 3 of the redo log file).
///
/// In MySQL 8.0.30+ (format version 6, including MySQL 9.x), the checkpoint
/// block only contains the checkpoint LSN at offset 8. The `number`, `offset`,
/// `buf_size`, and `archived_lsn` fields are not written and will be zero.
#[derive(Debug, Clone, Serialize)]
pub struct LogCheckpoint {
    /// Checkpoint sequence number (pre-8.0.30 only; zero in 8.0.30+).
    #[serde(skip_serializing_if = "is_zero_u64")]
    pub number: u64,
    /// LSN at the time of this checkpoint.
    pub lsn: u64,
    /// Byte offset of the checkpoint within the log file (pre-8.0.30 only).
    #[serde(skip_serializing_if = "is_zero_u32")]
    pub offset: u32,
    /// Log buffer size at checkpoint time (pre-8.0.30 only).
    #[serde(skip_serializing_if = "is_zero_u32")]
    pub buf_size: u32,
    /// LSN up to which log has been archived (pre-8.0.30 only).
    #[serde(skip_serializing_if = "is_zero_u64")]
    pub archived_lsn: u64,
}

fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}

impl LogCheckpoint {
    /// Parse a checkpoint from a 512-byte block.
    ///
    /// Reads all fields for backward compatibility. In MySQL 8.0.30+ (format
    /// version 6), only the `lsn` field at offset 8 is meaningful; all other
    /// fields will be zero.
    ///
    /// # Examples
    ///
    /// ```
    /// use idb::innodb::log::{LogCheckpoint, LOG_BLOCK_SIZE,
    ///     LOG_CHECKPOINT_NO, LOG_CHECKPOINT_LSN, LOG_CHECKPOINT_OFFSET,
    ///     LOG_CHECKPOINT_BUF_SIZE, LOG_CHECKPOINT_ARCHIVED_LSN};
    /// use byteorder::{BigEndian, ByteOrder};
    ///
    /// let mut block = vec![0u8; LOG_BLOCK_SIZE];
    /// BigEndian::write_u64(&mut block[LOG_CHECKPOINT_NO..], 42);
    /// BigEndian::write_u64(&mut block[LOG_CHECKPOINT_LSN..], 0xDEADBEEF);
    /// BigEndian::write_u32(&mut block[LOG_CHECKPOINT_OFFSET..], 2048);
    /// BigEndian::write_u32(&mut block[LOG_CHECKPOINT_BUF_SIZE..], 65536);
    /// BigEndian::write_u64(&mut block[LOG_CHECKPOINT_ARCHIVED_LSN..], 0xCAFEBABE);
    ///
    /// let cp = LogCheckpoint::parse(&block).unwrap();
    /// assert_eq!(cp.number, 42);
    /// assert_eq!(cp.lsn, 0xDEADBEEF);
    /// assert_eq!(cp.offset, 2048);
    /// assert_eq!(cp.buf_size, 65536);
    /// assert_eq!(cp.archived_lsn, 0xCAFEBABE);
    /// ```
    pub fn parse(block: &[u8]) -> Option<Self> {
        if block.len() < LOG_BLOCK_SIZE {
            return None;
        }

        let number = BigEndian::read_u64(&block[LOG_CHECKPOINT_NO..]);
        let lsn = BigEndian::read_u64(&block[LOG_CHECKPOINT_LSN..]);
        let offset = BigEndian::read_u32(&block[LOG_CHECKPOINT_OFFSET..]);
        let buf_size = BigEndian::read_u32(&block[LOG_CHECKPOINT_BUF_SIZE..]);
        let archived_lsn = BigEndian::read_u64(&block[LOG_CHECKPOINT_ARCHIVED_LSN..]);

        Some(LogCheckpoint {
            number,
            lsn,
            offset,
            buf_size,
            archived_lsn,
        })
    }
}

/// Log block header (first 12 bytes of each 512-byte block).
///
/// The block header layout is consistent across MySQL versions:
///
/// | Offset | Size | Field |
/// |--------|------|-------|
/// | 0 | 4 | Block number (bit 31 = flush flag) |
/// | 4 | 2 | Data length (including header bytes) |
/// | 6 | 2 | First record group offset |
/// | 8 | 4 | Epoch number (called checkpoint_no in older versions) |
///
/// In MySQL < 8.0.30, the header was 14 bytes with 2 extra bytes at offset
/// 12-13 that were part of the checkpoint number. In 8.0.30+ the header is
/// 12 bytes and offset 8 stores the epoch number.
#[derive(Debug, Clone, Serialize)]
pub struct LogBlockHeader {
    /// Block number (with flush bit masked out).
    pub block_no: u32,
    /// Whether this block was the first in a flush batch (bit 31).
    pub flush_flag: bool,
    /// Number of bytes of log data in this block (including header).
    pub data_len: u16,
    /// Offset of the first log record group starting in this block.
    pub first_rec_group: u16,
    /// Epoch number (MySQL 8.0.30+) or checkpoint number (older versions).
    ///
    /// In MySQL 8.0.30+ and 9.x, this field stores the epoch number used
    /// for log block validation. In older versions, it was the checkpoint
    /// sequence number.
    pub epoch_no: u32,
}

impl LogBlockHeader {
    /// Parse a log block header from a 512-byte block.
    ///
    /// # Examples
    ///
    /// ```
    /// use idb::innodb::log::{LogBlockHeader, LOG_BLOCK_HDR_SIZE};
    /// use byteorder::{BigEndian, ByteOrder};
    ///
    /// let mut block = vec![0u8; LOG_BLOCK_HDR_SIZE];
    /// BigEndian::write_u32(&mut block[0..], 0x80000005); // flush bit + block_no=5
    /// BigEndian::write_u16(&mut block[4..], 200);        // data_len
    /// BigEndian::write_u16(&mut block[6..], 14);         // first_rec_group
    /// BigEndian::write_u32(&mut block[8..], 3);          // epoch_no
    ///
    /// let hdr = LogBlockHeader::parse(&block).unwrap();
    /// assert_eq!(hdr.block_no, 5);
    /// assert!(hdr.flush_flag);
    /// assert_eq!(hdr.data_len, 200);
    /// assert_eq!(hdr.first_rec_group, 14);
    /// assert_eq!(hdr.epoch_no, 3);
    /// assert!(hdr.has_data());
    /// ```
    pub fn parse(block: &[u8]) -> Option<Self> {
        if block.len() < LOG_BLOCK_HDR_SIZE {
            return None;
        }

        let raw_block_no = BigEndian::read_u32(&block[0..]);
        let flush_flag = (raw_block_no & LOG_BLOCK_FLUSH_BIT_MASK) != 0;
        let block_no = raw_block_no & !LOG_BLOCK_FLUSH_BIT_MASK;

        let data_len = BigEndian::read_u16(&block[4..]);
        let first_rec_group = BigEndian::read_u16(&block[6..]);
        let epoch_no = BigEndian::read_u32(&block[8..]);

        Some(LogBlockHeader {
            block_no,
            flush_flag,
            data_len,
            first_rec_group,
            epoch_no,
        })
    }

    /// Backward-compatible accessor: returns `epoch_no` as the checkpoint number.
    ///
    /// In MySQL < 8.0.30, offset 8 stored the checkpoint number. In 8.0.30+
    /// it stores the epoch number. This accessor preserves API compatibility.
    pub fn checkpoint_no(&self) -> u32 {
        self.epoch_no
    }

    /// Returns true if this block contains log data (data_len > header size).
    pub fn has_data(&self) -> bool {
        self.data_len as usize > LOG_BLOCK_HDR_SIZE
    }
}

/// Log block trailer (last 4 bytes of each 512-byte block).
#[derive(Debug, Clone, Serialize)]
pub struct LogBlockTrailer {
    /// CRC-32C checksum of the block (bytes 0..508).
    pub checksum: u32,
}

impl LogBlockTrailer {
    /// Parse a log block trailer from a 512-byte block.
    pub fn parse(block: &[u8]) -> Option<Self> {
        if block.len() < LOG_BLOCK_SIZE {
            return None;
        }

        let checksum = BigEndian::read_u32(&block[LOG_BLOCK_CHECKSUM_OFFSET..]);

        Some(LogBlockTrailer { checksum })
    }
}

/// Validate a log block's CRC-32C checksum.
///
/// The checksum covers bytes 0..508 of the block (everything except the checksum field itself).
pub fn validate_log_block_checksum(block: &[u8]) -> bool {
    if block.len() < LOG_BLOCK_SIZE {
        return false;
    }
    let stored = BigEndian::read_u32(&block[LOG_BLOCK_CHECKSUM_OFFSET..]);
    let calculated = crc32c::crc32c(&block[..LOG_BLOCK_CHECKSUM_OFFSET]);
    stored == calculated
}

/// MLOG record types from MySQL `mtr0types.h`.
///
/// Type codes are assigned to match the MySQL source exactly. Types suffixed
/// with `_8027` are the pre-8.0.28 variants; MySQL 8.0.28+ introduced new
/// type codes (67-76) for records with row versioning support. The old codes
/// are retained for backward compatibility with older redo logs.
///
/// In MySQL 9.x, the `_8027` types are officially marked obsolete but may
/// still appear in redo logs created by MySQL 8.0.27 and earlier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum MlogRecordType {
    // ── Basic write types ────────────────────────────────────────────
    /// Write 1 byte to a page (type 1).
    Mlog1Byte,
    /// Write 2 bytes to a page (type 2).
    Mlog2Bytes,
    /// Write 4 bytes to a page (type 4).
    Mlog4Bytes,
    /// Write 8 bytes to a page (type 8).
    Mlog8Bytes,

    // ── Pre-8.0.28 record types (9-18) ──────────────────────────────
    /// Insert record, pre-8.0.28 format (type 9).
    MlogRecInsert8027,
    /// Clustered index delete-mark, pre-8.0.28 format (type 10).
    MlogRecClustDeleteMark8027,
    /// Secondary index delete-mark (type 11).
    MlogRecSecDeleteMark,
    /// Update in place, pre-8.0.28 format (type 13).
    MlogRecUpdateInPlace8027,
    /// Delete record, pre-8.0.28 format (type 14).
    MlogRecDelete8027,
    /// Delete from end of page list, pre-8.0.28 format (type 15).
    MlogListEndDelete8027,
    /// Delete from start of page list, pre-8.0.28 format (type 16).
    MlogListStartDelete8027,
    /// End-copy of created page list, pre-8.0.28 format (type 17).
    MlogListEndCopyCreated8027,
    /// Page reorganize, pre-8.0.28 format (type 18).
    MlogPageReorganize8027,

    // ── Page and undo types (19-24) ─────────────────────────────────
    /// Create a page (type 19).
    MlogPageCreate,
    /// Insert undo log record (type 20).
    MlogUndoInsert,
    /// Erase undo log page end (type 21).
    MlogUndoEraseEnd,
    /// Initialize undo log header (type 22).
    MlogUndoInit,
    /// Reuse undo log header (type 24).
    MlogUndoHdrReuse,

    // ── Types added/renumbered in MySQL 8.0 (25-35) ─────────────────
    /// Create undo log header (type 25).
    MlogUndoHdrCreate,
    /// Set minimum record mark (type 26).
    MlogRecMinMark,
    /// Initialize insert buffer bitmap (type 27).
    MlogIbufBitmapInit,
    /// LSN marker, debug only (type 28).
    MlogLsn,
    /// Initialize file page, deprecated (type 29).
    MlogInitFilePage,
    /// Write a string to a page (type 30).
    MlogWriteString,
    /// End of multi-record mini-transaction (type 31).
    MlogMultiRecEnd,
    /// Dummy record for padding (type 32).
    MlogDummyRecord,
    /// Create a tablespace file (type 33).
    MlogFileCreate,
    /// Rename a tablespace file (type 34).
    MlogFileRename,
    /// Delete a tablespace file (type 35).
    MlogFileDelete,

    // ── Compact page types (36-46, pre-8.0.28 variants) ─────────────
    /// Set minimum record mark, compact format (type 36).
    MlogCompRecMinMark,
    /// Create compact page (type 37).
    MlogCompPageCreate,
    /// Insert record, compact format, pre-8.0.28 (type 38).
    MlogCompRecInsert8027,
    /// Clustered delete-mark, compact format, pre-8.0.28 (type 39).
    MlogCompRecClustDeleteMark8027,
    /// Secondary delete-mark, compact format (type 40).
    MlogCompRecSecDeleteMark,
    /// Update in place, compact format, pre-8.0.28 (type 41).
    MlogCompRecUpdateInPlace8027,
    /// Delete record, compact format, pre-8.0.28 (type 42).
    MlogCompRecDelete8027,
    /// Delete from end of list, compact format, pre-8.0.28 (type 43).
    MlogCompListEndDelete8027,
    /// Delete from start of list, compact format, pre-8.0.28 (type 44).
    MlogCompListStartDelete8027,
    /// End-copy created, compact format, pre-8.0.28 (type 45).
    MlogCompListEndCopyCreated8027,
    /// Page reorganize, compact format, pre-8.0.28 (type 46).
    MlogCompPageReorganize8027,

    // ── Compressed page types (48-53) ───────────────────────────────
    /// Write node pointer in compressed page (type 48).
    MlogZipWriteNodePtr,
    /// Write BLOB pointer in compressed page (type 49).
    MlogZipWriteBlobPtr,
    /// Write header in compressed page (type 50).
    MlogZipWriteHeader,
    /// Compress a page (type 51).
    MlogZipPageCompress,
    /// Compress page with no data, pre-8.0.28 (type 52).
    MlogZipPageCompressNoData8027,
    /// Reorganize compressed page, pre-8.0.28 (type 53).
    MlogZipPageReorganize8027,

    // ── Extended types (57-66) ──────────────────────────────────────
    /// Create R-Tree page (type 57).
    MlogPageCreateRTree,
    /// Create compact R-Tree page (type 58).
    MlogCompPageCreateRTree,
    /// Initialize file page v2 (type 59).
    MlogInitFilePage2,
    /// Index load notification (type 61).
    MlogIndexLoad,
    /// Table dynamic metadata (type 62).
    MlogTableDynamicMeta,
    /// Create SDI page (type 63).
    MlogPageCreateSdi,
    /// Create compact SDI page (type 64).
    MlogCompPageCreateSdi,
    /// Extend a tablespace file (type 65).
    MlogFileExtend,
    /// Test record, unit tests only (type 66).
    MlogTest,

    // ── MySQL 8.0.28+ new record types (67-76) ─────────────────────
    /// Insert record with row versioning (type 67).
    MlogRecInsert,
    /// Clustered index delete-mark with row versioning (type 68).
    MlogRecClustDeleteMark,
    /// Delete record with row versioning (type 69).
    MlogRecDelete,
    /// Update in place with row versioning (type 70).
    MlogRecUpdateInPlace,
    /// End-copy of created page list with row versioning (type 71).
    MlogListEndCopyCreated,
    /// Page reorganize with row versioning (type 72).
    MlogPageReorganize,
    /// Compressed page reorganize with row versioning (type 73).
    MlogZipPageReorganize,
    /// Compress page with no data, with row versioning (type 74).
    MlogZipPageCompressNoData,
    /// Delete from end of page list with row versioning (type 75).
    MlogListEndDelete,
    /// Delete from start of page list with row versioning (type 76).
    MlogListStartDelete,

    /// Unknown or unrecognized record type.
    Unknown(u8),
}

impl MlogRecordType {
    /// Convert a u8 type code to MlogRecordType.
    ///
    /// Maps all known type codes from MySQL `mtr0types.h` across versions
    /// 5.7 through 9.1. Unrecognized codes are wrapped in `Unknown(N)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use idb::innodb::log::MlogRecordType;
    ///
    /// let rec_type = MlogRecordType::from_u8(1);
    /// assert_eq!(rec_type, MlogRecordType::Mlog1Byte);
    /// assert_eq!(rec_type.name(), "MLOG_1BYTE");
    ///
    /// // Pre-8.0.28 insert at type code 9
    /// let insert_old = MlogRecordType::from_u8(9);
    /// assert_eq!(insert_old, MlogRecordType::MlogRecInsert8027);
    ///
    /// // Post-8.0.28 insert at type code 67
    /// let insert_new = MlogRecordType::from_u8(67);
    /// assert_eq!(insert_new, MlogRecordType::MlogRecInsert);
    ///
    /// // Unknown type codes are preserved
    /// let unknown = MlogRecordType::from_u8(255);
    /// assert_eq!(unknown, MlogRecordType::Unknown(255));
    /// assert_eq!(format!("{}", unknown), "UNKNOWN(255)");
    /// ```
    pub fn from_u8(val: u8) -> Self {
        match val {
            1 => MlogRecordType::Mlog1Byte,
            2 => MlogRecordType::Mlog2Bytes,
            4 => MlogRecordType::Mlog4Bytes,
            8 => MlogRecordType::Mlog8Bytes,
            9 => MlogRecordType::MlogRecInsert8027,
            10 => MlogRecordType::MlogRecClustDeleteMark8027,
            11 => MlogRecordType::MlogRecSecDeleteMark,
            13 => MlogRecordType::MlogRecUpdateInPlace8027,
            14 => MlogRecordType::MlogRecDelete8027,
            15 => MlogRecordType::MlogListEndDelete8027,
            16 => MlogRecordType::MlogListStartDelete8027,
            17 => MlogRecordType::MlogListEndCopyCreated8027,
            18 => MlogRecordType::MlogPageReorganize8027,
            19 => MlogRecordType::MlogPageCreate,
            20 => MlogRecordType::MlogUndoInsert,
            21 => MlogRecordType::MlogUndoEraseEnd,
            22 => MlogRecordType::MlogUndoInit,
            24 => MlogRecordType::MlogUndoHdrReuse,
            25 => MlogRecordType::MlogUndoHdrCreate,
            26 => MlogRecordType::MlogRecMinMark,
            27 => MlogRecordType::MlogIbufBitmapInit,
            28 => MlogRecordType::MlogLsn,
            29 => MlogRecordType::MlogInitFilePage,
            30 => MlogRecordType::MlogWriteString,
            31 => MlogRecordType::MlogMultiRecEnd,
            32 => MlogRecordType::MlogDummyRecord,
            33 => MlogRecordType::MlogFileCreate,
            34 => MlogRecordType::MlogFileRename,
            35 => MlogRecordType::MlogFileDelete,
            36 => MlogRecordType::MlogCompRecMinMark,
            37 => MlogRecordType::MlogCompPageCreate,
            38 => MlogRecordType::MlogCompRecInsert8027,
            39 => MlogRecordType::MlogCompRecClustDeleteMark8027,
            40 => MlogRecordType::MlogCompRecSecDeleteMark,
            41 => MlogRecordType::MlogCompRecUpdateInPlace8027,
            42 => MlogRecordType::MlogCompRecDelete8027,
            43 => MlogRecordType::MlogCompListEndDelete8027,
            44 => MlogRecordType::MlogCompListStartDelete8027,
            45 => MlogRecordType::MlogCompListEndCopyCreated8027,
            46 => MlogRecordType::MlogCompPageReorganize8027,
            48 => MlogRecordType::MlogZipWriteNodePtr,
            49 => MlogRecordType::MlogZipWriteBlobPtr,
            50 => MlogRecordType::MlogZipWriteHeader,
            51 => MlogRecordType::MlogZipPageCompress,
            52 => MlogRecordType::MlogZipPageCompressNoData8027,
            53 => MlogRecordType::MlogZipPageReorganize8027,
            57 => MlogRecordType::MlogPageCreateRTree,
            58 => MlogRecordType::MlogCompPageCreateRTree,
            59 => MlogRecordType::MlogInitFilePage2,
            61 => MlogRecordType::MlogIndexLoad,
            62 => MlogRecordType::MlogTableDynamicMeta,
            63 => MlogRecordType::MlogPageCreateSdi,
            64 => MlogRecordType::MlogCompPageCreateSdi,
            65 => MlogRecordType::MlogFileExtend,
            66 => MlogRecordType::MlogTest,
            67 => MlogRecordType::MlogRecInsert,
            68 => MlogRecordType::MlogRecClustDeleteMark,
            69 => MlogRecordType::MlogRecDelete,
            70 => MlogRecordType::MlogRecUpdateInPlace,
            71 => MlogRecordType::MlogListEndCopyCreated,
            72 => MlogRecordType::MlogPageReorganize,
            73 => MlogRecordType::MlogZipPageReorganize,
            74 => MlogRecordType::MlogZipPageCompressNoData,
            75 => MlogRecordType::MlogListEndDelete,
            76 => MlogRecordType::MlogListStartDelete,
            v => MlogRecordType::Unknown(v),
        }
    }

    /// Display name for this record type.
    pub fn name(&self) -> &str {
        match self {
            MlogRecordType::Mlog1Byte => "MLOG_1BYTE",
            MlogRecordType::Mlog2Bytes => "MLOG_2BYTES",
            MlogRecordType::Mlog4Bytes => "MLOG_4BYTES",
            MlogRecordType::Mlog8Bytes => "MLOG_8BYTES",
            MlogRecordType::MlogRecInsert8027 => "MLOG_REC_INSERT_8027",
            MlogRecordType::MlogRecClustDeleteMark8027 => "MLOG_REC_CLUST_DELETE_MARK_8027",
            MlogRecordType::MlogRecSecDeleteMark => "MLOG_REC_SEC_DELETE_MARK",
            MlogRecordType::MlogRecUpdateInPlace8027 => "MLOG_REC_UPDATE_IN_PLACE_8027",
            MlogRecordType::MlogRecDelete8027 => "MLOG_REC_DELETE_8027",
            MlogRecordType::MlogListEndDelete8027 => "MLOG_LIST_END_DELETE_8027",
            MlogRecordType::MlogListStartDelete8027 => "MLOG_LIST_START_DELETE_8027",
            MlogRecordType::MlogListEndCopyCreated8027 => "MLOG_LIST_END_COPY_CREATED_8027",
            MlogRecordType::MlogPageReorganize8027 => "MLOG_PAGE_REORGANIZE_8027",
            MlogRecordType::MlogPageCreate => "MLOG_PAGE_CREATE",
            MlogRecordType::MlogUndoInsert => "MLOG_UNDO_INSERT",
            MlogRecordType::MlogUndoEraseEnd => "MLOG_UNDO_ERASE_END",
            MlogRecordType::MlogUndoInit => "MLOG_UNDO_INIT",
            MlogRecordType::MlogUndoHdrReuse => "MLOG_UNDO_HDR_REUSE",
            MlogRecordType::MlogUndoHdrCreate => "MLOG_UNDO_HDR_CREATE",
            MlogRecordType::MlogRecMinMark => "MLOG_REC_MIN_MARK",
            MlogRecordType::MlogIbufBitmapInit => "MLOG_IBUF_BITMAP_INIT",
            MlogRecordType::MlogLsn => "MLOG_LSN",
            MlogRecordType::MlogInitFilePage => "MLOG_INIT_FILE_PAGE",
            MlogRecordType::MlogWriteString => "MLOG_WRITE_STRING",
            MlogRecordType::MlogMultiRecEnd => "MLOG_MULTI_REC_END",
            MlogRecordType::MlogDummyRecord => "MLOG_DUMMY_RECORD",
            MlogRecordType::MlogFileCreate => "MLOG_FILE_CREATE",
            MlogRecordType::MlogFileRename => "MLOG_FILE_RENAME",
            MlogRecordType::MlogFileDelete => "MLOG_FILE_DELETE",
            MlogRecordType::MlogCompRecMinMark => "MLOG_COMP_REC_MIN_MARK",
            MlogRecordType::MlogCompPageCreate => "MLOG_COMP_PAGE_CREATE",
            MlogRecordType::MlogCompRecInsert8027 => "MLOG_COMP_REC_INSERT_8027",
            MlogRecordType::MlogCompRecClustDeleteMark8027 => {
                "MLOG_COMP_REC_CLUST_DELETE_MARK_8027"
            }
            MlogRecordType::MlogCompRecSecDeleteMark => "MLOG_COMP_REC_SEC_DELETE_MARK",
            MlogRecordType::MlogCompRecUpdateInPlace8027 => {
                "MLOG_COMP_REC_UPDATE_IN_PLACE_8027"
            }
            MlogRecordType::MlogCompRecDelete8027 => "MLOG_COMP_REC_DELETE_8027",
            MlogRecordType::MlogCompListEndDelete8027 => "MLOG_COMP_LIST_END_DELETE_8027",
            MlogRecordType::MlogCompListStartDelete8027 => "MLOG_COMP_LIST_START_DELETE_8027",
            MlogRecordType::MlogCompListEndCopyCreated8027 => {
                "MLOG_COMP_LIST_END_COPY_CREATED_8027"
            }
            MlogRecordType::MlogCompPageReorganize8027 => "MLOG_COMP_PAGE_REORGANIZE_8027",
            MlogRecordType::MlogZipWriteNodePtr => "MLOG_ZIP_WRITE_NODE_PTR",
            MlogRecordType::MlogZipWriteBlobPtr => "MLOG_ZIP_WRITE_BLOB_PTR",
            MlogRecordType::MlogZipWriteHeader => "MLOG_ZIP_WRITE_HEADER",
            MlogRecordType::MlogZipPageCompress => "MLOG_ZIP_PAGE_COMPRESS",
            MlogRecordType::MlogZipPageCompressNoData8027 => {
                "MLOG_ZIP_PAGE_COMPRESS_NO_DATA_8027"
            }
            MlogRecordType::MlogZipPageReorganize8027 => "MLOG_ZIP_PAGE_REORGANIZE_8027",
            MlogRecordType::MlogPageCreateRTree => "MLOG_PAGE_CREATE_RTREE",
            MlogRecordType::MlogCompPageCreateRTree => "MLOG_COMP_PAGE_CREATE_RTREE",
            MlogRecordType::MlogInitFilePage2 => "MLOG_INIT_FILE_PAGE2",
            MlogRecordType::MlogIndexLoad => "MLOG_INDEX_LOAD",
            MlogRecordType::MlogTableDynamicMeta => "MLOG_TABLE_DYNAMIC_META",
            MlogRecordType::MlogPageCreateSdi => "MLOG_PAGE_CREATE_SDI",
            MlogRecordType::MlogCompPageCreateSdi => "MLOG_COMP_PAGE_CREATE_SDI",
            MlogRecordType::MlogFileExtend => "MLOG_FILE_EXTEND",
            MlogRecordType::MlogTest => "MLOG_TEST",
            MlogRecordType::MlogRecInsert => "MLOG_REC_INSERT",
            MlogRecordType::MlogRecClustDeleteMark => "MLOG_REC_CLUST_DELETE_MARK",
            MlogRecordType::MlogRecDelete => "MLOG_REC_DELETE",
            MlogRecordType::MlogRecUpdateInPlace => "MLOG_REC_UPDATE_IN_PLACE",
            MlogRecordType::MlogListEndCopyCreated => "MLOG_LIST_END_COPY_CREATED",
            MlogRecordType::MlogPageReorganize => "MLOG_PAGE_REORGANIZE",
            MlogRecordType::MlogZipPageReorganize => "MLOG_ZIP_PAGE_REORGANIZE",
            MlogRecordType::MlogZipPageCompressNoData => "MLOG_ZIP_PAGE_COMPRESS_NO_DATA",
            MlogRecordType::MlogListEndDelete => "MLOG_LIST_END_DELETE",
            MlogRecordType::MlogListStartDelete => "MLOG_LIST_START_DELETE",
            MlogRecordType::Unknown(_) => "UNKNOWN",
        }
    }
}

impl std::fmt::Display for MlogRecordType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MlogRecordType::Unknown(v) => write!(f, "UNKNOWN({})", v),
            _ => write!(f, "{}", self.name()),
        }
    }
}

/// Redo log file reader.
pub struct LogFile {
    reader: Box<dyn ReadSeek>,
    file_size: u64,
}

impl LogFile {
    /// Open a redo log file.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open(path: &str) -> Result<Self, IdbError> {
        let file = std::fs::File::open(path)
            .map_err(|e| IdbError::Io(format!("Cannot open {}: {}", path, e)))?;
        let file_size = file
            .metadata()
            .map_err(|e| IdbError::Io(format!("Cannot stat {}: {}", path, e)))?
            .len();

        Self::init(Box::new(file), file_size)
    }

    /// Create a log file reader from an in-memory byte buffer.
    ///
    /// The buffer must be at least 2048 bytes (4 blocks of 512 bytes each for
    /// the header and checkpoint blocks).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use idb::innodb::log::LogFile;
    ///
    /// // Build a minimal valid redo log (4 header blocks + 1 data block)
    /// let data = vec![0u8; 512 * 5];
    /// let mut log = LogFile::from_bytes(data).unwrap();
    /// let header = log.read_header().unwrap();
    /// println!("Created by: {}", header.created_by);
    /// ```
    pub fn from_bytes(data: Vec<u8>) -> Result<Self, IdbError> {
        let file_size = data.len() as u64;
        Self::init(Box::new(Cursor::new(data)), file_size)
    }

    fn init(reader: Box<dyn ReadSeek>, file_size: u64) -> Result<Self, IdbError> {
        if file_size < (LOG_FILE_HDR_BLOCKS as usize * LOG_BLOCK_SIZE) as u64 {
            return Err(IdbError::Parse(format!(
                "File is too small for a redo log ({} bytes, minimum {})",
                file_size,
                LOG_FILE_HDR_BLOCKS as usize * LOG_BLOCK_SIZE
            )));
        }

        Ok(LogFile { reader, file_size })
    }

    /// Total number of 512-byte blocks in the file.
    pub fn block_count(&self) -> u64 {
        self.file_size / LOG_BLOCK_SIZE as u64
    }

    /// Number of data blocks (excluding the 4 header/checkpoint blocks).
    pub fn data_block_count(&self) -> u64 {
        self.block_count().saturating_sub(LOG_FILE_HDR_BLOCKS)
    }

    /// Read a single 512-byte block by block number.
    pub fn read_block(&mut self, block_no: u64) -> Result<Vec<u8>, IdbError> {
        let offset = block_no * LOG_BLOCK_SIZE as u64;
        if offset + LOG_BLOCK_SIZE as u64 > self.file_size {
            return Err(IdbError::Io(format!(
                "Block {} is beyond end of file (offset {}, file size {})",
                block_no, offset, self.file_size
            )));
        }

        self.reader
            .seek(SeekFrom::Start(offset))
            .map_err(|e| IdbError::Io(format!("Seek error: {}", e)))?;

        let mut buf = vec![0u8; LOG_BLOCK_SIZE];
        self.reader
            .read_exact(&mut buf)
            .map_err(|e| IdbError::Io(format!("Read error at block {}: {}", block_no, e)))?;

        Ok(buf)
    }

    /// Read and parse the log file header (block 0).
    pub fn read_header(&mut self) -> Result<LogFileHeader, IdbError> {
        let block = self.read_block(0)?;
        LogFileHeader::parse(&block)
            .ok_or_else(|| IdbError::Parse("Failed to parse log file header (block 0)".to_string()))
    }

    /// Read and parse a checkpoint (slot 0 = block 1, slot 1 = block 3).
    pub fn read_checkpoint(&mut self, slot: u8) -> Result<LogCheckpoint, IdbError> {
        let block_no = match slot {
            0 => 1,
            1 => 3,
            _ => {
                return Err(IdbError::Argument(format!(
                    "Invalid checkpoint slot {} (must be 0 or 1)",
                    slot
                )))
            }
        };
        let block = self.read_block(block_no)?;
        LogCheckpoint::parse(&block).ok_or_else(|| {
            IdbError::Parse(format!("Failed to parse checkpoint at block {}", block_no))
        })
    }

    /// File size in bytes.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_block() -> Vec<u8> {
        vec![0u8; LOG_BLOCK_SIZE]
    }

    #[test]
    fn test_log_block_header_parse() {
        let mut block = make_block();
        // block_no = 42 (no flush bit)
        BigEndian::write_u32(&mut block[0..], 42);
        // data_len = 200
        BigEndian::write_u16(&mut block[4..], 200);
        // first_rec_group = 50
        BigEndian::write_u16(&mut block[6..], 50);
        // epoch_no = 7
        BigEndian::write_u32(&mut block[8..], 7);

        let hdr = LogBlockHeader::parse(&block).unwrap();
        assert_eq!(hdr.block_no, 42);
        assert!(!hdr.flush_flag);
        assert_eq!(hdr.data_len, 200);
        assert_eq!(hdr.first_rec_group, 50);
        assert_eq!(hdr.epoch_no, 7);
        assert_eq!(hdr.checkpoint_no(), 7);
        assert!(hdr.has_data());
    }

    #[test]
    fn test_log_block_flush_bit() {
        let mut block = make_block();
        // Set flush bit (bit 31) + block_no = 100
        BigEndian::write_u32(&mut block[0..], 0x80000064);
        BigEndian::write_u16(&mut block[4..], 12); // data_len = header only

        let hdr = LogBlockHeader::parse(&block).unwrap();
        assert!(hdr.flush_flag);
        assert_eq!(hdr.block_no, 100);
        assert!(!hdr.has_data());
    }

    #[test]
    fn test_log_block_header_empty() {
        let block = make_block();
        let hdr = LogBlockHeader::parse(&block).unwrap();
        assert_eq!(hdr.block_no, 0);
        assert!(!hdr.flush_flag);
        assert_eq!(hdr.data_len, 0);
        assert_eq!(hdr.first_rec_group, 0);
        assert_eq!(hdr.epoch_no, 0);
        assert!(!hdr.has_data());
    }

    #[test]
    fn test_log_block_header_too_small() {
        let block = vec![0u8; 10]; // less than LOG_BLOCK_HDR_SIZE
        assert!(LogBlockHeader::parse(&block).is_none());
    }

    #[test]
    fn test_log_block_trailer_parse() {
        let mut block = make_block();
        BigEndian::write_u32(&mut block[LOG_BLOCK_CHECKSUM_OFFSET..], 0xCAFEBABE);

        let trailer = LogBlockTrailer::parse(&block).unwrap();
        assert_eq!(trailer.checksum, 0xCAFEBABE);
    }

    #[test]
    fn test_log_file_header_format_v6() {
        let mut block = make_block();
        BigEndian::write_u32(&mut block[LOG_HEADER_FORMAT..], 6);
        BigEndian::write_u32(&mut block[LOG_HEADER_LOG_UUID..], 0xABCD1234);
        BigEndian::write_u64(&mut block[LOG_HEADER_START_LSN..], 0x00000000001A2B3C);
        let creator = b"MySQL 9.0.1";
        block[LOG_HEADER_CREATED_BY..LOG_HEADER_CREATED_BY + creator.len()]
            .copy_from_slice(creator);

        let hdr = LogFileHeader::parse(&block).unwrap();
        assert_eq!(hdr.format_version, 6);
        assert_eq!(hdr.group_id(), 6);
        assert_eq!(hdr.start_lsn, 0x1A2B3C);
        assert_eq!(hdr.log_uuid, 0xABCD1234);
        assert_eq!(hdr.created_by, "MySQL 9.0.1");
    }

    #[test]
    fn test_log_file_header_empty_created_by() {
        let block = make_block();
        let hdr = LogFileHeader::parse(&block).unwrap();
        assert_eq!(hdr.created_by, "");
    }

    #[test]
    fn test_log_checkpoint_parse() {
        let mut block = make_block();
        BigEndian::write_u64(&mut block[LOG_CHECKPOINT_NO..], 99);
        BigEndian::write_u64(&mut block[LOG_CHECKPOINT_LSN..], 0x00000000DEADBEEF);
        BigEndian::write_u32(&mut block[LOG_CHECKPOINT_OFFSET..], 2048);
        BigEndian::write_u32(&mut block[LOG_CHECKPOINT_BUF_SIZE..], 65536);
        BigEndian::write_u64(
            &mut block[LOG_CHECKPOINT_ARCHIVED_LSN..],
            0x00000000CAFEBABE,
        );

        let cp = LogCheckpoint::parse(&block).unwrap();
        assert_eq!(cp.number, 99);
        assert_eq!(cp.lsn, 0xDEADBEEF);
        assert_eq!(cp.offset, 2048);
        assert_eq!(cp.buf_size, 65536);
        assert_eq!(cp.archived_lsn, 0xCAFEBABE);
    }

    #[test]
    fn test_log_checkpoint_format_v6() {
        // In MySQL 8.0.30+ (format v6), only LSN at offset 8 is set
        let mut block = make_block();
        BigEndian::write_u64(&mut block[LOG_CHECKPOINT_LSN..], 32193931);

        let cp = LogCheckpoint::parse(&block).unwrap();
        assert_eq!(cp.lsn, 32193931);
        assert_eq!(cp.number, 0);
        assert_eq!(cp.offset, 0);
        assert_eq!(cp.buf_size, 0);
        assert_eq!(cp.archived_lsn, 0);
    }

    #[test]
    fn test_mlog_record_type_basic_writes() {
        assert_eq!(MlogRecordType::from_u8(1), MlogRecordType::Mlog1Byte);
        assert_eq!(MlogRecordType::from_u8(2), MlogRecordType::Mlog2Bytes);
        assert_eq!(MlogRecordType::from_u8(4), MlogRecordType::Mlog4Bytes);
        assert_eq!(MlogRecordType::from_u8(8), MlogRecordType::Mlog8Bytes);
    }

    #[test]
    fn test_mlog_record_type_pre_8028() {
        // Pre-8.0.28 types at old positions
        assert_eq!(
            MlogRecordType::from_u8(9),
            MlogRecordType::MlogRecInsert8027
        );
        assert_eq!(
            MlogRecordType::from_u8(10),
            MlogRecordType::MlogRecClustDeleteMark8027
        );
        assert_eq!(
            MlogRecordType::from_u8(18),
            MlogRecordType::MlogPageReorganize8027
        );
        assert_eq!(
            MlogRecordType::from_u8(38),
            MlogRecordType::MlogCompRecInsert8027
        );
        assert_eq!(
            MlogRecordType::from_u8(52),
            MlogRecordType::MlogZipPageCompressNoData8027
        );
        assert_eq!(
            MlogRecordType::from_u8(53),
            MlogRecordType::MlogZipPageReorganize8027
        );
    }

    #[test]
    fn test_mlog_record_type_corrected_mappings() {
        // Types that were previously mapped to wrong codes
        assert_eq!(
            MlogRecordType::from_u8(25),
            MlogRecordType::MlogUndoHdrCreate
        );
        assert_eq!(
            MlogRecordType::from_u8(26),
            MlogRecordType::MlogRecMinMark
        );
        assert_eq!(
            MlogRecordType::from_u8(27),
            MlogRecordType::MlogIbufBitmapInit
        );
        assert_eq!(MlogRecordType::from_u8(28), MlogRecordType::MlogLsn);
        assert_eq!(
            MlogRecordType::from_u8(29),
            MlogRecordType::MlogInitFilePage
        );
        assert_eq!(
            MlogRecordType::from_u8(30),
            MlogRecordType::MlogWriteString
        );
        assert_eq!(
            MlogRecordType::from_u8(31),
            MlogRecordType::MlogMultiRecEnd
        );
        assert_eq!(
            MlogRecordType::from_u8(32),
            MlogRecordType::MlogDummyRecord
        );
        assert_eq!(
            MlogRecordType::from_u8(33),
            MlogRecordType::MlogFileCreate
        );
        assert_eq!(
            MlogRecordType::from_u8(34),
            MlogRecordType::MlogFileRename
        );
        assert_eq!(
            MlogRecordType::from_u8(35),
            MlogRecordType::MlogFileDelete
        );
        assert_eq!(
            MlogRecordType::from_u8(36),
            MlogRecordType::MlogCompRecMinMark
        );
        assert_eq!(
            MlogRecordType::from_u8(37),
            MlogRecordType::MlogCompPageCreate
        );
    }

    #[test]
    fn test_mlog_record_type_post_8028() {
        // New types added in MySQL 8.0.28+
        assert_eq!(
            MlogRecordType::from_u8(67),
            MlogRecordType::MlogRecInsert
        );
        assert_eq!(
            MlogRecordType::from_u8(68),
            MlogRecordType::MlogRecClustDeleteMark
        );
        assert_eq!(
            MlogRecordType::from_u8(69),
            MlogRecordType::MlogRecDelete
        );
        assert_eq!(
            MlogRecordType::from_u8(70),
            MlogRecordType::MlogRecUpdateInPlace
        );
        assert_eq!(
            MlogRecordType::from_u8(71),
            MlogRecordType::MlogListEndCopyCreated
        );
        assert_eq!(
            MlogRecordType::from_u8(72),
            MlogRecordType::MlogPageReorganize
        );
        assert_eq!(
            MlogRecordType::from_u8(73),
            MlogRecordType::MlogZipPageReorganize
        );
        assert_eq!(
            MlogRecordType::from_u8(74),
            MlogRecordType::MlogZipPageCompressNoData
        );
        assert_eq!(
            MlogRecordType::from_u8(75),
            MlogRecordType::MlogListEndDelete
        );
        assert_eq!(
            MlogRecordType::from_u8(76),
            MlogRecordType::MlogListStartDelete
        );
    }

    #[test]
    fn test_mlog_record_type_extended() {
        // Types in the 57-66 range
        assert_eq!(
            MlogRecordType::from_u8(57),
            MlogRecordType::MlogPageCreateRTree
        );
        assert_eq!(
            MlogRecordType::from_u8(59),
            MlogRecordType::MlogInitFilePage2
        );
        assert_eq!(
            MlogRecordType::from_u8(61),
            MlogRecordType::MlogIndexLoad
        );
        assert_eq!(
            MlogRecordType::from_u8(62),
            MlogRecordType::MlogTableDynamicMeta
        );
        assert_eq!(
            MlogRecordType::from_u8(63),
            MlogRecordType::MlogPageCreateSdi
        );
        assert_eq!(
            MlogRecordType::from_u8(64),
            MlogRecordType::MlogCompPageCreateSdi
        );
        assert_eq!(
            MlogRecordType::from_u8(65),
            MlogRecordType::MlogFileExtend
        );
        assert_eq!(MlogRecordType::from_u8(66), MlogRecordType::MlogTest);
    }

    #[test]
    fn test_mlog_record_type_unknown() {
        assert_eq!(MlogRecordType::from_u8(0), MlogRecordType::Unknown(0));
        assert_eq!(MlogRecordType::from_u8(255), MlogRecordType::Unknown(255));
        assert_eq!(MlogRecordType::from_u8(100), MlogRecordType::Unknown(100));
        // Gaps in the enum: 3, 5, 6, 7, 12, 23, 47, 54-56, 60, 77+
        assert_eq!(MlogRecordType::from_u8(3), MlogRecordType::Unknown(3));
        assert_eq!(MlogRecordType::from_u8(12), MlogRecordType::Unknown(12));
        assert_eq!(MlogRecordType::from_u8(23), MlogRecordType::Unknown(23));
        assert_eq!(MlogRecordType::from_u8(47), MlogRecordType::Unknown(47));
        assert_eq!(MlogRecordType::from_u8(60), MlogRecordType::Unknown(60));
        assert_eq!(MlogRecordType::from_u8(77), MlogRecordType::Unknown(77));
    }

    #[test]
    fn test_mlog_record_type_name() {
        assert_eq!(MlogRecordType::Mlog1Byte.name(), "MLOG_1BYTE");
        assert_eq!(MlogRecordType::MlogRecInsert.name(), "MLOG_REC_INSERT");
        assert_eq!(
            MlogRecordType::MlogRecInsert8027.name(),
            "MLOG_REC_INSERT_8027"
        );
        assert_eq!(MlogRecordType::MlogFileExtend.name(), "MLOG_FILE_EXTEND");
        assert_eq!(MlogRecordType::MlogLsn.name(), "MLOG_LSN");
        assert_eq!(MlogRecordType::Unknown(99).name(), "UNKNOWN");
    }

    #[test]
    fn test_mlog_record_type_display() {
        assert_eq!(format!("{}", MlogRecordType::Mlog1Byte), "MLOG_1BYTE");
        assert_eq!(format!("{}", MlogRecordType::Unknown(99)), "UNKNOWN(99)");
        assert_eq!(
            format!("{}", MlogRecordType::MlogListEndDelete),
            "MLOG_LIST_END_DELETE"
        );
    }

    #[test]
    fn test_log_block_checksum_validation() {
        let mut block = make_block();
        // Put some data in the block
        BigEndian::write_u32(&mut block[0..], 5); // block_no = 5
        BigEndian::write_u16(&mut block[4..], 100); // data_len
        BigEndian::write_u16(&mut block[6..], 12); // first_rec_group
        block[12] = 0xAB; // some log data

        // Calculate and store the correct CRC-32C
        let crc = crc32c::crc32c(&block[..LOG_BLOCK_CHECKSUM_OFFSET]);
        BigEndian::write_u32(&mut block[LOG_BLOCK_CHECKSUM_OFFSET..], crc);

        assert!(validate_log_block_checksum(&block));
    }

    #[test]
    fn test_log_from_bytes_empty() {
        let result = LogFile::from_bytes(vec![]);
        match result {
            Err(e) => assert!(
                e.to_string().contains("too small"),
                "Expected 'too small' in: {e}"
            ),
            Ok(_) => panic!("Expected error for empty input"),
        }
    }

    #[test]
    fn test_log_from_bytes_too_small() {
        let result = LogFile::from_bytes(vec![0u8; 100]);
        match result {
            Err(e) => assert!(
                e.to_string().contains("too small"),
                "Expected 'too small' in: {e}"
            ),
            Ok(_) => panic!("Expected error for 100-byte input"),
        }
    }

    #[test]
    fn test_log_block_checksum_invalid() {
        let mut block = make_block();
        BigEndian::write_u32(&mut block[0..], 5);
        // Wrong checksum
        BigEndian::write_u32(&mut block[LOG_BLOCK_CHECKSUM_OFFSET..], 0xDEADDEAD);

        assert!(!validate_log_block_checksum(&block));
    }
}
