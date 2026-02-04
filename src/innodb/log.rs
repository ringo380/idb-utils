use byteorder::{BigEndian, ByteOrder};
use serde::Serialize;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use crate::IdbError;

// Redo log block/file structure constants (from MySQL log0log.h)
pub const LOG_BLOCK_SIZE: usize = 512;
pub const LOG_BLOCK_HDR_SIZE: usize = 14;
pub const LOG_BLOCK_TRL_SIZE: usize = 4;
pub const LOG_BLOCK_FLUSH_BIT_MASK: u32 = 0x80000000;
pub const LOG_BLOCK_CHECKSUM_OFFSET: usize = 508; // within block: bytes 508-511
pub const LOG_FILE_HDR_BLOCKS: u64 = 4; // blocks 0-3 are reserved

// Log file header offsets (within block 0)
pub const LOG_HEADER_GROUP_ID: usize = 0;
pub const LOG_HEADER_START_LSN: usize = 4;
pub const LOG_HEADER_FILE_NO: usize = 12;
pub const LOG_HEADER_CREATED_BY: usize = 16;
pub const LOG_HEADER_CREATED_BY_LEN: usize = 32;

// Checkpoint offsets (within checkpoint block)
pub const LOG_CHECKPOINT_NO: usize = 0;
pub const LOG_CHECKPOINT_LSN: usize = 8;
pub const LOG_CHECKPOINT_OFFSET: usize = 16;
pub const LOG_CHECKPOINT_BUF_SIZE: usize = 20;
pub const LOG_CHECKPOINT_ARCHIVED_LSN: usize = 24;

/// Log file header (block 0 of the redo log file).
#[derive(Debug, Clone, Serialize)]
pub struct LogFileHeader {
    pub group_id: u32,
    pub start_lsn: u64,
    pub file_no: u32,
    pub created_by: String,
}

impl LogFileHeader {
    /// Parse a log file header from the first 512-byte block.
    pub fn parse(block: &[u8]) -> Option<Self> {
        if block.len() < LOG_BLOCK_SIZE {
            return None;
        }

        let group_id = BigEndian::read_u32(&block[LOG_HEADER_GROUP_ID..]);
        let start_lsn = BigEndian::read_u64(&block[LOG_HEADER_START_LSN..]);
        let file_no = BigEndian::read_u32(&block[LOG_HEADER_FILE_NO..]);

        let created_bytes =
            &block[LOG_HEADER_CREATED_BY..LOG_HEADER_CREATED_BY + LOG_HEADER_CREATED_BY_LEN];
        let created_by = created_bytes
            .iter()
            .take_while(|&&b| b != 0)
            .map(|&b| b as char)
            .collect::<String>();

        Some(LogFileHeader {
            group_id,
            start_lsn,
            file_no,
            created_by,
        })
    }
}

/// Checkpoint record (blocks 1 and 3 of the redo log file).
#[derive(Debug, Clone, Serialize)]
pub struct LogCheckpoint {
    pub number: u64,
    pub lsn: u64,
    pub offset: u32,
    pub buf_size: u32,
    pub archived_lsn: u64,
}

impl LogCheckpoint {
    /// Parse a checkpoint from a 512-byte block.
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

/// Log block header (first 14 bytes of each 512-byte block).
#[derive(Debug, Clone, Serialize)]
pub struct LogBlockHeader {
    pub block_no: u32,
    pub flush_flag: bool,
    pub data_len: u16,
    pub first_rec_group: u16,
    pub checkpoint_no: u32,
}

impl LogBlockHeader {
    /// Parse a log block header from a 512-byte block.
    pub fn parse(block: &[u8]) -> Option<Self> {
        if block.len() < LOG_BLOCK_HDR_SIZE {
            return None;
        }

        let raw_block_no = BigEndian::read_u32(&block[0..]);
        let flush_flag = (raw_block_no & LOG_BLOCK_FLUSH_BIT_MASK) != 0;
        let block_no = raw_block_no & !LOG_BLOCK_FLUSH_BIT_MASK;

        let data_len = BigEndian::read_u16(&block[4..]);
        let first_rec_group = BigEndian::read_u16(&block[6..]);
        let checkpoint_no = BigEndian::read_u32(&block[8..]);

        Some(LogBlockHeader {
            block_no,
            flush_flag,
            data_len,
            first_rec_group,
            checkpoint_no,
        })
    }

    /// Returns true if this block contains log data (data_len > header size).
    pub fn has_data(&self) -> bool {
        self.data_len as usize > LOG_BLOCK_HDR_SIZE
    }
}

/// Log block trailer (last 4 bytes of each 512-byte block).
#[derive(Debug, Clone, Serialize)]
pub struct LogBlockTrailer {
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

/// MLOG record types (from MySQL mtr0types.h).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum MlogRecordType {
    Mlog1Byte,
    Mlog2Bytes,
    Mlog4Bytes,
    Mlog8Bytes,
    MlogRecInsert,
    MlogRecClustDeleteMark,
    MlogRecSecDeleteMark,
    MlogRecUpdate,
    MlogRecDelete,
    MlogListEndDelete,
    MlogListStartDelete,
    MlogListEndCopyCreated,
    MlogPageReorganize,
    MlogPageCreate,
    MlogUndoInsert,
    MlogUndoEraseEnd,
    MlogUndoInit,
    MlogUndoHdrReuse,
    MlogRecMinMark,
    MlogIbufBitmapInit,
    MlogInitFilePage,
    MlogWriteString,
    MlogMultiRecEnd,
    MlogDummyRecord,
    MlogFileDelete,
    MlogCompPageCreate,
    MlogCompRecInsert,
    MlogCompRecClustDeleteMark,
    MlogCompRecSecDeleteMark,
    MlogCompRecUpdate,
    MlogCompRecDelete,
    MlogCompListEndDelete,
    MlogCompListStartDelete,
    MlogCompListEndCopyCreated,
    MlogCompPageReorganize,
    MlogFileRename,
    MlogPageCreateRTree,
    MlogCompPageCreateRTree,
    MlogTableDynamicMeta,
    MlogPageCreateSdi,
    MlogCompPageCreateSdi,
    MlogFileOpen,
    MlogFileCreate,
    MlogZipPageCompress,
    Unknown(u8),
}

impl MlogRecordType {
    /// Convert a u8 type code to MlogRecordType.
    pub fn from_u8(val: u8) -> Self {
        match val {
            1 => MlogRecordType::Mlog1Byte,
            2 => MlogRecordType::Mlog2Bytes,
            4 => MlogRecordType::Mlog4Bytes,
            8 => MlogRecordType::Mlog8Bytes,
            9 => MlogRecordType::MlogRecInsert,
            10 => MlogRecordType::MlogRecClustDeleteMark,
            11 => MlogRecordType::MlogRecSecDeleteMark,
            13 => MlogRecordType::MlogRecUpdate,
            14 => MlogRecordType::MlogRecDelete,
            15 => MlogRecordType::MlogListEndDelete,
            16 => MlogRecordType::MlogListStartDelete,
            17 => MlogRecordType::MlogListEndCopyCreated,
            18 => MlogRecordType::MlogPageReorganize,
            19 => MlogRecordType::MlogPageCreate,
            20 => MlogRecordType::MlogUndoInsert,
            21 => MlogRecordType::MlogUndoEraseEnd,
            22 => MlogRecordType::MlogUndoInit,
            24 => MlogRecordType::MlogUndoHdrReuse,
            28 => MlogRecordType::MlogRecMinMark,
            29 => MlogRecordType::MlogIbufBitmapInit,
            30 => MlogRecordType::MlogInitFilePage,
            31 => MlogRecordType::MlogWriteString,
            32 => MlogRecordType::MlogMultiRecEnd,
            33 => MlogRecordType::MlogDummyRecord,
            34 => MlogRecordType::MlogFileDelete,
            35 => MlogRecordType::MlogCompPageCreate,
            36 => MlogRecordType::MlogCompRecInsert,
            37 => MlogRecordType::MlogCompRecClustDeleteMark,
            38 => MlogRecordType::MlogCompRecSecDeleteMark,
            39 => MlogRecordType::MlogCompRecUpdate,
            40 => MlogRecordType::MlogCompRecDelete,
            41 => MlogRecordType::MlogCompListEndDelete,
            42 => MlogRecordType::MlogCompListStartDelete,
            43 => MlogRecordType::MlogCompListEndCopyCreated,
            44 => MlogRecordType::MlogCompPageReorganize,
            45 => MlogRecordType::MlogFileRename,
            46 => MlogRecordType::MlogPageCreateRTree,
            47 => MlogRecordType::MlogCompPageCreateRTree,
            48 => MlogRecordType::MlogTableDynamicMeta,
            49 => MlogRecordType::MlogPageCreateSdi,
            50 => MlogRecordType::MlogCompPageCreateSdi,
            51 => MlogRecordType::MlogFileOpen,
            52 => MlogRecordType::MlogFileCreate,
            53 => MlogRecordType::MlogZipPageCompress,
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
            MlogRecordType::MlogRecInsert => "MLOG_REC_INSERT",
            MlogRecordType::MlogRecClustDeleteMark => "MLOG_REC_CLUST_DELETE_MARK",
            MlogRecordType::MlogRecSecDeleteMark => "MLOG_REC_SEC_DELETE_MARK",
            MlogRecordType::MlogRecUpdate => "MLOG_REC_UPDATE_IN_PLACE",
            MlogRecordType::MlogRecDelete => "MLOG_REC_DELETE",
            MlogRecordType::MlogListEndDelete => "MLOG_LIST_END_DELETE",
            MlogRecordType::MlogListStartDelete => "MLOG_LIST_START_DELETE",
            MlogRecordType::MlogListEndCopyCreated => "MLOG_LIST_END_COPY_CREATED",
            MlogRecordType::MlogPageReorganize => "MLOG_PAGE_REORGANIZE",
            MlogRecordType::MlogPageCreate => "MLOG_PAGE_CREATE",
            MlogRecordType::MlogUndoInsert => "MLOG_UNDO_INSERT",
            MlogRecordType::MlogUndoEraseEnd => "MLOG_UNDO_ERASE_END",
            MlogRecordType::MlogUndoInit => "MLOG_UNDO_INIT",
            MlogRecordType::MlogUndoHdrReuse => "MLOG_UNDO_HDR_REUSE",
            MlogRecordType::MlogRecMinMark => "MLOG_REC_MIN_MARK",
            MlogRecordType::MlogIbufBitmapInit => "MLOG_IBUF_BITMAP_INIT",
            MlogRecordType::MlogInitFilePage => "MLOG_INIT_FILE_PAGE",
            MlogRecordType::MlogWriteString => "MLOG_WRITE_STRING",
            MlogRecordType::MlogMultiRecEnd => "MLOG_MULTI_REC_END",
            MlogRecordType::MlogDummyRecord => "MLOG_DUMMY_RECORD",
            MlogRecordType::MlogFileDelete => "MLOG_FILE_DELETE",
            MlogRecordType::MlogCompPageCreate => "MLOG_COMP_PAGE_CREATE",
            MlogRecordType::MlogCompRecInsert => "MLOG_COMP_REC_INSERT",
            MlogRecordType::MlogCompRecClustDeleteMark => "MLOG_COMP_REC_CLUST_DELETE_MARK",
            MlogRecordType::MlogCompRecSecDeleteMark => "MLOG_COMP_REC_SEC_DELETE_MARK",
            MlogRecordType::MlogCompRecUpdate => "MLOG_COMP_REC_UPDATE_IN_PLACE",
            MlogRecordType::MlogCompRecDelete => "MLOG_COMP_REC_DELETE",
            MlogRecordType::MlogCompListEndDelete => "MLOG_COMP_LIST_END_DELETE",
            MlogRecordType::MlogCompListStartDelete => "MLOG_COMP_LIST_START_DELETE",
            MlogRecordType::MlogCompListEndCopyCreated => "MLOG_COMP_LIST_END_COPY_CREATED",
            MlogRecordType::MlogCompPageReorganize => "MLOG_COMP_PAGE_REORGANIZE",
            MlogRecordType::MlogFileRename => "MLOG_FILE_RENAME",
            MlogRecordType::MlogPageCreateRTree => "MLOG_PAGE_CREATE_RTREE",
            MlogRecordType::MlogCompPageCreateRTree => "MLOG_COMP_PAGE_CREATE_RTREE",
            MlogRecordType::MlogTableDynamicMeta => "MLOG_TABLE_DYNAMIC_META",
            MlogRecordType::MlogPageCreateSdi => "MLOG_PAGE_CREATE_SDI",
            MlogRecordType::MlogCompPageCreateSdi => "MLOG_COMP_PAGE_CREATE_SDI",
            MlogRecordType::MlogFileOpen => "MLOG_FILE_OPEN",
            MlogRecordType::MlogFileCreate => "MLOG_FILE_CREATE",
            MlogRecordType::MlogZipPageCompress => "MLOG_ZIP_PAGE_COMPRESS",
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
    file: File,
    file_size: u64,
}

impl LogFile {
    /// Open a redo log file.
    pub fn open(path: &str) -> Result<Self, IdbError> {
        let file =
            File::open(path).map_err(|e| IdbError::Io(format!("Cannot open {}: {}", path, e)))?;
        let file_size = file
            .metadata()
            .map_err(|e| IdbError::Io(format!("Cannot stat {}: {}", path, e)))?
            .len();

        if file_size < (LOG_FILE_HDR_BLOCKS as usize * LOG_BLOCK_SIZE) as u64 {
            return Err(IdbError::Parse(format!(
                "File {} is too small for a redo log ({} bytes, minimum {})",
                path,
                file_size,
                LOG_FILE_HDR_BLOCKS as usize * LOG_BLOCK_SIZE
            )));
        }

        Ok(LogFile { file, file_size })
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

        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(|e| IdbError::Io(format!("Seek error: {}", e)))?;

        let mut buf = vec![0u8; LOG_BLOCK_SIZE];
        self.file
            .read_exact(&mut buf)
            .map_err(|e| IdbError::Io(format!("Read error at block {}: {}", block_no, e)))?;

        Ok(buf)
    }

    /// Read and parse the log file header (block 0).
    pub fn read_header(&mut self) -> Result<LogFileHeader, IdbError> {
        let block = self.read_block(0)?;
        LogFileHeader::parse(&block).ok_or_else(|| {
            IdbError::Parse("Failed to parse log file header (block 0)".to_string())
        })
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
        // checkpoint_no = 7
        BigEndian::write_u32(&mut block[8..], 7);

        let hdr = LogBlockHeader::parse(&block).unwrap();
        assert_eq!(hdr.block_no, 42);
        assert!(!hdr.flush_flag);
        assert_eq!(hdr.data_len, 200);
        assert_eq!(hdr.first_rec_group, 50);
        assert_eq!(hdr.checkpoint_no, 7);
        assert!(hdr.has_data());
    }

    #[test]
    fn test_log_block_flush_bit() {
        let mut block = make_block();
        // Set flush bit (bit 31) + block_no = 100
        BigEndian::write_u32(&mut block[0..], 0x80000064);
        BigEndian::write_u16(&mut block[4..], 14); // data_len = header only

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
        assert_eq!(hdr.checkpoint_no, 0);
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
    fn test_log_file_header_parse() {
        let mut block = make_block();
        BigEndian::write_u32(&mut block[LOG_HEADER_GROUP_ID..], 1);
        BigEndian::write_u64(&mut block[LOG_HEADER_START_LSN..], 0x00000000001A2B3C);
        BigEndian::write_u32(&mut block[LOG_HEADER_FILE_NO..], 0);
        // "MySQL 8.0.32" as created_by
        let creator = b"MySQL 8.0.32";
        block[LOG_HEADER_CREATED_BY..LOG_HEADER_CREATED_BY + creator.len()]
            .copy_from_slice(creator);

        let hdr = LogFileHeader::parse(&block).unwrap();
        assert_eq!(hdr.group_id, 1);
        assert_eq!(hdr.start_lsn, 0x1A2B3C);
        assert_eq!(hdr.file_no, 0);
        assert_eq!(hdr.created_by, "MySQL 8.0.32");
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
        BigEndian::write_u64(&mut block[LOG_CHECKPOINT_ARCHIVED_LSN..], 0x00000000CAFEBABE);

        let cp = LogCheckpoint::parse(&block).unwrap();
        assert_eq!(cp.number, 99);
        assert_eq!(cp.lsn, 0xDEADBEEF);
        assert_eq!(cp.offset, 2048);
        assert_eq!(cp.buf_size, 65536);
        assert_eq!(cp.archived_lsn, 0xCAFEBABE);
    }

    #[test]
    fn test_mlog_record_type_from_u8_known() {
        assert_eq!(MlogRecordType::from_u8(1), MlogRecordType::Mlog1Byte);
        assert_eq!(MlogRecordType::from_u8(9), MlogRecordType::MlogRecInsert);
        assert_eq!(
            MlogRecordType::from_u8(36),
            MlogRecordType::MlogCompRecInsert
        );
        assert_eq!(
            MlogRecordType::from_u8(53),
            MlogRecordType::MlogZipPageCompress
        );
    }

    #[test]
    fn test_mlog_record_type_unknown() {
        assert_eq!(MlogRecordType::from_u8(0), MlogRecordType::Unknown(0));
        assert_eq!(MlogRecordType::from_u8(255), MlogRecordType::Unknown(255));
        assert_eq!(MlogRecordType::from_u8(100), MlogRecordType::Unknown(100));
    }

    #[test]
    fn test_mlog_record_type_name() {
        assert_eq!(MlogRecordType::Mlog1Byte.name(), "MLOG_1BYTE");
        assert_eq!(MlogRecordType::MlogRecInsert.name(), "MLOG_REC_INSERT");
        assert_eq!(MlogRecordType::Unknown(99).name(), "UNKNOWN");
    }

    #[test]
    fn test_mlog_record_type_display() {
        assert_eq!(format!("{}", MlogRecordType::Mlog1Byte), "MLOG_1BYTE");
        assert_eq!(format!("{}", MlogRecordType::Unknown(99)), "UNKNOWN(99)");
    }

    #[test]
    fn test_log_block_checksum_validation() {
        let mut block = make_block();
        // Put some data in the block
        BigEndian::write_u32(&mut block[0..], 5); // block_no = 5
        BigEndian::write_u16(&mut block[4..], 100); // data_len
        BigEndian::write_u16(&mut block[6..], 14); // first_rec_group
        block[14] = 0xAB; // some log data

        // Calculate and store the correct CRC-32C
        let crc = crc32c::crc32c(&block[..LOG_BLOCK_CHECKSUM_OFFSET]);
        BigEndian::write_u32(&mut block[LOG_BLOCK_CHECKSUM_OFFSET..], crc);

        assert!(validate_log_block_checksum(&block));
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
