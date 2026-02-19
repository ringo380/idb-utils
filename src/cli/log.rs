use std::io::Write;

use colored::Colorize;
use serde::Serialize;

use crate::cli::wprintln;
use crate::innodb::log::{
    validate_log_block_checksum, LogBlockHeader, LogFile, LogFileHeader, MlogRecordType,
    LOG_BLOCK_HDR_SIZE, LOG_BLOCK_SIZE, LOG_FILE_HDR_BLOCKS,
};
use crate::IdbError;

/// Options for the `inno log` subcommand.
pub struct LogOptions {
    /// Path to the redo log file (`ib_logfile0`, `ib_logfile1`, or `#ib_redo*`).
    pub file: String,
    /// Limit output to the first N data blocks.
    pub blocks: Option<u64>,
    /// Skip blocks that contain no redo log data.
    pub no_empty: bool,
    /// Show MLOG record types within each data block.
    pub verbose: bool,
    /// Emit output as JSON.
    pub json: bool,
}

#[derive(Serialize)]
struct LogSummaryJson {
    file: String,
    file_size: u64,
    total_blocks: u64,
    data_blocks: u64,
    header: LogFileHeader,
    checkpoint_1: Option<crate::innodb::log::LogCheckpoint>,
    checkpoint_2: Option<crate::innodb::log::LogCheckpoint>,
    blocks: Vec<BlockJson>,
}

#[derive(Serialize)]
struct BlockJson {
    block_index: u64,
    block_no: u32,
    flush_flag: bool,
    data_len: u16,
    first_rec_group: u16,
    epoch_no: u32,
    checksum_valid: bool,
    record_types: Vec<String>,
}

/// Analyze the structure of an InnoDB redo log file.
///
/// InnoDB redo logs are organized as a sequence of 512-byte blocks. The first
/// four blocks are reserved: block 0 is the **log file header** (format version,
/// log UUID, start LSN, creator string), blocks 1 and 3 are **checkpoint
/// records** (checkpoint LSN), and block 2 is reserved/unused. All remaining
/// blocks are **data blocks** containing the actual redo log records.
///
/// This command reads and displays all three sections. For data blocks, each
/// block's header is decoded to show the block number, data length,
/// first-record-group offset, epoch number, flush flag, and CRC-32C
/// checksum validation status.
///
/// With `--verbose`, the payload bytes of each non-empty data block are
/// scanned for MLOG record type bytes (e.g., `MLOG_REC_INSERT`,
/// `MLOG_UNDO_INSERT`, `MLOG_WRITE_STRING`) and a frequency summary is
/// printed. Use `--blocks N` to limit output to the first N data blocks,
/// or `--no-empty` to skip blocks with zero data length.
pub fn execute(opts: &LogOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    let mut log = LogFile::open(&opts.file)?;

    let header = log.read_header()?;
    let cp1 = log.read_checkpoint(0).ok();
    let cp2 = log.read_checkpoint(1).ok();

    if opts.json {
        return execute_json(opts, &mut log, header, cp1, cp2, writer);
    }

    // Print file info
    wprintln!(writer, "{}", "InnoDB Redo Log File".bold())?;
    wprintln!(writer, "  File:       {}", opts.file)?;
    wprintln!(writer, "  Size:       {} bytes", log.file_size())?;
    wprintln!(
        writer,
        "  Blocks:     {} total ({} data)",
        log.block_count(),
        log.data_block_count()
    )?;
    wprintln!(writer)?;

    // Detect vendor from creator string
    let vendor = crate::innodb::vendor::detect_vendor_from_created_by(&header.created_by);
    let is_mariadb = vendor == crate::innodb::vendor::InnoDbVendor::MariaDB;

    // Print header
    wprintln!(writer, "{}", "Log File Header (block 0)".bold())?;
    wprintln!(writer, "  Format:     {}", header.format_version)?;
    wprintln!(writer, "  Start LSN:  {}", header.start_lsn)?;
    if header.log_uuid != 0 {
        wprintln!(writer, "  Log UUID:   0x{:08X}", header.log_uuid)?;
    }
    if !header.created_by.is_empty() {
        wprintln!(writer, "  Created by: {}", header.created_by)?;
    }
    wprintln!(writer, "  Vendor:     {}", vendor)?;
    if is_mariadb {
        wprintln!(
            writer,
            "  {}",
            "Note: MLOG record types are not decoded for MariaDB redo logs".yellow()
        )?;
    }
    wprintln!(writer)?;

    // Print checkpoints
    print_checkpoint(writer, "Checkpoint 1 (block 1)", &cp1)?;
    print_checkpoint(writer, "Checkpoint 2 (block 3)", &cp2)?;

    // Iterate data blocks
    let data_blocks = log.data_block_count();
    let limit = opts.blocks.unwrap_or(data_blocks).min(data_blocks);

    if limit > 0 {
        wprintln!(writer, "{}", "Data Blocks".bold())?;
    }

    let mut displayed = 0u64;
    let mut empty_skipped = 0u64;

    for i in 0..limit {
        let block_idx = LOG_FILE_HDR_BLOCKS + i;
        let block_data = log.read_block(block_idx)?;

        let hdr = match LogBlockHeader::parse(&block_data) {
            Some(h) => h,
            None => continue,
        };

        // Skip empty blocks if --no-empty
        if opts.no_empty && !hdr.has_data() {
            empty_skipped += 1;
            continue;
        }

        let checksum_ok = validate_log_block_checksum(&block_data);
        let checksum_str = if checksum_ok {
            "OK".green().to_string()
        } else {
            "INVALID".red().to_string()
        };

        let flush_str = if hdr.flush_flag { " FLUSH" } else { "" };

        wprintln!(
            writer,
            "  Block {:>6}  no={:<10} len={:<5} first_rec={:<5} epoch={:<10} csum={}{}",
            block_idx,
            hdr.block_no,
            hdr.data_len,
            hdr.first_rec_group,
            hdr.epoch_no,
            checksum_str,
            flush_str,
        )?;

        // Verbose: show MLOG record types (skip for MariaDB — incompatible format)
        if opts.verbose && hdr.has_data() && !is_mariadb {
            print_record_types(writer, &block_data, &hdr)?;
        }

        displayed += 1;
    }

    if opts.no_empty && empty_skipped > 0 {
        wprintln!(writer, "  ({} empty blocks skipped)", empty_skipped)?;
    }

    if displayed > 0 || empty_skipped > 0 {
        wprintln!(writer)?;
    }

    wprintln!(
        writer,
        "Displayed {} data blocks{}",
        displayed,
        if limit < data_blocks {
            format!(" (of {})", data_blocks)
        } else {
            String::new()
        }
    )?;

    Ok(())
}

fn print_checkpoint(
    writer: &mut dyn Write,
    label: &str,
    cp: &Option<crate::innodb::log::LogCheckpoint>,
) -> Result<(), IdbError> {
    wprintln!(writer, "{}", label.bold())?;
    match cp {
        Some(cp) => {
            if cp.number > 0 {
                wprintln!(writer, "  Number:       {}", cp.number)?;
            }
            wprintln!(writer, "  LSN:          {}", cp.lsn)?;
            if cp.offset > 0 {
                wprintln!(writer, "  Offset:       {}", cp.offset)?;
            }
            if cp.buf_size > 0 {
                wprintln!(writer, "  Buffer size:  {}", cp.buf_size)?;
            }
            if cp.archived_lsn > 0 {
                wprintln!(writer, "  Archived LSN: {}", cp.archived_lsn)?;
            }
        }
        None => {
            wprintln!(writer, "  {}", "(not present or unreadable)".yellow())?;
        }
    }
    wprintln!(writer)?;
    Ok(())
}

fn print_record_types(
    writer: &mut dyn Write,
    block_data: &[u8],
    hdr: &LogBlockHeader,
) -> Result<(), IdbError> {
    let data_end = (hdr.data_len as usize).min(LOG_BLOCK_SIZE - 4);
    if data_end <= LOG_BLOCK_HDR_SIZE {
        return Ok(());
    }

    let mut types: Vec<MlogRecordType> = Vec::new();
    let mut pos = LOG_BLOCK_HDR_SIZE;

    while pos < data_end {
        let type_byte = block_data[pos];
        // The single-record flag is bit 7 of the type byte
        let rec_type = MlogRecordType::from_u8(type_byte & 0x7F);
        types.push(rec_type);
        // We can't fully decode record lengths without schema info,
        // so just scan byte-by-byte for type bytes
        pos += 1;
    }

    if !types.is_empty() {
        // Count occurrences
        let mut counts: std::collections::BTreeMap<String, usize> =
            std::collections::BTreeMap::new();
        for t in &types {
            *counts.entry(t.to_string()).or_insert(0) += 1;
        }
        let summary: Vec<String> = counts
            .iter()
            .map(|(name, count)| format!("{}({})", name, count))
            .collect();
        wprintln!(writer, "    record types: {}", summary.join(", "))?;
    }

    Ok(())
}

fn execute_json(
    opts: &LogOptions,
    log: &mut LogFile,
    header: LogFileHeader,
    cp1: Option<crate::innodb::log::LogCheckpoint>,
    cp2: Option<crate::innodb::log::LogCheckpoint>,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    let data_blocks = log.data_block_count();
    let limit = opts.blocks.unwrap_or(data_blocks).min(data_blocks);

    let mut blocks_json = Vec::new();

    for i in 0..limit {
        let block_idx = LOG_FILE_HDR_BLOCKS + i;
        let block_data = log.read_block(block_idx)?;

        let hdr = match LogBlockHeader::parse(&block_data) {
            Some(h) => h,
            None => continue,
        };

        if opts.no_empty && !hdr.has_data() {
            continue;
        }

        let checksum_ok = validate_log_block_checksum(&block_data);

        let is_mariadb = crate::innodb::vendor::detect_vendor_from_created_by(&header.created_by)
            == crate::innodb::vendor::InnoDbVendor::MariaDB;
        let record_types = if opts.verbose && hdr.has_data() && !is_mariadb {
            collect_record_type_names(&block_data, &hdr)
        } else {
            Vec::new()
        };

        blocks_json.push(BlockJson {
            block_index: block_idx,
            block_no: hdr.block_no,
            flush_flag: hdr.flush_flag,
            data_len: hdr.data_len,
            first_rec_group: hdr.first_rec_group,
            epoch_no: hdr.epoch_no,
            checksum_valid: checksum_ok,
            record_types,
        });
    }

    let summary = LogSummaryJson {
        file: opts.file.clone(),
        file_size: log.file_size(),
        total_blocks: log.block_count(),
        data_blocks: log.data_block_count(),
        header,
        checkpoint_1: cp1,
        checkpoint_2: cp2,
        blocks: blocks_json,
    };

    let json = serde_json::to_string_pretty(&summary)
        .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
    wprintln!(writer, "{}", json)?;

    Ok(())
}

fn collect_record_type_names(block_data: &[u8], hdr: &LogBlockHeader) -> Vec<String> {
    let data_end = (hdr.data_len as usize).min(LOG_BLOCK_SIZE - 4);
    if data_end <= LOG_BLOCK_HDR_SIZE {
        return Vec::new();
    }

    let mut names = Vec::new();
    let mut pos = LOG_BLOCK_HDR_SIZE;

    while pos < data_end {
        let type_byte = block_data[pos];
        let rec_type = MlogRecordType::from_u8(type_byte & 0x7F);
        names.push(rec_type.to_string());
        pos += 1;
    }

    names
}
