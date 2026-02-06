use std::io::Write;

use colored::Colorize;
use serde::Serialize;

use crate::cli::wprintln;
use crate::innodb::log::{
    validate_log_block_checksum, LogBlockHeader, LogFile, LogFileHeader,
    MlogRecordType, LOG_BLOCK_HDR_SIZE, LOG_BLOCK_SIZE, LOG_FILE_HDR_BLOCKS,
};
use crate::IdbError;

pub struct LogOptions {
    pub file: String,
    pub blocks: Option<u64>,
    pub no_empty: bool,
    pub verbose: bool,
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
    checkpoint_no: u32,
    checksum_valid: bool,
    record_types: Vec<String>,
}

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
    wprintln!(writer, "  Blocks:     {} total ({} data)", log.block_count(), log.data_block_count())?;
    wprintln!(writer)?;

    // Print header
    wprintln!(writer, "{}", "Log File Header (block 0)".bold())?;
    wprintln!(writer, "  Group ID:   {}", header.group_id)?;
    wprintln!(writer, "  Start LSN:  {}", header.start_lsn)?;
    wprintln!(writer, "  File No:    {}", header.file_no)?;
    if !header.created_by.is_empty() {
        wprintln!(writer, "  Created by: {}", header.created_by)?;
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
            "  Block {:>6}  no={:<10} len={:<5} first_rec={:<5} chk_no={:<10} csum={}{}",
            block_idx, hdr.block_no, hdr.data_len, hdr.first_rec_group, hdr.checkpoint_no,
            checksum_str, flush_str,
        )?;

        // Verbose: show MLOG record types
        if opts.verbose && hdr.has_data() {
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

fn print_checkpoint(writer: &mut dyn Write, label: &str, cp: &Option<crate::innodb::log::LogCheckpoint>) -> Result<(), IdbError> {
    wprintln!(writer, "{}", label.bold())?;
    match cp {
        Some(cp) => {
            wprintln!(writer, "  Number:       {}", cp.number)?;
            wprintln!(writer, "  LSN:          {}", cp.lsn)?;
            wprintln!(writer, "  Offset:       {}", cp.offset)?;
            wprintln!(writer, "  Buffer size:  {}", cp.buf_size)?;
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

fn print_record_types(writer: &mut dyn Write, block_data: &[u8], hdr: &LogBlockHeader) -> Result<(), IdbError> {
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
        let mut counts: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
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

        let record_types = if opts.verbose && hdr.has_data() {
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
            checkpoint_no: hdr.checkpoint_no,
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
