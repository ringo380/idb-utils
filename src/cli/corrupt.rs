use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};

use colored::Colorize;
use rand::Rng;
use serde::Serialize;

use crate::innodb::constants::{SIZE_FIL_HEAD, SIZE_FIL_TRAILER};
use crate::innodb::tablespace::Tablespace;
use crate::util::hex::format_bytes;
use crate::IdbError;

pub struct CorruptOptions {
    pub file: String,
    pub page: Option<u64>,
    pub bytes: usize,
    pub header: bool,
    pub records: bool,
    pub offset: Option<u64>,
    pub json: bool,
    pub page_size: Option<u32>,
}

#[derive(Serialize)]
struct CorruptResultJson {
    file: String,
    offset: u64,
    page: Option<u64>,
    bytes_written: usize,
    data: String,
}

pub fn execute(opts: &CorruptOptions) -> Result<(), IdbError> {
    // Absolute offset mode: bypass page calculation entirely
    if let Some(abs_offset) = opts.offset {
        return corrupt_at_offset(opts, abs_offset);
    }

    // Open tablespace to get page size and count
    let ts = match opts.page_size {
        Some(ps) => Tablespace::open_with_page_size(&opts.file, ps)?,
        None => Tablespace::open(&opts.file)?,
    };

    let page_size = ts.page_size() as usize;
    let page_count = ts.page_count();

    let mut rng = rand::rng();

    // Choose page
    let page_num = match opts.page {
        Some(p) => {
            if p >= page_count {
                return Err(IdbError::Argument(format!(
                    "Page {} out of range (tablespace has {} pages)",
                    p, page_count
                )));
            }
            p
        }
        None => {
            let p = rng.random_range(0..page_count);
            if !opts.json {
                println!(
                    "No page specified. Choosing random page {}.",
                    format!("{}", p).yellow()
                );
            }
            p
        }
    };

    let byte_start = page_num * page_size as u64;

    // Calculate the offset to corrupt within the page
    let corrupt_offset = if opts.header {
        // Corrupt within the FIL header area (first 38 bytes)
        let header_offset = rng.random_range(0..SIZE_FIL_HEAD as u64);
        byte_start + header_offset
    } else if opts.records {
        // Corrupt within the record data area (after page header, before trailer)
        let user_data_start = 120u64; // matches Perl USER_DATA_START
        let max_offset = page_size as u64 - user_data_start - SIZE_FIL_TRAILER as u64;
        let record_offset = rng.random_range(0..max_offset);
        byte_start + user_data_start + record_offset
    } else {
        // Default: corrupt at page start
        byte_start
    };

    // Generate random bytes (full bytes, not nibbles like the Perl version)
    let random_data: Vec<u8> = (0..opts.bytes).map(|_| rng.random::<u8>()).collect();

    if opts.json {
        return output_json(opts, corrupt_offset, Some(page_num), &random_data);
    }

    println!(
        "Writing {} bytes of random data to {} at offset {} (page {})...",
        opts.bytes,
        opts.file,
        corrupt_offset,
        format!("{}", page_num).yellow()
    );

    write_corruption(&opts.file, corrupt_offset, &random_data)?;

    println!("Data written: {}", format_bytes(&random_data).red());
    println!("Completed.");

    Ok(())
}

fn corrupt_at_offset(opts: &CorruptOptions, abs_offset: u64) -> Result<(), IdbError> {
    // Validate offset is within file
    let file_size = File::open(&opts.file)
        .map_err(|e| IdbError::Io(format!("Cannot open {}: {}", opts.file, e)))?
        .metadata()
        .map_err(|e| IdbError::Io(format!("Cannot stat {}: {}", opts.file, e)))?
        .len();

    if abs_offset >= file_size {
        return Err(IdbError::Argument(format!(
            "Offset {} is beyond file size {}",
            abs_offset, file_size
        )));
    }

    let mut rng = rand::rng();
    let random_data: Vec<u8> = (0..opts.bytes).map(|_| rng.random::<u8>()).collect();

    if opts.json {
        return output_json(opts, abs_offset, None, &random_data);
    }

    println!(
        "Writing {} bytes of random data to {} at offset {}...",
        opts.bytes, opts.file, abs_offset
    );

    write_corruption(&opts.file, abs_offset, &random_data)?;

    println!("Data written: {}", format_bytes(&random_data).red());
    println!("Completed.");

    Ok(())
}

fn write_corruption(file_path: &str, offset: u64, data: &[u8]) -> Result<(), IdbError> {
    let mut file = OpenOptions::new()
        .write(true)
        .open(file_path)
        .map_err(|e| IdbError::Io(format!("Cannot open {} for writing: {}", file_path, e)))?;

    file.seek(SeekFrom::Start(offset))
        .map_err(|e| IdbError::Io(format!("Cannot seek to offset {}: {}", offset, e)))?;

    file.write_all(data)
        .map_err(|e| IdbError::Io(format!("Cannot write corruption data: {}", e)))?;

    Ok(())
}

fn output_json(
    opts: &CorruptOptions,
    offset: u64,
    page: Option<u64>,
    data: &[u8],
) -> Result<(), IdbError> {
    // Still write the corruption before outputting JSON
    write_corruption(&opts.file, offset, data)?;

    let result = CorruptResultJson {
        file: opts.file.clone(),
        offset,
        page,
        bytes_written: data.len(),
        data: format_bytes(data),
    };

    let json = serde_json::to_string_pretty(&result)
        .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
    println!("{}", json);

    Ok(())
}
