use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};

use colored::Colorize;

use serde::Serialize;

use crate::cli::wprintln;
use crate::innodb::checksum::{validate_checksum, ChecksumAlgorithm};
use crate::innodb::constants::{SIZE_FIL_HEAD, SIZE_FIL_TRAILER};
use crate::util::hex::format_bytes;
use crate::IdbError;

/// Options for the `inno corrupt` subcommand.
pub struct CorruptOptions {
    /// Path to the InnoDB tablespace file (.ibd).
    pub file: String,
    /// Page number to corrupt (random page chosen when not specified).
    pub page: Option<u64>,
    /// Number of random bytes to write.
    pub bytes: usize,
    /// Target the FIL header area (first 38 bytes of the page).
    pub header: bool,
    /// Target the record data area (after page header, before trailer).
    pub records: bool,
    /// Absolute byte offset to corrupt (bypasses page calculation).
    pub offset: Option<u64>,
    /// Show before/after checksum comparison.
    pub verify: bool,
    /// Emit output as JSON.
    pub json: bool,
    /// Override the auto-detected page size.
    pub page_size: Option<u32>,
    /// Use memory-mapped I/O for file access.
    pub mmap: bool,
}

#[derive(Serialize)]
struct CorruptResultJson {
    file: String,
    offset: u64,
    page: Option<u64>,
    bytes_written: usize,
    data: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    verify: Option<VerifyResultJson>,
}

#[derive(Serialize)]
struct VerifyResultJson {
    page: u64,
    before: ChecksumInfoJson,
    after: ChecksumInfoJson,
}

#[derive(Serialize)]
struct ChecksumInfoJson {
    valid: bool,
    algorithm: String,
    stored_checksum: u32,
    calculated_checksum: u32,
}

/// Inject random bytes into an InnoDB tablespace file to simulate corruption.
///
/// Generates cryptographically random bytes and writes them into the file at a
/// calculated or explicit offset. This is designed for testing checksum
/// validation (`inno checksum`), InnoDB crash recovery, and backup-restore
/// verification workflows.
///
/// Three targeting modes are available:
///
/// - **Header mode** (`-k`): Writes into the 38-byte FIL header area (bytes
///   0–37 of the page), which will corrupt page metadata like the checksum,
///   page number, LSN, or space ID.
/// - **Records mode** (`-r`): Writes into the user data area (after the page
///   header and before the FIL trailer), corrupting actual row or index data
///   without necessarily invalidating the stored checksum.
/// - **Offset mode** (`--offset`): Writes at an absolute file byte position,
///   bypassing page calculations entirely. Note that `--verify` is unavailable
///   in this mode since there is no page context.
///
/// If no page number is specified, one is chosen at random. With `--verify`,
/// the page's checksum is validated before and after the write, showing a
/// before/after comparison to confirm that corruption was successfully applied.
pub fn execute(opts: &CorruptOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    // Absolute offset mode: bypass page calculation entirely
    if let Some(abs_offset) = opts.offset {
        return corrupt_at_offset(opts, abs_offset, writer);
    }

    // Open tablespace to get page size and count
    let ts = crate::cli::open_tablespace(&opts.file, opts.page_size, opts.mmap)?;

    let page_size = ts.page_size() as usize;
    let page_count = ts.page_count();

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
            let p = rand::random_range(0..page_count);
            if !opts.json {
                wprintln!(
                    writer,
                    "No page specified. Choosing random page {}.",
                    format!("{}", p).yellow()
                )?;
            }
            p
        }
    };

    let byte_start = page_num * page_size as u64;

    // Calculate the offset to corrupt within the page
    let corrupt_offset = if opts.header {
        // Corrupt within the FIL header area (first 38 bytes)
        let header_offset = rand::random_range(0..SIZE_FIL_HEAD as u64);
        byte_start + header_offset
    } else if opts.records {
        // Corrupt within the record data area (after page header, before trailer)
        let user_data_start = 120u64; // matches Perl USER_DATA_START
        let max_offset = page_size as u64 - user_data_start - SIZE_FIL_TRAILER as u64;
        let record_offset = rand::random_range(0..max_offset);
        byte_start + user_data_start + record_offset
    } else {
        // Default: corrupt at page start
        byte_start
    };

    // Generate random bytes (full bytes, not nibbles like the Perl version)
    let random_data: Vec<u8> = (0..opts.bytes).map(|_| rand::random::<u8>()).collect();

    // Read pre-corruption page data for --verify
    let pre_checksum = if opts.verify {
        let pre_data = read_page_bytes(&opts.file, page_num, page_size as u32)?;
        Some(validate_checksum(&pre_data, page_size as u32, None))
    } else {
        None
    };

    if opts.json {
        // Write the corruption first, then verify
        write_corruption(&opts.file, corrupt_offset, &random_data)?;
        let verify_json = if opts.verify {
            let post_data = read_page_bytes(&opts.file, page_num, page_size as u32)?;
            let post_result = validate_checksum(&post_data, page_size as u32, None);
            let pre = pre_checksum.expect("pre_checksum set when --verify is active");
            Some(VerifyResultJson {
                page: page_num,
                before: checksum_to_json(&pre),
                after: checksum_to_json(&post_result),
            })
        } else {
            None
        };
        return output_json_with_verify(
            opts,
            corrupt_offset,
            Some(page_num),
            &random_data,
            verify_json,
            writer,
        );
    }

    wprintln!(
        writer,
        "Writing {} bytes of random data to {} at offset {} (page {})...",
        opts.bytes,
        opts.file,
        corrupt_offset,
        format!("{}", page_num).yellow()
    )?;

    write_corruption(&opts.file, corrupt_offset, &random_data)?;

    wprintln!(writer, "Data written: {}", format_bytes(&random_data).red())?;
    wprintln!(writer, "Completed.")?;

    // --verify: show before/after checksum comparison
    if opts.verify {
        let post_data = read_page_bytes(&opts.file, page_num, page_size as u32)?;
        let post_result = validate_checksum(&post_data, page_size as u32, None);
        let pre = pre_checksum.expect("pre_checksum set when --verify is active");
        wprintln!(writer)?;
        wprintln!(writer, "{}:", "Verification".bold())?;
        wprintln!(
            writer,
            "  Before: {} (algorithm={:?}, stored={}, calculated={})",
            if pre.valid {
                "OK".green().to_string()
            } else {
                "INVALID".red().to_string()
            },
            pre.algorithm,
            pre.stored_checksum,
            pre.calculated_checksum
        )?;
        wprintln!(
            writer,
            "  After:  {} (algorithm={:?}, stored={}, calculated={})",
            if post_result.valid {
                "OK".green().to_string()
            } else {
                "INVALID".red().to_string()
            },
            post_result.algorithm,
            post_result.stored_checksum,
            post_result.calculated_checksum
        )?;
    }

    Ok(())
}

fn corrupt_at_offset(
    opts: &CorruptOptions,
    abs_offset: u64,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
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

    let random_data: Vec<u8> = (0..opts.bytes).map(|_| rand::random::<u8>()).collect();

    // Write the corruption
    write_corruption(&opts.file, abs_offset, &random_data)?;

    if opts.json {
        return output_json_with_verify(opts, abs_offset, None, &random_data, None, writer);
    }

    wprintln!(
        writer,
        "Writing {} bytes of random data to {} at offset {}...",
        opts.bytes,
        opts.file,
        abs_offset
    )?;

    wprintln!(writer, "Data written: {}", format_bytes(&random_data).red())?;
    wprintln!(writer, "Completed.")?;

    if opts.verify {
        wprintln!(
            writer,
            "Note: --verify is not available in absolute offset mode (no page context)."
        )?;
    }

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

fn output_json_with_verify(
    opts: &CorruptOptions,
    offset: u64,
    page: Option<u64>,
    data: &[u8],
    verify: Option<VerifyResultJson>,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    let result = CorruptResultJson {
        file: opts.file.clone(),
        offset,
        page,
        bytes_written: data.len(),
        data: format_bytes(data),
        verify,
    };

    let json = serde_json::to_string_pretty(&result)
        .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
    wprintln!(writer, "{}", json)?;

    Ok(())
}

fn read_page_bytes(file_path: &str, page_num: u64, page_size: u32) -> Result<Vec<u8>, IdbError> {
    use std::io::Read;
    let offset = page_num * page_size as u64;
    let mut f = File::open(file_path)
        .map_err(|e| IdbError::Io(format!("Cannot open {}: {}", file_path, e)))?;
    f.seek(SeekFrom::Start(offset))
        .map_err(|e| IdbError::Io(format!("Cannot seek to offset {}: {}", offset, e)))?;
    let mut buf = vec![0u8; page_size as usize];
    f.read_exact(&mut buf)
        .map_err(|e| IdbError::Io(format!("Cannot read page {}: {}", page_num, e)))?;
    Ok(buf)
}

fn checksum_to_json(result: &crate::innodb::checksum::ChecksumResult) -> ChecksumInfoJson {
    let algorithm_name = match result.algorithm {
        ChecksumAlgorithm::Crc32c => "crc32c",
        ChecksumAlgorithm::InnoDB => "innodb",
        ChecksumAlgorithm::MariaDbFullCrc32 => "mariadb_full_crc32",
        ChecksumAlgorithm::None => "none",
    };
    ChecksumInfoJson {
        valid: result.valid,
        algorithm: algorithm_name.to_string(),
        stored_checksum: result.stored_checksum,
        calculated_checksum: result.calculated_checksum,
    }
}
