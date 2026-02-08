use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};

use crate::cli::wprintln;
use crate::innodb::tablespace::Tablespace;
use crate::util::hex::hex_dump;
use crate::IdbError;

/// Options for the `inno dump` subcommand.
pub struct DumpOptions {
    /// Path to the InnoDB tablespace file (.ibd).
    pub file: String,
    /// Page number to dump (defaults to page 0 when not specified).
    pub page: Option<u64>,
    /// Absolute byte offset to start dumping (bypasses page mode).
    pub offset: Option<u64>,
    /// Number of bytes to dump (defaults to page size in page mode, or 256 in offset mode).
    pub length: Option<usize>,
    /// Output raw binary bytes instead of formatted hex dump.
    pub raw: bool,
    /// Override the auto-detected page size.
    pub page_size: Option<u32>,
}

/// Produce a hex dump of raw bytes from an InnoDB tablespace file.
///
/// Operates in two modes:
///
/// - **Page mode** (default): Opens the file as a tablespace, reads the page
///   specified by `-p` (or page 0 if omitted), and prints a formatted hex dump
///   with file-relative byte offsets. The dump length defaults to the full page
///   size but can be shortened with `--length`.
/// - **Offset mode** (`--offset`): Reads bytes starting at an arbitrary
///   absolute file position without page-size awareness. The default read
///   length is 256 bytes. This is useful for inspecting raw structures that
///   do not align to page boundaries (e.g., redo log headers, doublewrite
///   buffer regions).
///
/// In either mode, `--raw` suppresses the formatted hex layout and writes
/// the raw binary bytes directly to the writer, suitable for piping into
/// `xxd`, `hexdump`, or other tools.
pub fn execute(opts: &DumpOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    if let Some(abs_offset) = opts.offset {
        // Absolute offset mode: dump raw bytes from file position
        return dump_at_offset(
            &opts.file,
            abs_offset,
            opts.length.unwrap_or(256),
            opts.raw,
            writer,
        );
    }

    // Page mode: dump a specific page (or page 0 by default)
    let mut ts = match opts.page_size {
        Some(ps) => Tablespace::open_with_page_size(&opts.file, ps)?,
        None => Tablespace::open(&opts.file)?,
    };

    let page_size = ts.page_size();
    let page_num = opts.page.unwrap_or(0);
    let page_data = ts.read_page(page_num)?;

    let length = opts.length.unwrap_or(page_size as usize);
    let dump_len = length.min(page_data.len());
    let base_offset = page_num * page_size as u64;

    if opts.raw {
        writer
            .write_all(&page_data[..dump_len])
            .map_err(|e| IdbError::Io(format!("Cannot write to stdout: {}", e)))?;
    } else {
        wprintln!(
            writer,
            "Hex dump of {} page {} ({} bytes):",
            opts.file,
            page_num,
            dump_len
        )?;
        wprintln!(writer)?;
        wprintln!(writer, "{}", hex_dump(&page_data[..dump_len], base_offset))?;
    }

    Ok(())
}

fn dump_at_offset(
    file: &str,
    offset: u64,
    length: usize,
    raw: bool,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    let mut f =
        File::open(file).map_err(|e| IdbError::Io(format!("Cannot open {}: {}", file, e)))?;

    let file_size = f
        .metadata()
        .map_err(|e| IdbError::Io(format!("Cannot stat {}: {}", file, e)))?
        .len();

    if offset >= file_size {
        return Err(IdbError::Argument(format!(
            "Offset {} is beyond file size {}",
            offset, file_size
        )));
    }

    let available = (file_size - offset) as usize;
    let read_len = length.min(available);

    f.seek(SeekFrom::Start(offset))
        .map_err(|e| IdbError::Io(format!("Cannot seek to offset {}: {}", offset, e)))?;

    let mut buf = vec![0u8; read_len];
    f.read_exact(&mut buf).map_err(|e| {
        IdbError::Io(format!(
            "Cannot read {} bytes at offset {}: {}",
            read_len, offset, e
        ))
    })?;

    if raw {
        writer
            .write_all(&buf)
            .map_err(|e| IdbError::Io(format!("Cannot write to stdout: {}", e)))?;
    } else {
        wprintln!(
            writer,
            "Hex dump of {} at offset {} ({} bytes):",
            file,
            offset,
            read_len
        )?;
        wprintln!(writer)?;
        wprintln!(writer, "{}", hex_dump(&buf, offset))?;
    }

    Ok(())
}
