use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};

use crate::cli::wprintln;
use crate::innodb::tablespace::Tablespace;
use crate::util::hex::hex_dump;
use crate::IdbError;

pub struct DumpOptions {
    pub file: String,
    pub page: Option<u64>,
    pub offset: Option<u64>,
    pub length: Option<usize>,
    pub raw: bool,
    pub page_size: Option<u32>,
}

pub fn execute(opts: &DumpOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    if let Some(abs_offset) = opts.offset {
        // Absolute offset mode: dump raw bytes from file position
        return dump_at_offset(&opts.file, abs_offset, opts.length.unwrap_or(256), opts.raw, writer);
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
            opts.file, page_num, dump_len
        )?;
        wprintln!(writer)?;
        wprintln!(writer, "{}", hex_dump(&page_data[..dump_len], base_offset))?;
    }

    Ok(())
}

fn dump_at_offset(file: &str, offset: u64, length: usize, raw: bool, writer: &mut dyn Write) -> Result<(), IdbError> {
    let mut f = File::open(file)
        .map_err(|e| IdbError::Io(format!("Cannot open {}: {}", file, e)))?;

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
    f.read_exact(&mut buf)
        .map_err(|e| IdbError::Io(format!("Cannot read {} bytes at offset {}: {}", read_len, offset, e)))?;

    if raw {
        writer
            .write_all(&buf)
            .map_err(|e| IdbError::Io(format!("Cannot write to stdout: {}", e)))?;
    } else {
        wprintln!(
            writer,
            "Hex dump of {} at offset {} ({} bytes):",
            file, offset, read_len
        )?;
        wprintln!(writer)?;
        wprintln!(writer, "{}", hex_dump(&buf, offset))?;
    }

    Ok(())
}
