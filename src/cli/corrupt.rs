use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};

use rand::Rng;

use crate::innodb::constants::{SIZE_FIL_HEAD, SIZE_FIL_TRAILER};
use crate::innodb::tablespace::Tablespace;
use crate::IdbError;

pub struct CorruptOptions {
    pub file: String,
    pub page: Option<u64>,
    pub bytes: usize,
    pub header: bool,
    pub records: bool,
    pub page_size: Option<u32>,
}

pub fn execute(opts: &CorruptOptions) -> Result<(), IdbError> {
    // Validate file extension
    if !opts.file.ends_with(".ibd") {
        return Err(IdbError::Argument(
            "File must have .ibd extension".to_string(),
        ));
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
            println!("No page specified. Choosing random page {}.", p);
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

    println!(
        "Writing {} bytes of random data to {} at offset {} (page {})...",
        opts.bytes, opts.file, corrupt_offset, page_num
    );

    // Write the corruption
    let mut file = OpenOptions::new()
        .write(true)
        .open(&opts.file)
        .map_err(|e| IdbError::Io(format!("Cannot open {} for writing: {}", opts.file, e)))?;

    file.seek(SeekFrom::Start(corrupt_offset))
        .map_err(|e| IdbError::Io(format!("Cannot seek to offset {}: {}", corrupt_offset, e)))?;

    file.write_all(&random_data)
        .map_err(|e| IdbError::Io(format!("Cannot write corruption data: {}", e)))?;

    print!("Data written: ");
    for b in &random_data {
        print!("{:02x}", b);
    }
    println!();
    println!("Completed.");

    Ok(())
}
