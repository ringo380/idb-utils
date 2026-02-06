use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;

use byteorder::{BigEndian, ByteOrder};
use serde::Serialize;

use crate::cli::wprintln;
use crate::innodb::constants::FIL_PAGE_DATA;
use crate::innodb::tablespace::Tablespace;
use crate::util::fs::find_tablespace_files;
use crate::IdbError;

pub struct TsidOptions {
    pub datadir: String,
    pub list: bool,
    pub tablespace_id: Option<u32>,
    pub json: bool,
    pub page_size: Option<u32>,
}

#[derive(Serialize)]
struct TsidResultJson {
    datadir: String,
    tablespaces: Vec<TsidEntryJson>,
}

#[derive(Serialize)]
struct TsidEntryJson {
    file: String,
    space_id: u32,
}

pub fn execute(opts: &TsidOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    let datadir = Path::new(&opts.datadir);
    if !datadir.is_dir() {
        return Err(IdbError::Argument(format!(
            "Data directory does not exist: {}",
            opts.datadir
        )));
    }

    let ibd_files = find_tablespace_files(datadir, &["ibd", "ibu"])?;

    if ibd_files.is_empty() {
        if opts.json {
            let result = TsidResultJson {
                datadir: opts.datadir.clone(),
                tablespaces: Vec::new(),
            };
            let json = serde_json::to_string_pretty(&result)
                .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
            wprintln!(writer, "{}", json)?;
        } else {
            wprintln!(writer, "No .ibd/.ibu files found in {}", opts.datadir)?;
        }
        return Ok(());
    }

    // Collect tablespace IDs
    let mut results: BTreeMap<String, u32> = BTreeMap::new();

    for ibd_path in &ibd_files {
        let mut ts = match match opts.page_size {
            Some(ps) => Tablespace::open_with_page_size(ibd_path, ps),
            None => Tablespace::open(ibd_path),
        } {
            Ok(t) => t,
            Err(_) => continue,
        };

        let space_id = match ts.fsp_header() {
            Some(fsp) => fsp.space_id,
            None => {
                // Try reading space_id directly from FSP header position
                match ts.read_page(0) {
                    Ok(page0) => {
                        if page0.len() >= FIL_PAGE_DATA + 4 {
                            BigEndian::read_u32(&page0[FIL_PAGE_DATA..])
                        } else {
                            continue;
                        }
                    }
                    Err(_) => continue,
                }
            }
        };

        let display_path = ibd_path
            .strip_prefix(datadir)
            .unwrap_or(ibd_path)
            .to_string_lossy()
            .to_string();

        // Filter by tablespace ID if specified
        if let Some(target_id) = opts.tablespace_id {
            if space_id != target_id {
                continue;
            }
        }

        results.insert(display_path, space_id);
    }

    if opts.json {
        let tablespaces: Vec<TsidEntryJson> = results
            .iter()
            .map(|(path, &space_id)| TsidEntryJson {
                file: path.clone(),
                space_id,
            })
            .collect();

        let result = TsidResultJson {
            datadir: opts.datadir.clone(),
            tablespaces,
        };

        let json = serde_json::to_string_pretty(&result)
            .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
        wprintln!(writer, "{}", json)?;
    } else {
        // Print results
        for (path, space_id) in &results {
            wprintln!(writer, "{} - Space ID: {}", path, space_id)?;
        }

        if results.is_empty() {
            if let Some(target_id) = opts.tablespace_id {
                wprintln!(writer, "Tablespace ID {} not found.", target_id)?;
            }
        }
    }

    Ok(())
}

