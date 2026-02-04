use std::collections::BTreeMap;
use std::path::Path;

use byteorder::{BigEndian, ByteOrder};
use serde::Serialize;

use crate::innodb::constants::FIL_PAGE_DATA;
use crate::innodb::tablespace::Tablespace;
use crate::IdbError;

pub struct TsidOptions {
    pub datadir: String,
    pub list: bool,
    pub tablespace_id: Option<u32>,
    pub json: bool,
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

pub fn execute(opts: &TsidOptions) -> Result<(), IdbError> {
    let datadir = Path::new(&opts.datadir);
    if !datadir.is_dir() {
        return Err(IdbError::Argument(format!(
            "Data directory does not exist: {}",
            opts.datadir
        )));
    }

    let ibd_files = find_ibd_and_ibu_files(datadir)?;

    if ibd_files.is_empty() {
        if opts.json {
            let result = TsidResultJson {
                datadir: opts.datadir.clone(),
                tablespaces: Vec::new(),
            };
            let json = serde_json::to_string_pretty(&result)
                .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
            println!("{}", json);
        } else {
            println!("No .ibd/.ibu files found in {}", opts.datadir);
        }
        return Ok(());
    }

    // Collect tablespace IDs
    let mut results: BTreeMap<String, u32> = BTreeMap::new();

    for ibd_path in &ibd_files {
        let mut ts = match Tablespace::open(ibd_path) {
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
        println!("{}", json);
    } else {
        // Print results
        for (path, space_id) in &results {
            println!("{} - Space ID: {}", path, space_id);
        }

        if results.is_empty() {
            if let Some(target_id) = opts.tablespace_id {
                println!("Tablespace ID {} not found.", target_id);
            }
        }
    }

    Ok(())
}

/// Find all .ibd and .ibu files in the data directory.
fn find_ibd_and_ibu_files(datadir: &Path) -> Result<Vec<std::path::PathBuf>, IdbError> {
    let mut files = Vec::new();

    let entries = std::fs::read_dir(datadir)
        .map_err(|e| IdbError::Io(format!("Cannot read directory {}: {}", datadir.display(), e)))?;

    for entry in entries {
        let entry =
            entry.map_err(|e| IdbError::Io(format!("Cannot read directory entry: {}", e)))?;
        let path = entry.path();

        if path.is_dir() {
            let sub_entries = std::fs::read_dir(&path)
                .map_err(|e| IdbError::Io(format!("Cannot read {}: {}", path.display(), e)))?;

            for sub_entry in sub_entries {
                let sub_entry = sub_entry
                    .map_err(|e| IdbError::Io(format!("Cannot read directory entry: {}", e)))?;
                let sub_path = sub_entry.path();
                if let Some(ext) = sub_path.extension() {
                    if ext == "ibd" || ext == "ibu" {
                        files.push(sub_path);
                    }
                }
            }
        } else if let Some(ext) = path.extension() {
            if ext == "ibd" || ext == "ibu" {
                files.push(path);
            }
        }
    }

    files.sort();
    Ok(files)
}
