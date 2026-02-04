use std::path::Path;

use serde::Serialize;

use crate::innodb::page::FilHeader;
use crate::innodb::tablespace::Tablespace;
use crate::IdbError;

pub struct FindOptions {
    pub datadir: String,
    pub page: u64,
    pub checksum: Option<u32>,
    pub space_id: Option<u32>,
    pub first: bool,
    pub json: bool,
}

#[derive(Serialize)]
struct FindResultJson {
    datadir: String,
    target_page: u64,
    matches: Vec<FindMatchJson>,
    files_searched: usize,
}

#[derive(Serialize)]
struct FindMatchJson {
    file: String,
    page_number: u64,
    checksum: u32,
    space_id: u32,
}

pub fn execute(opts: &FindOptions) -> Result<(), IdbError> {
    let datadir = Path::new(&opts.datadir);
    if !datadir.is_dir() {
        return Err(IdbError::Argument(format!(
            "Data directory does not exist: {}",
            opts.datadir
        )));
    }

    // Find all .ibd files in subdirectories
    let ibd_files = find_ibd_files(datadir)?;

    if ibd_files.is_empty() {
        if opts.json {
            let result = FindResultJson {
                datadir: opts.datadir.clone(),
                target_page: opts.page,
                matches: Vec::new(),
                files_searched: 0,
            };
            let json = serde_json::to_string_pretty(&result)
                .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
            println!("{}", json);
        } else {
            println!("No .ibd files found in {}", opts.datadir);
        }
        return Ok(());
    }

    let mut matches: Vec<FindMatchJson> = Vec::new();
    let mut files_searched = 0;

    for ibd_path in &ibd_files {
        let display_path = ibd_path.strip_prefix(datadir).unwrap_or(ibd_path);
        if !opts.json {
            println!("Checking {}.. ", display_path.display());
        }

        let mut ts = match Tablespace::open(ibd_path) {
            Ok(t) => t,
            Err(_) => continue,
        };

        files_searched += 1;

        for page_num in 0..ts.page_count() {
            let page_data = match ts.read_page(page_num) {
                Ok(d) => d,
                Err(_) => continue,
            };

            let header = match FilHeader::parse(&page_data) {
                Some(h) => h,
                None => continue,
            };

            if header.page_number as u64 == opts.page {
                // If checksum filter specified, must also match
                if let Some(expected_csum) = opts.checksum {
                    if header.checksum != expected_csum {
                        continue;
                    }
                }
                // If space_id filter specified, must also match
                if let Some(expected_sid) = opts.space_id {
                    if header.space_id != expected_sid {
                        continue;
                    }
                }

                if !opts.json {
                    println!(
                        "Found page {} in {} (checksum: {}, space_id: {})",
                        opts.page,
                        display_path.display(),
                        header.checksum,
                        header.space_id
                    );
                }

                matches.push(FindMatchJson {
                    file: display_path.display().to_string(),
                    page_number: header.page_number as u64,
                    checksum: header.checksum,
                    space_id: header.space_id,
                });

                if opts.first {
                    break;
                }
            }
        }

        if opts.first && !matches.is_empty() {
            break;
        }
    }

    if opts.json {
        let result = FindResultJson {
            datadir: opts.datadir.clone(),
            target_page: opts.page,
            matches,
            files_searched,
        };
        let json = serde_json::to_string_pretty(&result)
            .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
        println!("{}", json);
    } else if matches.is_empty() {
        println!("Page {} not found in any .ibd file.", opts.page);
    } else {
        println!();
        println!(
            "Found {} match(es) in {} file(s) searched.",
            matches.len(),
            files_searched
        );
    }

    Ok(())
}

/// Recursively find all .ibd files in subdirectories of the data directory.
fn find_ibd_files(datadir: &Path) -> Result<Vec<std::path::PathBuf>, IdbError> {
    let mut files = Vec::new();

    let entries = std::fs::read_dir(datadir)
        .map_err(|e| IdbError::Io(format!("Cannot read directory {}: {}", datadir.display(), e)))?;

    for entry in entries {
        let entry =
            entry.map_err(|e| IdbError::Io(format!("Cannot read directory entry: {}", e)))?;
        let path = entry.path();

        if path.is_dir() {
            // Look for .ibd files in subdirectories
            let sub_entries = std::fs::read_dir(&path)
                .map_err(|e| IdbError::Io(format!("Cannot read {}: {}", path.display(), e)))?;

            for sub_entry in sub_entries {
                let sub_entry = sub_entry
                    .map_err(|e| IdbError::Io(format!("Cannot read directory entry: {}", e)))?;
                let sub_path = sub_entry.path();
                if sub_path.extension().is_some_and(|ext| ext == "ibd") {
                    files.push(sub_path);
                }
            }
        } else if path.extension().is_some_and(|ext| ext == "ibd") {
            files.push(path);
        }
    }

    files.sort();
    Ok(files)
}
