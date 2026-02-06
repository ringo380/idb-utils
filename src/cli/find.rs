use std::io::Write;
use std::path::Path;

use serde::Serialize;

use crate::cli::{wprintln, create_progress_bar};
use crate::innodb::page::FilHeader;
use crate::innodb::tablespace::Tablespace;
use crate::util::fs::find_tablespace_files;
use crate::IdbError;

pub struct FindOptions {
    pub datadir: String,
    pub page: u64,
    pub checksum: Option<u32>,
    pub space_id: Option<u32>,
    pub first: bool,
    pub json: bool,
    pub page_size: Option<u32>,
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

pub fn execute(opts: &FindOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    let datadir = Path::new(&opts.datadir);
    if !datadir.is_dir() {
        return Err(IdbError::Argument(format!(
            "Data directory does not exist: {}",
            opts.datadir
        )));
    }

    // Find all .ibd files in subdirectories
    let ibd_files = find_tablespace_files(datadir, &["ibd"])?;

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
            wprintln!(writer, "{}", json)?;
        } else {
            wprintln!(writer, "No .ibd files found in {}", opts.datadir)?;
        }
        return Ok(());
    }

    let mut matches: Vec<FindMatchJson> = Vec::new();
    let mut files_searched = 0;

    let pb = if !opts.json {
        Some(create_progress_bar(ibd_files.len() as u64, "files"))
    } else {
        None
    };

    for ibd_path in &ibd_files {
        if let Some(ref pb) = pb {
            pb.inc(1);
        }
        let display_path = ibd_path.strip_prefix(datadir).unwrap_or(ibd_path);
        if !opts.json {
            wprintln!(writer, "Checking {}.. ", display_path.display())?;
        }

        let ts_result = match opts.page_size {
            Some(ps) => Tablespace::open_with_page_size(ibd_path, ps),
            None => Tablespace::open(ibd_path),
        };
        let mut ts = match ts_result {
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
                    wprintln!(
                        writer,
                        "Found page {} in {} (checksum: {}, space_id: {})",
                        opts.page,
                        display_path.display(),
                        header.checksum,
                        header.space_id
                    )?;
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

    if let Some(pb) = pb {
        pb.finish_and_clear();
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
        wprintln!(writer, "{}", json)?;
    } else if matches.is_empty() {
        wprintln!(writer, "Page {} not found in any .ibd file.", opts.page)?;
    } else {
        wprintln!(writer)?;
        wprintln!(
            writer,
            "Found {} match(es) in {} file(s) searched.",
            matches.len(),
            files_searched
        )?;
    }

    Ok(())
}

