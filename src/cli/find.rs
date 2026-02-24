use std::io::Write;
use std::path::Path;

use rayon::prelude::*;
use serde::Serialize;

use crate::cli::{create_progress_bar, wprintln};
use crate::innodb::checksum::validate_checksum;
use crate::innodb::corruption::classify_corruption;
use crate::innodb::page::FilHeader;
use crate::util::fs::find_tablespace_files;
use crate::IdbError;

/// Options for the `inno find` subcommand.
pub struct FindOptions {
    /// MySQL data directory path to search.
    pub datadir: String,
    /// Page number to search for across all tablespace files.
    pub page: Option<u64>,
    /// Optional checksum filter — only match pages with this stored checksum.
    pub checksum: Option<u32>,
    /// Optional space ID filter — only match pages in this tablespace.
    pub space_id: Option<u32>,
    /// Scan for pages with checksum mismatches.
    pub corrupt: bool,
    /// Stop searching after the first match.
    pub first: bool,
    /// Emit output as JSON.
    pub json: bool,
    /// Override the auto-detected page size.
    pub page_size: Option<u32>,
    /// Number of threads for parallel processing (0 = auto-detect).
    pub threads: usize,
    /// Use memory-mapped I/O for file access.
    pub mmap: bool,
}

// -----------------------------------------------------------------------
// Page-number search structs
// -----------------------------------------------------------------------

#[derive(Serialize)]
struct FindResultJson {
    datadir: String,
    target_page: u64,
    matches: Vec<FindMatchJson>,
    files_searched: usize,
}

#[derive(Serialize, Clone)]
struct FindMatchJson {
    file: String,
    page_number: u64,
    checksum: u32,
    space_id: u32,
}

// -----------------------------------------------------------------------
// Corrupt search structs
// -----------------------------------------------------------------------

#[derive(Serialize)]
struct FindCorruptResultJson {
    datadir: String,
    corrupt_pages: Vec<FindCorruptMatchJson>,
    files_searched: usize,
    total_corrupt: usize,
}

#[derive(Serialize, Clone)]
struct FindCorruptMatchJson {
    file: String,
    page_number: u64,
    stored_checksum: u32,
    calculated_checksum: u32,
    algorithm: String,
    corruption_pattern: String,
}

// -----------------------------------------------------------------------
// Page-number search implementation
// -----------------------------------------------------------------------

/// Search a single tablespace file for pages matching the target page number.
#[allow(clippy::too_many_arguments)]
fn search_file(
    ibd_path: &Path,
    datadir: &Path,
    target_page: u64,
    checksum_filter: Option<u32>,
    space_id_filter: Option<u32>,
    page_size_override: Option<u32>,
    first: bool,
    use_mmap: bool,
) -> (Vec<FindMatchJson>, bool) {
    let display_path = ibd_path.strip_prefix(datadir).unwrap_or(ibd_path);

    let path_str = ibd_path.to_string_lossy();
    let ts_result = crate::cli::open_tablespace(&path_str, page_size_override, use_mmap);
    let mut ts = match ts_result {
        Ok(t) => t,
        Err(_) => return (Vec::new(), false),
    };

    let all_data = match ts.read_all_pages() {
        Ok(d) => d,
        Err(_) => return (Vec::new(), true),
    };

    let page_size = ts.page_size() as usize;
    let page_count = ts.page_count();

    let file_matches: Vec<FindMatchJson> = (0..page_count)
        .into_par_iter()
        .filter_map(|page_num| {
            let offset = page_num as usize * page_size;
            if offset + page_size > all_data.len() {
                return None;
            }
            let page_data = &all_data[offset..offset + page_size];

            let header = FilHeader::parse(page_data)?;

            if header.page_number as u64 != target_page {
                return None;
            }

            if let Some(expected_csum) = checksum_filter {
                if header.checksum != expected_csum {
                    return None;
                }
            }

            if let Some(expected_sid) = space_id_filter {
                if header.space_id != expected_sid {
                    return None;
                }
            }

            Some(FindMatchJson {
                file: display_path.display().to_string(),
                page_number: header.page_number as u64,
                checksum: header.checksum,
                space_id: header.space_id,
            })
        })
        .collect();

    let matches = if first {
        file_matches.into_iter().take(1).collect()
    } else {
        file_matches
    };
    (matches, true)
}

fn execute_find_page(opts: &FindOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    let target_page = opts.page.unwrap();
    let datadir = Path::new(&opts.datadir);

    let ibd_files = find_tablespace_files(datadir, &["ibd"])?;

    if ibd_files.is_empty() {
        if opts.json {
            let result = FindResultJson {
                datadir: opts.datadir.clone(),
                target_page,
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

    let pb = if !opts.json {
        Some(create_progress_bar(ibd_files.len() as u64, "files"))
    } else {
        None
    };

    let checksum_filter = opts.checksum;
    let space_id_filter = opts.space_id;
    let page_size_override = opts.page_size;
    let first = opts.first;
    let use_mmap = opts.mmap;

    let all_results: Vec<(Vec<FindMatchJson>, bool)> = ibd_files
        .par_iter()
        .map(|ibd_path| {
            let result = search_file(
                ibd_path,
                datadir,
                target_page,
                checksum_filter,
                space_id_filter,
                page_size_override,
                first,
                use_mmap,
            );
            if let Some(ref pb) = pb {
                pb.inc(1);
            }
            result
        })
        .collect();

    if let Some(ref pb) = pb {
        pb.finish_and_clear();
    }

    let mut matches: Vec<FindMatchJson> = Vec::new();
    let files_searched: usize = all_results.iter().filter(|(_, opened)| *opened).count();

    for (file_matches, _opened) in &all_results {
        for m in file_matches {
            if !opts.json {
                wprintln!(
                    writer,
                    "Found page {} in {} (checksum: {}, space_id: {})",
                    target_page,
                    m.file,
                    m.checksum,
                    m.space_id
                )?;
            }
            matches.push(m.clone());
            if opts.first {
                break;
            }
        }
        if opts.first && !matches.is_empty() {
            break;
        }
    }

    if opts.json {
        let result = FindResultJson {
            datadir: opts.datadir.clone(),
            target_page,
            matches,
            files_searched,
        };
        let json = serde_json::to_string_pretty(&result)
            .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
        wprintln!(writer, "{}", json)?;
    } else if matches.is_empty() {
        wprintln!(writer, "Page {} not found in any .ibd file.", target_page)?;
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

// -----------------------------------------------------------------------
// Corrupt search implementation
// -----------------------------------------------------------------------

/// Search a single tablespace file for pages with invalid checksums.
fn search_file_corrupt(
    ibd_path: &Path,
    datadir: &Path,
    space_id_filter: Option<u32>,
    page_size_override: Option<u32>,
    first: bool,
    use_mmap: bool,
) -> (Vec<FindCorruptMatchJson>, bool) {
    let display_path = ibd_path.strip_prefix(datadir).unwrap_or(ibd_path);

    let path_str = ibd_path.to_string_lossy();
    let ts_result = crate::cli::open_tablespace(&path_str, page_size_override, use_mmap);
    let mut ts = match ts_result {
        Ok(t) => t,
        Err(_) => return (Vec::new(), false),
    };

    let page_size = ts.page_size();
    let page_count = ts.page_count();
    let vendor_info = ts.vendor_info().clone();

    let all_data = match ts.read_all_pages() {
        Ok(d) => d,
        Err(_) => return (Vec::new(), true),
    };

    let ps = page_size as usize;
    let file_str = display_path.display().to_string();

    let file_matches: Vec<FindCorruptMatchJson> = (0..page_count)
        .into_par_iter()
        .filter_map(|page_num| {
            let offset = page_num as usize * ps;
            if offset + ps > all_data.len() {
                return None;
            }
            let page_data = &all_data[offset..offset + ps];

            // Skip empty pages
            if page_data.iter().all(|&b| b == 0) {
                return None;
            }

            // Apply space_id filter if specified
            if let Some(expected_sid) = space_id_filter {
                if let Some(header) = FilHeader::parse(page_data) {
                    if header.space_id != expected_sid {
                        return None;
                    }
                }
            }

            let csum = validate_checksum(page_data, page_size, Some(&vendor_info));
            if csum.valid {
                return None;
            }

            let pattern = classify_corruption(page_data, page_size);

            Some(FindCorruptMatchJson {
                file: file_str.clone(),
                page_number: page_num,
                stored_checksum: csum.stored_checksum,
                calculated_checksum: csum.calculated_checksum,
                algorithm: format!("{:?}", csum.algorithm),
                corruption_pattern: pattern.name().to_string(),
            })
        })
        .collect();

    let matches = if first {
        file_matches.into_iter().take(1).collect()
    } else {
        file_matches
    };
    (matches, true)
}

fn execute_find_corrupt(opts: &FindOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    let datadir = Path::new(&opts.datadir);

    let ibd_files = find_tablespace_files(datadir, &["ibd"])?;

    if ibd_files.is_empty() {
        if opts.json {
            let result = FindCorruptResultJson {
                datadir: opts.datadir.clone(),
                corrupt_pages: Vec::new(),
                files_searched: 0,
                total_corrupt: 0,
            };
            let json = serde_json::to_string_pretty(&result)
                .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
            wprintln!(writer, "{}", json)?;
        } else {
            wprintln!(writer, "No .ibd files found in {}", opts.datadir)?;
        }
        return Ok(());
    }

    let pb = if !opts.json {
        Some(create_progress_bar(ibd_files.len() as u64, "files"))
    } else {
        None
    };

    let space_id_filter = opts.space_id;
    let page_size_override = opts.page_size;
    let first = opts.first;
    let use_mmap = opts.mmap;

    let all_results: Vec<(Vec<FindCorruptMatchJson>, bool)> = ibd_files
        .par_iter()
        .map(|ibd_path| {
            let result = search_file_corrupt(
                ibd_path,
                datadir,
                space_id_filter,
                page_size_override,
                first,
                use_mmap,
            );
            if let Some(ref pb) = pb {
                pb.inc(1);
            }
            result
        })
        .collect();

    if let Some(ref pb) = pb {
        pb.finish_and_clear();
    }

    let mut corrupt_pages: Vec<FindCorruptMatchJson> = Vec::new();
    let files_searched: usize = all_results.iter().filter(|(_, opened)| *opened).count();

    for (file_matches, _opened) in &all_results {
        for m in file_matches {
            if !opts.json {
                wprintln!(
                    writer,
                    "Corrupt page {} in {} (stored: 0x{:08x}, calculated: 0x{:08x}, algo: {}, pattern: {})",
                    m.page_number,
                    m.file,
                    m.stored_checksum,
                    m.calculated_checksum,
                    m.algorithm,
                    m.corruption_pattern
                )?;
            }
            corrupt_pages.push(m.clone());
            if opts.first {
                break;
            }
        }
        if opts.first && !corrupt_pages.is_empty() {
            break;
        }
    }

    if opts.json {
        let total_corrupt = corrupt_pages.len();
        let result = FindCorruptResultJson {
            datadir: opts.datadir.clone(),
            corrupt_pages,
            files_searched,
            total_corrupt,
        };
        let json = serde_json::to_string_pretty(&result)
            .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
        wprintln!(writer, "{}", json)?;
    } else if corrupt_pages.is_empty() {
        wprintln!(
            writer,
            "No corrupt pages found in {} file(s) searched.",
            files_searched
        )?;
    } else {
        wprintln!(writer)?;
        wprintln!(
            writer,
            "Found {} corrupt page(s) in {} file(s) searched.",
            corrupt_pages.len(),
            files_searched
        )?;
    }

    Ok(())
}

// -----------------------------------------------------------------------
// Entry point
// -----------------------------------------------------------------------

/// Search a MySQL data directory for pages matching a given page number,
/// or scan for pages with checksum mismatches (`--corrupt`).
pub fn execute(opts: &FindOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    // Validate mutually exclusive modes
    if opts.corrupt && opts.page.is_some() {
        return Err(IdbError::Argument(
            "--corrupt and --page are mutually exclusive".to_string(),
        ));
    }
    if !opts.corrupt && opts.page.is_none() {
        return Err(IdbError::Argument(
            "Either --page or --corrupt must be specified".to_string(),
        ));
    }
    if opts.corrupt && opts.checksum.is_some() {
        return Err(IdbError::Argument(
            "--checksum is not compatible with --corrupt".to_string(),
        ));
    }

    let datadir = Path::new(&opts.datadir);
    if !datadir.is_dir() {
        return Err(IdbError::Argument(format!(
            "Data directory does not exist: {}",
            opts.datadir
        )));
    }

    if opts.corrupt {
        execute_find_corrupt(opts, writer)
    } else {
        execute_find_page(opts, writer)
    }
}
