use std::io::Write;
use std::path::Path;

use rayon::prelude::*;
use serde::Serialize;

use crate::cli::{create_progress_bar, wprintln};
use crate::innodb::page::FilHeader;
use crate::util::fs::find_tablespace_files;
use crate::IdbError;

/// Options for the `inno find` subcommand.
pub struct FindOptions {
    /// MySQL data directory path to search.
    pub datadir: String,
    /// Page number to search for across all tablespace files.
    pub page: u64,
    /// Optional checksum filter — only match pages with this stored checksum.
    pub checksum: Option<u32>,
    /// Optional space ID filter — only match pages in this tablespace.
    pub space_id: Option<u32>,
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

/// Search a single tablespace file for pages matching the target page number.
/// Returns `(matches, opened)` where `opened` indicates whether the file was
/// successfully opened. Pure function safe for parallel execution.
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

    // Read all pages into memory for parallel page scanning within this file
    let all_data = match ts.read_all_pages() {
        Ok(d) => d,
        Err(_) => return (Vec::new(), true),
    };

    let page_size = ts.page_size() as usize;
    let page_count = ts.page_count();

    // Scan pages in parallel within this file
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

/// Search a MySQL data directory for pages matching a given page number.
///
/// Recursively discovers all `.ibd` files under the specified data directory
/// using [`find_tablespace_files`],
/// opens each as a [`Tablespace`], and
/// iterates over every page reading the FIL header. A page is considered a
/// match when its stored `page_number` field (bytes 4-7 of the FIL header)
/// equals the target value.
///
/// Files are searched in parallel using rayon, and within each file pages are
/// also scanned in parallel. Results are collected in deterministic order.
///
/// Optional filters narrow the results:
/// - `--checksum`: only match pages whose stored checksum (bytes 0-3) equals
///   the given value.
/// - `--space-id`: only match pages whose space ID (bytes 34-37) equals the
///   given value, useful when the same page number exists in multiple
///   tablespaces.
///
/// With `--first`, searching stops after the first match across all files,
/// providing a fast lookup when only one hit is expected. A progress bar is
/// displayed for the file-level scan (suppressed in `--json` mode).
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

    let pb = if !opts.json {
        Some(create_progress_bar(ibd_files.len() as u64, "files"))
    } else {
        None
    };

    // Search files in parallel
    let target_page = opts.page;
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

    // Collect results — count all opened files (all were searched in parallel)
    let mut matches: Vec<FindMatchJson> = Vec::new();
    let files_searched: usize = all_results.iter().filter(|(_, opened)| *opened).count();

    for (file_matches, _opened) in &all_results {
        for m in file_matches {
            if !opts.json {
                wprintln!(
                    writer,
                    "Found page {} in {} (checksum: {}, space_id: {})",
                    opts.page,
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
