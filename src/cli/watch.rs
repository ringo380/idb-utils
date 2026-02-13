use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use chrono::Local;
use colored::Colorize;
use serde::Serialize;

use crate::cli::{wprintln, setup_decryption};
use crate::innodb::checksum::validate_checksum;
use crate::innodb::page::FilHeader;
use crate::innodb::tablespace::Tablespace;
use crate::IdbError;

/// Options for the `inno watch` subcommand.
pub struct WatchOptions {
    /// Path to the InnoDB tablespace file.
    pub file: String,
    /// Polling interval in milliseconds.
    pub interval: u64,
    /// Show per-field diffs for changed pages.
    pub verbose: bool,
    /// Output in NDJSON streaming format.
    pub json: bool,
    /// Override the auto-detected page size.
    pub page_size: Option<u32>,
    /// Path to MySQL keyring file for decrypting encrypted tablespaces.
    pub keyring: Option<String>,
}

// ── Internal types ──────────────────────────────────────────────────

#[derive(Clone)]
struct PageSnapshot {
    lsn: u64,
    page_type: String,
}

struct WatchState {
    snapshots: HashMap<u64, PageSnapshot>,
    page_count: u64,
    vendor_name: String,
}

// ── JSON output structs ─────────────────────────────────────────────

#[derive(Serialize)]
struct WatchEvent {
    timestamp: String,
    event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pages: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_size: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    vendor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    modified: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    added: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    removed: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    changes: Option<Vec<PageChange>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_changes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_polls: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct PageChange {
    page: u64,
    kind: String,
    page_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    old_lsn: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    new_lsn: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lsn_delta: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checksum_valid: Option<bool>,
}

// ── Helpers ─────────────────────────────────────────────────────────

fn now_timestamp() -> String {
    Local::now().format("%Y-%m-%dT%H:%M:%S%.3f%:z").to_string()
}

fn now_time_short() -> String {
    Local::now().format("%H:%M:%S").to_string()
}

fn open_tablespace(opts: &WatchOptions) -> Result<Tablespace, IdbError> {
    let mut ts = match opts.page_size {
        Some(ps) => Tablespace::open_with_page_size(&opts.file, ps)?,
        None => Tablespace::open(&opts.file)?,
    };
    if let Some(ref keyring_path) = opts.keyring {
        setup_decryption(&mut ts, keyring_path)?;
    }
    Ok(ts)
}

fn take_snapshot(ts: &mut Tablespace) -> Result<HashMap<u64, PageSnapshot>, IdbError> {
    let mut snapshots = HashMap::new();
    ts.for_each_page(|page_num, data| {
        if let Some(hdr) = FilHeader::parse(data) {
            snapshots.insert(
                page_num,
                PageSnapshot {
                    lsn: hdr.lsn,
                    page_type: hdr.page_type.name().to_string(),
                },
            );
        }
        Ok(())
    })?;
    Ok(snapshots)
}

fn emit_json_line(writer: &mut dyn Write, event: &WatchEvent) -> Result<(), IdbError> {
    let json = serde_json::to_string(event)
        .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
    wprintln!(writer, "{}", json)?;
    Ok(())
}

// ── Main entry point ────────────────────────────────────────────────

/// Monitor a tablespace for page-level changes.
pub fn execute(opts: &WatchOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    // Set up Ctrl+C handler
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    })
    .map_err(|e| IdbError::Io(format!("Cannot set Ctrl+C handler: {}", e)))?;

    // Take initial snapshot
    let mut ts = open_tablespace(opts)?;
    let page_size = ts.page_size();
    let page_count = ts.page_count();
    let vendor_name = ts.vendor_info().vendor.to_string();
    let initial_snapshots = take_snapshot(&mut ts)?;

    let mut state = WatchState {
        snapshots: initial_snapshots,
        page_count,
        vendor_name: vendor_name.clone(),
    };

    // Emit start message
    if opts.json {
        emit_json_line(
            writer,
            &WatchEvent {
                timestamp: now_timestamp(),
                event: "started".to_string(),
                pages: Some(page_count),
                page_size: Some(page_size),
                vendor: Some(vendor_name),
                modified: None,
                added: None,
                removed: None,
                changes: None,
                total_changes: None,
                total_polls: None,
                error: None,
            },
        )?;
    } else {
        wprintln!(
            writer,
            "Watching {} ({} pages, {} bytes/page, {})",
            opts.file,
            page_count,
            page_size,
            state.vendor_name,
        )?;
        wprintln!(
            writer,
            "Polling every {}ms. Press Ctrl+C to stop.",
            opts.interval
        )?;
        wprintln!(writer)?;
    }

    let interval = Duration::from_millis(opts.interval);
    let mut total_changes: u64 = 0;
    let mut total_polls: u64 = 0;

    // Poll loop
    while running.load(Ordering::SeqCst) {
        thread::sleep(interval);

        if !running.load(Ordering::SeqCst) {
            break;
        }

        // Check if file still exists
        if !Path::new(&opts.file).exists() {
            if opts.json {
                emit_json_line(
                    writer,
                    &WatchEvent {
                        timestamp: now_timestamp(),
                        event: "error".to_string(),
                        pages: None,
                        page_size: None,
                        vendor: None,
                        modified: None,
                        added: None,
                        removed: None,
                        changes: None,
                        total_changes: None,
                        total_polls: None,
                        error: Some("File no longer exists".to_string()),
                    },
                )?;
            } else {
                wprintln!(
                    writer,
                    "{}  {}",
                    now_time_short(),
                    "File no longer exists — stopping.".red()
                )?;
            }
            break;
        }

        // Re-open tablespace and take new snapshot
        let poll_result = open_tablespace(opts).and_then(|mut new_ts| {
            let new_page_count = new_ts.page_count();
            let new_snapshots = take_snapshot(&mut new_ts)?;
            Ok((new_page_count, new_snapshots))
        });

        let (new_page_count, new_snapshots) = match poll_result {
            Ok(r) => r,
            Err(e) => {
                if opts.json {
                    emit_json_line(
                        writer,
                        &WatchEvent {
                            timestamp: now_timestamp(),
                            event: "error".to_string(),
                            pages: None,
                            page_size: None,
                            vendor: None,
                            modified: None,
                            added: None,
                            removed: None,
                            changes: None,
                            total_changes: None,
                            total_polls: None,
                            error: Some(e.to_string()),
                        },
                    )?;
                } else {
                    wprintln!(
                        writer,
                        "{}  {} {}",
                        now_time_short(),
                        "Error:".red(),
                        e
                    )?;
                }
                continue;
            }
        };

        total_polls += 1;

        // Compare snapshots
        let mut changes: Vec<PageChange> = Vec::new();
        let mut modified_count: u64 = 0;
        let mut added_count: u64 = 0;
        let mut removed_count: u64 = 0;

        // Detect modified and added pages
        for (&page_num, new_snap) in &new_snapshots {
            match state.snapshots.get(&page_num) {
                Some(old_snap) => {
                    if new_snap.lsn != old_snap.lsn {
                        modified_count += 1;

                        // Validate checksum for changed pages
                        let checksum_valid = open_tablespace(opts)
                            .and_then(|mut ts2| ts2.read_page(page_num))
                            .map(|data| {
                                validate_checksum(&data, page_size, None).valid
                            })
                            .unwrap_or(false);

                        let lsn_delta = new_snap.lsn as i64 - old_snap.lsn as i64;

                        changes.push(PageChange {
                            page: page_num,
                            kind: "modified".to_string(),
                            page_type: new_snap.page_type.clone(),
                            old_lsn: Some(old_snap.lsn),
                            new_lsn: Some(new_snap.lsn),
                            lsn_delta: Some(lsn_delta),
                            checksum_valid: Some(checksum_valid),
                        });
                    }
                }
                None => {
                    added_count += 1;
                    changes.push(PageChange {
                        page: page_num,
                        kind: "added".to_string(),
                        page_type: new_snap.page_type.clone(),
                        old_lsn: None,
                        new_lsn: Some(new_snap.lsn),
                        lsn_delta: None,
                        checksum_valid: None,
                    });
                }
            }
        }

        // Detect removed pages
        for &page_num in state.snapshots.keys() {
            if !new_snapshots.contains_key(&page_num) {
                removed_count += 1;
                let old_snap = &state.snapshots[&page_num];
                changes.push(PageChange {
                    page: page_num,
                    kind: "removed".to_string(),
                    page_type: old_snap.page_type.clone(),
                    old_lsn: Some(old_snap.lsn),
                    new_lsn: None,
                    lsn_delta: None,
                    checksum_valid: None,
                });
            }
        }

        // Sort changes by page number for stable output
        changes.sort_by_key(|c| c.page);

        let cycle_changes = modified_count + added_count + removed_count;
        total_changes += cycle_changes;

        // Only emit output when something changed
        if cycle_changes > 0 {
            if opts.json {
                emit_json_line(
                    writer,
                    &WatchEvent {
                        timestamp: now_timestamp(),
                        event: "poll".to_string(),
                        pages: Some(new_page_count),
                        page_size: None,
                        vendor: None,
                        modified: Some(modified_count),
                        added: Some(added_count),
                        removed: Some(removed_count),
                        changes: Some(changes),
                        total_changes: None,
                        total_polls: None,
                        error: None,
                    },
                )?;
            } else {
                // Build summary line
                let mut parts = Vec::new();
                if modified_count > 0 {
                    let word = if modified_count == 1 { "page" } else { "pages" };
                    parts.push(format!("{} {} modified", modified_count, word));
                }
                if added_count > 0 {
                    let word = if added_count == 1 { "page" } else { "pages" };
                    parts.push(format!("{} {} added", added_count, word));
                }
                if removed_count > 0 {
                    let word = if removed_count == 1 { "page" } else { "pages" };
                    parts.push(format!("{} {} removed", removed_count, word));
                }
                wprintln!(writer, "{}  {}", now_time_short(), parts.join(", "))?;

                // Print per-page details
                for change in &changes {
                    match change.kind.as_str() {
                        "modified" => {
                            let old_lsn = change.old_lsn.unwrap_or(0);
                            let new_lsn = change.new_lsn.unwrap_or(0);
                            let delta = change.lsn_delta.unwrap_or(0);
                            let cksum_str = if change.checksum_valid.unwrap_or(false) {
                                "checksum valid".green().to_string()
                            } else {
                                "CHECKSUM INVALID".red().to_string()
                            };

                            if opts.verbose {
                                wprintln!(
                                    writer,
                                    "  Page {:<5} {:<12} LSN {} -> {} ({:+})  {}",
                                    change.page,
                                    change.page_type,
                                    old_lsn,
                                    new_lsn,
                                    delta,
                                    cksum_str,
                                )?;
                            } else {
                                wprintln!(
                                    writer,
                                    "  Page {:<5} {:<12} LSN {:+}  {}",
                                    change.page,
                                    change.page_type,
                                    delta,
                                    cksum_str,
                                )?;
                            }
                        }
                        "added" => {
                            wprintln!(
                                writer,
                                "  Page {:<5} {:<12} {}",
                                change.page,
                                change.page_type,
                                "(new page)".cyan(),
                            )?;
                        }
                        "removed" => {
                            wprintln!(
                                writer,
                                "  Page {:<5} {:<12} {}",
                                change.page,
                                change.page_type,
                                "(removed)".yellow(),
                            )?;
                        }
                        _ => {}
                    }
                }

                wprintln!(writer)?;
            }
        }

        // Update state
        state.snapshots = new_snapshots;
        state.page_count = new_page_count;
    }

    // Emit stop summary
    if opts.json {
        emit_json_line(
            writer,
            &WatchEvent {
                timestamp: now_timestamp(),
                event: "stopped".to_string(),
                pages: None,
                page_size: None,
                vendor: None,
                modified: None,
                added: None,
                removed: None,
                changes: None,
                total_changes: Some(total_changes),
                total_polls: Some(total_polls),
                error: None,
            },
        )?;
    } else {
        wprintln!(
            writer,
            "Stopped after {} polls. Total page changes: {}",
            total_polls,
            total_changes,
        )?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::{BigEndian, ByteOrder};
    use std::io::Write as IoWrite;
    use tempfile::NamedTempFile;

    use crate::innodb::constants::*;

    const PS: usize = SIZE_PAGE_DEFAULT as usize;

    fn build_fsp_page(space_id: u32, total_pages: u32) -> Vec<u8> {
        let mut page = vec![0u8; PS];
        BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], 0);
        BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
        BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
        BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], 1000);
        BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 8); // FSP_HDR
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);
        let fsp = FIL_PAGE_DATA;
        BigEndian::write_u32(&mut page[fsp + FSP_SPACE_ID..], space_id);
        BigEndian::write_u32(&mut page[fsp + FSP_SIZE..], total_pages);
        BigEndian::write_u32(&mut page[fsp + FSP_FREE_LIMIT..], total_pages);
        BigEndian::write_u32(&mut page[fsp + FSP_SPACE_FLAGS..], 0);
        let trailer = PS - SIZE_FIL_TRAILER;
        BigEndian::write_u32(&mut page[trailer + 4..], 1000 & 0xFFFFFFFF);
        let end = PS - SIZE_FIL_TRAILER;
        let crc1 = crc32c::crc32c(&page[FIL_PAGE_OFFSET..FIL_PAGE_FILE_FLUSH_LSN]);
        let crc2 = crc32c::crc32c(&page[FIL_PAGE_DATA..end]);
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_OR_CHKSUM..], crc1 ^ crc2);
        page
    }

    fn build_index_page(page_num: u32, space_id: u32, lsn: u64) -> Vec<u8> {
        let mut page = vec![0u8; PS];
        BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], page_num);
        BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
        BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
        BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], lsn);
        BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855); // INDEX
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);
        let trailer = PS - SIZE_FIL_TRAILER;
        BigEndian::write_u32(&mut page[trailer + 4..], (lsn & 0xFFFFFFFF) as u32);
        let end = PS - SIZE_FIL_TRAILER;
        let crc1 = crc32c::crc32c(&page[FIL_PAGE_OFFSET..FIL_PAGE_FILE_FLUSH_LSN]);
        let crc2 = crc32c::crc32c(&page[FIL_PAGE_DATA..end]);
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_OR_CHKSUM..], crc1 ^ crc2);
        page
    }

    fn write_tablespace(pages: &[Vec<u8>]) -> NamedTempFile {
        let mut tmp = NamedTempFile::new().expect("create temp file");
        for page in pages {
            tmp.write_all(page).expect("write page");
        }
        tmp.flush().expect("flush");
        tmp
    }

    #[test]
    fn test_take_snapshot() {
        let tmp = write_tablespace(&[
            build_fsp_page(1, 3),
            build_index_page(1, 1, 2000),
            build_index_page(2, 1, 3000),
        ]);
        let mut ts = Tablespace::open(tmp.path()).unwrap();
        let snaps = take_snapshot(&mut ts).unwrap();
        assert_eq!(snaps.len(), 3);
        assert_eq!(snaps[&0].lsn, 1000);
        assert_eq!(snaps[&1].lsn, 2000);
        assert_eq!(snaps[&2].lsn, 3000);
        assert_eq!(snaps[&1].page_type, "INDEX");
    }

    #[test]
    fn test_snapshot_detects_page_type() {
        let tmp = write_tablespace(&[build_fsp_page(1, 1)]);
        let mut ts = Tablespace::open(tmp.path()).unwrap();
        let snaps = take_snapshot(&mut ts).unwrap();
        assert_eq!(snaps[&0].page_type, "FSP_HDR");
    }

    #[test]
    fn test_open_tablespace_helper() {
        let tmp = write_tablespace(&[build_fsp_page(1, 2), build_index_page(1, 1, 2000)]);
        let opts = WatchOptions {
            file: tmp.path().to_str().unwrap().to_string(),
            interval: 100,
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
        };
        let ts = open_tablespace(&opts).unwrap();
        assert_eq!(ts.page_count(), 2);
        assert_eq!(ts.page_size(), SIZE_PAGE_DEFAULT);
    }

    #[test]
    fn test_open_tablespace_with_page_size_override() {
        let tmp = write_tablespace(&[build_fsp_page(1, 2), build_index_page(1, 1, 2000)]);
        let opts = WatchOptions {
            file: tmp.path().to_str().unwrap().to_string(),
            interval: 100,
            verbose: false,
            json: false,
            page_size: Some(16384),
            keyring: None,
        };
        let ts = open_tablespace(&opts).unwrap();
        assert_eq!(ts.page_count(), 2);
    }

    #[test]
    fn test_open_tablespace_missing_file() {
        let opts = WatchOptions {
            file: "/nonexistent/path.ibd".to_string(),
            interval: 100,
            verbose: false,
            json: false,
            page_size: None,
            keyring: None,
        };
        assert!(open_tablespace(&opts).is_err());
    }
}
