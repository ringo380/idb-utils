//! Transaction timeline — correlate redo log, undo log, and binary log data
//! into a unified chronological view of page and table modifications.
//!
//! This module builds [`TimelineReport`]s by merging entries from three
//! independent sources:
//!
//! * **Redo log** — MLOG records with `(space_id, page_no)` and LSN ordering
//! * **Undo log** — transaction history with `trx_id`, `table_id`, and operation type
//! * **Binary log** — row events with `(database, table)` and Unix timestamps
//!
//! Use [`extract_redo_timeline`] / [`extract_undo_timeline`] /
//! [`extract_binlog_timeline`] to produce entries from each source, then
//! [`merge_timeline`] to combine, sort, and summarize them.

use serde::Serialize;
use std::collections::HashMap;
use std::io::{Read, Seek};

use crate::innodb::log::{
    compute_record_lsn, parse_mlog_records, LogBlockHeader, LogFile, LOG_FILE_HDR_BLOCKS,
};
use crate::innodb::page::FilHeader;
use crate::innodb::page_types::PageType;
use crate::innodb::undo::{parse_undo_records, UndoRecordType};
use crate::IdbError;

// ── Core types ──────────────────────────────────────────────────────────

/// Source of a timeline entry.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub enum TimelineSource {
    RedoLog,
    UndoLog,
    Binlog,
}

impl std::fmt::Display for TimelineSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimelineSource::RedoLog => write!(f, "REDO"),
            TimelineSource::UndoLog => write!(f, "UNDO"),
            TimelineSource::Binlog => write!(f, "BINLOG"),
        }
    }
}

/// What kind of modification this timeline entry represents.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum TimelineAction {
    /// MLOG record from the redo log.
    Redo { mlog_type: String, single_rec: bool },
    /// Undo record from the undo log.
    Undo {
        record_type: String,
        trx_id: u64,
        undo_no: u64,
        table_id: u64,
    },
    /// Row event from the binary log.
    Binlog {
        event_type: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        database: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        table: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        xid: Option<u64>,
        /// Primary key values decoded from the row image (if correlation succeeded).
        #[serde(skip_serializing_if = "Option::is_none")]
        pk_values: Option<Vec<String>>,
    },
}

/// A single entry in the unified timeline.
#[derive(Debug, Clone, Serialize)]
pub struct TimelineEntry {
    /// Sequence number assigned after sorting (1-based).
    pub seq: u64,
    /// Which log source produced this entry.
    pub source: TimelineSource,
    /// LSN (available for redo and undo entries).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lsn: Option<u64>,
    /// Unix timestamp (available for binlog entries).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u32>,
    /// Tablespace space ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub space_id: Option<u32>,
    /// Page number within the tablespace.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_no: Option<u32>,
    /// The action/modification details.
    pub action: TimelineAction,
}

/// Per-page summary within the timeline.
#[derive(Debug, Clone, Serialize)]
pub struct PageTimelineSummary {
    pub space_id: u32,
    pub page_no: u32,
    pub redo_entries: usize,
    pub undo_entries: usize,
    pub binlog_entries: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_lsn: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_lsn: Option<u64>,
}

/// Result of timeline correlation.
#[derive(Debug, Clone, Serialize)]
pub struct TimelineReport {
    /// Number of entries from each source.
    pub redo_count: usize,
    pub undo_count: usize,
    pub binlog_count: usize,
    /// Number of `(space_id, page_no)` pairs that appear in more than one source.
    pub correlated_count: usize,
    /// All entries, sorted by LSN (primary) then timestamp (secondary).
    pub entries: Vec<TimelineEntry>,
    /// Per-page aggregation.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub page_summaries: Vec<PageTimelineSummary>,
}

// ── Redo log extraction ─────────────────────────────────────────────────

/// Extract timeline entries from a redo log file.
///
/// Iterates all data blocks, parses MLOG records with
/// [`parse_mlog_records`], and computes an approximate LSN for each record.
pub fn extract_redo_timeline(log: &mut LogFile) -> Result<Vec<TimelineEntry>, IdbError> {
    let header = log.read_header()?;
    let data_blocks = log.data_block_count();
    let mut entries = Vec::new();

    for i in 0..data_blocks {
        let block_idx = LOG_FILE_HDR_BLOCKS + i;
        let block_data = log.read_block(block_idx)?;
        let hdr = match LogBlockHeader::parse(&block_data) {
            Some(h) if h.has_data() => h,
            _ => continue,
        };

        let records = parse_mlog_records(&block_data, &hdr);
        for rec in records {
            let lsn = compute_record_lsn(header.start_lsn, i, rec.block_offset);
            entries.push(TimelineEntry {
                seq: 0, // assigned later by merge_timeline
                source: TimelineSource::RedoLog,
                lsn: Some(lsn),
                timestamp: None,
                space_id: rec.space_id,
                page_no: rec.page_no,
                action: TimelineAction::Redo {
                    mlog_type: rec.record_type.to_string(),
                    single_rec: rec.single_rec,
                },
            });
        }
    }

    Ok(entries)
}

// ── Undo log extraction ─────────────────────────────────────────────────

/// Extract timeline entries from an undo tablespace.
///
/// Scans all `FIL_PAGE_UNDO_LOG` pages, parses detailed undo records, and
/// uses each page's FIL header LSN as the timeline ordering key.
pub fn extract_undo_timeline(
    ts: &mut crate::innodb::tablespace::Tablespace,
) -> Result<Vec<TimelineEntry>, IdbError> {
    let page_count = ts.page_count();
    let mut entries = Vec::new();

    for pn in 0..page_count {
        let page_data = ts.read_page(pn)?;
        let fil = match FilHeader::parse(&page_data) {
            Some(h) => h,
            None => continue,
        };

        // Only process undo log pages
        if fil.page_type != PageType::UndoLog {
            continue;
        }

        let records = parse_undo_records(&page_data);
        for rec in records {
            let record_type_str = match rec.record_type {
                UndoRecordType::InsertRec => "INSERT",
                UndoRecordType::UpdExistRec => "UPDATE",
                UndoRecordType::UpdDelRec => "UPDATE_DELETE",
                UndoRecordType::DelMarkRec => "DELETE_MARK",
                UndoRecordType::Unknown(_) => "UNKNOWN",
            };

            entries.push(TimelineEntry {
                seq: 0,
                source: TimelineSource::UndoLog,
                lsn: Some(fil.lsn),
                timestamp: None,
                space_id: None,
                page_no: Some(fil.page_number),
                action: TimelineAction::Undo {
                    record_type: record_type_str.to_string(),
                    trx_id: rec.trx_id.unwrap_or(0),
                    undo_no: rec.undo_no,
                    table_id: rec.table_id,
                },
            });
        }
    }

    Ok(entries)
}

// ── Binlog extraction ───────────────────────────────────────────────────

/// Extract timeline entries from a binary log file.
///
/// Uses [`analyze_binlog`] to iterate events.  TABLE_MAP events build a
/// `table_id -> (database, table)` lookup; row events (WRITE/UPDATE/DELETE)
/// and XID events become timeline entries.
pub fn extract_binlog_timeline<R: Read + Seek>(reader: R) -> Result<Vec<TimelineEntry>, IdbError> {
    let analysis = crate::binlog::events::analyze_binlog(reader)?;

    // Track table context: TABLE_MAP events (type 19) always precede their
    // row events in the binlog stream.  We consume table_maps in order as we
    // encounter type-19 events so that each row event inherits the correct
    // database and table name.
    let mut table_map_idx: usize = 0;
    let mut current_db: Option<String> = None;
    let mut current_table: Option<String> = None;

    let mut entries = Vec::new();

    for ev in &analysis.events {
        match ev.type_code {
            // TABLE_MAP_EVENT — advance to next parsed TABLE_MAP
            19 => {
                if table_map_idx < analysis.table_maps.len() {
                    let tme = &analysis.table_maps[table_map_idx];
                    current_db = Some(tme.database_name.clone());
                    current_table = Some(tme.table_name.clone());
                    table_map_idx += 1;
                }
            }
            // WRITE_ROWS_EVENT_V2 (30), UPDATE_ROWS_EVENT_V2 (31), DELETE_ROWS_EVENT_V2 (32)
            30..=32 => {
                entries.push(TimelineEntry {
                    seq: 0,
                    source: TimelineSource::Binlog,
                    lsn: None,
                    timestamp: Some(ev.timestamp),
                    space_id: None,
                    page_no: None,
                    action: TimelineAction::Binlog {
                        event_type: ev.event_type.clone(),
                        database: current_db.clone(),
                        table: current_table.clone(),
                        xid: None,
                        pk_values: None,
                    },
                });
            }
            // QUERY_EVENT (2) — DDL or BEGIN
            2 => {
                entries.push(TimelineEntry {
                    seq: 0,
                    source: TimelineSource::Binlog,
                    lsn: None,
                    timestamp: Some(ev.timestamp),
                    space_id: None,
                    page_no: None,
                    action: TimelineAction::Binlog {
                        event_type: ev.event_type.clone(),
                        database: None,
                        table: None,
                        xid: None,
                        pk_values: None,
                    },
                });
            }
            _ => {}
        }
    }

    Ok(entries)
}

/// Result of enriched binlog extraction, carrying row data for correlation.
pub struct BinlogExtractionResult {
    /// Timeline entries (with `page_no: None` initially for row events).
    pub entries: Vec<TimelineEntry>,
    /// TABLE_MAP events keyed by table_id.
    pub table_maps: HashMap<u64, crate::binlog::events::TableMapEvent>,
    /// Raw row data keyed by entry index in `entries`.
    pub row_data: HashMap<usize, Vec<u8>>,
}

/// Extract timeline entries from a binlog, retaining row data for correlation.
///
/// Like [`extract_binlog_timeline`] but also captures TABLE_MAP metadata and
/// raw row image data from WRITE/UPDATE/DELETE events, enabling subsequent
/// [`correlate_binlog_pages`] to resolve page numbers via B+Tree lookup.
pub fn extract_binlog_timeline_enriched<R: Read + Seek>(
    reader: R,
) -> Result<BinlogExtractionResult, IdbError> {
    use crate::binlog::constants::COMMON_HEADER_SIZE;
    use crate::binlog::events::{RowsEvent, TableMapEvent};
    use crate::binlog::header::{validate_binlog_magic, BinlogEventHeader};
    use std::io::SeekFrom;

    let mut reader = reader;

    // Validate magic
    let mut magic = [0u8; 4];
    reader
        .read_exact(&mut magic)
        .map_err(|e| IdbError::Io(format!("Failed to read binlog magic: {e}")))?;

    if !validate_binlog_magic(&magic) {
        return Err(IdbError::Parse(
            "Not a valid MySQL binary log file (bad magic)".to_string(),
        ));
    }

    let file_size = reader
        .seek(SeekFrom::End(0))
        .map_err(|e| IdbError::Io(format!("Failed to seek: {e}")))?;
    reader
        .seek(SeekFrom::Start(4))
        .map_err(|e| IdbError::Io(format!("Failed to seek: {e}")))?;

    let mut entries = Vec::new();
    let mut table_maps: HashMap<u64, TableMapEvent> = HashMap::new();
    let mut row_data_map: HashMap<usize, Vec<u8>> = HashMap::new();

    let mut current_db: Option<String> = None;
    let mut current_table: Option<String> = None;

    let mut position = 4u64;
    let mut header_buf = vec![0u8; COMMON_HEADER_SIZE];

    while position + COMMON_HEADER_SIZE as u64 <= file_size {
        if reader.read_exact(&mut header_buf).is_err() {
            break;
        }

        let hdr = match BinlogEventHeader::parse(&header_buf) {
            Some(h) => h,
            None => break,
        };

        if hdr.event_length < COMMON_HEADER_SIZE as u32 {
            break;
        }

        let data_len = hdr.event_length as usize - COMMON_HEADER_SIZE;
        let mut event_data = vec![0u8; data_len];
        if reader.read_exact(&mut event_data).is_err() {
            break;
        }

        match hdr.type_code {
            // TABLE_MAP_EVENT
            19 => {
                if let Some(tme) = TableMapEvent::parse(&event_data) {
                    current_db = Some(tme.database_name.clone());
                    current_table = Some(tme.table_name.clone());
                    table_maps.insert(tme.table_id, tme);
                }
            }
            // WRITE_ROWS_EVENT_V2, UPDATE_ROWS_EVENT_V2, DELETE_ROWS_EVENT_V2
            30..=32 => {
                let entry_idx = entries.len();
                entries.push(TimelineEntry {
                    seq: 0,
                    source: TimelineSource::Binlog,
                    lsn: None,
                    timestamp: Some(hdr.timestamp),
                    space_id: None,
                    page_no: None,
                    action: TimelineAction::Binlog {
                        event_type: crate::binlog::event::BinlogEventType::from_u8(hdr.type_code)
                            .name()
                            .to_string(),
                        database: current_db.clone(),
                        table: current_table.clone(),
                        xid: None,
                        pk_values: None,
                    },
                });

                // Parse the RowsEvent to capture row data
                if let Some(rows_ev) = RowsEvent::parse(&event_data, hdr.type_code) {
                    if !rows_ev.row_data.is_empty() {
                        row_data_map.insert(entry_idx, rows_ev.row_data);
                    }
                }
            }
            // QUERY_EVENT
            2 => {
                entries.push(TimelineEntry {
                    seq: 0,
                    source: TimelineSource::Binlog,
                    lsn: None,
                    timestamp: Some(hdr.timestamp),
                    space_id: None,
                    page_no: None,
                    action: TimelineAction::Binlog {
                        event_type: "QUERY".to_string(),
                        database: None,
                        table: None,
                        xid: None,
                        pk_values: None,
                    },
                });
            }
            _ => {}
        }

        position = if hdr.next_position > 0 {
            hdr.next_position as u64
        } else {
            position + hdr.event_length as u64
        };

        if reader.seek(SeekFrom::Start(position)).is_err() {
            break;
        }
    }

    Ok(BinlogExtractionResult {
        entries,
        table_maps,
        row_data: row_data_map,
    })
}

/// Correlate binlog timeline entries with tablespace pages via B+Tree lookup.
///
/// For each row event entry that has raw row data, this function:
/// 1. Resolves the TABLE_MAP metadata for the entry's table
/// 2. Extracts PK values from the binlog row image
/// 3. Searches the clustered index B+Tree to find the leaf page
/// 4. Updates the entry's `page_no`, `space_id`, and `pk_values`
///
/// Returns the number of entries successfully correlated.
pub fn correlate_binlog_pages(
    entries: &mut [TimelineEntry],
    ts: &mut crate::innodb::tablespace::Tablespace,
    table_maps: &HashMap<u64, crate::binlog::events::TableMapEvent>,
    row_data_map: &HashMap<usize, Vec<u8>>,
) -> Result<usize, IdbError> {
    use crate::binlog::row_image::{
        extract_pk_from_row_image, parse_column_metadata, BinlogColumnMeta,
    };
    use crate::innodb::btree::{extract_clustered_index_info, search_btree, PkValue};

    // Extract clustered index info from the tablespace SDI
    let (root_page_no, index_id, pk_columns) = match extract_clustered_index_info(ts) {
        Some(info) => info,
        None => return Ok(0), // No SDI or no clustered index
    };

    // Get space_id from page 0
    let page0 = ts.read_page(0)?;
    let space_id = FilHeader::parse(&page0).map(|h| h.space_id);
    let page_size = ts.page_size();

    let mut correlated = 0usize;

    for (entry_idx, entry) in entries.iter_mut().enumerate() {
        // Only process Binlog entries that have row data
        let row_data = match row_data_map.get(&entry_idx) {
            Some(data) if !data.is_empty() => data,
            _ => continue,
        };

        // Find the TABLE_MAP for this entry's table
        // We need to match by database.table name since we don't store table_id on entries
        let (db_name, tbl_name) = match &entry.action {
            TimelineAction::Binlog {
                database: Some(db),
                table: Some(tbl),
                ..
            } => (db.clone(), tbl.clone()),
            _ => continue,
        };

        // Find the TABLE_MAP that matches this database.table
        let tme = match table_maps
            .values()
            .find(|t| t.database_name == db_name && t.table_name == tbl_name)
        {
            Some(t) => t,
            None => continue,
        };

        // Parse column metadata from the TABLE_MAP
        let meta_values = parse_column_metadata(&tme.column_types, &tme.column_metadata);

        // Build BinlogColumnMeta for each column, marking PK columns
        let columns: Vec<BinlogColumnMeta> = tme
            .column_types
            .iter()
            .enumerate()
            .map(|(i, &col_type)| {
                let type_metadata = if i < meta_values.len() {
                    meta_values[i]
                } else {
                    0
                };
                // Mark the first N columns as PK (matching the clustered index layout)
                let is_pk = i < pk_columns.len();
                let pk_ordinal = if is_pk { Some(i) } else { None };
                // Determine signedness: default to unsigned for common PK types
                // In practice, SDI provides this info through pk_columns
                let is_unsigned = if is_pk && i < pk_columns.len() {
                    pk_columns[i].is_unsigned
                } else {
                    false
                };
                BinlogColumnMeta {
                    column_type: col_type,
                    is_unsigned,
                    type_metadata,
                    is_pk,
                    pk_ordinal,
                }
            })
            .collect();

        // Extract PK values from the row image
        let pk_values = match extract_pk_from_row_image(row_data, &columns) {
            Some(pks) => pks,
            None => continue,
        };

        // Convert BinlogPkValue → PkValue for B+Tree search
        let search_key: Vec<PkValue> = pk_values
            .iter()
            .map(|v| match v {
                crate::binlog::row_image::BinlogPkValue::Int(n) => PkValue::Int(*n),
                crate::binlog::row_image::BinlogPkValue::Uint(n) => PkValue::Uint(*n),
                crate::binlog::row_image::BinlogPkValue::Str(s) => PkValue::Str(s.clone()),
                crate::binlog::row_image::BinlogPkValue::Bytes(b) => PkValue::Bytes(b.clone()),
            })
            .collect();

        // Search the B+Tree for the leaf page
        match search_btree(
            ts,
            root_page_no,
            index_id,
            &pk_columns,
            &search_key,
            page_size,
        ) {
            Ok(result) => {
                entry.page_no = Some(result.leaf_page_no);
                entry.space_id = space_id;

                // Store PK values as display strings
                let pk_strs: Vec<String> = pk_values.iter().map(|v| v.to_string()).collect();
                if let TimelineAction::Binlog {
                    ref mut pk_values, ..
                } = entry.action
                {
                    *pk_values = Some(pk_strs);
                }

                correlated += 1;
            }
            Err(_) => {
                // B+Tree search failed for this entry; skip silently
                continue;
            }
        }
    }

    Ok(correlated)
}

// ── Merge & correlation ─────────────────────────────────────────────────

/// Merge timeline entries from all sources into a sorted, sequenced report.
///
/// Entries are sorted by LSN (primary, ascending) then timestamp (secondary).
/// Entries lacking both LSN and timestamp sort to the end.  After sorting,
/// each entry is assigned a 1-based `seq` number.
///
/// The `page_summaries` field aggregates entry counts per `(space_id, page_no)`
/// pair across all sources.
pub fn merge_timeline(
    mut redo: Vec<TimelineEntry>,
    mut undo: Vec<TimelineEntry>,
    mut binlog: Vec<TimelineEntry>,
) -> TimelineReport {
    let redo_count = redo.len();
    let undo_count = undo.len();
    let binlog_count = binlog.len();

    let mut all = Vec::with_capacity(redo_count + undo_count + binlog_count);
    all.append(&mut redo);
    all.append(&mut undo);
    all.append(&mut binlog);

    // Sort: LSN ascending (primary), timestamp ascending (secondary)
    all.sort_by(|a, b| {
        let lsn_cmp = a.lsn.unwrap_or(u64::MAX).cmp(&b.lsn.unwrap_or(u64::MAX));
        if lsn_cmp != std::cmp::Ordering::Equal {
            return lsn_cmp;
        }
        a.timestamp
            .unwrap_or(u32::MAX)
            .cmp(&b.timestamp.unwrap_or(u32::MAX))
    });

    // Assign sequence numbers
    for (i, entry) in all.iter_mut().enumerate() {
        entry.seq = (i + 1) as u64;
    }

    // Build page summaries
    let mut page_agg: HashMap<(u32, u32), PageTimelineSummary> = HashMap::new();
    for entry in &all {
        if let (Some(sid), Some(pno)) = (entry.space_id, entry.page_no) {
            let summary = page_agg.entry((sid, pno)).or_insert(PageTimelineSummary {
                space_id: sid,
                page_no: pno,
                redo_entries: 0,
                undo_entries: 0,
                binlog_entries: 0,
                first_lsn: None,
                last_lsn: None,
            });
            match entry.source {
                TimelineSource::RedoLog => summary.redo_entries += 1,
                TimelineSource::UndoLog => summary.undo_entries += 1,
                TimelineSource::Binlog => summary.binlog_entries += 1,
            }
            if let Some(lsn) = entry.lsn {
                summary.first_lsn = Some(summary.first_lsn.map_or(lsn, |v: u64| v.min(lsn)));
                summary.last_lsn = Some(summary.last_lsn.map_or(lsn, |v: u64| v.max(lsn)));
            }
        }
    }

    // Count pages appearing in multiple sources
    let correlated_count = page_agg
        .values()
        .filter(|s| {
            let sources = [s.redo_entries > 0, s.undo_entries > 0, s.binlog_entries > 0];
            sources.iter().filter(|&&v| v).count() >= 2
        })
        .count();

    let mut page_summaries: Vec<PageTimelineSummary> = page_agg.into_values().collect();
    page_summaries.sort_by_key(|s| (s.space_id, s.page_no));

    TimelineReport {
        redo_count,
        undo_count,
        binlog_count,
        correlated_count,
        entries: all,
        page_summaries,
    }
}

// ── Space ID → table name resolution ────────────────────────────────────

/// Build a `space_id → "database.table"` mapping by scanning a MySQL data
/// directory for `.ibd` files and extracting SDI metadata.
///
/// This is used to annotate binlog entries with `space_id` when a data
/// directory is available.
#[cfg(not(target_arch = "wasm32"))]
pub fn build_space_table_map(datadir: &str) -> Result<HashMap<u32, String>, IdbError> {
    use std::path::Path;

    use crate::innodb::tablespace::Tablespace;
    use crate::util::fs::find_tablespace_files;

    let files = find_tablespace_files(Path::new(datadir), &["ibd"], None)?;
    let mut map = HashMap::new();

    for path in files {
        let path_str = path.to_string_lossy().to_string();
        if let Ok(mut ts) = Tablespace::open(&path_str) {
            // Read space_id from page 0 FIL header
            let space_id = ts
                .read_page(0)
                .ok()
                .and_then(|p| FilHeader::parse(&p))
                .map(|h| h.space_id);

            if let Some(sid) = space_id {
                // Derive table name from file path: <datadir>/<db>/<table>.ibd
                let p = Path::new(&path_str);
                let table = p.file_stem().and_then(|s| s.to_str()).unwrap_or("unknown");
                let db = p
                    .parent()
                    .and_then(|d| d.file_name())
                    .and_then(|s| s.to_str())
                    .unwrap_or("");
                let full = if db.is_empty() {
                    table.to_string()
                } else {
                    format!("{}.{}", db, table)
                };
                map.insert(sid, full);
            }
        }
    }

    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_empty() {
        let report = merge_timeline(vec![], vec![], vec![]);
        assert_eq!(report.redo_count, 0);
        assert_eq!(report.undo_count, 0);
        assert_eq!(report.binlog_count, 0);
        assert_eq!(report.correlated_count, 0);
        assert!(report.entries.is_empty());
        assert!(report.page_summaries.is_empty());
    }

    #[test]
    fn test_merge_sort_by_lsn() {
        let redo = vec![
            TimelineEntry {
                seq: 0,
                source: TimelineSource::RedoLog,
                lsn: Some(200),
                timestamp: None,
                space_id: Some(5),
                page_no: Some(3),
                action: TimelineAction::Redo {
                    mlog_type: "MLOG_REC_INSERT".to_string(),
                    single_rec: true,
                },
            },
            TimelineEntry {
                seq: 0,
                source: TimelineSource::RedoLog,
                lsn: Some(100),
                timestamp: None,
                space_id: Some(5),
                page_no: Some(3),
                action: TimelineAction::Redo {
                    mlog_type: "MLOG_REC_DELETE".to_string(),
                    single_rec: false,
                },
            },
        ];

        let undo = vec![TimelineEntry {
            seq: 0,
            source: TimelineSource::UndoLog,
            lsn: Some(150),
            timestamp: None,
            space_id: None,
            page_no: Some(7),
            action: TimelineAction::Undo {
                record_type: "INSERT".to_string(),
                trx_id: 42,
                undo_no: 1,
                table_id: 10,
            },
        }];

        let report = merge_timeline(redo, undo, vec![]);
        assert_eq!(report.redo_count, 2);
        assert_eq!(report.undo_count, 1);
        assert_eq!(report.entries.len(), 3);

        // Check sort order: LSN 100, 150, 200
        assert_eq!(report.entries[0].lsn, Some(100));
        assert_eq!(report.entries[0].seq, 1);
        assert_eq!(report.entries[1].lsn, Some(150));
        assert_eq!(report.entries[1].seq, 2);
        assert_eq!(report.entries[2].lsn, Some(200));
        assert_eq!(report.entries[2].seq, 3);
    }

    #[test]
    fn test_merge_page_summaries() {
        let redo = vec![
            TimelineEntry {
                seq: 0,
                source: TimelineSource::RedoLog,
                lsn: Some(100),
                timestamp: None,
                space_id: Some(5),
                page_no: Some(3),
                action: TimelineAction::Redo {
                    mlog_type: "MLOG_REC_INSERT".to_string(),
                    single_rec: true,
                },
            },
            TimelineEntry {
                seq: 0,
                source: TimelineSource::RedoLog,
                lsn: Some(200),
                timestamp: None,
                space_id: Some(5),
                page_no: Some(3),
                action: TimelineAction::Redo {
                    mlog_type: "MLOG_REC_DELETE".to_string(),
                    single_rec: true,
                },
            },
        ];

        let report = merge_timeline(redo, vec![], vec![]);
        assert_eq!(report.page_summaries.len(), 1);
        let ps = &report.page_summaries[0];
        assert_eq!(ps.space_id, 5);
        assert_eq!(ps.page_no, 3);
        assert_eq!(ps.redo_entries, 2);
        assert_eq!(ps.first_lsn, Some(100));
        assert_eq!(ps.last_lsn, Some(200));
    }

    #[test]
    fn test_merge_correlated_count() {
        let redo = vec![TimelineEntry {
            seq: 0,
            source: TimelineSource::RedoLog,
            lsn: Some(100),
            timestamp: None,
            space_id: Some(5),
            page_no: Some(3),
            action: TimelineAction::Redo {
                mlog_type: "MLOG_REC_INSERT".to_string(),
                single_rec: true,
            },
        }];
        let undo = vec![TimelineEntry {
            seq: 0,
            source: TimelineSource::UndoLog,
            lsn: Some(150),
            timestamp: None,
            space_id: Some(5),
            page_no: Some(3),
            action: TimelineAction::Undo {
                record_type: "INSERT".to_string(),
                trx_id: 1,
                undo_no: 1,
                table_id: 1,
            },
        }];

        let report = merge_timeline(redo, undo, vec![]);
        assert_eq!(report.correlated_count, 1);
    }

    #[test]
    fn test_binlog_entries_sort_after_lsn() {
        let redo = vec![TimelineEntry {
            seq: 0,
            source: TimelineSource::RedoLog,
            lsn: Some(100),
            timestamp: None,
            space_id: Some(5),
            page_no: Some(3),
            action: TimelineAction::Redo {
                mlog_type: "MLOG_REC_INSERT".to_string(),
                single_rec: true,
            },
        }];
        let binlog = vec![TimelineEntry {
            seq: 0,
            source: TimelineSource::Binlog,
            lsn: None,
            timestamp: Some(1700000000),
            space_id: None,
            page_no: None,
            action: TimelineAction::Binlog {
                event_type: "WRITE_ROWS_EVENT_V2".to_string(),
                database: Some("test".to_string()),
                table: Some("users".to_string()),
                xid: None,
                pk_values: None,
            },
        }];

        let report = merge_timeline(redo, vec![], binlog);
        assert_eq!(report.entries.len(), 2);
        // Redo (LSN=100) sorts before binlog (LSN=MAX)
        assert_eq!(report.entries[0].source, TimelineSource::RedoLog);
        assert_eq!(report.entries[1].source, TimelineSource::Binlog);
    }

    #[test]
    fn test_timeline_source_display() {
        assert_eq!(TimelineSource::RedoLog.to_string(), "REDO");
        assert_eq!(TimelineSource::UndoLog.to_string(), "UNDO");
        assert_eq!(TimelineSource::Binlog.to_string(), "BINLOG");
    }
}
