//! Record undelete / recovery from deleted records.
//!
//! Provides three strategies for recovering deleted records from InnoDB
//! tablespaces:
//!
//! 1. **Delete-marked records** (confidence 1.0) — still in the active record
//!    chain with `delete_mark=1`, full column data intact.
//! 2. **Free-list records** (confidence 0.3–0.7) — purged from active chain
//!    but still in the page's free record list, data may be partially overwritten.
//! 3. **Undo log records** (confidence 0.1–0.3) — PK-only recovery from
//!    `DEL_MARK_REC` entries in undo pages.
//!
//! Use [`scan_undeleted`] as the main entry point to run all applicable scans,
//! or call individual scan functions for targeted recovery.

use serde::Serialize;

use crate::innodb::export::{decode_page_records, extract_column_layout, extract_table_name};
use crate::innodb::field_decode::{self, ColumnStorageInfo, FieldValue};
use crate::innodb::index::IndexHeader;
use crate::innodb::page::FilHeader;
use crate::innodb::page_types::PageType;
use crate::innodb::record::{CompactRecordHeader, RecordType};
use crate::innodb::schema::SdiEnvelope;
use crate::innodb::sdi;
use crate::innodb::tablespace::Tablespace;
use crate::innodb::undo::{parse_undo_records, UndoRecordType, UndoState};
use crate::IdbError;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Source of a recovered record.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoverySource {
    /// Record still in active chain with delete_mark=1.
    DeleteMarked,
    /// Record in the page's free list (purged but not overwritten).
    FreeList,
    /// Record recovered from undo log (PK fields only).
    UndoLog,
}

/// A single recovered (undeleted) record.
#[derive(Debug, Clone, Serialize)]
pub struct UndeletedRecord {
    /// Where this record was found.
    pub source: RecoverySource,
    /// Confidence score (0.0 to 1.0).
    pub confidence: f64,
    /// Transaction ID if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trx_id: Option<u64>,
    /// Page number where the record was found.
    pub page_number: u64,
    /// Byte offset within the page.
    pub offset: usize,
    /// Decoded column name/value pairs.
    pub columns: Vec<(String, FieldValue)>,
    /// Hex dump fallback when full decode fails.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_hex: Option<String>,
}

/// Summary statistics for an undelete scan.
#[derive(Debug, Clone, Serialize)]
pub struct UndeleteSummary {
    /// Total recovered records.
    pub total: usize,
    /// Records from delete-marked source.
    pub delete_marked: usize,
    /// Records from free list source.
    pub free_list: usize,
    /// Records from undo log source.
    pub undo_log: usize,
}

/// Complete result of an undelete scan.
#[derive(Debug, Clone, Serialize)]
pub struct UndeleteScanResult {
    /// Table name (from SDI metadata, if available).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_name: Option<String>,
    /// Column names in order.
    pub column_names: Vec<String>,
    /// All recovered records.
    pub records: Vec<UndeletedRecord>,
    /// Summary statistics.
    pub summary: UndeleteSummary,
}

// ---------------------------------------------------------------------------
// Scan: delete-marked records
// ---------------------------------------------------------------------------

/// Scan a single page for delete-marked records (confidence 1.0).
///
/// Reuses `decode_page_records()` with `where_delete_mark=true` to find
/// records still in the active record chain but flagged as deleted.
pub fn scan_delete_marked_records(
    page_data: &[u8],
    page_number: u64,
    columns: &[ColumnStorageInfo],
    page_size: u32,
) -> Vec<UndeletedRecord> {
    let rows = decode_page_records(page_data, columns, true, true, page_size);

    rows.into_iter()
        .map(|row| {
            // Extract trx_id from system columns
            let trx_id = row.iter().find_map(|(name, val)| {
                if name == "DB_TRX_ID" {
                    match val {
                        FieldValue::Uint(v) => Some(*v),
                        FieldValue::Int(v) => Some(*v as u64),
                        FieldValue::Hex(h) => {
                            u64::from_str_radix(h.trim_start_matches("0x"), 16).ok()
                        }
                        _ => None,
                    }
                } else {
                    None
                }
            });

            // Filter out system columns for output
            let user_columns: Vec<(String, FieldValue)> = row
                .into_iter()
                .filter(|(name, _)| name != "DB_TRX_ID" && name != "DB_ROLL_PTR")
                .collect();

            UndeletedRecord {
                source: RecoverySource::DeleteMarked,
                confidence: 1.0,
                trx_id,
                page_number,
                offset: 0,
                columns: user_columns,
                raw_hex: None,
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Scan: free-list records
// ---------------------------------------------------------------------------

/// Scan the free record list on a single INDEX page (confidence 0.2–0.7).
///
/// Walks the freed record chain starting at `IndexHeader::free`, parses
/// compact record headers, and attempts field decoding. Confidence is
/// scored based on decode success.
pub fn scan_free_list_records(
    page_data: &[u8],
    page_number: u64,
    columns: &[ColumnStorageInfo],
    page_size: u32,
) -> Vec<UndeletedRecord> {
    let mut records = Vec::new();

    let idx_hdr = match IndexHeader::parse(page_data) {
        Some(h) => h,
        None => return records,
    };

    if idx_hdr.free == 0 || !idx_hdr.is_compact() {
        return records;
    }

    let mut visited = std::collections::HashSet::new();
    let mut offset = idx_hdr.free as usize;
    let ps = page_size as usize;
    let max_iterations = 10_000;
    let mut iterations = 0;

    while offset > 0 && offset < ps && iterations < max_iterations {
        if !visited.insert(offset) {
            break; // cycle
        }
        iterations += 1;

        // Record header is 5 bytes before the record origin
        if offset < 5 || offset >= ps {
            break;
        }
        let hdr_start = offset - 5;
        if hdr_start + 5 > page_data.len() {
            break;
        }

        let hdr = match CompactRecordHeader::parse(&page_data[hdr_start..]) {
            Some(h) => h,
            None => break,
        };

        // Infimum/supremum should never appear in the free list — if we
        // encounter one, the page is corrupt or the offset is wrong.
        if matches!(hdr.rec_type, RecordType::Infimum | RecordType::Supremum) {
            break;
        }

        // Attempt field decoding at this offset
        let (decoded_cols, confidence, raw_hex) = attempt_field_decode(page_data, offset, columns);

        if !decoded_cols.is_empty() || raw_hex.is_some() {
            records.push(UndeletedRecord {
                source: RecoverySource::FreeList,
                confidence,
                trx_id: None,
                page_number,
                offset,
                columns: decoded_cols,
                raw_hex,
            });
        }

        // Follow the free list: next_offset is relative to current record origin
        let next_rel = hdr.next_offset;
        if next_rel == 0 {
            break;
        }
        let next_abs = offset as i64 + next_rel as i64;
        if next_abs <= 0 || next_abs as usize >= ps {
            break;
        }
        offset = next_abs as usize;
    }

    records
}

/// Try to decode fields at a given record offset. Returns (columns, confidence, optional hex).
fn attempt_field_decode(
    page_data: &[u8],
    record_offset: usize,
    columns: &[ColumnStorageInfo],
) -> (Vec<(String, FieldValue)>, f64, Option<String>) {
    let n_nullable = columns.iter().filter(|c| c.is_nullable).count();
    let n_variable = columns.iter().filter(|c| c.is_variable).count();

    let (nulls, var_lengths) = match crate::innodb::record::read_variable_field_lengths(
        page_data,
        record_offset,
        n_nullable,
        n_variable,
    ) {
        Some(r) => r,
        None => {
            // Can't parse variable-length headers; provide hex fallback
            let hex = hex_at_offset(page_data, record_offset, 64);
            return (Vec::new(), 0.2, Some(hex));
        }
    };

    let mut row = Vec::new();
    let mut pos = record_offset;
    let mut null_idx = 0;
    let mut var_idx = 0;
    let mut decoded_count = 0;
    let mut total_user_cols = 0;

    for col in columns {
        if col.is_system_column {
            if col.fixed_len > 0 {
                pos += col.fixed_len;
            }
            continue;
        }
        total_user_cols += 1;

        if col.is_nullable {
            if null_idx < nulls.len() && nulls[null_idx] {
                row.push((col.name.clone(), FieldValue::Null));
                null_idx += 1;
                decoded_count += 1;
                continue;
            }
            null_idx += 1;
        }

        if col.is_variable {
            let len = if var_idx < var_lengths.len() {
                var_lengths[var_idx]
            } else {
                0
            };
            var_idx += 1;

            if pos + len <= page_data.len() && len < 65536 {
                let val = field_decode::decode_field(&page_data[pos..pos + len], col);
                row.push((col.name.clone(), val));
                pos += len;
                decoded_count += 1;
            } else {
                row.push((col.name.clone(), FieldValue::Null));
            }
        } else {
            let len = col.fixed_len;
            if len > 0 && pos + len <= page_data.len() {
                let val = field_decode::decode_field(&page_data[pos..pos + len], col);
                row.push((col.name.clone(), val));
                pos += len;
                decoded_count += 1;
            } else {
                row.push((col.name.clone(), FieldValue::Null));
            }
        }
    }

    let confidence = if total_user_cols == 0 {
        0.2
    } else if decoded_count == total_user_cols {
        0.7
    } else if decoded_count > 0 {
        0.4
    } else {
        0.2
    };

    (row, confidence, None)
}

/// Extract a hex string of `max_len` bytes starting at `offset`.
fn hex_at_offset(data: &[u8], offset: usize, max_len: usize) -> String {
    let end = (offset + max_len).min(data.len());
    if offset >= data.len() {
        return String::new();
    }
    data[offset..end]
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join("")
}

// ---------------------------------------------------------------------------
// Scan: undo log records
// ---------------------------------------------------------------------------

/// Scan undo log pages for DEL_MARK_REC entries matching a target table ID.
///
/// Returns records with PK fields only (confidence 0.1–0.3).
pub fn scan_undo_for_deletes(
    ts: &mut Tablespace,
    target_table_id: u64,
    pk_columns: &[ColumnStorageInfo],
) -> Result<Vec<UndeletedRecord>, IdbError> {
    let mut records = Vec::new();

    ts.for_each_page(|page_num, page_data| {
        let hdr = match FilHeader::parse(page_data) {
            Some(h) => h,
            None => return Ok(()),
        };

        if hdr.page_type != PageType::UndoLog {
            return Ok(());
        }

        // Check segment state for confidence scoring
        let seg_state = crate::innodb::undo::UndoSegmentHeader::parse(page_data).map(|s| s.state);

        let undo_recs = parse_undo_records(page_data);

        for urec in &undo_recs {
            if urec.record_type != UndoRecordType::DelMarkRec {
                continue;
            }
            if urec.table_id != target_table_id {
                continue;
            }

            // Decode PK fields
            let mut cols = Vec::new();
            for (i, pk_bytes) in urec.pk_fields.iter().enumerate() {
                let col_name = if i < pk_columns.len() {
                    pk_columns[i].name.clone()
                } else {
                    format!("pk_{}", i)
                };

                let val = if i < pk_columns.len() {
                    field_decode::decode_field(pk_bytes, &pk_columns[i])
                } else {
                    FieldValue::Hex(
                        pk_bytes
                            .iter()
                            .map(|b| format!("{:02x}", b))
                            .collect::<Vec<_>>()
                            .join(""),
                    )
                };

                cols.push((col_name, val));
            }

            let confidence = match seg_state {
                Some(UndoState::Active) => 0.3,
                Some(UndoState::Cached) | Some(UndoState::ToPurge) => 0.1,
                _ => 0.2,
            };

            records.push(UndeletedRecord {
                source: RecoverySource::UndoLog,
                confidence,
                trx_id: urec.trx_id,
                page_number: page_num,
                offset: urec.offset,
                columns: cols,
                raw_hex: None,
            });
        }

        Ok(())
    })?;

    Ok(records)
}

// ---------------------------------------------------------------------------
// Extract table ID from SDI
// ---------------------------------------------------------------------------

/// Extract the InnoDB internal table ID (`se_private_id`) from SDI metadata.
pub fn extract_table_id(ts: &mut Tablespace) -> Option<u64> {
    let sdi_pages = sdi::find_sdi_pages(ts).ok()?;
    if sdi_pages.is_empty() {
        return None;
    }
    let records = sdi::extract_sdi_from_pages(ts, &sdi_pages).ok()?;

    for rec in &records {
        if rec.sdi_type == 1 {
            let envelope: SdiEnvelope = serde_json::from_str(&rec.data).ok()?;
            if envelope.dd_object.se_private_id > 0 {
                return Some(envelope.dd_object.se_private_id);
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Main orchestrator
// ---------------------------------------------------------------------------

/// Run a complete undelete scan on a tablespace.
///
/// Scans all leaf INDEX pages for delete-marked and free-list records.
/// If `undo_ts` is provided, also scans undo log pages for DEL_MARK_REC entries.
/// Results are filtered by `min_confidence` and `min_trx_id`, then sorted by
/// confidence descending.
pub fn scan_undeleted(
    ts: &mut Tablespace,
    undo_ts: Option<&mut Tablespace>,
    min_confidence: f64,
    min_trx_id: Option<u64>,
    target_page: Option<u64>,
) -> Result<UndeleteScanResult, IdbError> {
    let table_name = extract_table_name(ts);

    // Extract column layout
    let (columns, clustered_index_id) = extract_column_layout(ts).ok_or_else(|| {
        IdbError::Parse(
            "Cannot extract column layout from SDI (pre-8.0 tablespace or missing SDI)".to_string(),
        )
    })?;

    let page_size = ts.page_size();
    let col_names: Vec<String> = columns
        .iter()
        .filter(|c| !c.is_system_column)
        .map(|c| c.name.clone())
        .collect();

    let mut all_records = Vec::new();

    // Collect leaf INDEX pages matching the clustered index
    let mut leaf_pages: Vec<(u64, Vec<u8>)> = Vec::new();
    ts.for_each_page(|pn, pdata| {
        if let Some(target) = target_page {
            if pn != target {
                return Ok(());
            }
        }
        let hdr = match FilHeader::parse(pdata) {
            Some(h) => h,
            None => return Ok(()),
        };
        if hdr.page_type != PageType::Index {
            return Ok(());
        }
        let idx_hdr = match IndexHeader::parse(pdata) {
            Some(h) => h,
            None => return Ok(()),
        };
        if idx_hdr.index_id != clustered_index_id || !idx_hdr.is_leaf() {
            return Ok(());
        }
        leaf_pages.push((pn, pdata.to_vec()));
        Ok(())
    })?;

    // Scan each leaf page for delete-marked and free-list records
    for (pn, pdata) in &leaf_pages {
        let mut dm = scan_delete_marked_records(pdata, *pn, &columns, page_size);
        all_records.append(&mut dm);

        let mut fl = scan_free_list_records(pdata, *pn, &columns, page_size);
        all_records.append(&mut fl);
    }

    // Undo log scan (if undo tablespace provided)
    if let Some(uts) = undo_ts {
        let table_id = extract_table_id(ts);
        if let Some(tid) = table_id {
            // Extract PK columns from layout
            let pk_cols: Vec<ColumnStorageInfo> = columns
                .iter()
                .filter(|c| !c.is_system_column && !c.is_nullable)
                .take(1) // Simplified: take first non-null non-system col as PK proxy
                .cloned()
                .collect();

            let mut undo_recs = scan_undo_for_deletes(uts, tid, &pk_cols)?;
            all_records.append(&mut undo_recs);
        }
    }

    // Apply filters
    all_records.retain(|r| r.confidence >= min_confidence);
    if let Some(min_trx) = min_trx_id {
        all_records.retain(|r| r.trx_id.is_some_and(|t| t >= min_trx));
    }

    // Sort: confidence descending, then page number ascending
    all_records.sort_by(|a, b| {
        b.confidence
            .partial_cmp(&a.confidence)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.page_number.cmp(&b.page_number))
    });

    let summary = UndeleteSummary {
        total: all_records.len(),
        delete_marked: all_records
            .iter()
            .filter(|r| matches!(r.source, RecoverySource::DeleteMarked))
            .count(),
        free_list: all_records
            .iter()
            .filter(|r| matches!(r.source, RecoverySource::FreeList))
            .count(),
        undo_log: all_records
            .iter()
            .filter(|r| matches!(r.source, RecoverySource::UndoLog))
            .count(),
    };

    Ok(UndeleteScanResult {
        table_name,
        column_names: col_names,
        records: all_records,
        summary,
    })
}

// ---------------------------------------------------------------------------
// WASM-friendly variant (no second tablespace)
// ---------------------------------------------------------------------------

/// Scan for deleted records from in-memory tablespace data (WASM-compatible).
///
/// Only performs delete-marked and free-list scans (no undo log).
/// Returns `None` if SDI metadata is unavailable.
pub fn scan_deleted_from_bytes(
    data: &[u8],
    target_page: Option<u64>,
) -> Result<Option<UndeleteScanResult>, IdbError> {
    let mut ts = Tablespace::from_bytes(data.to_vec())?;

    let table_name = extract_table_name(&mut ts);

    let (columns, clustered_index_id) = match extract_column_layout(&mut ts) {
        Some(pair) => pair,
        None => return Ok(None),
    };

    let page_size = ts.page_size();
    let col_names: Vec<String> = columns
        .iter()
        .filter(|c| !c.is_system_column)
        .map(|c| c.name.clone())
        .collect();

    let mut all_records = Vec::new();

    ts.for_each_page(|pn, pdata| {
        if let Some(target) = target_page {
            if pn != target {
                return Ok(());
            }
        }
        let hdr = match FilHeader::parse(pdata) {
            Some(h) => h,
            None => return Ok(()),
        };
        if hdr.page_type != PageType::Index {
            return Ok(());
        }
        let idx_hdr = match IndexHeader::parse(pdata) {
            Some(h) => h,
            None => return Ok(()),
        };
        if idx_hdr.index_id != clustered_index_id || !idx_hdr.is_leaf() {
            return Ok(());
        }

        let mut dm = scan_delete_marked_records(pdata, pn, &columns, page_size);
        all_records.append(&mut dm);

        let mut fl = scan_free_list_records(pdata, pn, &columns, page_size);
        all_records.append(&mut fl);

        Ok(())
    })?;

    all_records.sort_by(|a, b| {
        b.confidence
            .partial_cmp(&a.confidence)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.page_number.cmp(&b.page_number))
    });

    let summary = UndeleteSummary {
        total: all_records.len(),
        delete_marked: all_records
            .iter()
            .filter(|r| matches!(r.source, RecoverySource::DeleteMarked))
            .count(),
        free_list: all_records
            .iter()
            .filter(|r| matches!(r.source, RecoverySource::FreeList))
            .count(),
        undo_log: 0,
    };

    Ok(Some(UndeleteScanResult {
        table_name,
        column_names: col_names,
        records: all_records,
        summary,
    }))
}

// ---------------------------------------------------------------------------
// Formatting helpers (used by CLI)
// ---------------------------------------------------------------------------

/// Format a FieldValue as a SQL literal string.
pub fn field_value_to_sql(val: &FieldValue) -> String {
    match val {
        FieldValue::Null => "NULL".to_string(),
        FieldValue::Int(n) => n.to_string(),
        FieldValue::Uint(n) => n.to_string(),
        FieldValue::Float(f) => f.to_string(),
        FieldValue::Double(d) => d.to_string(),
        FieldValue::Str(s) => format!("'{}'", s.replace('\'', "''")),
        FieldValue::Hex(h) => format!("X'{}'", h),
    }
}

/// Format a FieldValue for JSON output (as a serde_json::Value).
pub fn field_value_to_json(val: &FieldValue) -> serde_json::Value {
    match val {
        FieldValue::Null => serde_json::Value::Null,
        FieldValue::Int(n) => serde_json::json!(*n),
        FieldValue::Uint(n) => serde_json::json!(*n),
        FieldValue::Float(f) => serde_json::json!(*f),
        FieldValue::Double(d) => serde_json::json!(*d),
        FieldValue::Str(s) => serde_json::json!(s),
        FieldValue::Hex(h) => serde_json::json!(h),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recovery_source_serialization() {
        let json = serde_json::to_string(&RecoverySource::DeleteMarked).unwrap();
        assert_eq!(json, "\"delete_marked\"");
    }

    #[test]
    fn test_undelete_summary_serialization() {
        let summary = UndeleteSummary {
            total: 5,
            delete_marked: 3,
            free_list: 2,
            undo_log: 0,
        };
        let json = serde_json::to_string(&summary).unwrap();
        assert!(json.contains("\"total\":5"));
        assert!(json.contains("\"delete_marked\":3"));
    }

    #[test]
    fn test_undeleted_record_serialization() {
        let rec = UndeletedRecord {
            source: RecoverySource::FreeList,
            confidence: 0.7,
            trx_id: Some(42),
            page_number: 4,
            offset: 200,
            columns: vec![
                ("id".to_string(), FieldValue::Int(1)),
                ("name".to_string(), FieldValue::Str("test".to_string())),
            ],
            raw_hex: None,
        };
        let json = serde_json::to_string(&rec).unwrap();
        assert!(json.contains("\"free_list\""));
        assert!(json.contains("\"confidence\":0.7"));
        assert!(!json.contains("raw_hex")); // skip_serializing_if
    }

    #[test]
    fn test_field_value_to_sql() {
        assert_eq!(field_value_to_sql(&FieldValue::Null), "NULL");
        assert_eq!(field_value_to_sql(&FieldValue::Int(42)), "42");
        assert_eq!(
            field_value_to_sql(&FieldValue::Str("hello".into())),
            "'hello'"
        );
        assert_eq!(
            field_value_to_sql(&FieldValue::Str("it's".into())),
            "'it''s'"
        );
        assert_eq!(
            field_value_to_sql(&FieldValue::Hex("DEADBEEF".into())),
            "X'DEADBEEF'"
        );
    }

    #[test]
    fn test_field_value_to_json() {
        assert_eq!(
            field_value_to_json(&FieldValue::Null),
            serde_json::Value::Null
        );
        assert_eq!(
            field_value_to_json(&FieldValue::Int(42)),
            serde_json::json!(42)
        );
        assert_eq!(
            field_value_to_json(&FieldValue::Str("test".into())),
            serde_json::json!("test")
        );
    }

    #[test]
    fn test_hex_at_offset() {
        let data = [0xDE, 0xAD, 0xBE, 0xEF];
        assert_eq!(hex_at_offset(&data, 0, 4), "deadbeef");
        assert_eq!(hex_at_offset(&data, 2, 10), "beef");
        assert_eq!(hex_at_offset(&data, 10, 4), "");
    }

    #[test]
    fn test_scan_delete_marked_empty_page() {
        // An all-zeros page has no valid INDEX header so should return nothing
        let page = vec![0u8; 16384];
        let cols = vec![];
        let result = scan_delete_marked_records(&page, 0, &cols, 16384);
        assert!(result.is_empty());
    }

    #[test]
    fn test_scan_free_list_no_index() {
        // Non-INDEX page returns empty
        let page = vec![0u8; 16384];
        let cols = vec![];
        let result = scan_free_list_records(&page, 0, &cols, 16384);
        assert!(result.is_empty());
    }

    #[test]
    fn test_scan_result_full_serialization() {
        let result = UndeleteScanResult {
            table_name: Some("users".to_string()),
            column_names: vec!["id".to_string(), "name".to_string()],
            records: vec![],
            summary: UndeleteSummary {
                total: 0,
                delete_marked: 0,
                free_list: 0,
                undo_log: 0,
            },
        };
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"table_name\":\"users\""));
        assert!(json.contains("\"column_names\""));
    }
}
