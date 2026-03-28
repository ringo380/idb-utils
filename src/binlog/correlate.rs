//! Binlog-to-tablespace page correlation.
//!
//! Maps binary log row events (INSERT/UPDATE/DELETE) to specific InnoDB
//! tablespace pages by extracting primary key values from binlog row images
//! and searching the clustered index B+Tree.

use std::collections::HashMap;

use serde::Serialize;

use crate::binlog::constants::{
    DELETE_ROWS_EVENT, TABLE_MAP_EVENT, UPDATE_ROWS_EVENT, WRITE_ROWS_EVENT,
};
use crate::binlog::events::{RowsEvent, TableMapEvent};
use crate::binlog::file::BinlogFile;
use crate::binlog::row_image::{
    extract_pk_from_row_image, parse_column_metadata, BinlogColumnMeta, BinlogPkValue,
};
use crate::innodb::btree::{extract_clustered_index_info, search_btree, PkValue};
use crate::innodb::field_decode::ColumnStorageInfo;
use crate::innodb::page::FilHeader;
use crate::innodb::tablespace::Tablespace;
use crate::IdbError;

// ── Public types ────────────────────────────────────────────────────────

/// Row-level DML operation type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum RowEventType {
    Insert,
    Update,
    Delete,
}

impl RowEventType {
    /// Convert from a binlog event type code.
    ///
    /// Returns `None` for non-row event types.
    pub fn from_type_code(code: u8) -> Option<Self> {
        match code {
            WRITE_ROWS_EVENT => Some(RowEventType::Insert),
            UPDATE_ROWS_EVENT => Some(RowEventType::Update),
            DELETE_ROWS_EVENT => Some(RowEventType::Delete),
            _ => None,
        }
    }
}

impl std::fmt::Display for RowEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RowEventType::Insert => write!(f, "INSERT"),
            RowEventType::Update => write!(f, "UPDATE"),
            RowEventType::Delete => write!(f, "DELETE"),
        }
    }
}

/// A binlog row event correlated to a specific tablespace page.
#[derive(Debug, Clone, Serialize)]
pub struct CorrelatedEvent {
    /// Absolute byte offset of this event in the binlog file.
    pub binlog_pos: u64,
    /// Row event type (INSERT, UPDATE, DELETE).
    pub event_type: RowEventType,
    /// Database (schema) name.
    pub database: String,
    /// Table name.
    pub table: String,
    /// Tablespace page number containing this row.
    pub page_no: u32,
    /// Space ID of the tablespace.
    pub space_id: u32,
    /// LSN of the target page (current state, not at binlog time).
    pub page_lsn: u64,
    /// Primary key values decoded from the row image.
    pub pk_values: Vec<String>,
    /// Unix timestamp from the binlog event header.
    pub timestamp: u32,
}

// ── Shared helpers ──────────────────────────────────────────────────────

/// Build `BinlogColumnMeta` for each column in a `TableMapEvent`, marking PK
/// columns based on the clustered index layout from SDI.
///
/// `ddl_column_names` provides the DDL-ordered column names from SDI metadata.
/// TABLE_MAP columns follow DDL definition order (NOT InnoDB's PK-first
/// physical order), so PK columns are identified by matching names against
/// `pk_columns` rather than by positional index.
pub(crate) fn build_column_meta(
    tme: &TableMapEvent,
    pk_columns: &[ColumnStorageInfo],
    ddl_column_names: &[String],
) -> Vec<BinlogColumnMeta> {
    let meta_values = parse_column_metadata(&tme.column_types, &tme.column_metadata);

    // Build a set of PK column names for lookup
    let pk_names: HashMap<&str, (usize, bool)> = pk_columns
        .iter()
        .enumerate()
        .map(|(ord, c)| (c.name.as_str(), (ord, c.is_unsigned)))
        .collect();

    tme.column_types
        .iter()
        .enumerate()
        .map(|(i, &col_type)| {
            let type_metadata = if i < meta_values.len() {
                meta_values[i]
            } else {
                0
            };

            // Match by column name: DDL-ordered name at position i
            let pk_info = ddl_column_names
                .get(i)
                .and_then(|name| pk_names.get(name.as_str()));

            let (is_pk, pk_ordinal, is_unsigned) = match pk_info {
                Some(&(ord, unsigned)) => (true, Some(ord), unsigned),
                None => (false, None, false),
            };

            BinlogColumnMeta {
                column_type: col_type,
                is_unsigned,
                type_metadata,
                is_pk,
                pk_ordinal,
            }
        })
        .collect()
}

/// Extract DDL-ordered visible column names from tablespace SDI.
///
/// Returns column names in DDL definition order (by `ordinal_position`),
/// excluding virtual/generated columns and SE-hidden system columns.
/// This order matches the TABLE_MAP column order in binlog events.
pub(crate) fn extract_ddl_column_names(
    ts: &mut Tablespace,
) -> Option<Vec<String>> {
    use crate::innodb::schema::SdiEnvelope;
    use crate::innodb::sdi;

    let sdi_pages = sdi::find_sdi_pages(ts).ok()?;
    if sdi_pages.is_empty() {
        return None;
    }
    let records = sdi::extract_sdi_from_pages(ts, &sdi_pages).ok()?;

    for rec in &records {
        if rec.sdi_type == 1 {
            let envelope: SdiEnvelope = serde_json::from_str(&rec.data).ok()?;
            let mut cols: Vec<_> = envelope
                .dd_object
                .columns
                .iter()
                .filter(|c| !c.is_virtual && (c.hidden == 1 || c.hidden == 4))
                .collect();
            cols.sort_by_key(|c| c.ordinal_position);
            return Some(cols.iter().map(|c| c.name.clone()).collect());
        }
    }
    None
}

/// Convert `BinlogPkValue` slice to `PkValue` slice for B+Tree search.
pub(crate) fn convert_pk_values(values: &[BinlogPkValue]) -> Vec<PkValue> {
    values
        .iter()
        .map(|v| match v {
            BinlogPkValue::Int(n) => PkValue::Int(*n),
            BinlogPkValue::Uint(n) => PkValue::Uint(*n),
            BinlogPkValue::Str(s) => PkValue::Str(s.clone()),
            BinlogPkValue::Bytes(b) => PkValue::Bytes(b.clone()),
        })
        .collect()
}

// ── Main correlation function ───────────────────────────────────────────

/// Correlate binlog row events with tablespace pages via B+Tree lookup.
///
/// Iterates all events in the binlog, tracking TABLE_MAP events to maintain
/// schema context. For each row event (WRITE/UPDATE/DELETE), extracts the
/// primary key from the row image, searches the tablespace's clustered index
/// B+Tree, and returns the leaf page number.
///
/// Events for tables not present in the given tablespace are silently skipped.
/// Returns an empty vec if the tablespace has no SDI or no clustered index.
///
/// # Arguments
///
/// * `binlog` - Binlog file to read events from
/// * `ts` - Tablespace containing the clustered index to search
pub fn correlate_events(
    binlog: &mut BinlogFile,
    ts: &mut Tablespace,
) -> Result<Vec<CorrelatedEvent>, IdbError> {
    // Extract clustered index info from the tablespace SDI
    let (root_page_no, index_id, pk_columns) = match extract_clustered_index_info(ts) {
        Some(info) => info,
        None => return Ok(vec![]),
    };

    // Get DDL-ordered column names for correct PK matching
    let ddl_column_names = extract_ddl_column_names(ts).unwrap_or_default();

    // Get space_id and page_size from page 0
    let page0 = ts.read_page(0)?;
    let space_id = match FilHeader::parse(&page0) {
        Some(h) => h.space_id,
        None => return Ok(vec![]),
    };
    let page_size = ts.page_size();

    // Collect TABLE_MAP events and row events in a single pass
    let mut table_maps: HashMap<u64, TableMapEvent> = HashMap::new();
    let mut results = Vec::new();

    for event_result in binlog.events() {
        let (offset, header, event) = event_result?;

        let type_code = header.type_code.type_code();

        // Track TABLE_MAP events for schema context
        if type_code == TABLE_MAP_EVENT {
            if let crate::binlog::event::BinlogEvent::Unknown { payload, .. } = &event {
                if let Some(tme) = TableMapEvent::parse(payload) {
                    table_maps.insert(tme.table_id, tme);
                }
            }
            continue;
        }

        // Process row events
        let row_event_type = match RowEventType::from_type_code(type_code) {
            Some(t) => t,
            None => continue,
        };

        // Parse RowsEvent from the payload
        let row_data = match &event {
            crate::binlog::event::BinlogEvent::Unknown { payload, .. } => {
                match RowsEvent::parse(payload, type_code) {
                    Some(re) if !re.row_data.is_empty() => re,
                    _ => continue,
                }
            }
            _ => continue,
        };

        // Look up TABLE_MAP by table_id
        let tme = match table_maps.get(&row_data.table_id) {
            Some(t) => t,
            None => continue,
        };

        // Build column metadata and extract PK
        let columns = build_column_meta(tme, &pk_columns, &ddl_column_names);
        let pk_values = match extract_pk_from_row_image(&row_data.row_data, &columns) {
            Some(pks) => pks,
            None => continue,
        };

        let search_key = convert_pk_values(&pk_values);

        // Search B+Tree for the leaf page
        let search_result =
            match search_btree(ts, root_page_no, index_id, &pk_columns, &search_key, page_size) {
                Ok(r) => r,
                Err(_) => continue,
            };

        // Read the leaf page to get its current LSN
        let page_lsn = ts
            .read_page(search_result.leaf_page_no as u64)
            .ok()
            .and_then(|page_data| FilHeader::parse(&page_data))
            .map(|h| h.lsn)
            .unwrap_or(0);

        results.push(CorrelatedEvent {
            binlog_pos: offset,
            event_type: row_event_type,
            database: tme.database_name.clone(),
            table: tme.table_name.clone(),
            page_no: search_result.leaf_page_no,
            space_id,
            page_lsn,
            pk_values: pk_values.iter().map(|v| v.to_string()).collect(),
            timestamp: header.timestamp,
        });
    }

    Ok(results)
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_event_type_from_type_code() {
        assert_eq!(
            RowEventType::from_type_code(WRITE_ROWS_EVENT),
            Some(RowEventType::Insert)
        );
        assert_eq!(
            RowEventType::from_type_code(UPDATE_ROWS_EVENT),
            Some(RowEventType::Update)
        );
        assert_eq!(
            RowEventType::from_type_code(DELETE_ROWS_EVENT),
            Some(RowEventType::Delete)
        );
        assert_eq!(RowEventType::from_type_code(TABLE_MAP_EVENT), None);
        assert_eq!(RowEventType::from_type_code(0), None);
    }

    #[test]
    fn test_row_event_type_display() {
        assert_eq!(RowEventType::Insert.to_string(), "INSERT");
        assert_eq!(RowEventType::Update.to_string(), "UPDATE");
        assert_eq!(RowEventType::Delete.to_string(), "DELETE");
    }

    #[test]
    fn test_convert_pk_values() {
        let binlog_pks = vec![
            BinlogPkValue::Int(42),
            BinlogPkValue::Uint(100),
            BinlogPkValue::Str("hello".to_string()),
            BinlogPkValue::Bytes(vec![0xde, 0xad]),
        ];

        let pk_values = convert_pk_values(&binlog_pks);
        assert_eq!(pk_values.len(), 4);
        assert_eq!(pk_values[0], PkValue::Int(42));
        assert_eq!(pk_values[1], PkValue::Uint(100));
        assert_eq!(pk_values[2], PkValue::Str("hello".to_string()));
        assert_eq!(pk_values[3], PkValue::Bytes(vec![0xde, 0xad]));
    }

    #[test]
    fn test_convert_pk_values_empty() {
        let result = convert_pk_values(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_build_column_meta_pk_first_in_ddl() {
        // DDL: CREATE TABLE t1 (id INT UNSIGNED PRIMARY KEY, name VARCHAR(100), val BIGINT)
        let tme = TableMapEvent {
            table_id: 1,
            database_name: "test".to_string(),
            table_name: "t1".to_string(),
            column_count: 3,
            column_types: vec![3, 15, 8], // LONG, VARCHAR, LONGLONG
            column_metadata: vec![0, 100, 0],
            null_bitmap: vec![0],
        };

        let pk_columns = vec![ColumnStorageInfo {
            name: "id".to_string(),
            column_type: "int".to_string(),
            dd_type: 0,
            is_nullable: false,
            is_unsigned: true,
            fixed_len: 4,
            is_variable: false,
            charset_max_bytes: 0,
            datetime_precision: 0,
            is_system_column: false,
            elements: vec![],
            numeric_precision: 0,
            numeric_scale: 0,
        }];

        let ddl_names = vec!["id".to_string(), "name".to_string(), "val".to_string()];

        let meta = build_column_meta(&tme, &pk_columns, &ddl_names);
        assert_eq!(meta.len(), 3);

        // First column (id) is PK
        assert!(meta[0].is_pk);
        assert_eq!(meta[0].pk_ordinal, Some(0));
        assert!(meta[0].is_unsigned);
        assert_eq!(meta[0].column_type, 3);

        // Second column is not PK
        assert!(!meta[1].is_pk);
        assert_eq!(meta[1].pk_ordinal, None);

        // Third column is not PK
        assert!(!meta[2].is_pk);
    }

    #[test]
    fn test_build_column_meta_pk_not_first_in_ddl() {
        // DDL: CREATE TABLE t2 (name VARCHAR(100), id INT UNSIGNED PRIMARY KEY, val BIGINT)
        // TABLE_MAP columns follow DDL order: name, id, val
        let tme = TableMapEvent {
            table_id: 2,
            database_name: "test".to_string(),
            table_name: "t2".to_string(),
            column_count: 3,
            column_types: vec![15, 3, 8], // VARCHAR, LONG, LONGLONG
            column_metadata: vec![100, 0, 0],
            null_bitmap: vec![0],
        };

        let pk_columns = vec![ColumnStorageInfo {
            name: "id".to_string(),
            column_type: "int".to_string(),
            dd_type: 0,
            is_nullable: false,
            is_unsigned: true,
            fixed_len: 4,
            is_variable: false,
            charset_max_bytes: 0,
            datetime_precision: 0,
            is_system_column: false,
            elements: vec![],
            numeric_precision: 0,
            numeric_scale: 0,
        }];

        // DDL order: name, id, val
        let ddl_names = vec!["name".to_string(), "id".to_string(), "val".to_string()];

        let meta = build_column_meta(&tme, &pk_columns, &ddl_names);
        assert_eq!(meta.len(), 3);

        // First column (name) is NOT PK
        assert!(!meta[0].is_pk);
        assert_eq!(meta[0].pk_ordinal, None);
        assert!(!meta[0].is_unsigned);

        // Second column (id) IS PK — correctly identified by name match
        assert!(meta[1].is_pk);
        assert_eq!(meta[1].pk_ordinal, Some(0));
        assert!(meta[1].is_unsigned);
        assert_eq!(meta[1].column_type, 3);

        // Third column (val) is NOT PK
        assert!(!meta[2].is_pk);
    }
}
