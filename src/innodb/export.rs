//! Record export logic for InnoDB tablespaces.
//!
//! Provides reusable functions for extracting typed records from clustered
//! index leaf pages. Used by both the CLI `inno export` subcommand and the
//! WASM `export_records` binding.
//!
//! # Functions
//!
//! | Function | Purpose |
//! |----------|---------|
//! | [`extract_column_layout`] | Parse SDI metadata to build column layout and find clustered index ID |
//! | [`decode_page_records`] | Walk compact records on a page and decode fields using column metadata |
//! | [`csv_escape`] | RFC 4180 CSV escaping for [`FieldValue`] |

use crate::innodb::field_decode::{self, ColumnStorageInfo, FieldValue};
use crate::innodb::record::walk_compact_records;
use crate::innodb::schema::SdiEnvelope;
use crate::innodb::sdi;
use crate::innodb::tablespace::Tablespace;

/// Extract column layout from SDI metadata.
///
/// Reads SDI pages from the tablespace, deserializes the table definition,
/// builds the column storage layout, and finds the clustered (PRIMARY) index
/// ID from `se_private_data`.
///
/// Returns `(column_layout, clustered_index_id)` or `None` if SDI metadata
/// is unavailable or cannot be parsed.
pub fn extract_column_layout(ts: &mut Tablespace) -> Option<(Vec<ColumnStorageInfo>, u64)> {
    let sdi_pages = sdi::find_sdi_pages(ts).ok()?;
    if sdi_pages.is_empty() {
        return None;
    }
    let records = sdi::extract_sdi_from_pages(ts, &sdi_pages).ok()?;

    for rec in &records {
        if rec.sdi_type == 1 {
            let envelope: SdiEnvelope = serde_json::from_str(&rec.data).ok()?;
            let cols = field_decode::build_column_layout(&envelope.dd_object);

            // Find clustered index ID from se_private_data
            let raw: serde_json::Value = serde_json::from_str(&rec.data).ok()?;
            let indexes = raw.get("dd_object")?.get("indexes")?.as_array()?;
            for idx in indexes {
                let idx_type = idx.get("type")?.as_u64()?;
                if idx_type == 1 {
                    // PRIMARY
                    let se_data = idx.get("se_private_data")?.as_str()?;
                    for part in se_data.split(';') {
                        if let Some(id_str) = part.strip_prefix("id=") {
                            if let Ok(id) = id_str.parse::<u64>() {
                                return Some((cols, id));
                            }
                        }
                    }
                }
            }

            // If no PRIMARY found, try any index with an id in se_private_data
            for idx in indexes {
                let se_data = match idx.get("se_private_data").and_then(|v| v.as_str()) {
                    Some(s) => s,
                    None => continue,
                };
                for part in se_data.split(';') {
                    if let Some(id_str) = part.strip_prefix("id=") {
                        if let Ok(id) = id_str.parse::<u64>() {
                            return Some((cols, id));
                        }
                    }
                }
            }

            return None;
        }
    }
    None
}

/// Extract the table name from SDI metadata.
///
/// Returns the `dd_object.name` field from the first Table SDI record,
/// or `None` if SDI is unavailable.
pub fn extract_table_name(ts: &mut Tablespace) -> Option<String> {
    let sdi_pages = sdi::find_sdi_pages(ts).ok()?;
    if sdi_pages.is_empty() {
        return None;
    }
    let records = sdi::extract_sdi_from_pages(ts, &sdi_pages).ok()?;

    for rec in &records {
        if rec.sdi_type == 1 {
            let envelope: SdiEnvelope = serde_json::from_str(&rec.data).ok()?;
            let name = envelope.dd_object.name.clone();
            if !name.is_empty() {
                return Some(name);
            }
        }
    }
    None
}

/// Decode records from a single page using the column layout.
///
/// Walks compact-format records on the page, applies delete-mark and
/// system-column filters, and returns a list of rows. Each row is a list
/// of `(column_name, decoded_value)` pairs.
///
/// # Arguments
///
/// * `page_data` - Raw page bytes (must be a full InnoDB page).
/// * `columns` - Column layout from [`extract_column_layout`].
/// * `where_delete_mark` - If `true`, only include delete-marked records;
///   if `false`, only include non-delete-marked records.
/// * `system_columns` - If `true`, include system columns (DB_TRX_ID,
///   DB_ROLL_PTR) in the output.
/// * `_page_size` - Page size (reserved for future use).
pub fn decode_page_records(
    page_data: &[u8],
    columns: &[ColumnStorageInfo],
    where_delete_mark: bool,
    system_columns: bool,
    _page_size: u32,
) -> Vec<Vec<(String, FieldValue)>> {
    let records = walk_compact_records(page_data);
    let mut rows = Vec::new();

    for rec in &records {
        // Filter by delete mark
        let delete_mark = rec.header.delete_mark();
        if where_delete_mark && !delete_mark {
            continue;
        }
        if !where_delete_mark && delete_mark {
            continue;
        }

        // Decode fields
        let mut row = Vec::new();
        let mut pos = rec.offset;

        // Read nullable bitmap and variable-length headers
        let n_nullable = columns.iter().filter(|c| c.is_nullable).count();
        let n_variable = columns.iter().filter(|c| c.is_variable).count();

        let (nulls, var_lengths) = match crate::innodb::record::read_variable_field_lengths(
            page_data, rec.offset, n_nullable, n_variable,
        ) {
            Some(r) => r,
            None => continue,
        };

        let mut null_idx = 0;
        let mut var_idx = 0;

        for col in columns {
            // Skip system columns unless requested
            if !system_columns && col.is_system_column {
                if col.fixed_len > 0 {
                    pos += col.fixed_len;
                }
                continue;
            }

            // Check null
            if col.is_nullable {
                if null_idx < nulls.len() && nulls[null_idx] {
                    row.push((col.name.clone(), FieldValue::Null));
                    null_idx += 1;
                    continue;
                }
                null_idx += 1;
            }

            if col.is_variable {
                // Variable-length field
                let len = if var_idx < var_lengths.len() {
                    var_lengths[var_idx]
                } else {
                    0
                };
                var_idx += 1;

                if pos + len <= page_data.len() {
                    let val = field_decode::decode_field(&page_data[pos..pos + len], col);
                    row.push((col.name.clone(), val));
                    pos += len;
                } else {
                    row.push((col.name.clone(), FieldValue::Null));
                }
            } else {
                // Fixed-length field
                let len = col.fixed_len;
                if len > 0 && pos + len <= page_data.len() {
                    let val = field_decode::decode_field(&page_data[pos..pos + len], col);
                    row.push((col.name.clone(), val));
                    pos += len;
                } else {
                    row.push((col.name.clone(), FieldValue::Null));
                }
            }
        }

        rows.push(row);
    }

    rows
}

/// CSV-escape a field value per RFC 4180.
///
/// - `Null` produces an empty string.
/// - Numeric types produce their string representation.
/// - Strings containing commas, double quotes, or newlines are quoted and
///   internal double quotes are doubled.
/// - Hex values are passed through unquoted.
pub fn csv_escape(val: &FieldValue) -> String {
    match val {
        FieldValue::Null => String::new(),
        FieldValue::Int(n) => n.to_string(),
        FieldValue::Uint(n) => n.to_string(),
        FieldValue::Float(f) => f.to_string(),
        FieldValue::Double(d) => d.to_string(),
        FieldValue::Str(s) => {
            if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
                format!("\"{}\"", s.replace('"', "\"\""))
            } else {
                s.clone()
            }
        }
        FieldValue::Hex(h) => h.clone(),
    }
}
