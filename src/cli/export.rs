//! CLI implementation for the `inno export` subcommand.
//!
//! Extracts user records from clustered index leaf pages and outputs them
//! as CSV, JSON, or raw hex. Uses SDI metadata for typed field decoding
//! when available.

use std::io::Write;

use crate::cli::wprintln;
use crate::innodb::field_decode::{self, ColumnStorageInfo, FieldValue};
use crate::innodb::index::IndexHeader;
use crate::innodb::page::FilHeader;
use crate::innodb::page_types::PageType;
use crate::innodb::record::walk_compact_records;
use crate::innodb::schema::SdiEnvelope;
use crate::innodb::sdi;
use crate::IdbError;

/// Output format for exported records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    Csv,
    Json,
    Hex,
}

impl ExportFormat {
    fn from_str(s: &str) -> Result<Self, IdbError> {
        match s.to_lowercase().as_str() {
            "csv" => Ok(ExportFormat::Csv),
            "json" => Ok(ExportFormat::Json),
            "hex" => Ok(ExportFormat::Hex),
            _ => Err(IdbError::Argument(format!(
                "Unknown format '{}'. Use csv, json, or hex.",
                s
            ))),
        }
    }
}

/// Options for the `inno export` subcommand.
pub struct ExportOptions {
    /// Path to the InnoDB tablespace file (.ibd).
    pub file: String,
    /// Export records from a specific page only.
    pub page: Option<u64>,
    /// Output format: csv, json, or hex.
    pub format: String,
    /// Include only delete-marked records.
    pub where_delete_mark: bool,
    /// Include system columns (DB_TRX_ID, DB_ROLL_PTR) in output.
    pub system_columns: bool,
    /// Show additional details.
    pub verbose: bool,
    /// Override the auto-detected page size.
    pub page_size: Option<u32>,
    /// Path to MySQL keyring file for decrypting encrypted tablespaces.
    pub keyring: Option<String>,
    /// Use memory-mapped I/O for file access.
    pub mmap: bool,
}

/// Export records from a tablespace.
pub fn execute(opts: &ExportOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    let format = ExportFormat::from_str(&opts.format)?;

    let mut ts = crate::cli::open_tablespace(&opts.file, opts.page_size, opts.mmap)?;

    if let Some(ref keyring_path) = opts.keyring {
        crate::cli::setup_decryption(&mut ts, keyring_path)?;
    }

    let page_size = ts.page_size();

    // Try SDI extraction for typed decoding
    let column_layout = extract_column_layout(&mut ts);
    let (columns, clustered_index_id) = match column_layout {
        Some((cols, idx_id)) => (Some(cols), Some(idx_id)),
        None => {
            if format != ExportFormat::Hex {
                eprintln!("Warning: No SDI metadata found. Falling back to hex output.");
            }
            (None, None)
        }
    };

    let use_hex = columns.is_none() || format == ExportFormat::Hex;

    // Determine which index_id to export (clustered/PRIMARY)
    // If we don't have SDI, we'll export all leaf INDEX pages
    let target_index_id = clustered_index_id;

    // Collect pages to process
    let mut pages_data: Vec<(u64, Vec<u8>)> = Vec::new();
    ts.for_each_page(|page_num, data| {
        if let Some(specific_page) = opts.page {
            if page_num != specific_page {
                return Ok(());
            }
        }
        let fil = match FilHeader::parse(data) {
            Some(h) => h,
            None => return Ok(()),
        };
        if fil.page_type != PageType::Index {
            return Ok(());
        }
        let idx = match IndexHeader::parse(data) {
            Some(h) => h,
            None => return Ok(()),
        };
        // Only leaf pages
        if !idx.is_leaf() {
            return Ok(());
        }
        // Filter by clustered index if known
        if let Some(target_id) = target_index_id {
            if idx.index_id != target_id {
                return Ok(());
            }
        }
        pages_data.push((page_num, data.to_vec()));
        Ok(())
    })?;

    if use_hex {
        output_hex(writer, &pages_data, opts)?;
    } else {
        let cols = columns.as_ref().unwrap();
        match format {
            ExportFormat::Csv => output_csv(writer, &pages_data, cols, opts, page_size)?,
            ExportFormat::Json => output_json(writer, &pages_data, cols, opts, page_size)?,
            ExportFormat::Hex => unreachable!(),
        }
    }

    Ok(())
}

/// Extract column layout from SDI metadata.
/// Returns (column_layout, clustered_index_id) or None if SDI unavailable.
fn extract_column_layout(
    ts: &mut crate::innodb::tablespace::Tablespace,
) -> Option<(Vec<ColumnStorageInfo>, u64)> {
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

            // If no PRIMARY found, try any index_type 2 (UNIQUE) or fall back
            // to first index
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

/// Decode records from a page using the column layout.
fn decode_page_records(
    page_data: &[u8],
    columns: &[ColumnStorageInfo],
    opts: &ExportOptions,
    _page_size: u32,
) -> Vec<Vec<(String, FieldValue)>> {
    let records = walk_compact_records(page_data);
    let mut rows = Vec::new();

    for rec in &records {
        // Filter by delete mark
        let delete_mark = rec.header.delete_mark();
        if opts.where_delete_mark && !delete_mark {
            continue;
        }
        if !opts.where_delete_mark && delete_mark {
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
            if !opts.system_columns && col.is_system_column {
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

/// Output records as CSV.
fn output_csv(
    writer: &mut dyn Write,
    pages: &[(u64, Vec<u8>)],
    columns: &[ColumnStorageInfo],
    opts: &ExportOptions,
    page_size: u32,
) -> Result<(), IdbError> {
    // Header row
    let headers: Vec<&str> = columns
        .iter()
        .filter(|c| opts.system_columns || !c.is_system_column)
        .map(|c| c.name.as_str())
        .collect();
    wprintln!(writer, "{}", headers.join(","))?;

    for (_, page_data) in pages {
        let rows = decode_page_records(page_data, columns, opts, page_size);
        for row in &rows {
            let values: Vec<String> = row.iter().map(|(_, v)| csv_escape(v)).collect();
            wprintln!(writer, "{}", values.join(","))?;
        }
    }

    Ok(())
}

/// CSV-escape a field value (RFC 4180).
fn csv_escape(val: &FieldValue) -> String {
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

/// Output records as JSON (array of objects).
fn output_json(
    writer: &mut dyn Write,
    pages: &[(u64, Vec<u8>)],
    columns: &[ColumnStorageInfo],
    opts: &ExportOptions,
    page_size: u32,
) -> Result<(), IdbError> {
    let mut all_rows: Vec<serde_json::Map<String, serde_json::Value>> = Vec::new();

    for (_, page_data) in pages {
        let rows = decode_page_records(page_data, columns, opts, page_size);
        for row in rows {
            let mut obj = serde_json::Map::new();
            for (name, val) in row {
                let json_val = match val {
                    FieldValue::Null => serde_json::Value::Null,
                    FieldValue::Int(n) => serde_json::Value::Number(n.into()),
                    FieldValue::Uint(n) => serde_json::Value::Number(n.into()),
                    FieldValue::Float(f) => serde_json::Number::from_f64(f as f64)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                    FieldValue::Double(d) => serde_json::Number::from_f64(d)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                    FieldValue::Str(s) => serde_json::Value::String(s),
                    FieldValue::Hex(h) => serde_json::Value::String(h),
                };
                obj.insert(name, json_val);
            }
            all_rows.push(obj);
        }
    }

    let json_output =
        serde_json::to_string_pretty(&all_rows).map_err(|e| IdbError::Parse(e.to_string()))?;
    wprintln!(writer, "{}", json_output)?;

    Ok(())
}

/// Output records as hex (page/offset/heap_no/delete_mark/data).
fn output_hex(
    writer: &mut dyn Write,
    pages: &[(u64, Vec<u8>)],
    opts: &ExportOptions,
) -> Result<(), IdbError> {
    wprintln!(
        writer,
        "{:<8} {:<8} {:<8} {:<6} {}",
        "PAGE",
        "OFFSET",
        "HEAP_NO",
        "DEL",
        "DATA (hex)"
    )?;

    for (page_num, page_data) in pages {
        let records = walk_compact_records(page_data);
        for rec in &records {
            let delete_mark = rec.header.delete_mark();
            if opts.where_delete_mark && !delete_mark {
                continue;
            }
            if !opts.where_delete_mark && delete_mark {
                continue;
            }

            let heap_no = rec.header.heap_no();
            // Show up to 64 bytes of record data
            let data_end = (rec.offset + 64).min(page_data.len());
            let data_hex: String = page_data[rec.offset..data_end]
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect();

            wprintln!(
                writer,
                "{:<8} {:<8} {:<8} {:<6} {}",
                page_num,
                rec.offset,
                heap_no,
                if delete_mark { "Y" } else { "N" },
                data_hex
            )?;
        }
    }

    Ok(())
}
