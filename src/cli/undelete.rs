//! CLI implementation for the `inno undelete` subcommand.
//!
//! Recovers deleted records from InnoDB tablespaces using three strategies:
//! delete-marked records, free-list records, and (optionally) undo log records.
//! Supports CSV, JSON, SQL, and metadata JSON output formats.

use std::io::Write;

use crate::cli::wprintln;
use crate::innodb::export::csv_escape;
use crate::innodb::undelete::{
    field_value_to_json, field_value_to_sql, scan_undeleted, RecoverySource, UndeleteScanResult,
};
use crate::IdbError;

/// Output format for undeleted records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UndeleteFormat {
    Csv,
    Json,
    Sql,
    Hex,
}

impl UndeleteFormat {
    fn from_str(s: &str) -> Result<Self, IdbError> {
        match s.to_lowercase().as_str() {
            "csv" => Ok(UndeleteFormat::Csv),
            "json" => Ok(UndeleteFormat::Json),
            "sql" => Ok(UndeleteFormat::Sql),
            "hex" => Ok(UndeleteFormat::Hex),
            _ => Err(IdbError::Argument(format!(
                "Unknown format '{}'. Use csv, json, sql, or hex.",
                s
            ))),
        }
    }
}

/// Options for the `inno undelete` subcommand.
pub struct UndeleteOptions {
    /// Path to the InnoDB tablespace file (.ibd).
    pub file: String,
    /// Path to an undo tablespace (ibdata1 or .ibu) for undo log scanning.
    pub undo_file: Option<String>,
    /// Filter by table name.
    pub table: Option<String>,
    /// Minimum transaction ID to include.
    pub min_trx_id: Option<u64>,
    /// Minimum confidence threshold (0.0–1.0).
    pub confidence: f64,
    /// Record output format: csv, json, sql, hex.
    pub format: String,
    /// Output full metadata JSON envelope (overrides format).
    pub json: bool,
    /// Show additional detail.
    pub verbose: bool,
    /// Recover from a specific page only.
    pub page: Option<u64>,
    /// Override page size.
    pub page_size: Option<u32>,
    /// Path to MySQL keyring file.
    pub keyring: Option<String>,
    /// Use memory-mapped I/O.
    pub mmap: bool,
}

/// Execute the undelete subcommand.
pub fn execute(opts: &UndeleteOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    let format = UndeleteFormat::from_str(&opts.format)?;

    let mut ts = crate::cli::open_tablespace(&opts.file, opts.page_size, opts.mmap)?;
    if let Some(ref keyring_path) = opts.keyring {
        crate::cli::setup_decryption(&mut ts, keyring_path)?;
    }

    // Open undo tablespace if provided
    let mut undo_ts_opt = match &opts.undo_file {
        Some(path) => Some(crate::cli::open_tablespace(
            path,
            opts.page_size,
            opts.mmap,
        )?),
        None => None,
    };

    let result = scan_undeleted(
        &mut ts,
        undo_ts_opt.as_mut(),
        opts.confidence,
        opts.min_trx_id,
        opts.page,
    )?;

    // Filter by table name if requested
    if let Some(ref filter_table) = opts.table {
        if let Some(ref table_name) = result.table_name {
            if !table_name.eq_ignore_ascii_case(filter_table) {
                return Err(IdbError::Argument(format!(
                    "Table name '{}' does not match filter '{}'",
                    table_name, filter_table
                )));
            }
        }
    }

    if opts.verbose && !opts.json {
        eprintln!(
            "Recovered {} records ({} delete-marked, {} free-list, {} undo-log)",
            result.summary.total,
            result.summary.delete_marked,
            result.summary.free_list,
            result.summary.undo_log,
        );
    }

    if opts.json {
        // Full metadata JSON envelope
        let json =
            serde_json::to_string_pretty(&result).map_err(|e| IdbError::Parse(e.to_string()))?;
        wprintln!(writer, "{}", json)?;
    } else {
        match format {
            UndeleteFormat::Csv => output_csv(writer, &result)?,
            UndeleteFormat::Json => output_json(writer, &result)?,
            UndeleteFormat::Sql => output_sql(writer, &result)?,
            UndeleteFormat::Hex => output_hex(writer, &result)?,
        }
    }

    Ok(())
}

/// Output records as CSV.
fn output_csv(writer: &mut dyn Write, result: &UndeleteScanResult) -> Result<(), IdbError> {
    // Header: _source,_confidence,_trx_id,_page,<column names>
    let mut headers = vec![
        "_source".to_string(),
        "_confidence".to_string(),
        "_trx_id".to_string(),
        "_page".to_string(),
    ];
    headers.extend(result.column_names.clone());
    wprintln!(writer, "{}", headers.join(","))?;

    for rec in &result.records {
        let source_str = match rec.source {
            RecoverySource::DeleteMarked => "delete_marked",
            RecoverySource::FreeList => "free_list",
            RecoverySource::UndoLog => "undo_log",
        };
        let mut values = vec![
            source_str.to_string(),
            format!("{:.2}", rec.confidence),
            rec.trx_id.map_or(String::new(), |t| t.to_string()),
            rec.page_number.to_string(),
        ];

        for (_, val) in &rec.columns {
            values.push(csv_escape(val));
        }

        wprintln!(writer, "{}", values.join(","))?;
    }

    Ok(())
}

/// Output records as JSON array.
fn output_json(writer: &mut dyn Write, result: &UndeleteScanResult) -> Result<(), IdbError> {
    let mut json_records: Vec<serde_json::Value> = Vec::new();

    for rec in &result.records {
        let mut obj = serde_json::Map::new();
        obj.insert(
            "source".to_string(),
            serde_json::json!(match rec.source {
                RecoverySource::DeleteMarked => "delete_marked",
                RecoverySource::FreeList => "free_list",
                RecoverySource::UndoLog => "undo_log",
            }),
        );
        obj.insert("confidence".to_string(), serde_json::json!(rec.confidence));
        if let Some(trx) = rec.trx_id {
            obj.insert("trx_id".to_string(), serde_json::json!(trx));
        }
        obj.insert("page".to_string(), serde_json::json!(rec.page_number));

        let mut cols = serde_json::Map::new();
        for (name, val) in &rec.columns {
            cols.insert(name.clone(), field_value_to_json(val));
        }
        obj.insert("columns".to_string(), serde_json::Value::Object(cols));

        json_records.push(serde_json::Value::Object(obj));
    }

    let output =
        serde_json::to_string_pretty(&json_records).map_err(|e| IdbError::Parse(e.to_string()))?;
    wprintln!(writer, "{}", output)?;

    Ok(())
}

/// Output records as SQL INSERT statements.
fn output_sql(writer: &mut dyn Write, result: &UndeleteScanResult) -> Result<(), IdbError> {
    let table_name = result.table_name.as_deref().unwrap_or("unknown_table");

    let col_names = if !result.column_names.is_empty() {
        result.column_names.join(", ")
    } else if let Some(first_rec) = result.records.first() {
        first_rec
            .columns
            .iter()
            .map(|(n, _)| n.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    } else {
        return Ok(());
    };

    for rec in &result.records {
        let source_str = match rec.source {
            RecoverySource::DeleteMarked => "delete_marked",
            RecoverySource::FreeList => "free_list",
            RecoverySource::UndoLog => "undo_log",
        };
        wprintln!(
            writer,
            "-- source: {}, confidence: {:.2}, page: {}",
            source_str,
            rec.confidence,
            rec.page_number
        )?;

        let values: Vec<String> = rec
            .columns
            .iter()
            .map(|(_, val)| field_value_to_sql(val))
            .collect();

        wprintln!(
            writer,
            "INSERT INTO {} ({}) VALUES ({});",
            table_name,
            col_names,
            values.join(", ")
        )?;
    }

    Ok(())
}

/// Output records as hex dump.
fn output_hex(writer: &mut dyn Write, result: &UndeleteScanResult) -> Result<(), IdbError> {
    wprintln!(
        writer,
        "{:<12} {:<8} {:<8} {:<6} {}",
        "SOURCE",
        "CONF",
        "PAGE",
        "OFFSET",
        "DATA (hex)"
    )?;

    for rec in &result.records {
        let source_str = match rec.source {
            RecoverySource::DeleteMarked => "delete_mark",
            RecoverySource::FreeList => "free_list",
            RecoverySource::UndoLog => "undo_log",
        };

        let hex = rec.raw_hex.as_deref().unwrap_or("");

        wprintln!(
            writer,
            "{:<12} {:<8.2} {:<8} {:<6} {}",
            source_str,
            rec.confidence,
            rec.page_number,
            rec.offset,
            hex
        )?;
    }

    Ok(())
}
