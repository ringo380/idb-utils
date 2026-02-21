//! CLI implementation for the `inno schema` subcommand.
//!
//! Extracts table schema from SDI metadata in MySQL 8.0+ tablespaces and
//! reconstructs human-readable `CREATE TABLE` DDL. For pre-8.0 tablespaces,
//! provides a best-effort inference from INDEX page structure.

use std::io::Write;

use crate::cli::wprintln;
use crate::innodb::schema::{self, InferredSchema, TableSchema};
use crate::innodb::sdi;
use crate::IdbError;

/// Options for the `inno schema` subcommand.
pub struct Options {
    /// Path to the InnoDB tablespace file (.ibd).
    pub file: String,
    /// Show additional structured details above the DDL.
    pub verbose: bool,
    /// Output in JSON format.
    pub json: bool,
    /// Override the auto-detected page size.
    pub page_size: Option<u32>,
    /// Path to MySQL keyring file for decrypting encrypted tablespaces.
    pub keyring: Option<String>,
    /// Use memory-mapped I/O for file access.
    pub mmap: bool,
}

/// Extract schema and reconstruct DDL from tablespace metadata.
///
/// For MySQL 8.0+ tablespaces with SDI, extracts the data dictionary JSON,
/// parses it into typed structs, and generates `CREATE TABLE` DDL. For
/// pre-8.0 tablespaces without SDI, scans INDEX pages to infer basic
/// index structure.
pub fn execute(opts: &Options, writer: &mut dyn Write) -> Result<(), IdbError> {
    let mut ts = crate::cli::open_tablespace(&opts.file, opts.page_size, opts.mmap)?;

    if let Some(ref keyring_path) = opts.keyring {
        crate::cli::setup_decryption(&mut ts, keyring_path)?;
    }

    // MariaDB does not use SDI
    if ts.vendor_info().vendor == crate::innodb::vendor::InnoDbVendor::MariaDB {
        if opts.json {
            let inferred = schema::infer_schema_from_pages(&mut ts)?;
            wprintln!(writer, "{}", serde_json::to_string_pretty(&inferred)
                .map_err(|e| IdbError::Parse(e.to_string()))?)?;
        } else {
            let inferred = schema::infer_schema_from_pages(&mut ts)?;
            print_inferred_text(writer, &inferred)?;
        }
        return Ok(());
    }

    // Try SDI extraction
    let sdi_pages = sdi::find_sdi_pages(&mut ts)?;

    if sdi_pages.is_empty() {
        // Pre-8.0 fallback
        let inferred = schema::infer_schema_from_pages(&mut ts)?;
        if opts.json {
            wprintln!(writer, "{}", serde_json::to_string_pretty(&inferred)
                .map_err(|e| IdbError::Parse(e.to_string()))?)?;
        } else {
            print_inferred_text(writer, &inferred)?;
        }
        return Ok(());
    }

    // Extract SDI records
    let records = sdi::extract_sdi_from_pages(&mut ts, &sdi_pages)?;

    // Filter for Table records (sdi_type == 1)
    let table_records: Vec<_> = records.iter().filter(|r| r.sdi_type == 1).collect();

    if table_records.is_empty() {
        wprintln!(writer, "No table SDI records found in {}.", opts.file)?;
        return Ok(());
    }

    for rec in &table_records {
        let table_schema = schema::extract_schema_from_sdi(&rec.data)?;

        if opts.json {
            wprintln!(writer, "{}", serde_json::to_string_pretty(&table_schema)
                .map_err(|e| IdbError::Parse(e.to_string()))?)?;
        } else if opts.verbose {
            print_verbose_text(writer, &table_schema)?;
        } else {
            print_default_text(writer, &table_schema)?;
        }
    }

    Ok(())
}

/// Print default text output: comment header + DDL.
fn print_default_text(writer: &mut dyn Write, schema: &TableSchema) -> Result<(), IdbError> {
    // Header comment
    if let Some(ref db) = schema.schema_name {
        wprintln!(writer, "-- Table: `{}`.`{}`", db, schema.table_name)?;
    } else {
        wprintln!(writer, "-- Table: `{}`", schema.table_name)?;
    }

    if let Some(ref ver) = schema.mysql_version {
        wprintln!(writer, "-- Source: SDI (MySQL {})", ver)?;
    } else {
        wprintln!(writer, "-- Source: SDI")?;
    }

    wprintln!(writer)?;
    wprintln!(writer, "{}", schema.ddl)?;

    Ok(())
}

/// Print verbose text output: structured breakdown + DDL.
fn print_verbose_text(writer: &mut dyn Write, schema: &TableSchema) -> Result<(), IdbError> {
    if let Some(ref db) = schema.schema_name {
        wprintln!(writer, "Schema:  {}", db)?;
    }
    wprintln!(writer, "Table:   {}", schema.table_name)?;
    wprintln!(writer, "Engine:  {}", schema.engine)?;
    if let Some(ref fmt) = schema.row_format {
        wprintln!(writer, "Format:  {}", fmt)?;
    }
    if let Some(ref ver) = schema.mysql_version {
        wprintln!(writer, "Source:  SDI (MySQL {})", ver)?;
    }
    if let Some(ref coll) = schema.collation {
        wprintln!(writer, "Collation: {}", coll)?;
    }
    if let Some(ref cs) = schema.charset {
        wprintln!(writer, "Charset: {}", cs)?;
    }
    if let Some(ref comment) = schema.comment {
        wprintln!(writer, "Comment: {}", comment)?;
    }

    // Columns
    wprintln!(writer)?;
    wprintln!(writer, "Columns ({}):", schema.columns.len())?;
    for (i, col) in schema.columns.iter().enumerate() {
        let mut parts = vec![format!("  {}. {:<16} {:<20}", i + 1, col.name, col.column_type)];
        if !col.is_nullable {
            parts.push("NOT NULL".to_string());
        }
        if col.is_auto_increment {
            parts.push("AUTO_INCREMENT".to_string());
        }
        if let Some(ref expr) = col.generation_expression {
            let kind = if col.is_virtual == Some(true) { "VIRTUAL" } else { "STORED" };
            parts.push(format!("AS ({}) {}", expr, kind));
        }
        if col.is_invisible {
            parts.push("INVISIBLE".to_string());
        }
        wprintln!(writer, "{}", parts.join("  "))?;
    }

    // Indexes
    if !schema.indexes.is_empty() {
        wprintln!(writer)?;
        wprintln!(writer, "Indexes ({}):", schema.indexes.len())?;
        for idx in &schema.indexes {
            let cols: Vec<String> = idx.columns.iter().map(|c| {
                let mut s = c.name.clone();
                if let Some(len) = c.prefix_length {
                    s.push_str(&format!("({})", len));
                }
                if let Some(ref ord) = c.order {
                    s.push(' ');
                    s.push_str(ord);
                }
                s
            }).collect();
            wprintln!(writer, "  {} ({})", idx.index_type, cols.join(", "))?;
        }
    }

    // Foreign keys
    if !schema.foreign_keys.is_empty() {
        wprintln!(writer)?;
        wprintln!(writer, "Foreign Keys ({}):", schema.foreign_keys.len())?;
        for fk in &schema.foreign_keys {
            wprintln!(
                writer,
                "  {} ({}) -> {} ({})",
                fk.name,
                fk.columns.join(", "),
                fk.referenced_table,
                fk.referenced_columns.join(", ")
            )?;
        }
    }

    // DDL
    wprintln!(writer)?;
    wprintln!(writer, "DDL:")?;
    wprintln!(writer, "{}", schema.ddl)?;

    Ok(())
}

/// Print inferred schema (pre-8.0 / no SDI).
fn print_inferred_text(writer: &mut dyn Write, inferred: &InferredSchema) -> Result<(), IdbError> {
    wprintln!(writer, "-- Source: {}", inferred.source)?;
    wprintln!(
        writer,
        "-- Note: Column names and types cannot be determined without SDI."
    )?;
    wprintln!(writer)?;
    wprintln!(writer, "Record format: {}", inferred.record_format)?;
    wprintln!(writer, "Indexes detected: {}", inferred.indexes.len())?;
    for idx in &inferred.indexes {
        let levels = if idx.max_level > 0 {
            format!(", {} non-leaf level(s)", idx.max_level)
        } else {
            String::new()
        };
        wprintln!(
            writer,
            "  Index ID {}: {} leaf page(s){}",
            idx.index_id,
            idx.leaf_pages,
            levels
        )?;
    }

    Ok(())
}
