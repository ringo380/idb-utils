//! CLI implementation for the `inno validate` subcommand.
//!
//! Cross-validates on-disk tablespace files against live MySQL metadata.
//! Requires the `mysql` feature for actual MySQL queries; without it,
//! provides a helpful error message.

use std::io::Write;

#[cfg(feature = "mysql")]
use colored::Colorize;
use serde::Serialize;

use crate::cli::wprintln;
use crate::IdbError;

/// Options for the `inno validate` subcommand.
pub struct ValidateOptions {
    /// Path to MySQL data directory.
    pub datadir: String,
    /// MySQL database name to filter.
    pub database: Option<String>,
    /// Deep-validate a specific table (format: db.table or db/table).
    pub table: Option<String>,
    /// MySQL host for live queries.
    pub host: Option<String>,
    /// MySQL port for live queries.
    pub port: Option<u16>,
    /// MySQL user for live queries.
    pub user: Option<String>,
    /// MySQL password for live queries.
    pub password: Option<String>,
    /// Path to MySQL defaults file.
    pub defaults_file: Option<String>,
    /// Emit output as JSON.
    pub json: bool,
    /// Show detailed output.
    pub verbose: bool,
    /// Override page size.
    pub page_size: Option<u32>,
    /// Maximum directory recursion depth.
    pub depth: Option<u32>,
    /// Use memory-mapped I/O.
    pub mmap: bool,
}

/// JSON output for disk-only mode (without MySQL).
#[derive(Debug, Serialize)]
struct DiskScanReport {
    files_scanned: usize,
    tablespaces: Vec<DiskTablespace>,
}

#[derive(Debug, Serialize)]
struct DiskTablespace {
    file: String,
    space_id: u32,
}

/// Execute the validate subcommand.
pub fn execute(opts: &ValidateOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    // Handle --table mode (requires MySQL)
    #[cfg(feature = "mysql")]
    {
        if opts.table.is_some() {
            if opts.host.is_none() && opts.user.is_none() && opts.defaults_file.is_none() {
                return Err(IdbError::Argument(
                    "--table requires MySQL connection options (--host, --user, or --defaults-file)"
                        .to_string(),
                ));
            }
            return execute_table_validate(opts, writer);
        }
    }

    #[cfg(not(feature = "mysql"))]
    {
        if opts.table.is_some() {
            return Err(IdbError::Argument(
                "MySQL support not compiled. Rebuild with: cargo build --features mysql"
                    .to_string(),
            ));
        }
    }

    // Scan disk files
    let files = crate::util::fs::find_tablespace_files(
        std::path::Path::new(&opts.datadir),
        &["ibd"],
        opts.depth,
    )?;

    let mut disk_entries: Vec<(std::path::PathBuf, u32)> = Vec::new();

    for file_path in &files {
        let path_str = file_path.to_string_lossy().to_string();
        match crate::cli::open_tablespace(&path_str, opts.page_size, opts.mmap) {
            Ok(ts) => {
                let space_id = ts.fsp_header().map(|h| h.space_id).unwrap_or(0);
                disk_entries.push((file_path.clone(), space_id));
            }
            Err(e) => {
                if opts.verbose {
                    eprintln!("Warning: skipping {}: {}", path_str, e);
                }
            }
        }
    }

    #[cfg(feature = "mysql")]
    {
        if opts.host.is_some() || opts.user.is_some() || opts.defaults_file.is_some() {
            return execute_mysql_validate(opts, &disk_entries, writer);
        }
    }

    #[cfg(not(feature = "mysql"))]
    {
        if opts.host.is_some() || opts.user.is_some() || opts.defaults_file.is_some() {
            return Err(IdbError::Argument(
                "MySQL support not compiled. Rebuild with: cargo build --features mysql"
                    .to_string(),
            ));
        }
    }

    // Disk-only mode: just list what we found
    if opts.json {
        let report = DiskScanReport {
            files_scanned: disk_entries.len(),
            tablespaces: disk_entries
                .iter()
                .map(|(p, sid)| DiskTablespace {
                    file: p.to_string_lossy().to_string(),
                    space_id: *sid,
                })
                .collect(),
        };
        let json = serde_json::to_string_pretty(&report)
            .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
        wprintln!(writer, "{}", json)?;
    } else {
        wprintln!(
            writer,
            "Disk scan: {} tablespace files found",
            disk_entries.len()
        )?;
        wprintln!(writer)?;
        wprintln!(writer, "  {:<60} {:>12}", "File", "Space ID")?;
        wprintln!(writer, "  {}", "-".repeat(74))?;
        for (path, space_id) in &disk_entries {
            wprintln!(writer, "  {:<60} {:>12}", path.to_string_lossy(), space_id)?;
        }
        wprintln!(writer)?;
        wprintln!(
            writer,
            "  Provide MySQL connection options (--host, --user) to cross-validate against live MySQL."
        )?;
    }

    Ok(())
}

/// Execute MySQL cross-validation (mysql feature only).
#[cfg(feature = "mysql")]
fn execute_mysql_validate(
    opts: &ValidateOptions,
    disk_entries: &[(std::path::PathBuf, u32)],
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    use crate::innodb::validate::{cross_validate, TablespaceMapping};

    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| IdbError::Io(format!("Failed to create async runtime: {}", e)))?;

    let mysql_mappings = rt.block_on(async {
        let pool = crate::util::mysql::connect_mysql(
            opts.host.as_deref(),
            opts.port,
            opts.user.as_deref(),
            opts.password.as_deref(),
            opts.defaults_file.as_deref(),
        )
        .await?;

        let mut query = "SELECT NAME, SPACE, ROW_FORMAT FROM INFORMATION_SCHEMA.INNODB_TABLESPACES WHERE SPACE_TYPE = 'Single'".to_string();
        if let Some(ref db) = opts.database {
            query.push_str(&format!(" AND NAME LIKE '{db}/%'"));
        }

        use mysql_async::prelude::*;
        let rows: Vec<(String, u32, String)> = pool
            .get_conn()
            .await
            .map_err(|e| IdbError::Io(format!("MySQL connection error: {}", e)))?
            .query(query)
            .await
            .map_err(|e| IdbError::Io(format!("MySQL query error: {}", e)))?;

        let mappings: Vec<TablespaceMapping> = rows
            .into_iter()
            .map(|(name, space_id, row_format)| TablespaceMapping {
                name,
                space_id,
                row_format: Some(row_format),
            })
            .collect();

        Ok::<_, IdbError>(mappings)
    })?;

    let report = cross_validate(disk_entries, &mysql_mappings);
    output_validation_report(&report, opts.json, opts.verbose, writer)
}

#[cfg(feature = "mysql")]
fn output_validation_report(
    report: &ValidationReport,
    json: bool,
    verbose: bool,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    if json {
        let json_str = serde_json::to_string_pretty(report)
            .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
        wprintln!(writer, "{}", json_str)?;
    } else {
        wprintln!(writer, "Cross-Validation Report")?;
        wprintln!(writer, "  Disk files:         {}", report.disk_files)?;
        wprintln!(writer, "  MySQL tablespaces:  {}", report.mysql_tablespaces)?;
        wprintln!(writer)?;

        if !report.orphans.is_empty() {
            wprintln!(
                writer,
                "  {} (on disk, not in MySQL):",
                "Orphan files".yellow()
            )?;
            for o in &report.orphans {
                wprintln!(writer, "    {} (space_id={})", o.path, o.space_id)?;
            }
            wprintln!(writer)?;
        }

        if !report.missing.is_empty() {
            wprintln!(
                writer,
                "  {} (in MySQL, not on disk):",
                "Missing files".red()
            )?;
            for m in &report.missing {
                wprintln!(writer, "    {} (space_id={})", m.name, m.space_id)?;
            }
            wprintln!(writer)?;
        }

        if !report.mismatches.is_empty() {
            wprintln!(writer, "  {} :", "Space ID mismatches".red())?;
            for m in &report.mismatches {
                wprintln!(
                    writer,
                    "    {} : disk={}, mysql={} ({})",
                    m.path,
                    m.disk_space_id,
                    m.mysql_space_id,
                    m.mysql_name
                )?;
            }
            wprintln!(writer)?;
        }

        if verbose && report.passed {
            wprintln!(
                writer,
                "  All {} files match MySQL metadata.",
                report.disk_files
            )?;
        }

        let status = if report.passed {
            "PASS".green().to_string()
        } else {
            "FAIL".red().to_string()
        };
        wprintln!(writer, "  Overall: {}", status)?;
    }

    if !report.passed {
        return Err(IdbError::Argument("Validation failed".to_string()));
    }

    Ok(())
}

/// Deep table validation via MySQL (feature-gated).
#[cfg(feature = "mysql")]
fn execute_table_validate(opts: &ValidateOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    use colored::Colorize;
    use mysql_async::prelude::*;

    use crate::innodb::validate::{
        deep_validate_table, MysqlIndexInfo, TableValidationReport, TablespaceMapping,
    };

    let table_spec = opts.table.as_ref().unwrap();

    // Parse table_name: accept "db.table" or "db/table"
    let table_name = if table_spec.contains('.') {
        table_spec.replacen('.', "/", 1)
    } else if table_spec.contains('/') {
        table_spec.to_string()
    } else {
        return Err(IdbError::Argument(format!(
            "Invalid table format '{}'. Use db.table or db/table",
            table_spec
        )));
    };

    // Build MySQL config
    let mut config = crate::util::mysql::MysqlConfig::default();

    if let Some(ref df) = opts.defaults_file {
        if let Some(parsed) = crate::util::mysql::parse_defaults_file(std::path::Path::new(df)) {
            config = parsed;
        }
    } else if let Some(df) = crate::util::mysql::find_defaults_file() {
        if let Some(parsed) = crate::util::mysql::parse_defaults_file(&df) {
            config = parsed;
        }
    }

    if let Some(ref h) = opts.host {
        config.host = h.clone();
    }
    if let Some(p) = opts.port {
        config.port = p;
    }
    if let Some(ref u) = opts.user {
        config.user = u.clone();
    }
    if opts.password.is_some() {
        config.password = opts.password.clone();
    }

    let datadir_path = std::path::Path::new(&opts.datadir);
    if !datadir_path.is_dir() {
        return Err(IdbError::Argument(format!(
            "Data directory does not exist: {}",
            opts.datadir
        )));
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| IdbError::Io(format!("Cannot create async runtime: {}", e)))?;

    rt.block_on(async {
        let pool = mysql_async::Pool::new(config.to_opts());
        let mut conn = pool
            .get_conn()
            .await
            .map_err(|e| IdbError::Io(format!("MySQL connection failed: {}", e)))?;

        // Query tablespace mapping
        let ts_query = format!(
            "SELECT NAME, SPACE, ROW_FORMAT FROM INFORMATION_SCHEMA.INNODB_TABLESPACES WHERE NAME = '{}'",
            table_name
        );
        let ts_rows: Vec<(String, u32, String)> =
            conn.query(&ts_query).await.unwrap_or_default();

        if ts_rows.is_empty() {
            pool.disconnect().await.ok();
            return Err(IdbError::Argument(format!(
                "Table '{}' not found in INFORMATION_SCHEMA.INNODB_TABLESPACES",
                table_name
            )));
        }

        let (name, space_id, row_format) = &ts_rows[0];
        let mapping = TablespaceMapping {
            name: name.clone(),
            space_id: *space_id,
            row_format: Some(row_format.clone()),
        };

        // Query index info
        let idx_query = format!(
            "SELECT I.NAME, I.TABLE_ID, I.SPACE, I.PAGE_NO \
             FROM INFORMATION_SCHEMA.INNODB_INDEXES I \
             JOIN INFORMATION_SCHEMA.INNODB_TABLES T ON I.TABLE_ID = T.TABLE_ID \
             WHERE T.NAME = '{}'",
            table_name
        );
        let idx_rows: Vec<(String, u64, u32, u64)> =
            conn.query(&idx_query).await.unwrap_or_default();

        let indexes: Vec<MysqlIndexInfo> = idx_rows
            .into_iter()
            .map(|(name, table_id, space_id, page_no)| MysqlIndexInfo {
                name,
                table_id,
                space_id,
                page_no: Some(page_no),
            })
            .collect();

        pool.disconnect().await.ok();

        // Run deep validation
        let report = deep_validate_table(
            datadir_path,
            &table_name,
            &mapping,
            &indexes,
            opts.page_size,
            opts.mmap,
        );

        // Output results
        if opts.json {
            output_table_json(&report, writer)?;
        } else {
            output_table_text(&report, writer, opts.verbose)?;
        }

        Ok(())
    })
}

#[cfg(feature = "mysql")]
fn output_table_json(
    report: &crate::innodb::validate::TableValidationReport,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    let json = serde_json::to_string_pretty(report)
        .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
    wprintln!(writer, "{}", json)?;
    Ok(())
}

#[cfg(feature = "mysql")]
fn output_table_text(
    report: &crate::innodb::validate::TableValidationReport,
    writer: &mut dyn Write,
    verbose: bool,
) -> Result<(), IdbError> {
    use colored::Colorize;

    wprintln!(
        writer,
        "{}",
        format!("Table Validation: {}", report.table_name).bold()
    )?;
    wprintln!(writer)?;

    if let Some(ref path) = report.file_path {
        wprintln!(writer, "  File:           {}", path)?;
    } else {
        wprintln!(writer, "  File:           {}", "NOT FOUND".red())?;
    }

    wprintln!(writer, "  MySQL Space ID: {}", report.mysql_space_id)?;
    if let Some(disk_id) = report.disk_space_id {
        wprintln!(writer, "  Disk Space ID:  {}", disk_id)?;
    } else {
        wprintln!(writer, "  Disk Space ID:  {}", "N/A".dimmed())?;
    }

    if report.space_id_match {
        wprintln!(writer, "  Space ID Match: {}", "YES".green())?;
    } else {
        wprintln!(writer, "  Space ID Match: {}", "NO".red())?;
    }

    if let Some(ref fmt) = report.mysql_row_format {
        wprintln!(writer, "  Row Format:     {}", fmt)?;
    }

    wprintln!(writer)?;
    wprintln!(
        writer,
        "  Indexes Verified: {}/{}",
        report.indexes_verified,
        report.indexes.len()
    )?;

    if verbose || !report.passed {
        for idx in &report.indexes {
            let status = if idx.root_page_valid {
                "OK".green().to_string()
            } else {
                "FAIL".red().to_string()
            };

            let root = idx
                .root_page
                .map(|p| p.to_string())
                .unwrap_or_else(|| "N/A".to_string());

            wprintln!(writer, "    {} (root_page={}) [{}]", idx.name, root, status)?;

            if let Some(ref msg) = idx.message {
                wprintln!(writer, "      {}", msg)?;
            }
        }
    }

    wprintln!(writer)?;
    if report.passed {
        wprintln!(writer, "  Result: {}", "PASSED".green().bold())?;
    } else {
        wprintln!(writer, "  Result: {}", "FAILED".red().bold())?;
    }

    Ok(())
}
