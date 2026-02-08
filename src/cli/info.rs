use std::io::Write;

use byteorder::{BigEndian, ByteOrder};
use colored::Colorize;
use serde::Serialize;

use crate::cli::wprintln;
use crate::innodb::constants::*;
use crate::innodb::page::FilHeader;
use crate::IdbError;

/// Options for the `inno info` subcommand.
pub struct InfoOptions {
    /// Inspect the `ibdata1` page 0 header.
    pub ibdata: bool,
    /// Compare `ibdata1` and redo log checkpoint LSNs.
    pub lsn_check: bool,
    /// MySQL data directory path (defaults to `/var/lib/mysql`).
    pub datadir: Option<String>,
    /// Database name (for MySQL table/index queries, requires `mysql` feature).
    pub database: Option<String>,
    /// Table name (for MySQL table/index queries, requires `mysql` feature).
    pub table: Option<String>,
    /// MySQL host for live queries.
    pub host: Option<String>,
    /// MySQL port for live queries.
    pub port: Option<u16>,
    /// MySQL user for live queries.
    pub user: Option<String>,
    /// MySQL password for live queries.
    pub password: Option<String>,
    /// Path to a MySQL defaults file (`.my.cnf`).
    pub defaults_file: Option<String>,
    /// Emit output as JSON.
    pub json: bool,
    /// Override the auto-detected page size.
    pub page_size: Option<u32>,
}

#[derive(Serialize)]
struct IbdataInfoJson {
    ibdata_file: String,
    page_checksum: u32,
    page_number: u32,
    page_type: u16,
    lsn: u64,
    flush_lsn: u64,
    space_id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    redo_checkpoint_1_lsn: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    redo_checkpoint_2_lsn: Option<u64>,
}

#[derive(Serialize)]
struct LsnCheckJson {
    ibdata_lsn: u64,
    redo_checkpoint_lsn: u64,
    in_sync: bool,
}

/// Display InnoDB system-level information from the data directory or a live instance.
///
/// Operates in three mutually exclusive modes:
///
/// - **`--ibdata`**: Reads page 0 of `ibdata1` (the system tablespace) and
///   decodes its FIL header — checksum, page type, LSN, flush LSN, and space ID.
///   Also attempts to read checkpoint LSNs from the redo log, trying the
///   MySQL 8.0.30+ `#innodb_redo/#ib_redo*` directory first, then falling back
///   to the legacy `ib_logfile0`. This gives a quick snapshot of the system
///   tablespace state without starting MySQL.
///
/// - **`--lsn-check`**: Compares the LSN from the `ibdata1` page 0 header with
///   the latest redo log checkpoint LSN. If they match, the system is "in sync";
///   if not, the difference in bytes is reported. This is useful for diagnosing
///   whether InnoDB shut down cleanly or needs crash recovery.
///
/// - **`-D <database> -t <table>`** (requires the `mysql` feature): Connects to
///   a live MySQL instance and queries `INFORMATION_SCHEMA.INNODB_TABLES` and
///   `INNODB_INDEXES` for the space ID, table ID, index names, and root page
///   numbers. Also parses `SHOW ENGINE INNODB STATUS` for the current log
///   sequence number and transaction ID counter. Connection parameters come
///   from CLI flags or a `.my.cnf` defaults file.
pub fn execute(opts: &InfoOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    if opts.ibdata || opts.lsn_check {
        let datadir = opts.datadir.as_deref().unwrap_or("/var/lib/mysql");
        let datadir_path = std::path::Path::new(datadir);

        if !datadir_path.is_dir() {
            return Err(IdbError::Argument(format!(
                "Data directory does not exist: {}",
                datadir
            )));
        }

        if opts.ibdata {
            return execute_ibdata(opts, datadir_path, writer);
        }
        if opts.lsn_check {
            return execute_lsn_check(opts, datadir_path, writer);
        }
    }

    #[cfg(feature = "mysql")]
    {
        if opts.database.is_some() || opts.table.is_some() {
            return execute_table_info(opts, writer);
        }
    }

    #[cfg(not(feature = "mysql"))]
    {
        if opts.database.is_some() || opts.table.is_some() {
            return Err(IdbError::Argument(
                "MySQL support not compiled. Rebuild with: cargo build --features mysql"
                    .to_string(),
            ));
        }
    }

    // No mode specified, show help
    wprintln!(writer, "Usage:")?;
    wprintln!(
        writer,
        "  idb info --ibdata -d <datadir>          Read ibdata1 page 0 header"
    )?;
    wprintln!(
        writer,
        "  idb info --lsn-check -d <datadir>       Compare ibdata1 and redo log LSNs"
    )?;
    wprintln!(writer, "  idb info -D <database> -t <table>       Show table/index info (requires --features mysql)")?;
    Ok(())
}

fn execute_ibdata(
    opts: &InfoOptions,
    datadir: &std::path::Path,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    let ibdata_path = datadir.join("ibdata1");
    if !ibdata_path.exists() {
        return Err(IdbError::Io(format!(
            "ibdata1 not found in {}",
            datadir.display()
        )));
    }

    // Read page 0 of ibdata1
    let page0 = read_file_bytes(&ibdata_path, 0, SIZE_PAGE_DEFAULT as usize)?;
    let header = FilHeader::parse(&page0)
        .ok_or_else(|| IdbError::Parse("Cannot parse ibdata1 page 0 FIL header".to_string()))?;

    // Try to read redo log checkpoint LSNs
    let (cp1_lsn, cp2_lsn) = read_redo_checkpoint_lsns(datadir);

    if opts.json {
        let info = IbdataInfoJson {
            ibdata_file: ibdata_path.display().to_string(),
            page_checksum: header.checksum,
            page_number: header.page_number,
            page_type: header.page_type.as_u16(),
            lsn: header.lsn,
            flush_lsn: header.flush_lsn,
            space_id: header.space_id,
            redo_checkpoint_1_lsn: cp1_lsn,
            redo_checkpoint_2_lsn: cp2_lsn,
        };
        let json = serde_json::to_string_pretty(&info)
            .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
        wprintln!(writer, "{}", json)?;
        return Ok(());
    }

    wprintln!(writer, "{}", "ibdata1 Page 0 Header".bold())?;
    wprintln!(writer, "  File:       {}", ibdata_path.display())?;
    wprintln!(writer, "  Checksum:   {}", header.checksum)?;
    wprintln!(writer, "  Page No:    {}", header.page_number)?;
    wprintln!(
        writer,
        "  Page Type:  {} ({})",
        header.page_type.as_u16(),
        header.page_type.name()
    )?;
    wprintln!(writer, "  LSN:        {}", header.lsn)?;
    wprintln!(writer, "  Flush LSN:  {}", header.flush_lsn)?;
    wprintln!(writer, "  Space ID:   {}", header.space_id)?;
    wprintln!(writer)?;

    if let Some(lsn) = cp1_lsn {
        wprintln!(writer, "Redo Log Checkpoint 1 LSN: {}", lsn)?;
    }
    if let Some(lsn) = cp2_lsn {
        wprintln!(writer, "Redo Log Checkpoint 2 LSN: {}", lsn)?;
    }

    Ok(())
}

fn execute_lsn_check(
    opts: &InfoOptions,
    datadir: &std::path::Path,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    let ibdata_path = datadir.join("ibdata1");
    if !ibdata_path.exists() {
        return Err(IdbError::Io(format!(
            "ibdata1 not found in {}",
            datadir.display()
        )));
    }

    // Read ibdata1 LSN from page 0 header (offset 16, 8 bytes)
    let page0 = read_file_bytes(&ibdata_path, 0, SIZE_PAGE_DEFAULT as usize)?;
    let ibdata_lsn = BigEndian::read_u64(&page0[FIL_PAGE_LSN..]);

    // Read redo log checkpoint LSN
    let (cp1_lsn, _cp2_lsn) = read_redo_checkpoint_lsns(datadir);

    let redo_lsn = cp1_lsn.unwrap_or(0);
    let in_sync = ibdata_lsn == redo_lsn;

    if opts.json {
        let check = LsnCheckJson {
            ibdata_lsn,
            redo_checkpoint_lsn: redo_lsn,
            in_sync,
        };
        let json = serde_json::to_string_pretty(&check)
            .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
        wprintln!(writer, "{}", json)?;
        return Ok(());
    }

    wprintln!(writer, "{}", "LSN Sync Check".bold())?;
    wprintln!(writer, "  ibdata1 LSN:          {}", ibdata_lsn)?;
    wprintln!(writer, "  Redo checkpoint LSN:  {}", redo_lsn)?;

    if in_sync {
        wprintln!(writer, "  Status: {}", "IN SYNC".green())?;
    } else {
        wprintln!(writer, "  Status: {}", "OUT OF SYNC".red())?;
        wprintln!(
            writer,
            "  Difference: {} bytes",
            ibdata_lsn.abs_diff(redo_lsn)
        )?;
    }

    Ok(())
}

/// Read checkpoint LSNs from redo log files.
///
/// Tries MySQL 8.0+ (#innodb_redo/#ib_redo*) format first, falls back to
/// legacy ib_logfile0.
fn read_redo_checkpoint_lsns(datadir: &std::path::Path) -> (Option<u64>, Option<u64>) {
    // Checkpoint 1 is at offset 512+8=520 in ib_logfile0 (LSN field at +8 within checkpoint)
    // Checkpoint 2 is at offset 1536+8=1544
    const CP1_OFFSET: u64 = 512 + 8;
    const CP2_OFFSET: u64 = 1536 + 8;

    // Try MySQL 8.0.30+ redo log in #innodb_redo/ directory
    let redo_dir = datadir.join("#innodb_redo");
    if redo_dir.is_dir() {
        // Find the first #ib_redo* file
        if let Ok(entries) = std::fs::read_dir(&redo_dir) {
            let mut redo_files: Vec<_> = entries
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().starts_with("#ib_redo"))
                .collect();
            redo_files.sort_by_key(|e| e.file_name());
            if let Some(first) = redo_files.first() {
                let path = first.path();
                let cp1 = read_u64_at(&path, CP1_OFFSET);
                let cp2 = read_u64_at(&path, CP2_OFFSET);
                return (cp1, cp2);
            }
        }
    }

    // Try legacy ib_logfile0
    let logfile0 = datadir.join("ib_logfile0");
    if logfile0.exists() {
        let cp1 = read_u64_at(&logfile0, CP1_OFFSET);
        let cp2 = read_u64_at(&logfile0, CP2_OFFSET);
        return (cp1, cp2);
    }

    (None, None)
}

fn read_file_bytes(
    path: &std::path::Path,
    offset: u64,
    length: usize,
) -> Result<Vec<u8>, IdbError> {
    use std::io::{Read, Seek, SeekFrom};

    let mut file = std::fs::File::open(path)
        .map_err(|e| IdbError::Io(format!("Cannot open {}: {}", path.display(), e)))?;

    file.seek(SeekFrom::Start(offset))
        .map_err(|e| IdbError::Io(format!("Cannot seek in {}: {}", path.display(), e)))?;

    let mut buf = vec![0u8; length];
    file.read_exact(&mut buf)
        .map_err(|e| IdbError::Io(format!("Cannot read from {}: {}", path.display(), e)))?;

    Ok(buf)
}

fn read_u64_at(path: &std::path::Path, offset: u64) -> Option<u64> {
    let bytes = read_file_bytes(path, offset, 8).ok()?;
    Some(BigEndian::read_u64(&bytes))
}

// MySQL connection mode (feature-gated)
#[cfg(feature = "mysql")]
fn execute_table_info(opts: &InfoOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    use mysql_async::prelude::*;

    let database = opts
        .database
        .as_deref()
        .ok_or_else(|| IdbError::Argument("Database name required (-D <database>)".to_string()))?;
    let table = opts
        .table
        .as_deref()
        .ok_or_else(|| IdbError::Argument("Table name required (-t <table>)".to_string()))?;

    // Build MySQL config from CLI args or defaults file
    let mut config = crate::util::mysql::MysqlConfig::default();

    // Try to load defaults file
    if let Some(ref df) = opts.defaults_file {
        if let Some(parsed) = crate::util::mysql::parse_defaults_file(std::path::Path::new(df)) {
            config = parsed;
        }
    } else if let Some(df) = crate::util::mysql::find_defaults_file() {
        if let Some(parsed) = crate::util::mysql::parse_defaults_file(&df) {
            config = parsed;
        }
    }

    // CLI args override defaults file
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
    config.database = Some(database.to_string());

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

        // Query table info — try MySQL 8.0+ tables first
        let table_query = format!(
            "SELECT SPACE, TABLE_ID FROM information_schema.innodb_tables WHERE NAME = '{}/{}'",
            database, table
        );
        let table_rows: Vec<(u64, u64)> = conn
            .query(&table_query)
            .await
            .unwrap_or_default();

        if table_rows.is_empty() {
            // Try MySQL 5.7 system tables
            let sys_query = format!(
                "SELECT SPACE, TABLE_ID FROM information_schema.innodb_sys_tables WHERE NAME = '{}/{}'",
                database, table
            );
            let sys_rows: Vec<(u64, u64)> = conn
                .query(&sys_query)
                .await
                .unwrap_or_default();

            if sys_rows.is_empty() {
                wprintln!(writer, "Table {}.{} not found in InnoDB system tables.", database, table)?;
                pool.disconnect().await.ok();
                return Ok(());
            }

            print_table_info(writer, database, table, &sys_rows)?;
        } else {
            print_table_info(writer, database, table, &table_rows)?;
        }

        // Query index info
        let idx_query = format!(
            "SELECT NAME, INDEX_ID, PAGE_NO FROM information_schema.innodb_indexes \
             WHERE TABLE_ID = (SELECT TABLE_ID FROM information_schema.innodb_tables WHERE NAME = '{}/{}')",
            database, table
        );
        let idx_rows: Vec<(String, u64, u64)> = conn
            .query(&idx_query)
            .await
            .unwrap_or_default();

        if !idx_rows.is_empty() {
            wprintln!(writer)?;
            wprintln!(writer, "{}", "Indexes:".bold())?;
            for (name, index_id, root_page) in &idx_rows {
                wprintln!(writer, "  {} (index_id={}, root_page={})", name, index_id, root_page)?;
            }
        }

        // Parse SHOW ENGINE INNODB STATUS for key metrics
        let status_rows: Vec<(String, String, String)> = conn
            .query("SHOW ENGINE INNODB STATUS")
            .await
            .unwrap_or_default();

        if let Some((_type, _name, status)) = status_rows.first() {
            wprintln!(writer)?;
            wprintln!(writer, "{}", "InnoDB Status:".bold())?;
            for line in status.lines() {
                if line.starts_with("Log sequence number") || line.starts_with("Log flushed up to") {
                    wprintln!(writer, "  {}", line.trim())?;
                }
                if line.starts_with("Trx id counter") {
                    wprintln!(writer, "  {}", line.trim())?;
                }
            }
        }

        pool.disconnect().await.ok();
        Ok(())
    })
}

#[cfg(feature = "mysql")]
fn print_table_info(
    writer: &mut dyn Write,
    database: &str,
    table: &str,
    rows: &[(u64, u64)],
) -> Result<(), IdbError> {
    wprintln!(
        writer,
        "{}",
        format!("Table: {}.{}", database, table).bold()
    )?;
    for (space_id, table_id) in rows {
        wprintln!(writer, "  Space ID:  {}", space_id)?;
        wprintln!(writer, "  Table ID:  {}", table_id)?;
    }
    Ok(())
}
