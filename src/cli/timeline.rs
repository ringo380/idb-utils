//! CLI implementation for the `inno timeline` subcommand.

use std::io::Write;

use crate::cli::{csv_escape, wprintln};
use crate::innodb::timeline::{
    extract_binlog_timeline, extract_redo_timeline, extract_undo_timeline, merge_timeline,
    TimelineAction, TimelineReport,
};
use crate::IdbError;

/// Options for the `inno timeline` subcommand.
pub struct TimelineOptions {
    pub redo_log: Option<String>,
    pub undo_file: Option<String>,
    pub binlog: Option<String>,
    pub file: Option<String>,
    pub datadir: Option<String>,
    pub space_id: Option<u32>,
    pub page: Option<u64>,
    pub table: Option<String>,
    pub limit: Option<usize>,
    pub verbose: bool,
    pub json: bool,
    pub page_size: Option<u32>,
    pub keyring: Option<String>,
}

pub fn execute(opts: &TimelineOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    if opts.redo_log.is_none() && opts.undo_file.is_none() && opts.binlog.is_none() {
        return Err(IdbError::Argument(
            "At least one of --redo-log, --undo-file, or --binlog is required".to_string(),
        ));
    }

    // Extract from each source
    let redo_entries = if let Some(ref path) = opts.redo_log {
        let mut log = crate::innodb::log::LogFile::open(path)?;
        extract_redo_timeline(&mut log)?
    } else {
        Vec::new()
    };

    let undo_entries = if let Some(ref path) = opts.undo_file {
        let mut ts = crate::cli::open_tablespace(path, opts.page_size, false)?;
        if let Some(ref keyring_path) = opts.keyring {
            crate::cli::setup_decryption(&mut ts, keyring_path)?;
        }
        extract_undo_timeline(&mut ts)?
    } else {
        Vec::new()
    };

    let binlog_entries = if let Some(ref path) = opts.binlog {
        if opts.file.is_some() {
            // Use enriched extraction for B+Tree correlation
            let file = std::fs::File::open(path)
                .map_err(|e| IdbError::Io(format!("Cannot open {}: {}", path, e)))?;
            let reader = std::io::BufReader::new(file);
            let mut result = crate::innodb::timeline::extract_binlog_timeline_enriched(reader)?;

            // Correlate with tablespace if provided
            if let Some(ref ibd_path) = opts.file {
                let mut ts = crate::cli::open_tablespace(ibd_path, opts.page_size, false)?;
                if let Some(ref keyring_path) = opts.keyring {
                    crate::cli::setup_decryption(&mut ts, keyring_path)?;
                }
                let _correlated = crate::innodb::timeline::correlate_binlog_pages(
                    &mut result.entries,
                    &mut ts,
                    &result.table_maps,
                    &result.row_data,
                )?;
            }
            result.entries
        } else {
            let file = std::fs::File::open(path)
                .map_err(|e| IdbError::Io(format!("Cannot open {}: {}", path, e)))?;
            let reader = std::io::BufReader::new(file);
            extract_binlog_timeline(reader)?
        }
    } else {
        Vec::new()
    };

    let mut report = merge_timeline(redo_entries, undo_entries, binlog_entries);

    // Annotate binlog entries with space_id if a data directory is available
    if let Some(ref datadir) = opts.datadir {
        if let Ok(space_map) = crate::innodb::timeline::build_space_table_map(datadir) {
            // Build reverse map: table_name -> space_id
            let reverse: std::collections::HashMap<String, u32> = space_map
                .into_iter()
                .map(|(sid, name)| (name, sid))
                .collect();
            for entry in &mut report.entries {
                if let crate::innodb::timeline::TimelineAction::Binlog {
                    database, table, ..
                } = &entry.action
                {
                    if let (Some(db), Some(tbl)) = (database, table) {
                        let full = format!("{}.{}", db, tbl);
                        if let Some(&sid) = reverse.get(&full) {
                            entry.space_id = Some(sid);
                        }
                    }
                }
            }
        }
    }

    // Apply filters
    apply_filters(&mut report, opts);

    if opts.json {
        let json = serde_json::to_string_pretty(&report)
            .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
        wprintln!(writer, "{}", json)?;
    } else {
        write_text(&report, opts, writer)?;
    }

    Ok(())
}

fn apply_filters(report: &mut TimelineReport, opts: &TimelineOptions) {
    if let Some(sid) = opts.space_id {
        report.entries.retain(|e| e.space_id == Some(sid));
    }
    if let Some(pno) = opts.page {
        report.entries.retain(|e| e.page_no == Some(pno as u32));
    }
    if let Some(ref table) = opts.table {
        let lower = table.to_lowercase();
        report.entries.retain(|e| match &e.action {
            TimelineAction::Binlog {
                database, table, ..
            } => {
                let db = database.as_deref().unwrap_or("");
                let tbl = table.as_deref().unwrap_or("");
                db.to_lowercase().contains(&lower) || tbl.to_lowercase().contains(&lower)
            }
            _ => true, // redo/undo entries don't have table names; keep them
        });
    }
    if let Some(limit) = opts.limit {
        report.entries.truncate(limit);
    }
}

fn write_text(
    report: &TimelineReport,
    opts: &TimelineOptions,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    wprintln!(writer, "Transaction Timeline")?;

    // Sources line
    let mut sources = Vec::new();
    if let Some(ref p) = opts.redo_log {
        sources.push(format!("redo log ({})", short_name(p)));
    }
    if let Some(ref p) = opts.undo_file {
        sources.push(format!("undo ({})", short_name(p)));
    }
    if let Some(ref p) = opts.binlog {
        sources.push(format!("binlog ({})", short_name(p)));
    }
    wprintln!(writer, "  Sources: {}", sources.join(", "))?;
    wprintln!(
        writer,
        "  Redo entries: {}  |  Undo entries: {}  |  Binlog entries: {}  |  Correlated: {}",
        report.redo_count,
        report.undo_count,
        report.binlog_count,
        report.correlated_count
    )?;
    wprintln!(writer)?;

    // Entry table
    wprintln!(
        writer,
        "  {:<6} {:<18} {:<8} {:<12} {}",
        "SEQ",
        "LSN",
        "SOURCE",
        "SPACE:PAGE",
        "ACTION"
    )?;

    for entry in &report.entries {
        let lsn_str = entry
            .lsn
            .map(|l| l.to_string())
            .unwrap_or_else(|| "-".to_string());
        let page_str = match (entry.space_id, entry.page_no) {
            (Some(s), Some(p)) => format!("{}:{}", s, p),
            (None, Some(p)) => format!("-:{}", p),
            _ => "-".to_string(),
        };
        let action_str = format_action(&entry.action, opts.verbose);

        wprintln!(
            writer,
            "  {:<6} {:<18} {:<8} {:<12} {}",
            entry.seq,
            lsn_str,
            entry.source,
            page_str,
            action_str
        )?;
    }

    // Page summary
    if !report.page_summaries.is_empty() {
        wprintln!(writer)?;
        wprintln!(writer, "Page Summary:")?;
        wprintln!(
            writer,
            "  {:<12} {:<6} {:<6} {:<8} {:<18} {}",
            "SPACE:PAGE",
            "REDO",
            "UNDO",
            "BINLOG",
            "FIRST_LSN",
            "LAST_LSN"
        )?;
        for ps in &report.page_summaries {
            let first = ps
                .first_lsn
                .map(|l| l.to_string())
                .unwrap_or_else(|| "-".to_string());
            let last = ps
                .last_lsn
                .map(|l| l.to_string())
                .unwrap_or_else(|| "-".to_string());
            wprintln!(
                writer,
                "  {:<12} {:<6} {:<6} {:<8} {:<18} {}",
                format!("{}:{}", ps.space_id, ps.page_no),
                ps.redo_entries,
                ps.undo_entries,
                ps.binlog_entries,
                first,
                last
            )?;
        }
    }

    Ok(())
}

pub fn write_csv(report: &TimelineReport, writer: &mut dyn Write) -> Result<(), IdbError> {
    wprintln!(
        writer,
        "seq,lsn,timestamp,source,space_id,page_no,action_type,details"
    )?;
    for entry in &report.entries {
        let lsn = entry.lsn.map(|l| l.to_string()).unwrap_or_default();
        let ts = entry.timestamp.map(|t| t.to_string()).unwrap_or_default();
        let sid = entry.space_id.map(|s| s.to_string()).unwrap_or_default();
        let pno = entry.page_no.map(|p| p.to_string()).unwrap_or_default();
        let (action_type, details) = csv_action(&entry.action);

        wprintln!(
            writer,
            "{},{},{},{},{},{},{},{}",
            entry.seq,
            lsn,
            ts,
            entry.source,
            sid,
            pno,
            csv_escape(&action_type),
            csv_escape(&details)
        )?;
    }
    Ok(())
}

fn format_action(action: &TimelineAction, verbose: bool) -> String {
    match action {
        TimelineAction::Redo {
            mlog_type,
            single_rec,
        } => {
            if verbose {
                format!("{} (single_rec={})", mlog_type, single_rec)
            } else {
                mlog_type.clone()
            }
        }
        TimelineAction::Undo {
            record_type,
            trx_id,
            table_id,
            ..
        } => {
            if verbose {
                format!("trx={} {} table_id={}", trx_id, record_type, table_id)
            } else {
                format!("trx={} {}", trx_id, record_type)
            }
        }
        TimelineAction::Binlog {
            event_type,
            database,
            table,
            xid,
            pk_values,
        } => {
            let mut s = event_type.clone();
            if let Some(db) = database {
                if let Some(tbl) = table {
                    s.push_str(&format!(" {}.{}", db, tbl));
                }
            }
            if let Some(x) = xid {
                s.push_str(&format!(" (xid={})", x));
            }
            if let Some(pks) = pk_values {
                s.push_str(&format!(" PK=({})", pks.join(", ")));
            }
            s
        }
    }
}

fn csv_action(action: &TimelineAction) -> (String, String) {
    match action {
        TimelineAction::Redo {
            mlog_type,
            single_rec,
        } => (mlog_type.clone(), format!("single_rec={}", single_rec)),
        TimelineAction::Undo {
            record_type,
            trx_id,
            undo_no,
            table_id,
        } => (
            record_type.clone(),
            format!(
                "trx_id={} undo_no={} table_id={}",
                trx_id, undo_no, table_id
            ),
        ),
        TimelineAction::Binlog {
            event_type,
            database,
            table,
            xid,
            ..
        } => {
            let mut detail = String::new();
            if let Some(db) = database {
                detail.push_str(&format!("db={}", db));
            }
            if let Some(tbl) = table {
                if !detail.is_empty() {
                    detail.push(' ');
                }
                detail.push_str(&format!("table={}", tbl));
            }
            if let Some(x) = xid {
                if !detail.is_empty() {
                    detail.push(' ');
                }
                detail.push_str(&format!("xid={}", x));
            }
            (event_type.clone(), detail)
        }
    }
}

fn short_name(path: &str) -> &str {
    std::path::Path::new(path)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(path)
}
