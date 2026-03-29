//! CLI implementation for the `inno binlog` subcommand.
//!
//! Parses MySQL binary log files and displays event summaries, format
//! description info, table map details, and row-based event statistics.
//! With `--correlate`, maps row events to tablespace pages via B+Tree lookup.

use std::collections::HashMap;
use std::io::Write;

use serde::Serialize;

use crate::cli::{csv_escape, wprintln};
use crate::IdbError;

/// Options for the `inno binlog` subcommand.
pub struct BinlogOptions {
    /// Path to the MySQL binary log file.
    pub file: String,
    /// Maximum number of events to display.
    pub limit: Option<usize>,
    /// Filter events by type name (e.g. "TABLE_MAP", "WRITE_ROWS").
    pub filter_type: Option<String>,
    /// Show additional detail (column types for TABLE_MAP events).
    pub verbose: bool,
    /// Output in JSON format.
    pub json: bool,
    /// Output as CSV.
    pub csv: bool,
    /// Path to .ibd tablespace for page correlation.
    pub correlate: Option<String>,
}

/// Combined analysis with correlated events for JSON output.
#[derive(Serialize)]
struct CorrelatedBinlogAnalysis {
    #[serde(flatten)]
    analysis: crate::binlog::BinlogAnalysis,
    correlated_events: Vec<crate::binlog::CorrelatedEvent>,
}

/// Analyze a binary log file and display results.
pub fn execute(opts: &BinlogOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    if opts.correlate.is_some() {
        return execute_correlated(opts, writer);
    }

    let file = std::fs::File::open(&opts.file)
        .map_err(|e| IdbError::Io(format!("{}: {}", opts.file, e)))?;

    let reader = std::io::BufReader::new(file);
    let analysis = crate::binlog::analyze_binlog(reader)?;

    if opts.json {
        let json =
            serde_json::to_string_pretty(&analysis).map_err(|e| IdbError::Parse(e.to_string()))?;
        wprintln!(writer, "{}", json)?;
        return Ok(());
    }

    if opts.csv {
        return write_csv(&analysis, opts, writer);
    }

    write_text(&analysis, opts, writer)
}

/// Execute with page correlation: maps row events to tablespace pages.
fn execute_correlated(opts: &BinlogOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    let ts_path = opts.correlate.as_ref().unwrap();

    // Run correlation
    let mut binlog = crate::binlog::BinlogFile::open(&opts.file)?;
    let mut ts = crate::cli::open_tablespace(ts_path, None, false)?;
    let correlated = crate::binlog::correlate_events(&mut binlog, &mut ts)?;

    // Also run standard analysis for event context
    let file = std::fs::File::open(&opts.file)
        .map_err(|e| IdbError::Io(format!("{}: {}", opts.file, e)))?;
    let reader = std::io::BufReader::new(file);
    let analysis = crate::binlog::analyze_binlog(reader)?;

    if opts.json {
        let combined = CorrelatedBinlogAnalysis {
            analysis,
            correlated_events: correlated,
        };
        let json =
            serde_json::to_string_pretty(&combined).map_err(|e| IdbError::Parse(e.to_string()))?;
        wprintln!(writer, "{}", json)?;
        return Ok(());
    }

    // Build lookup by binlog position
    let correlated_map: HashMap<u64, &crate::binlog::CorrelatedEvent> =
        correlated.iter().map(|e| (e.binlog_pos, e)).collect();

    if opts.csv {
        return write_correlated_csv(&analysis, &correlated_map, opts, writer);
    }

    write_correlated_text(&analysis, &correlated_map, opts, writer)
}

/// Write text output for correlated binlog analysis.
fn write_correlated_text(
    analysis: &crate::binlog::BinlogAnalysis,
    correlated: &HashMap<u64, &crate::binlog::CorrelatedEvent>,
    opts: &BinlogOptions,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    // Format description header
    wprintln!(writer, "Binary Log: {}", opts.file)?;
    wprintln!(
        writer,
        "  Server Version: {}",
        analysis.format_description.server_version
    )?;
    wprintln!(
        writer,
        "  Binlog Version: {}",
        analysis.format_description.binlog_version
    )?;
    wprintln!(
        writer,
        "  Checksum Algorithm: {}",
        analysis.format_description.checksum_alg
    )?;
    wprintln!(
        writer,
        "  Correlated Events: {} (tablespace: {})",
        correlated.len(),
        opts.correlate.as_deref().unwrap_or("--")
    )?;
    wprintln!(writer)?;

    // Event type summary
    wprintln!(
        writer,
        "Event Type Summary ({} total):",
        analysis.event_count
    )?;
    let mut type_counts: Vec<_> = analysis.event_type_counts.iter().collect();
    type_counts.sort_by(|a, b| b.1.cmp(a.1));
    for (name, count) in &type_counts {
        wprintln!(writer, "  {:<30} {:>6}", name, count)?;
    }
    wprintln!(writer)?;

    // Table maps
    if !analysis.table_maps.is_empty() {
        wprintln!(writer, "Table Maps ({}):", analysis.table_maps.len())?;
        for tm in &analysis.table_maps {
            wprintln!(
                writer,
                "  table_id={} {}.{} ({} columns)",
                tm.table_id,
                tm.database_name,
                tm.table_name,
                tm.column_count
            )?;
            if opts.verbose {
                wprintln!(writer, "    Column types: {:?}", &tm.column_types)?;
            }
        }
        wprintln!(writer)?;
    }

    // Event listing with page correlation columns
    let events = filter_events(&analysis.events, opts);
    let limit = opts.limit.unwrap_or(events.len());
    let display_events = &events[..limit.min(events.len())];

    if !display_events.is_empty() {
        wprintln!(
            writer,
            "{:<12} {:<30} {:<10} {:<12} {:<8} {}",
            "Position",
            "Type",
            "Size",
            "Timestamp",
            "Page",
            "PK"
        )?;
        wprintln!(writer, "{}", "-".repeat(90))?;
        for evt in display_events {
            if let Some(ce) = correlated.get(&evt.offset) {
                let pk_display = if ce.pk_values.is_empty() {
                    "--".to_string()
                } else {
                    format!("({})", ce.pk_values.join(", "))
                };
                wprintln!(
                    writer,
                    "{:<12} {:<30} {:<10} {:<12} {:<8} {}",
                    evt.offset,
                    evt.event_type,
                    evt.event_length,
                    evt.timestamp,
                    ce.page_no,
                    pk_display
                )?;
            } else {
                wprintln!(
                    writer,
                    "{:<12} {:<30} {:<10} {:<12} {:<8} {}",
                    evt.offset,
                    evt.event_type,
                    evt.event_length,
                    evt.timestamp,
                    "--",
                    "--"
                )?;
            }
        }
    }

    if events.len() > limit {
        wprintln!(
            writer,
            "\n... {} more events (use --limit to show more)",
            events.len() - limit
        )?;
    }

    Ok(())
}

/// Write CSV output for correlated binlog events.
fn write_correlated_csv(
    analysis: &crate::binlog::BinlogAnalysis,
    correlated: &HashMap<u64, &crate::binlog::CorrelatedEvent>,
    opts: &BinlogOptions,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    wprintln!(
        writer,
        "position,type,size,timestamp,server_id,page_no,space_id,pk_values"
    )?;

    let events = filter_events(&analysis.events, opts);
    let limit = opts.limit.unwrap_or(events.len());
    let display_events = &events[..limit.min(events.len())];

    for evt in display_events {
        if let Some(ce) = correlated.get(&evt.offset) {
            let pk_str = ce.pk_values.join(";");
            wprintln!(
                writer,
                "{},{},{},{},{},{},{},{}",
                evt.offset,
                csv_escape(&evt.event_type),
                evt.event_length,
                evt.timestamp,
                evt.server_id,
                ce.page_no,
                ce.space_id,
                csv_escape(&pk_str)
            )?;
        } else {
            wprintln!(
                writer,
                "{},{},{},{},{},,,",
                evt.offset,
                csv_escape(&evt.event_type),
                evt.event_length,
                evt.timestamp,
                evt.server_id
            )?;
        }
    }

    Ok(())
}

/// Write text output for binlog analysis.
fn write_text(
    analysis: &crate::binlog::BinlogAnalysis,
    opts: &BinlogOptions,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    // Format description header
    wprintln!(writer, "Binary Log: {}", opts.file)?;
    wprintln!(
        writer,
        "  Server Version: {}",
        analysis.format_description.server_version
    )?;
    wprintln!(
        writer,
        "  Binlog Version: {}",
        analysis.format_description.binlog_version
    )?;
    wprintln!(
        writer,
        "  Checksum Algorithm: {}",
        analysis.format_description.checksum_alg
    )?;
    wprintln!(writer)?;

    // Event type summary
    wprintln!(
        writer,
        "Event Type Summary ({} total):",
        analysis.event_count
    )?;
    let mut type_counts: Vec<_> = analysis.event_type_counts.iter().collect();
    type_counts.sort_by(|a, b| b.1.cmp(a.1));
    for (name, count) in &type_counts {
        wprintln!(writer, "  {:<30} {:>6}", name, count)?;
    }
    wprintln!(writer)?;

    // Table maps
    if !analysis.table_maps.is_empty() {
        wprintln!(writer, "Table Maps ({}):", analysis.table_maps.len())?;
        for tm in &analysis.table_maps {
            wprintln!(
                writer,
                "  table_id={} {}.{} ({} columns)",
                tm.table_id,
                tm.database_name,
                tm.table_name,
                tm.column_count
            )?;
            if opts.verbose {
                wprintln!(writer, "    Column types: {:?}", &tm.column_types)?;
            }
        }
        wprintln!(writer)?;
    }

    // Event listing
    let events = filter_events(&analysis.events, opts);
    let limit = opts.limit.unwrap_or(events.len());
    let display_events = &events[..limit.min(events.len())];

    if !display_events.is_empty() {
        wprintln!(
            writer,
            "{:<12} {:<30} {:<10} {:<12}",
            "Position",
            "Type",
            "Size",
            "Timestamp"
        )?;
        wprintln!(writer, "{}", "-".repeat(66))?;
        for evt in display_events {
            wprintln!(
                writer,
                "{:<12} {:<30} {:<10} {:<12}",
                evt.offset,
                evt.event_type,
                evt.event_length,
                evt.timestamp
            )?;
        }
    }

    if events.len() > limit {
        wprintln!(
            writer,
            "\n... {} more events (use --limit to show more)",
            events.len() - limit
        )?;
    }

    Ok(())
}

/// Write CSV output for binlog events.
fn write_csv(
    analysis: &crate::binlog::BinlogAnalysis,
    opts: &BinlogOptions,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    wprintln!(writer, "position,type,size,timestamp,server_id")?;

    let events = filter_events(&analysis.events, opts);
    let limit = opts.limit.unwrap_or(events.len());
    let display_events = &events[..limit.min(events.len())];

    for evt in display_events {
        wprintln!(
            writer,
            "{},{},{},{},{}",
            evt.offset,
            csv_escape(&evt.event_type),
            evt.event_length,
            evt.timestamp,
            evt.server_id
        )?;
    }

    Ok(())
}

/// Filter events by type name if a filter is provided.
fn filter_events<'a>(
    events: &'a [crate::binlog::BinlogEventSummary],
    opts: &BinlogOptions,
) -> Vec<&'a crate::binlog::BinlogEventSummary> {
    match &opts.filter_type {
        Some(filter) => {
            let filter_upper = filter.to_uppercase();
            events
                .iter()
                .filter(|e| e.event_type.to_uppercase().contains(&filter_upper))
                .collect()
        }
        None => events.iter().collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binlog::{BinlogAnalysis, BinlogEventSummary, FormatDescriptionEvent};

    fn sample_analysis() -> BinlogAnalysis {
        let mut event_type_counts = HashMap::new();
        event_type_counts.insert("QUERY_EVENT".to_string(), 5);
        event_type_counts.insert("TABLE_MAP_EVENT".to_string(), 2);

        BinlogAnalysis {
            format_description: FormatDescriptionEvent {
                binlog_version: 4,
                server_version: "8.0.35".to_string(),
                create_timestamp: 0,
                header_length: 19,
                checksum_alg: 1,
            },
            event_count: 7,
            event_type_counts,
            table_maps: Vec::new(),
            events: vec![
                BinlogEventSummary {
                    offset: 4,
                    event_type: "FORMAT_DESCRIPTION_EVENT".to_string(),
                    type_code: 15,
                    event_length: 119,
                    timestamp: 1700000000,
                    server_id: 1,
                },
                BinlogEventSummary {
                    offset: 123,
                    event_type: "QUERY_EVENT".to_string(),
                    type_code: 2,
                    event_length: 50,
                    timestamp: 1700000001,
                    server_id: 1,
                },
            ],
        }
    }

    #[test]
    fn test_write_text_output() {
        let analysis = sample_analysis();
        let opts = BinlogOptions {
            file: "test-bin.000001".to_string(),
            limit: None,
            filter_type: None,
            verbose: false,
            json: false,
            csv: false,
            correlate: None,
        };

        let mut buf = Vec::new();
        write_text(&analysis, &opts, &mut buf).unwrap();
        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("Binary Log: test-bin.000001"));
        assert!(output.contains("Server Version: 8.0.35"));
        assert!(output.contains("7 total"));
        assert!(output.contains("FORMAT_DESCRIPTION_EVENT"));
    }

    #[test]
    fn test_write_csv_output() {
        let analysis = sample_analysis();
        let opts = BinlogOptions {
            file: "test-bin.000001".to_string(),
            limit: None,
            filter_type: None,
            verbose: false,
            json: false,
            csv: true,
            correlate: None,
        };

        let mut buf = Vec::new();
        write_csv(&analysis, &opts, &mut buf).unwrap();
        let output = String::from_utf8(buf).unwrap();
        assert!(output.starts_with("position,type,size,timestamp,server_id"));
        assert!(output.contains("FORMAT_DESCRIPTION_EVENT"));
    }

    #[test]
    fn test_filter_events_by_type() {
        let analysis = sample_analysis();
        let opts = BinlogOptions {
            file: "test".to_string(),
            limit: None,
            filter_type: Some("query".to_string()),
            verbose: false,
            json: false,
            csv: false,
            correlate: None,
        };

        let filtered = filter_events(&analysis.events, &opts);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].event_type, "QUERY_EVENT");
    }

    #[test]
    fn test_json_output() {
        let analysis = sample_analysis();
        let json = serde_json::to_string_pretty(&analysis).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["event_count"], 7);
    }

    #[test]
    fn test_correlated_text_output() {
        let analysis = sample_analysis();
        let ce = crate::binlog::CorrelatedEvent {
            binlog_pos: 123,
            event_type: crate::binlog::RowEventType::Insert,
            database: "test".to_string(),
            table: "users".to_string(),
            page_no: 42,
            space_id: 5,
            page_lsn: 999,
            pk_values: vec!["1".to_string(), "alice".to_string()],
            timestamp: 1700000001,
        };
        let correlated: HashMap<u64, &crate::binlog::CorrelatedEvent> =
            [(123, &ce)].into_iter().collect();
        let opts = BinlogOptions {
            file: "test-bin.000001".to_string(),
            limit: None,
            filter_type: None,
            verbose: false,
            json: false,
            csv: false,
            correlate: Some("/tmp/users.ibd".to_string()),
        };

        let mut buf = Vec::new();
        write_correlated_text(&analysis, &correlated, &opts, &mut buf).unwrap();
        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("Correlated Events: 1"));
        assert!(output.contains("Page"));
        assert!(output.contains("PK"));
        // The correlated row event should show page 42
        assert!(output.contains("42"));
        assert!(output.contains("(1, alice)"));
        // Non-correlated event should show --
        let lines: Vec<&str> = output.lines().collect();
        let fde_line = lines
            .iter()
            .find(|l| l.contains("FORMAT_DESCRIPTION"))
            .unwrap();
        assert!(fde_line.contains("--"));
    }

    #[test]
    fn test_correlated_csv_output() {
        let analysis = sample_analysis();
        let ce = crate::binlog::CorrelatedEvent {
            binlog_pos: 123,
            event_type: crate::binlog::RowEventType::Insert,
            database: "test".to_string(),
            table: "users".to_string(),
            page_no: 42,
            space_id: 5,
            page_lsn: 999,
            pk_values: vec!["1".to_string()],
            timestamp: 1700000001,
        };
        let correlated: HashMap<u64, &crate::binlog::CorrelatedEvent> =
            [(123, &ce)].into_iter().collect();
        let opts = BinlogOptions {
            file: "test-bin.000001".to_string(),
            limit: None,
            filter_type: None,
            verbose: false,
            json: false,
            csv: true,
            correlate: Some("/tmp/users.ibd".to_string()),
        };

        let mut buf = Vec::new();
        write_correlated_csv(&analysis, &correlated, &opts, &mut buf).unwrap();
        let output = String::from_utf8(buf).unwrap();
        assert!(
            output.starts_with("position,type,size,timestamp,server_id,page_no,space_id,pk_values")
        );
        // Correlated event should have page_no=42, space_id=5
        assert!(output.contains(",42,5,"));
        // Non-correlated event should have empty page/space columns
        let fde_line = output
            .lines()
            .find(|l| l.contains("FORMAT_DESCRIPTION"))
            .unwrap();
        assert!(fde_line.ends_with(",,,"));
    }

    #[test]
    fn test_correlated_json_output() {
        let analysis = sample_analysis();
        let correlated = vec![crate::binlog::CorrelatedEvent {
            binlog_pos: 123,
            event_type: crate::binlog::RowEventType::Insert,
            database: "test".to_string(),
            table: "users".to_string(),
            page_no: 42,
            space_id: 5,
            page_lsn: 999,
            pk_values: vec!["1".to_string()],
            timestamp: 1700000001,
        }];

        let combined = CorrelatedBinlogAnalysis {
            analysis,
            correlated_events: correlated,
        };
        let json = serde_json::to_string_pretty(&combined).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["event_count"], 7);
        assert_eq!(parsed["correlated_events"][0]["page_no"], 42);
        assert_eq!(parsed["correlated_events"][0]["space_id"], 5);
        assert_eq!(parsed["correlated_events"][0]["event_type"], "Insert");
    }
}
