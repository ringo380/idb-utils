//! CLI implementation for the `inno binlog` subcommand.
//!
//! Parses MySQL binary log files and displays event summaries, format
//! description info, table map details, and row-based event statistics.

use std::io::Write;

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
}

/// Analyze a binary log file and display results.
pub fn execute(opts: &BinlogOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    let file = std::fs::File::open(&opts.file)
        .map_err(|e| IdbError::Io(format!("{}: {}", opts.file, e)))?;

    let reader = std::io::BufReader::new(file);
    let analysis = crate::binlog::analyze_binlog(reader)?;

    if opts.json {
        let json = serde_json::to_string_pretty(&analysis)
            .map_err(|e| IdbError::Parse(e.to_string()))?;
        wprintln!(writer, "{}", json)?;
        return Ok(());
    }

    if opts.csv {
        return write_csv(&analysis, opts, writer);
    }

    write_text(&analysis, opts, writer)
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
    wprintln!(writer, "Event Type Summary ({} total):", analysis.event_count)?;
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
                tm.table_id, tm.database_name, tm.table_name, tm.column_count
            )?;
            if opts.verbose {
                wprintln!(
                    writer,
                    "    Column types: {:?}",
                    &tm.column_types
                )?;
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
            "Position", "Type", "Size", "Timestamp"
        )?;
        wprintln!(writer, "{}", "-".repeat(66))?;
        for evt in display_events {
            wprintln!(
                writer,
                "{:<12} {:<30} {:<10} {:<12}",
                evt.offset, evt.event_type, evt.event_length, evt.timestamp
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
    use std::collections::HashMap;

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
}
