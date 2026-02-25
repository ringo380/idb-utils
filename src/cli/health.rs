//! CLI implementation for the `inno health` subcommand.
//!
//! Computes per-index B+Tree health metrics (fill factor, fragmentation,
//! garbage ratio, tree depth) by scanning all INDEX pages in a tablespace.

use std::io::Write;
use std::time::Instant;

use crate::cli::wprintln;
use crate::innodb::health;
use crate::innodb::schema::SdiEnvelope;
use crate::innodb::sdi;
use crate::util::prometheus as prom;
use crate::IdbError;

/// Options for the `inno health` subcommand.
pub struct HealthOptions {
    /// Path to the InnoDB tablespace file (.ibd).
    pub file: String,
    /// Show per-level page counts and total records.
    pub verbose: bool,
    /// Output in JSON format.
    pub json: bool,
    /// Output as CSV.
    pub csv: bool,
    /// Output in Prometheus exposition format.
    pub prometheus: bool,
    /// Override the auto-detected page size.
    pub page_size: Option<u32>,
    /// Path to MySQL keyring file for decrypting encrypted tablespaces.
    pub keyring: Option<String>,
    /// Use memory-mapped I/O for file access.
    pub mmap: bool,
}

/// Analyze B+Tree health metrics for all indexes in a tablespace.
pub fn execute(opts: &HealthOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    if opts.prometheus && (opts.json || opts.csv) {
        return Err(IdbError::Argument(
            "--prometheus cannot be combined with JSON or CSV output".to_string(),
        ));
    }

    let start = Instant::now();

    let mut ts = crate::cli::open_tablespace(&opts.file, opts.page_size, opts.mmap)?;

    if let Some(ref keyring_path) = opts.keyring {
        crate::cli::setup_decryption(&mut ts, keyring_path)?;
    }

    let page_size = ts.page_size();
    let total_file_pages = ts.page_count();

    // Single-pass collection
    let mut snapshots = Vec::new();
    let mut empty_pages = 0u64;

    ts.for_each_page(|page_num, data| {
        if data.iter().all(|&b| b == 0) {
            empty_pages += 1;
        } else if let Some(snap) = health::extract_index_page_snapshot(data, page_num) {
            snapshots.push(snap);
        }
        Ok(())
    })?;

    let mut report = health::analyze_health(
        snapshots,
        page_size,
        total_file_pages,
        empty_pages,
        &opts.file,
    );

    // Best-effort SDI index name resolution
    resolve_index_names(
        &opts.file,
        opts.page_size,
        opts.mmap,
        &opts.keyring,
        &mut report,
    );

    let duration_secs = start.elapsed().as_secs_f64();

    if opts.prometheus {
        print_prometheus(writer, &report, duration_secs)?;
        return Ok(());
    }

    if opts.json {
        wprintln!(
            writer,
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| IdbError::Parse(e.to_string()))?
        )?;
    } else if opts.csv {
        wprintln!(
            writer,
            "index_id,index_name,tree_depth,total_pages,leaf_pages,avg_fill_factor,garbage_ratio,fragmentation"
        )?;
        for idx in &report.indexes {
            wprintln!(
                writer,
                "{},{},{},{},{},{},{},{}",
                idx.index_id,
                crate::cli::csv_escape(idx.index_name.as_deref().unwrap_or("")),
                idx.tree_depth,
                idx.total_pages,
                idx.leaf_pages,
                idx.avg_fill_factor,
                idx.avg_garbage_ratio,
                idx.fragmentation
            )?;
        }
    } else {
        print_text(writer, &report, opts.verbose)?;
    }

    Ok(())
}

/// Try to resolve index names from SDI metadata (best-effort, display-only).
fn resolve_index_names(
    file: &str,
    page_size: Option<u32>,
    mmap: bool,
    keyring: &Option<String>,
    report: &mut health::HealthReport,
) {
    let resolve = || -> Result<std::collections::HashMap<u64, String>, IdbError> {
        let mut ts = crate::cli::open_tablespace(file, page_size, mmap)?;
        if let Some(ref kp) = keyring {
            crate::cli::setup_decryption(&mut ts, kp)?;
        }
        let sdi_pages = sdi::find_sdi_pages(&mut ts)?;
        if sdi_pages.is_empty() {
            return Ok(std::collections::HashMap::new());
        }
        let records = sdi::extract_sdi_from_pages(&mut ts, &sdi_pages)?;
        let mut name_map = std::collections::HashMap::new();
        for rec in &records {
            if rec.sdi_type == 1 {
                if let Ok(envelope) = serde_json::from_str::<SdiEnvelope>(&rec.data) {
                    for dd_idx in &envelope.dd_object.indexes {
                        // InnoDB assigns se_private_data with index IDs; the SDI
                        // JSON index ordering matches the clustered index first,
                        // then secondary indexes. We can't directly map dd_idx to
                        // index_id without parsing se_private_data, but we can use
                        // the name if we find a matching pattern.
                        // Use se_private_data parsing to extract "id=N"
                        if let Some(id) = parse_se_private_id(&rec.data, &dd_idx.name) {
                            name_map.insert(id, dd_idx.name.clone());
                        }
                    }
                }
            }
        }
        Ok(name_map)
    };

    if let Ok(name_map) = resolve() {
        for idx in &mut report.indexes {
            if let Some(name) = name_map.get(&idx.index_id) {
                idx.index_name = Some(name.clone());
            }
        }
    }
}

/// Parse an index ID from SDI JSON se_private_data field.
///
/// The JSON contains `"se_private_data": "id=N;root=M;..."` for each index.
/// We search the raw JSON for the pattern matching this index name.
fn parse_se_private_id(sdi_json: &str, name: &str) -> Option<u64> {
    // Parse the full JSON to extract se_private_data for the named index
    let val: serde_json::Value = serde_json::from_str(sdi_json).ok()?;
    let indexes = val.get("dd_object")?.get("indexes")?.as_array()?;
    for idx in indexes {
        let idx_name = idx.get("name")?.as_str()?;
        if idx_name == name {
            let se_data = idx.get("se_private_data")?.as_str()?;
            for part in se_data.split(';') {
                if let Some(id_str) = part.strip_prefix("id=") {
                    return id_str.parse::<u64>().ok();
                }
            }
        }
    }
    None
}

/// Print health report as human-readable text.
fn print_text(
    writer: &mut dyn Write,
    report: &health::HealthReport,
    verbose: bool,
) -> Result<(), IdbError> {
    wprintln!(writer, "Tablespace Health Report: {}", report.file)?;
    wprintln!(writer)?;

    if report.indexes.is_empty() {
        wprintln!(writer, "No INDEX pages found.")?;
        wprintln!(writer)?;
    }

    for idx in &report.indexes {
        let name = idx.index_name.as_deref().unwrap_or("(unknown)");
        wprintln!(writer, "Index {} ({}):", idx.index_id, name)?;
        wprintln!(writer, "  Tree depth:     {}", idx.tree_depth)?;
        wprintln!(
            writer,
            "  Pages:          {} total ({} leaf, {} non-leaf)",
            idx.total_pages,
            idx.leaf_pages,
            idx.non_leaf_pages
        )?;
        wprintln!(
            writer,
            "  Fill factor:    avg {:.0}%, min {:.0}%, max {:.0}%",
            idx.avg_fill_factor * 100.0,
            idx.min_fill_factor * 100.0,
            idx.max_fill_factor * 100.0
        )?;
        wprintln!(
            writer,
            "  Garbage:        {:.1}% ({} bytes)",
            idx.avg_garbage_ratio * 100.0,
            idx.total_garbage_bytes
        )?;
        wprintln!(
            writer,
            "  Fragmentation:  {:.1}%",
            idx.fragmentation * 100.0
        )?;

        if verbose {
            wprintln!(writer, "  Total records:  {}", idx.total_records)?;
            if idx.empty_leaf_pages > 0 {
                wprintln!(writer, "  Empty leaves:   {}", idx.empty_leaf_pages)?;
            }
        }

        wprintln!(writer)?;
    }

    // Summary
    wprintln!(writer, "Summary:")?;
    wprintln!(
        writer,
        "  Total pages:      {} ({} INDEX, {} non-INDEX, {} empty)",
        report.summary.total_pages,
        report.summary.index_pages,
        report.summary.non_index_pages,
        report.summary.empty_pages
    )?;
    wprintln!(
        writer,
        "  Page size:        {} bytes",
        report.summary.page_size
    )?;
    wprintln!(writer, "  Indexes:          {}", report.summary.index_count)?;
    wprintln!(
        writer,
        "  Avg fill factor:  {:.0}%",
        report.summary.avg_fill_factor * 100.0
    )?;
    wprintln!(
        writer,
        "  Avg garbage:      {:.1}%",
        report.summary.avg_garbage_ratio * 100.0
    )?;
    wprintln!(
        writer,
        "  Avg fragmentation: {:.1}%",
        report.summary.avg_fragmentation * 100.0
    )?;

    Ok(())
}

/// Print health report as Prometheus exposition format.
fn print_prometheus(
    writer: &mut dyn Write,
    report: &health::HealthReport,
    duration_secs: f64,
) -> Result<(), IdbError> {
    let file = &report.file;

    // innodb_pages
    wprintln!(
        writer,
        "{}",
        prom::help_line("innodb_pages", "Total pages in the tablespace by type")
    )?;
    wprintln!(writer, "{}", prom::type_line("innodb_pages", "gauge"))?;
    wprintln!(
        writer,
        "{}",
        prom::format_gauge_int(
            "innodb_pages",
            &[("file", file), ("type", "index")],
            report.summary.index_pages
        )
    )?;
    wprintln!(
        writer,
        "{}",
        prom::format_gauge_int(
            "innodb_pages",
            &[("file", file), ("type", "non_index")],
            report.summary.non_index_pages
        )
    )?;
    wprintln!(
        writer,
        "{}",
        prom::format_gauge_int(
            "innodb_pages",
            &[("file", file), ("type", "empty")],
            report.summary.empty_pages
        )
    )?;

    // innodb_fill_factor
    wprintln!(
        writer,
        "{}",
        prom::help_line("innodb_fill_factor", "Average B+Tree fill factor per index")
    )?;
    wprintln!(writer, "{}", prom::type_line("innodb_fill_factor", "gauge"))?;
    for idx in &report.indexes {
        let id_str = idx.index_id.to_string();
        let index_label = idx.index_name.as_deref().unwrap_or(&id_str);
        wprintln!(
            writer,
            "{}",
            prom::format_gauge(
                "innodb_fill_factor",
                &[("file", file), ("index", index_label)],
                idx.avg_fill_factor
            )
        )?;
    }

    // innodb_fragmentation_ratio
    wprintln!(
        writer,
        "{}",
        prom::help_line(
            "innodb_fragmentation_ratio",
            "Leaf-level fragmentation ratio per index"
        )
    )?;
    wprintln!(
        writer,
        "{}",
        prom::type_line("innodb_fragmentation_ratio", "gauge")
    )?;
    for idx in &report.indexes {
        let id_str = idx.index_id.to_string();
        let index_label = idx.index_name.as_deref().unwrap_or(&id_str);
        wprintln!(
            writer,
            "{}",
            prom::format_gauge(
                "innodb_fragmentation_ratio",
                &[("file", file), ("index", index_label)],
                idx.fragmentation
            )
        )?;
    }

    // innodb_garbage_ratio
    wprintln!(
        writer,
        "{}",
        prom::help_line("innodb_garbage_ratio", "Average garbage ratio per index")
    )?;
    wprintln!(
        writer,
        "{}",
        prom::type_line("innodb_garbage_ratio", "gauge")
    )?;
    for idx in &report.indexes {
        let id_str = idx.index_id.to_string();
        let index_label = idx.index_name.as_deref().unwrap_or(&id_str);
        wprintln!(
            writer,
            "{}",
            prom::format_gauge(
                "innodb_garbage_ratio",
                &[("file", file), ("index", index_label)],
                idx.avg_garbage_ratio
            )
        )?;
    }

    // innodb_index_pages
    wprintln!(
        writer,
        "{}",
        prom::help_line("innodb_index_pages", "Total pages per index")
    )?;
    wprintln!(writer, "{}", prom::type_line("innodb_index_pages", "gauge"))?;
    for idx in &report.indexes {
        let id_str = idx.index_id.to_string();
        let index_label = idx.index_name.as_deref().unwrap_or(&id_str);
        wprintln!(
            writer,
            "{}",
            prom::format_gauge_int(
                "innodb_index_pages",
                &[("file", file), ("index", index_label)],
                idx.total_pages
            )
        )?;
    }

    // innodb_scan_duration_seconds
    wprintln!(
        writer,
        "{}",
        prom::help_line(
            "innodb_scan_duration_seconds",
            "Time spent scanning the tablespace"
        )
    )?;
    wprintln!(
        writer,
        "{}",
        prom::type_line("innodb_scan_duration_seconds", "gauge")
    )?;
    wprintln!(
        writer,
        "{}",
        prom::format_gauge(
            "innodb_scan_duration_seconds",
            &[("file", file)],
            duration_secs
        )
    )?;

    Ok(())
}
