//! CLI implementation for the `inno health` subcommand.
//!
//! Computes per-index B+Tree health metrics (fill factor, fragmentation,
//! garbage ratio, tree depth) by scanning all INDEX pages in a tablespace.
//! Optionally computes bloat scores (A-F grades) and cardinality estimates.

use std::collections::HashMap;
use std::io::Write;
use std::time::Instant;

use crate::cli::wprintln;
use crate::innodb::health;
use crate::innodb::record::walk_compact_records;
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
    /// Compute index bloat scores.
    pub bloat: bool,
    /// Estimate cardinality of leading index columns.
    pub cardinality: bool,
    /// Number of leaf pages to sample per index for cardinality.
    pub sample_size: usize,
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
    let mut rtree_pages = 0u64;
    let mut lob_pages = 0u64;
    let mut undo_pages = 0u64;

    // Delete-mark counting (per index_id: deleted, total walked)
    let mut delete_counts: HashMap<u64, (u64, u64)> = HashMap::new();
    // Leaf page numbers per index (for cardinality sampling)
    let mut leaf_pages_by_index: HashMap<u64, Vec<u64>> = HashMap::new();

    ts.for_each_page(|page_num, data| {
        if data.iter().all(|&b| b == 0) {
            empty_pages += 1;
        } else if let Some(snap) = health::extract_index_page_snapshot(data, page_num) {
            // Track leaf pages for cardinality sampling
            if snap.level == 0 && opts.cardinality {
                leaf_pages_by_index
                    .entry(snap.index_id)
                    .or_default()
                    .push(snap.page_number);
            }

            // Count delete-marked records for bloat scoring (leaf pages only)
            if snap.level == 0 && opts.bloat {
                let recs = walk_compact_records(data);
                let total = recs.len() as u64;
                let deleted = recs.iter().filter(|r| r.header.delete_mark()).count() as u64;
                let entry = delete_counts.entry(snap.index_id).or_insert((0, 0));
                entry.0 += deleted;
                entry.1 += total;
            }

            snapshots.push(snap);
        }

        // Count special page types
        if let Some(fil) = crate::innodb::page::FilHeader::parse(data) {
            use crate::innodb::page_types::PageType;
            match fil.page_type {
                PageType::Rtree | PageType::EncryptedRtree => rtree_pages += 1,
                PageType::Blob
                | PageType::ZBlob
                | PageType::ZBlob2
                | PageType::LobFirst
                | PageType::LobData
                | PageType::LobIndex
                | PageType::ZlobFirst
                | PageType::ZlobData
                | PageType::ZlobFrag
                | PageType::ZlobFragEntry
                | PageType::ZlobIndex => lob_pages += 1,
                PageType::UndoLog => undo_pages += 1,
                _ => {}
            }
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
    report.summary.rtree_pages = rtree_pages;
    report.summary.lob_pages = lob_pages;
    report.summary.undo_pages = undo_pages;

    // Best-effort SDI index name resolution
    resolve_index_names(
        &opts.file,
        opts.page_size,
        opts.mmap,
        &opts.keyring,
        &mut report,
    );

    // Bloat scoring
    if opts.bloat {
        for idx in &mut report.indexes {
            let (deleted, total) = delete_counts.get(&idx.index_id).copied().unwrap_or((0, 0));
            let delete_mark_ratio = if total > 0 {
                deleted as f64 / total as f64
            } else {
                0.0
            };
            idx.delete_marked_records = Some(deleted);
            idx.total_walked_records = Some(total);
            idx.bloat = Some(health::score_bloat(idx, delete_mark_ratio));
        }
    }

    // Cardinality estimation (separate pass — needs random page access)
    if opts.cardinality {
        // Extract column layout from SDI (soft-fail for pre-8.0)
        let columns_opt = crate::innodb::export::extract_column_layout(&mut ts);
        if let Some((columns, _clustered_index_id)) = columns_opt {
            let col_name = columns.first().map(|c| c.name.clone()).unwrap_or_default();
            for idx in &mut report.indexes {
                if let Some(leaf_pages) = leaf_pages_by_index.get(&idx.index_id) {
                    idx.cardinality = health::estimate_cardinality(
                        &mut ts,
                        leaf_pages,
                        &columns,
                        &col_name,
                        page_size,
                        opts.sample_size,
                    );
                }
            }
        }
    }

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
        print_csv(writer, &report, opts.bloat, opts.cardinality)?;
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
                name_map.extend(sdi::build_index_name_map(&rec.data));
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

// ---------------------------------------------------------------------------
// Text output
// ---------------------------------------------------------------------------

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

        // Bloat score
        if let Some(ref bloat) = idx.bloat {
            wprintln!(
                writer,
                "  Bloat:          {} ({:.2})",
                bloat.grade,
                bloat.score
            )?;
            if let Some(ref rec) = bloat.recommendation {
                wprintln!(writer, "                  {}", rec)?;
            }
        }

        // Cardinality
        if let Some(ref card) = idx.cardinality {
            wprintln!(
                writer,
                "  Cardinality:    ~{} distinct (column: {}, {}/{} pages, {:.0}% confidence)",
                card.estimated_distinct,
                card.column_name,
                card.sampled_pages,
                card.total_leaf_pages,
                card.confidence * 100.0
            )?;
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
    if report.summary.rtree_pages > 0 {
        wprintln!(writer, "  RTREE pages:      {}", report.summary.rtree_pages)?;
    }
    if report.summary.lob_pages > 0 {
        wprintln!(writer, "  LOB/BLOB pages:   {}", report.summary.lob_pages)?;
    }
    if report.summary.undo_pages > 0 {
        wprintln!(writer, "  UNDO pages:       {}", report.summary.undo_pages)?;
    }
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

// ---------------------------------------------------------------------------
// CSV output
// ---------------------------------------------------------------------------

fn print_csv(
    writer: &mut dyn Write,
    report: &health::HealthReport,
    bloat: bool,
    cardinality: bool,
) -> Result<(), IdbError> {
    let mut header = String::from(
        "index_id,index_name,tree_depth,total_pages,leaf_pages,avg_fill_factor,garbage_ratio,fragmentation",
    );
    if bloat {
        header.push_str(",bloat_score,bloat_grade,delete_marked,total_walked");
    }
    if cardinality {
        header.push_str(",est_cardinality,cardinality_confidence");
    }
    wprintln!(writer, "{}", header)?;

    for idx in &report.indexes {
        let mut row = format!(
            "{},{},{},{},{},{},{},{}",
            idx.index_id,
            crate::cli::csv_escape(idx.index_name.as_deref().unwrap_or("")),
            idx.tree_depth,
            idx.total_pages,
            idx.leaf_pages,
            idx.avg_fill_factor,
            idx.avg_garbage_ratio,
            idx.fragmentation
        );
        if bloat {
            if let Some(ref b) = idx.bloat {
                row.push_str(&format!(
                    ",{},{},{}",
                    b.score,
                    b.grade,
                    idx.delete_marked_records.unwrap_or(0)
                ));
                row.push_str(&format!(",{}", idx.total_walked_records.unwrap_or(0)));
            } else {
                row.push_str(",,,,");
            }
        }
        if cardinality {
            if let Some(ref c) = idx.cardinality {
                row.push_str(&format!(",{},{}", c.estimated_distinct, c.confidence));
            } else {
                row.push_str(",,");
            }
        }
        wprintln!(writer, "{}", row)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Prometheus output
// ---------------------------------------------------------------------------

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

    // innodb_bloat_score (only when bloat data present)
    let has_bloat = report.indexes.iter().any(|i| i.bloat.is_some());
    if has_bloat {
        wprintln!(
            writer,
            "{}",
            prom::help_line("innodb_bloat_score", "Index bloat score (0.0-1.0)")
        )?;
        wprintln!(writer, "{}", prom::type_line("innodb_bloat_score", "gauge"))?;
        for idx in &report.indexes {
            if let Some(ref b) = idx.bloat {
                let id_str = idx.index_id.to_string();
                let index_label = idx.index_name.as_deref().unwrap_or(&id_str);
                wprintln!(
                    writer,
                    "{}",
                    prom::format_gauge(
                        "innodb_bloat_score",
                        &[("file", file), ("index", index_label)],
                        b.score
                    )
                )?;
            }
        }

        wprintln!(
            writer,
            "{}",
            prom::help_line(
                "innodb_delete_mark_ratio",
                "Ratio of delete-marked records per index"
            )
        )?;
        wprintln!(
            writer,
            "{}",
            prom::type_line("innodb_delete_mark_ratio", "gauge")
        )?;
        for idx in &report.indexes {
            if let Some(ref b) = idx.bloat {
                let id_str = idx.index_id.to_string();
                let index_label = idx.index_name.as_deref().unwrap_or(&id_str);
                wprintln!(
                    writer,
                    "{}",
                    prom::format_gauge(
                        "innodb_delete_mark_ratio",
                        &[("file", file), ("index", index_label)],
                        b.components.delete_mark_ratio
                    )
                )?;
            }
        }
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
