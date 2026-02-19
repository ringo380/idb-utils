use std::io::Write;

use colored::Colorize;
use rayon::prelude::*;
use serde::Serialize;

use crate::cli::{create_progress_bar, wprintln};
use crate::innodb::checksum::{validate_checksum, validate_lsn};
use crate::innodb::constants::*;
use crate::innodb::page::FilHeader;
use crate::innodb::page_types::PageType;
use crate::innodb::record::walk_compact_records;
use crate::innodb::tablespace::Tablespace;
use crate::IdbError;

/// Options for the `inno recover` subcommand.
pub struct RecoverOptions {
    /// Path to the InnoDB tablespace file (.ibd).
    pub file: String,
    /// Analyze a single page instead of full scan.
    pub page: Option<u64>,
    /// Show per-page details.
    pub verbose: bool,
    /// Emit output as JSON.
    pub json: bool,
    /// Extract records from corrupt pages with valid headers.
    pub force: bool,
    /// Override the auto-detected page size.
    pub page_size: Option<u32>,
    /// Path to MySQL keyring file for decrypting encrypted tablespaces.
    pub keyring: Option<String>,
    /// Number of threads for parallel processing (0 = auto-detect).
    pub threads: usize,
    /// Use memory-mapped I/O for file access.
    pub mmap: bool,
    /// Stream results incrementally for lower memory usage.
    pub streaming: bool,
}

/// Page integrity status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum PageStatus {
    Intact,
    Corrupt,
    Empty,
    Unreadable,
}

impl PageStatus {
    fn label(self) -> &'static str {
        match self {
            PageStatus::Intact => "intact",
            PageStatus::Corrupt => "CORRUPT",
            PageStatus::Empty => "empty",
            PageStatus::Unreadable => "UNREADABLE",
        }
    }
}

/// Top-level JSON output for the recovery report.
#[derive(Serialize)]
struct RecoverReport {
    file: String,
    file_size: u64,
    page_size: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_size_source: Option<String>,
    total_pages: u64,
    summary: RecoverSummary,
    recoverable_records: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    force_recoverable_records: Option<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pages: Vec<PageRecoveryInfo>,
}

/// Status counts by category.
#[derive(Serialize)]
struct RecoverSummary {
    intact: u64,
    corrupt: u64,
    empty: u64,
    unreadable: u64,
}

/// Per-page recovery info for JSON output.
#[derive(Serialize)]
struct PageRecoveryInfo {
    page_number: u64,
    status: PageStatus,
    page_type: String,
    checksum_valid: bool,
    lsn_valid: bool,
    lsn: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    record_count: Option<usize>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    records: Vec<RecoveredRecord>,
}

/// A single recovered record for verbose JSON output.
#[derive(Serialize)]
struct RecoveredRecord {
    offset: usize,
    heap_no: u16,
    delete_mark: bool,
    data_hex: String,
}

/// Computed statistics from page analysis, used by output functions.
struct RecoverStats {
    file_size: u64,
    page_size: u32,
    page_size_source: Option<String>,
    scan_count: u64,
    intact: u64,
    corrupt: u64,
    empty: u64,
    unreadable: u64,
    total_records: u64,
    corrupt_records: u64,
    corrupt_page_numbers: Vec<u64>,
    index_pages_total: u64,
    index_pages_recoverable: u64,
}

/// Internal per-page analysis result.
struct PageAnalysis {
    page_number: u64,
    status: PageStatus,
    page_type: PageType,
    checksum_valid: bool,
    lsn_valid: bool,
    lsn: u64,
    record_count: Option<usize>,
    records: Vec<RecoveredRecord>,
}

/// Try to open the tablespace, with smart page size fallback when page 0 is damaged.
fn open_tablespace(
    file: &str,
    page_size_override: Option<u32>,
    use_mmap: bool,
    writer: &mut dyn Write,
) -> Result<(Tablespace, Option<String>), IdbError> {
    if let Some(ps) = page_size_override {
        let ts = crate::cli::open_tablespace(file, Some(ps), use_mmap)?;
        return Ok((ts, Some("user-specified".to_string())));
    }

    match crate::cli::open_tablespace(file, None, use_mmap) {
        Ok(ts) => Ok((ts, None)),
        Err(_) => {
            // Page 0 may be corrupt — try common page sizes
            let candidates = [
                SIZE_PAGE_16K,
                SIZE_PAGE_8K,
                SIZE_PAGE_4K,
                SIZE_PAGE_32K,
                SIZE_PAGE_64K,
            ];

            let file_size = std::fs::metadata(file)
                .map_err(|e| IdbError::Io(format!("Cannot stat {}: {}", file, e)))?
                .len();

            for &ps in &candidates {
                if file_size >= ps as u64 && file_size % ps as u64 == 0 {
                    if let Ok(ts) = crate::cli::open_tablespace(file, Some(ps), use_mmap) {
                        let _ = wprintln!(
                            writer,
                            "Warning: auto-detect failed, using page size {} (file size divisible)",
                            ps
                        );
                        return Ok((ts, Some(format!("fallback ({})", ps))));
                    }
                }
            }

            // Last resort: default 16K
            let ts = crate::cli::open_tablespace(file, Some(SIZE_PAGE_DEFAULT), use_mmap)?;
            let _ = wprintln!(
                writer,
                "Warning: using default page size {} (no size divides evenly)",
                SIZE_PAGE_DEFAULT
            );
            Ok((ts, Some("default-fallback".to_string())))
        }
    }
}

/// Analyze a single page and return its status and recovery info.
fn analyze_page(
    page_data: &[u8],
    page_num: u64,
    page_size: u32,
    force: bool,
    verbose_json: bool,
    vendor_info: Option<&crate::innodb::vendor::VendorInfo>,
) -> PageAnalysis {
    // Check all-zeros (empty/allocated page)
    if page_data.iter().all(|&b| b == 0) {
        return PageAnalysis {
            page_number: page_num,
            status: PageStatus::Empty,
            page_type: PageType::Allocated,
            checksum_valid: true,
            lsn_valid: true,
            lsn: 0,
            record_count: None,
            records: Vec::new(),
        };
    }

    // Parse FIL header
    let header = match FilHeader::parse(page_data) {
        Some(h) => h,
        None => {
            return PageAnalysis {
                page_number: page_num,
                status: PageStatus::Unreadable,
                page_type: PageType::Unknown(0),
                checksum_valid: false,
                lsn_valid: false,
                lsn: 0,
                record_count: None,
                records: Vec::new(),
            };
        }
    };

    let csum_result = validate_checksum(page_data, page_size, vendor_info);
    let lsn_valid = validate_lsn(page_data, page_size);
    let status = if csum_result.valid && lsn_valid {
        PageStatus::Intact
    } else {
        PageStatus::Corrupt
    };

    // Count records on INDEX pages
    let (record_count, records) =
        if header.page_type == PageType::Index && (status == PageStatus::Intact || force) {
            let recs = walk_compact_records(page_data);
            let count = recs.len();
            let recovered = if verbose_json {
                extract_records(page_data, &recs, page_size)
            } else {
                Vec::new()
            };
            (Some(count), recovered)
        } else {
            (None, Vec::new())
        };

    PageAnalysis {
        page_number: page_num,
        status,
        page_type: header.page_type,
        checksum_valid: csum_result.valid,
        lsn_valid,
        lsn: header.lsn,
        record_count,
        records,
    }
}

/// Encode bytes as a lowercase hex string.
fn to_hex(data: &[u8]) -> String {
    let mut s = String::with_capacity(data.len() * 2);
    for &b in data {
        use std::fmt::Write;
        let _ = write!(s, "{:02x}", b);
    }
    s
}

/// Extract raw record bytes as hex from an INDEX page.
fn extract_records(
    page_data: &[u8],
    recs: &[crate::innodb::record::RecordInfo],
    page_size: u32,
) -> Vec<RecoveredRecord> {
    let ps = page_size as usize;
    let data_end = ps - SIZE_FIL_TRAILER;

    recs.iter()
        .enumerate()
        .map(|(i, rec)| {
            let start = rec.offset;
            let end = if i + 1 < recs.len() {
                // Next record's origin minus its extra header
                recs[i + 1].offset.saturating_sub(REC_N_NEW_EXTRA_BYTES)
            } else {
                // Use heap top or end of data area
                data_end
            };

            let end = end.min(data_end);
            let data = if start < end && end <= page_data.len() {
                &page_data[start..end]
            } else {
                &[]
            };

            RecoveredRecord {
                offset: rec.offset,
                heap_no: rec.header.heap_no(),
                delete_mark: rec.header.delete_mark(),
                data_hex: to_hex(data),
            }
        })
        .collect()
}

/// Run the recovery analysis and output results.
pub fn execute(opts: &RecoverOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    let (mut ts, page_size_source) =
        open_tablespace(&opts.file, opts.page_size, opts.mmap, writer)?;

    if let Some(ref keyring_path) = opts.keyring {
        crate::cli::setup_decryption(&mut ts, keyring_path)?;
    }

    let page_size = ts.page_size();
    let page_count = ts.page_count();
    let file_size = ts.file_size();

    let verbose_json = opts.verbose && opts.json;

    // Determine which pages to analyze
    let (start_page, end_page) = match opts.page {
        Some(p) => {
            if p >= page_count {
                return Err(IdbError::Parse(format!(
                    "Page {} out of range (tablespace has {} pages)",
                    p, page_count
                )));
            }
            (p, p + 1)
        }
        None => (0, page_count),
    };
    let scan_count = end_page - start_page;

    // Streaming mode: process one page at a time, output immediately
    if opts.streaming && opts.page.is_none() {
        return execute_streaming(
            opts,
            &mut ts,
            page_size,
            file_size,
            page_size_source,
            scan_count,
            verbose_json,
            writer,
        );
    }

    // Read all pages into memory for parallel processing
    let all_data = ts.read_all_pages()?;
    let ps = page_size as usize;
    let vendor_info = ts.vendor_info().clone();

    let pb = if !opts.json && scan_count > 1 {
        Some(create_progress_bar(scan_count, "pages"))
    } else {
        None
    };

    // Analyze pages in parallel
    let force = opts.force;
    let analyses: Vec<PageAnalysis> = (start_page..end_page)
        .into_par_iter()
        .map(|page_num| {
            let offset = page_num as usize * ps;
            if offset + ps > all_data.len() {
                return PageAnalysis {
                    page_number: page_num,
                    status: PageStatus::Unreadable,
                    page_type: PageType::Unknown(0),
                    checksum_valid: false,
                    lsn_valid: false,
                    lsn: 0,
                    record_count: None,
                    records: Vec::new(),
                };
            }
            let page_data = &all_data[offset..offset + ps];
            analyze_page(
                page_data,
                page_num,
                page_size,
                force,
                verbose_json,
                Some(&vendor_info),
            )
        })
        .collect();

    if let Some(pb) = pb {
        pb.set_position(scan_count);
        pb.finish_and_clear();
    }

    // Compute summary
    let mut intact = 0u64;
    let mut corrupt = 0u64;
    let mut empty = 0u64;
    let mut unreadable = 0u64;
    let mut total_records = 0u64;
    let mut corrupt_records = 0u64;
    let mut corrupt_page_numbers = Vec::new();
    let mut index_pages_total = 0u64;
    let mut index_pages_recoverable = 0u64;

    for a in &analyses {
        match a.status {
            PageStatus::Intact => intact += 1,
            PageStatus::Corrupt => {
                corrupt += 1;
                corrupt_page_numbers.push(a.page_number);
            }
            PageStatus::Empty => empty += 1,
            PageStatus::Unreadable => unreadable += 1,
        }

        if a.page_type == PageType::Index {
            index_pages_total += 1;
            if a.status == PageStatus::Intact {
                index_pages_recoverable += 1;
            }
            if let Some(count) = a.record_count {
                if a.status == PageStatus::Intact {
                    total_records += count as u64;
                } else {
                    corrupt_records += count as u64;
                }
            }
        }
    }

    // If --force, corrupt INDEX pages with records are also recoverable
    if opts.force {
        for a in &analyses {
            if a.page_type == PageType::Index
                && a.status == PageStatus::Corrupt
                && a.record_count.is_some()
            {
                index_pages_recoverable += 1;
            }
        }
    }

    let stats = RecoverStats {
        file_size,
        page_size,
        page_size_source,
        scan_count,
        intact,
        corrupt,
        empty,
        unreadable,
        total_records,
        corrupt_records,
        corrupt_page_numbers,
        index_pages_total,
        index_pages_recoverable,
    };

    if opts.json {
        output_json(opts, &analyses, &stats, writer)
    } else {
        output_text(opts, &analyses, &stats, writer)
    }
}

/// Streaming mode: process pages one at a time via `for_each_page()`, writing
/// per-page results immediately and accumulating running counters for the summary.
/// JSON output uses NDJSON (one JSON object per line per page, plus a final summary line).
#[allow(clippy::too_many_arguments)]
fn execute_streaming(
    opts: &RecoverOptions,
    ts: &mut Tablespace,
    page_size: u32,
    file_size: u64,
    page_size_source: Option<String>,
    scan_count: u64,
    verbose_json: bool,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    let force = opts.force;
    let vendor_info = ts.vendor_info().clone();

    // Running counters
    let mut intact = 0u64;
    let mut corrupt = 0u64;
    let mut empty = 0u64;
    let mut unreadable = 0u64;
    let mut total_records = 0u64;
    let mut corrupt_records = 0u64;
    let mut corrupt_page_numbers: Vec<u64> = Vec::new();
    let mut index_pages_total = 0u64;
    let mut index_pages_recoverable = 0u64;

    if !opts.json {
        wprintln!(writer, "Recovery Analysis: {}", opts.file)?;
        wprintln!(
            writer,
            "File size: {} bytes ({} pages x {} bytes)",
            file_size,
            scan_count,
            page_size
        )?;
        let source_note = match &page_size_source {
            Some(s) => format!(" ({})", s),
            None => " (auto-detected)".to_string(),
        };
        wprintln!(writer, "Page size: {}{}", page_size, source_note)?;
        wprintln!(writer)?;
    }

    ts.for_each_page(|page_num, page_data| {
        let a = analyze_page(
            page_data,
            page_num,
            page_size,
            force,
            verbose_json,
            Some(&vendor_info),
        );

        // Update running counters
        match a.status {
            PageStatus::Intact => intact += 1,
            PageStatus::Corrupt => {
                corrupt += 1;
                corrupt_page_numbers.push(a.page_number);
            }
            PageStatus::Empty => empty += 1,
            PageStatus::Unreadable => unreadable += 1,
        }

        if a.page_type == PageType::Index {
            index_pages_total += 1;
            if a.status == PageStatus::Intact {
                index_pages_recoverable += 1;
            }
            if force && a.status == PageStatus::Corrupt && a.record_count.is_some() {
                index_pages_recoverable += 1;
            }
            if let Some(count) = a.record_count {
                if a.status == PageStatus::Intact {
                    total_records += count as u64;
                } else {
                    corrupt_records += count as u64;
                }
            }
        }

        if opts.json {
            // NDJSON: emit per-page info (always in streaming, or only verbose)
            if opts.verbose {
                let info = PageRecoveryInfo {
                    page_number: a.page_number,
                    status: a.status,
                    page_type: a.page_type.name().to_string(),
                    checksum_valid: a.checksum_valid,
                    lsn_valid: a.lsn_valid,
                    lsn: a.lsn,
                    record_count: a.record_count,
                    records: a.records,
                };
                let line = serde_json::to_string(&info)
                    .map_err(|e| IdbError::Parse(format!("JSON error: {}", e)))?;
                wprintln!(writer, "{}", line)?;
            }
        } else if opts.verbose {
            // Text: per-page detail
            let status_str = match a.status {
                PageStatus::Intact => a.status.label().to_string(),
                PageStatus::Corrupt => format!("{}", a.status.label().red()),
                PageStatus::Empty => a.status.label().to_string(),
                PageStatus::Unreadable => format!("{}", a.status.label().red()),
            };

            let mut line = format!(
                "Page {:>4}: {:<14} {:<12} LSN={}",
                a.page_number,
                a.page_type.name(),
                status_str,
                a.lsn,
            );

            if let Some(count) = a.record_count {
                line.push_str(&format!("  records={}", count));
            }

            if a.status == PageStatus::Corrupt {
                if !a.checksum_valid {
                    line.push_str("  checksum mismatch");
                }
                if !a.lsn_valid {
                    line.push_str("  LSN mismatch");
                }
            }

            wprintln!(writer, "{}", line)?;
        }

        Ok(())
    })?;

    // Output summary
    let stats = RecoverStats {
        file_size,
        page_size,
        page_size_source,
        scan_count,
        intact,
        corrupt,
        empty,
        unreadable,
        total_records,
        corrupt_records,
        corrupt_page_numbers,
        index_pages_total,
        index_pages_recoverable,
    };

    if opts.json {
        // Emit a final summary line as NDJSON
        let all_records = stats.total_records + if opts.force { stats.corrupt_records } else { 0 };
        let force_recs = if stats.corrupt_records > 0 && !opts.force {
            Some(stats.corrupt_records)
        } else {
            None
        };

        let summary = serde_json::json!({
            "type": "summary",
            "file": opts.file,
            "file_size": stats.file_size,
            "page_size": stats.page_size,
            "page_size_source": stats.page_size_source,
            "total_pages": stats.scan_count,
            "summary": {
                "intact": stats.intact,
                "corrupt": stats.corrupt,
                "empty": stats.empty,
                "unreadable": stats.unreadable,
            },
            "recoverable_records": all_records,
            "force_recoverable_records": force_recs,
        });
        let line = serde_json::to_string(&summary)
            .map_err(|e| IdbError::Parse(format!("JSON error: {}", e)))?;
        wprintln!(writer, "{}", line)?;
    } else {
        // Print text summary (verbose per-page was already output above)
        if opts.verbose {
            wprintln!(writer)?;
        }
        output_text_summary(opts, &stats, writer)?;
    }

    Ok(())
}

fn output_text(
    opts: &RecoverOptions,
    analyses: &[PageAnalysis],
    stats: &RecoverStats,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    wprintln!(writer, "Recovery Analysis: {}", opts.file)?;
    wprintln!(
        writer,
        "File size: {} bytes ({} pages x {} bytes)",
        stats.file_size,
        stats.scan_count,
        stats.page_size
    )?;

    let source_note = match &stats.page_size_source {
        Some(s) => format!(" ({})", s),
        None => " (auto-detected)".to_string(),
    };
    wprintln!(writer, "Page size: {}{}", stats.page_size, source_note)?;
    wprintln!(writer)?;

    // Verbose: per-page detail
    if opts.verbose {
        for a in analyses {
            let status_str = match a.status {
                PageStatus::Intact => a.status.label().to_string(),
                PageStatus::Corrupt => format!("{}", a.status.label().red()),
                PageStatus::Empty => a.status.label().to_string(),
                PageStatus::Unreadable => format!("{}", a.status.label().red()),
            };

            let mut line = format!(
                "Page {:>4}: {:<14} {:<12} LSN={}",
                a.page_number,
                a.page_type.name(),
                status_str,
                a.lsn,
            );

            if let Some(count) = a.record_count {
                line.push_str(&format!("  records={}", count));
            }

            if a.status == PageStatus::Corrupt {
                if !a.checksum_valid {
                    line.push_str("  checksum mismatch");
                }
                if !a.lsn_valid {
                    line.push_str("  LSN mismatch");
                }
            }

            wprintln!(writer, "{}", line)?;
        }
        wprintln!(writer)?;
    }

    output_text_summary(opts, stats, writer)
}

/// Print the text-mode recovery summary (shared by streaming and non-streaming paths).
fn output_text_summary(
    opts: &RecoverOptions,
    stats: &RecoverStats,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    wprintln!(writer, "Page Status Summary:")?;
    wprintln!(writer, "  Intact:      {:>4} pages", stats.intact)?;
    if stats.corrupt > 0 {
        let pages_str = if stats.corrupt_page_numbers.len() <= 10 {
            let nums: Vec<String> = stats
                .corrupt_page_numbers
                .iter()
                .map(|n| n.to_string())
                .collect();
            format!(" (pages {})", nums.join(", "))
        } else {
            format!(" ({} pages)", stats.corrupt)
        };
        wprintln!(
            writer,
            "  Corrupt:     {:>4} pages{}",
            format!("{}", stats.corrupt).red(),
            pages_str
        )?;
    } else {
        wprintln!(writer, "  Corrupt:     {:>4} pages", stats.corrupt)?;
    }
    wprintln!(writer, "  Empty:       {:>4} pages", stats.empty)?;
    if stats.unreadable > 0 {
        wprintln!(
            writer,
            "  Unreadable:  {:>4} pages",
            format!("{}", stats.unreadable).red()
        )?;
    } else {
        wprintln!(writer, "  Unreadable:  {:>4} pages", stats.unreadable)?;
    }
    wprintln!(writer, "  Total:       {:>4} pages", stats.scan_count)?;
    wprintln!(writer)?;

    if stats.index_pages_total > 0 {
        wprintln!(
            writer,
            "Recoverable INDEX Pages: {} of {}",
            stats.index_pages_recoverable,
            stats.index_pages_total
        )?;
        wprintln!(writer, "  Total user records: {}", stats.total_records)?;
        if stats.corrupt_records > 0 && !opts.force {
            wprintln!(
                writer,
                "  Records on corrupt pages: {} (use --force to include)",
                stats.corrupt_records
            )?;
        } else if stats.corrupt_records > 0 {
            wprintln!(
                writer,
                "  Records on corrupt pages: {} (included with --force)",
                stats.corrupt_records
            )?;
        }
        wprintln!(writer)?;
    }

    let total_non_empty = stats.intact + stats.corrupt + stats.unreadable;
    if total_non_empty > 0 {
        let pct = (stats.intact as f64 / total_non_empty as f64) * 100.0;
        wprintln!(writer, "Overall: {:.1}% of pages intact", pct)?;
    }

    Ok(())
}

fn output_json(
    opts: &RecoverOptions,
    analyses: &[PageAnalysis],
    stats: &RecoverStats,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    let all_records = stats.total_records + if opts.force { stats.corrupt_records } else { 0 };

    let pages: Vec<PageRecoveryInfo> = if opts.verbose {
        analyses
            .iter()
            .map(|a| PageRecoveryInfo {
                page_number: a.page_number,
                status: a.status,
                page_type: a.page_type.name().to_string(),
                checksum_valid: a.checksum_valid,
                lsn_valid: a.lsn_valid,
                lsn: a.lsn,
                record_count: a.record_count,
                records: a
                    .records
                    .iter()
                    .map(|r| RecoveredRecord {
                        offset: r.offset,
                        heap_no: r.heap_no,
                        delete_mark: r.delete_mark,
                        data_hex: r.data_hex.clone(),
                    })
                    .collect(),
            })
            .collect()
    } else {
        Vec::new()
    };

    let force_recs = if stats.corrupt_records > 0 && !opts.force {
        Some(stats.corrupt_records)
    } else {
        None
    };

    let report = RecoverReport {
        file: opts.file.clone(),
        file_size: stats.file_size,
        page_size: stats.page_size,
        page_size_source: stats.page_size_source.clone(),
        total_pages: stats.scan_count,
        summary: RecoverSummary {
            intact: stats.intact,
            corrupt: stats.corrupt,
            empty: stats.empty,
            unreadable: stats.unreadable,
        },
        recoverable_records: all_records,
        force_recoverable_records: force_recs,
        pages,
    };

    let json = serde_json::to_string_pretty(&report)
        .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
    wprintln!(writer, "{}", json)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_status_label() {
        assert_eq!(PageStatus::Intact.label(), "intact");
        assert_eq!(PageStatus::Corrupt.label(), "CORRUPT");
        assert_eq!(PageStatus::Empty.label(), "empty");
        assert_eq!(PageStatus::Unreadable.label(), "UNREADABLE");
    }

    #[test]
    fn test_analyze_empty_page() {
        let page = vec![0u8; 16384];
        let result = analyze_page(&page, 0, 16384, false, false, None);
        assert_eq!(result.status, PageStatus::Empty);
        assert_eq!(result.page_type, PageType::Allocated);
    }

    #[test]
    fn test_analyze_short_page_is_unreadable() {
        let page = vec![0xFF; 10];
        let result = analyze_page(&page, 0, 16384, false, false, None);
        assert_eq!(result.status, PageStatus::Unreadable);
    }

    #[test]
    fn test_analyze_valid_index_page() {
        use byteorder::{BigEndian, ByteOrder};

        let mut page = vec![0u8; 16384];
        BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], 1);
        BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
        BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
        BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], 5000);
        BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855); // INDEX
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], 1);

        // Trailer
        let trailer = 16384 - SIZE_FIL_TRAILER;
        BigEndian::write_u32(&mut page[trailer + 4..], (5000u64 & 0xFFFFFFFF) as u32);

        // CRC-32C checksum
        let end = 16384 - SIZE_FIL_TRAILER;
        let crc1 = crc32c::crc32c(&page[FIL_PAGE_OFFSET..FIL_PAGE_FILE_FLUSH_LSN]);
        let crc2 = crc32c::crc32c(&page[FIL_PAGE_DATA..end]);
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_OR_CHKSUM..], crc1 ^ crc2);

        let result = analyze_page(&page, 1, 16384, false, false, None);
        assert_eq!(result.status, PageStatus::Intact);
        assert_eq!(result.page_type, PageType::Index);
        assert!(result.record_count.is_some());
    }

    #[test]
    fn test_analyze_corrupt_page() {
        use byteorder::{BigEndian, ByteOrder};

        let mut page = vec![0u8; 16384];
        BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], 1);
        BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], 5000);
        BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855);
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], 1);
        // Bad checksum — leave it as 0 while page has data
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_OR_CHKSUM..], 0xDEAD);

        let result = analyze_page(&page, 1, 16384, false, false, None);
        assert_eq!(result.status, PageStatus::Corrupt);
        // Without --force, no record count on corrupt pages
        assert!(result.record_count.is_none());
    }

    #[test]
    fn test_analyze_corrupt_page_with_force() {
        use byteorder::{BigEndian, ByteOrder};

        let mut page = vec![0u8; 16384];
        BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], 1);
        BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], 5000);
        BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855);
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], 1);
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_OR_CHKSUM..], 0xDEAD);

        let result = analyze_page(&page, 1, 16384, true, false, None);
        assert_eq!(result.status, PageStatus::Corrupt);
        // With --force, records are counted even on corrupt pages
        assert!(result.record_count.is_some());
    }
}
