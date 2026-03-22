//! CLI implementation for the `inno simulate` subcommand.
//!
//! Simulates InnoDB crash recovery levels 1-6 (`innodb_force_recovery`) to
//! predict data recoverability at each level without modifying any files.

use std::io::Write;
use std::path::Path;

use rayon::prelude::*;

use crate::cli::{create_progress_bar, csv_escape, open_tablespace, setup_decryption, wprintln};
use crate::innodb::sdi;
use crate::innodb::simulate::{self, SimulationReport};
use crate::util::fs::find_tablespace_files;
use crate::IdbError;

/// Options for the `inno simulate` subcommand.
pub struct SimulateOptions {
    /// Path to a single InnoDB tablespace file (.ibd).
    pub file: Option<String>,
    /// Path to a MySQL data directory (simulates all tablespaces).
    pub datadir: Option<String>,
    /// Show detailed analysis at a specific recovery level (1-6).
    pub level: Option<u8>,
    /// Show per-page details.
    pub verbose: bool,
    /// Output in JSON format.
    pub json: bool,
    /// Output in CSV format.
    pub csv: bool,
    /// Override page size.
    pub page_size: Option<u32>,
    /// Path to MySQL keyring file.
    pub keyring: Option<String>,
    /// Use memory-mapped I/O.
    pub mmap: bool,
    /// Maximum directory recursion depth.
    pub depth: Option<u32>,
}

/// Execute the `inno simulate` subcommand.
pub fn execute(opts: &SimulateOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    if let Some(ref file) = opts.file {
        execute_single(opts, file, writer)
    } else if let Some(ref datadir) = opts.datadir {
        execute_directory(opts, datadir, writer)
    } else {
        Err(IdbError::Argument(
            "Either --file or --datadir must be specified".to_string(),
        ))
    }
}

/// Simulate recovery for a single tablespace file.
fn execute_single(
    opts: &SimulateOptions,
    file: &str,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    let mut ts = open_tablespace(file, opts.page_size, opts.mmap)?;
    if let Some(ref kp) = opts.keyring {
        setup_decryption(&mut ts, kp)?;
    }

    // Try to extract SDI for name resolution (soft-fail)
    let sdi_json = extract_sdi_json(&mut ts);

    let report = simulate::simulate_recovery(&mut ts, sdi_json.as_deref(), file, opts.verbose)?;

    if opts.json {
        let json =
            serde_json::to_string_pretty(&report).map_err(|e| IdbError::Parse(e.to_string()))?;
        wprintln!(writer, "{}", json)?;
    } else if opts.csv {
        print_csv(writer, &[report], opts.level)?;
    } else {
        print_text(writer, &report, opts.level)?;
    }

    Ok(())
}

/// Simulate recovery for all tablespaces in a directory.
fn execute_directory(
    opts: &SimulateOptions,
    datadir: &str,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    let files = find_tablespace_files(Path::new(datadir), &["ibd"], opts.depth)?;

    if files.is_empty() {
        wprintln!(writer, "No .ibd files found in {}", datadir)?;
        return Ok(());
    }

    let pb = create_progress_bar(files.len() as u64, "files");

    let reports: Vec<SimulationReport> = files
        .par_iter()
        .filter_map(|path| {
            let file_str = path.to_str()?;
            let result = simulate_file(file_str, opts);
            pb.inc(1);
            result.ok()
        })
        .collect();

    pb.finish_and_clear();

    if opts.json {
        let json =
            serde_json::to_string_pretty(&reports).map_err(|e| IdbError::Parse(e.to_string()))?;
        wprintln!(writer, "{}", json)?;
    } else if opts.csv {
        print_csv(writer, &reports, opts.level)?;
    } else {
        print_directory_text(writer, &reports, opts.level)?;
    }

    Ok(())
}

/// Simulate recovery for a single file (used in parallel directory scan).
fn simulate_file(file: &str, opts: &SimulateOptions) -> Result<SimulationReport, IdbError> {
    let mut ts = open_tablespace(file, opts.page_size, opts.mmap)?;
    if let Some(ref kp) = opts.keyring {
        setup_decryption(&mut ts, kp)?;
    }
    let sdi_json = extract_sdi_json(&mut ts);
    simulate::simulate_recovery(&mut ts, sdi_json.as_deref(), file, false)
}

/// Try to extract SDI JSON from a tablespace (returns None on failure).
fn extract_sdi_json(ts: &mut crate::innodb::tablespace::Tablespace) -> Option<String> {
    let sdi_pages = sdi::find_sdi_pages(ts).ok()?;
    if sdi_pages.is_empty() {
        return None;
    }
    let records = sdi::extract_sdi_from_pages(ts, &sdi_pages).ok()?;
    records
        .into_iter()
        .find(|r| r.sdi_type == 1)
        .map(|r| r.data)
}

// ---------------------------------------------------------------------------
// Text output
// ---------------------------------------------------------------------------

/// Print single-file simulation report as human-readable text.
fn print_text(
    writer: &mut dyn Write,
    report: &SimulationReport,
    filter_level: Option<u8>,
) -> Result<(), IdbError> {
    wprintln!(writer, "Crash Recovery Simulation: {}", report.file)?;
    wprintln!(
        writer,
        "  Pages: {} total ({} intact, {} corrupt, {} empty)",
        report.total_pages,
        report.page_summary.intact,
        report.page_summary.corrupt,
        report.page_summary.empty,
    )?;
    wprintln!(writer, "  Vendor: {}", report.vendor)?;
    wprintln!(writer)?;

    // Recommended level
    let plan = &report.plan;
    wprintln!(
        writer,
        "  Recommended Recovery Level: {} ({})",
        plan.recommended_level,
        plan.levels[plan.recommended_level as usize].name,
    )?;
    wprintln!(writer, "  Rationale: {}", plan.rationale)?;
    wprintln!(writer)?;

    // Level comparison table
    if filter_level.is_none() {
        wprintln!(
            writer,
            "  {:>5}  {:<30}  {:>9}  {:>12}",
            "Level",
            "Name",
            "Tables OK",
            "Data at Risk"
        )?;
        wprintln!(
            writer,
            "  {:>5}  {:<30}  {:>9}  {:>12}",
            "-----",
            "------------------------------",
            "---------",
            "------------"
        )?;

        for la in &plan.levels {
            let marker = if la.level == plan.recommended_level {
                "*"
            } else {
                " "
            };
            let suffix = if la.level == plan.recommended_level {
                "  <-- recommended"
            } else {
                ""
            };
            wprintln!(
                writer,
                "  {:>4}{}  {:<30}  {:>4}/{:<4}  {:>11.1}%{}",
                la.level,
                marker,
                la.name,
                la.tables_accessible,
                la.total_tables,
                la.pct_overall_risk,
                suffix,
            )?;
        }
        wprintln!(writer)?;
    }

    // Per-table impact
    if !report.tables.is_empty() {
        wprintln!(
            writer,
            "  {:<30}  {:>13}  {:>15}  {:>9}",
            "Table",
            "Corrupt Pages",
            "Records at Risk",
            "Min Level"
        )?;
        wprintln!(
            writer,
            "  {:<30}  {:>13}  {:>15}  {:>9}",
            "------------------------------",
            "-------------",
            "---------------",
            "---------"
        )?;

        for table in &report.tables {
            let name = table.table_name.as_deref().unwrap_or("(unknown)");
            let total_corrupt: u64 = table.indexes.iter().map(|i| i.corrupt_pages).sum();
            let records_at_risk = table
                .data_loss_by_level
                .get(&1)
                .map(|e| e.records_at_risk)
                .unwrap_or(0);
            // Min level needed for this table = max of levels needed for its corrupt pages
            let min_level = if total_corrupt > 0 { 1 } else { 0 };

            wprintln!(
                writer,
                "  {:<30}  {:>13}  {:>15}  {:>9}",
                name,
                total_corrupt,
                format!("~{}", records_at_risk),
                min_level,
            )?;
        }
        wprintln!(writer)?;
    }

    // Verbose: per-page details
    if !report.pages.is_empty() {
        wprintln!(writer, "  Page Details:")?;
        for p in &report.pages {
            if p.min_recovery_level > 0 || filter_level.is_some() {
                if let Some(fl) = filter_level {
                    if p.min_recovery_level > fl {
                        continue;
                    }
                }
                wprintln!(
                    writer,
                    "    Page {:>6}  {:>12}  checksum={}  level_needed={}{}",
                    p.page_number,
                    p.page_type,
                    if p.checksum_valid { "OK" } else { "FAIL" },
                    p.min_recovery_level,
                    p.corruption_pattern
                        .as_ref()
                        .map(|c| format!("  pattern={}", c))
                        .unwrap_or_default(),
                )?;
            }
        }
        wprintln!(writer)?;
    }

    Ok(())
}

/// Print directory-level summary.
fn print_directory_text(
    writer: &mut dyn Write,
    reports: &[SimulationReport],
    filter_level: Option<u8>,
) -> Result<(), IdbError> {
    let total_files = reports.len();
    let files_needing_recovery = reports
        .iter()
        .filter(|r| r.plan.recommended_level > 0)
        .count();
    let max_recommended = reports
        .iter()
        .map(|r| r.plan.recommended_level)
        .max()
        .unwrap_or(0);

    wprintln!(writer, "Crash Recovery Simulation: {} files", total_files)?;
    wprintln!(
        writer,
        "  Files needing recovery: {}",
        files_needing_recovery
    )?;
    wprintln!(writer, "  Maximum recommended level: {}", max_recommended)?;
    wprintln!(writer)?;

    if files_needing_recovery > 0 {
        wprintln!(
            writer,
            "  {:<50}  {:>7}  {:>13}  {:>15}",
            "File",
            "Level",
            "Corrupt Pages",
            "Records at Risk"
        )?;
        wprintln!(
            writer,
            "  {:<50}  {:>7}  {:>13}  {:>15}",
            "--------------------------------------------------",
            "-------",
            "-------------",
            "---------------"
        )?;

        for report in reports {
            if report.plan.recommended_level == 0 && filter_level.is_none() {
                continue;
            }
            if let Some(fl) = filter_level {
                if report.plan.recommended_level > fl {
                    continue;
                }
            }
            let total_records_at_risk: u64 = report
                .tables
                .iter()
                .filter_map(|t| t.data_loss_by_level.get(&1))
                .map(|e| e.records_at_risk)
                .sum();
            wprintln!(
                writer,
                "  {:<50}  {:>7}  {:>13}  {:>15}",
                report.file,
                report.plan.recommended_level,
                report.page_summary.corrupt,
                format!("~{}", total_records_at_risk),
            )?;
        }
        wprintln!(writer)?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// CSV output
// ---------------------------------------------------------------------------

/// Print CSV output for one or more reports.
fn print_csv(
    writer: &mut dyn Write,
    reports: &[SimulationReport],
    filter_level: Option<u8>,
) -> Result<(), IdbError> {
    wprintln!(
        writer,
        "file,table,index,index_id,level,accessible,corrupt_pages,records_at_risk,pct_at_risk"
    )?;

    for report in reports {
        for table in &report.tables {
            let table_name = table.table_name.as_deref().unwrap_or("");
            for index in &table.indexes {
                let index_name = index.index_name.as_deref().unwrap_or("");
                for level in 0..=6u8 {
                    if let Some(fl) = filter_level {
                        if level != fl {
                            continue;
                        }
                    }
                    let records_at_risk = index
                        .lost_records_by_level
                        .get(&level)
                        .copied()
                        .unwrap_or(0);
                    let total = index.total_records + records_at_risk;
                    let pct = if total > 0 {
                        (records_at_risk as f64 / total as f64) * 100.0
                    } else {
                        0.0
                    };
                    let accessible = if level == 0 && index.corrupt_pages > 0 {
                        "false"
                    } else {
                        "true"
                    };
                    wprintln!(
                        writer,
                        "{},{},{},{},{},{},{},{},{:.2}",
                        csv_escape(&report.file),
                        csv_escape(table_name),
                        csv_escape(index_name),
                        index.index_id,
                        level,
                        accessible,
                        index.corrupt_pages,
                        records_at_risk,
                        pct,
                    )?;
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::innodb::simulate::SimulationReport;

    #[test]
    fn test_extract_sdi_json_no_sdi() {
        // A tablespace without SDI pages should return None
        use crate::innodb::constants::*;
        use crate::innodb::tablespace::Tablespace;
        use byteorder::{BigEndian, ByteOrder};

        let page_size = 16384u32;
        let ps = page_size as usize;

        // Build a minimal valid tablespace (FSP_HDR + 1 INDEX page)
        let mut fsp = vec![0u8; ps];
        BigEndian::write_u32(&mut fsp[FIL_PAGE_OFFSET..], 0);
        BigEndian::write_u16(&mut fsp[FIL_PAGE_TYPE..], 8); // FSP_HDR
        BigEndian::write_u32(&mut fsp[FIL_PAGE_SPACE_ID..], 1);
        BigEndian::write_u64(&mut fsp[FIL_PAGE_LSN..], 1000);
        BigEndian::write_u32(&mut fsp[ps - 4..], 1000);
        let crc1 = crc32c::crc32c(&fsp[4..26]);
        let crc2 = crc32c::crc32c(&fsp[38..ps - 8]);
        BigEndian::write_u32(&mut fsp[0..4], crc1 ^ crc2);
        BigEndian::write_u32(&mut fsp[ps - 8..ps - 4], crc1 ^ crc2);

        let mut ts = Tablespace::from_bytes(fsp).unwrap();
        assert!(extract_sdi_json(&mut ts).is_none());
    }

    #[test]
    fn test_json_output_format() {
        // Build a minimal report and verify JSON serialization
        let report = SimulationReport {
            file: "test.ibd".to_string(),
            page_size: 16384,
            total_pages: 10,
            vendor: "MySQL".to_string(),
            page_summary: simulate::PageSummary {
                intact: 10,
                corrupt: 0,
                empty: 0,
                unreadable: 0,
            },
            pages: Vec::new(),
            tables: Vec::new(),
            plan: simulate::RecoveryPlan {
                recommended_level: 0,
                rationale: "No corrupt pages.".to_string(),
                levels: Vec::new(),
            },
        };

        let json = serde_json::to_string_pretty(&report).unwrap();
        assert!(json.contains("\"recommended_level\": 0"));
        assert!(json.contains("\"file\": \"test.ibd\""));
    }
}
