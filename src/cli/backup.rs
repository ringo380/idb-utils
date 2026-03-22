//! CLI implementation for the `inno backup` subcommand.
//!
//! Two modes: `diff` compares page LSNs between a base and current tablespace
//! to detect changed pages; `chain` validates XtraBackup backup chain LSN continuity.

use std::io::Write;
use std::path::Path;

use crate::cli::{csv_escape, open_tablespace, setup_decryption, wprintln};
use crate::innodb::backup;
use crate::IdbError;

/// Options for `inno backup diff`.
pub struct BackupDiffOptions {
    pub base: String,
    pub current: String,
    pub verbose: bool,
    pub json: bool,
    pub csv: bool,
    pub page_size: Option<u32>,
    pub keyring: Option<String>,
    pub mmap: bool,
}

/// Options for `inno backup chain`.
pub struct BackupChainOptions {
    pub dir: String,
    pub verbose: bool,
    pub json: bool,
    pub csv: bool,
}

/// Execute the `inno backup diff` subcommand.
pub fn execute_diff(opts: &BackupDiffOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    let mut base = open_tablespace(&opts.base, opts.page_size, opts.mmap)?;
    if let Some(ref kp) = opts.keyring {
        setup_decryption(&mut base, kp)?;
    }
    let mut current = open_tablespace(&opts.current, opts.page_size, opts.mmap)?;
    if let Some(ref kp) = opts.keyring {
        setup_decryption(&mut current, kp)?;
    }

    let report = backup::diff_backup_lsn(
        &mut base,
        &mut current,
        &opts.base,
        &opts.current,
        opts.verbose,
    )?;

    if opts.json {
        let json =
            serde_json::to_string_pretty(&report).map_err(|e| IdbError::Parse(e.to_string()))?;
        wprintln!(writer, "{}", json)?;
    } else if opts.csv {
        print_diff_csv(writer, &report)?;
    } else {
        print_diff_text(writer, &report)?;
    }

    Ok(())
}

/// Execute the `inno backup chain` subcommand.
pub fn execute_chain(opts: &BackupChainOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    let report = backup::scan_backup_chain(Path::new(&opts.dir))?;

    if opts.json {
        let json =
            serde_json::to_string_pretty(&report).map_err(|e| IdbError::Parse(e.to_string()))?;
        wprintln!(writer, "{}", json)?;
    } else if opts.csv {
        print_chain_csv(writer, &report)?;
    } else {
        print_chain_text(writer, &report, opts.verbose)?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Text output
// ---------------------------------------------------------------------------

fn print_diff_text(
    writer: &mut dyn Write,
    report: &backup::BackupDiffReport,
) -> Result<(), IdbError> {
    wprintln!(
        writer,
        "Backup Delta: {} -> {} (Space ID: {})",
        report.base_file,
        report.current_file,
        report.space_id
    )?;
    wprintln!(
        writer,
        "  Base max LSN: {:>14}  ->  Current max LSN: {:>14}",
        report.base_max_lsn,
        report.current_max_lsn
    )?;
    if report.current_max_lsn > report.base_max_lsn {
        wprintln!(
            writer,
            "  LSN advance: {}",
            report.current_max_lsn - report.base_max_lsn
        )?;
    }
    wprintln!(writer)?;

    let total = report.base_page_count.max(report.current_page_count);
    let s = &report.summary;
    wprintln!(writer, "  Pages: {} total", total)?;
    wprintln!(
        writer,
        "    Unchanged:  {:>6} ({:>5.1}%)",
        s.unchanged,
        pct(s.unchanged, total)
    )?;
    wprintln!(
        writer,
        "    Modified:   {:>6} ({:>5.1}%)",
        s.modified,
        pct(s.modified, total)
    )?;
    if s.added > 0 {
        wprintln!(
            writer,
            "    Added:      {:>6} ({:>5.1}%)",
            s.added,
            pct(s.added, total)
        )?;
    }
    if s.removed > 0 {
        wprintln!(
            writer,
            "    Removed:    {:>6} ({:>5.1}%)",
            s.removed,
            pct(s.removed, total)
        )?;
    }
    if s.regressed > 0 {
        wprintln!(
            writer,
            "    Regressed:  {:>6} ({:>5.1}%)",
            s.regressed,
            pct(s.regressed, total)
        )?;
    }
    wprintln!(writer)?;

    if !report.modified_page_types.is_empty() {
        wprintln!(writer, "  Modified by type:")?;
        for (pt, count) in &report.modified_page_types {
            wprintln!(writer, "    {:<16} {}", pt, count)?;
        }
        wprintln!(writer)?;
    }

    // Verbose: per-page details
    if !report.pages.is_empty() {
        wprintln!(
            writer,
            "  {:>8}  {:>12}  {:>14}  {:>14}  {:>10}",
            "Page",
            "Type",
            "Base LSN",
            "Current LSN",
            "Status"
        )?;
        wprintln!(
            writer,
            "  {:>8}  {:>12}  {:>14}  {:>14}  {:>10}",
            "--------",
            "------------",
            "--------------",
            "--------------",
            "----------"
        )?;
        for p in &report.pages {
            if p.status == backup::PageChangeStatus::Unchanged {
                continue; // skip unchanged in verbose output
            }
            let status_str = match p.status {
                backup::PageChangeStatus::Unchanged => "unchanged",
                backup::PageChangeStatus::Modified => "modified",
                backup::PageChangeStatus::Added => "added",
                backup::PageChangeStatus::Removed => "removed",
                backup::PageChangeStatus::Regressed => "regressed",
            };
            wprintln!(
                writer,
                "  {:>8}  {:>12}  {:>14}  {:>14}  {:>10}",
                p.page_number,
                p.page_type,
                p.base_lsn
                    .map(|l| l.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                p.current_lsn
                    .map(|l| l.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                status_str,
            )?;
        }
        wprintln!(writer)?;
    }

    Ok(())
}

fn print_chain_text(
    writer: &mut dyn Write,
    report: &backup::BackupChainReport,
    verbose: bool,
) -> Result<(), IdbError> {
    wprintln!(writer, "Backup Chain: {}", report.chain_dir)?;
    wprintln!(writer, "  Backups found: {}", report.backups.len())?;
    wprintln!(writer)?;

    if !report.backups.is_empty() {
        wprintln!(
            writer,
            "  {:>3}  {:<16}  {:>14}  {:>14}  {:>8}",
            "#",
            "Type",
            "From LSN",
            "To LSN",
            "Status"
        )?;
        wprintln!(
            writer,
            "  {:>3}  {:<16}  {:>14}  {:>14}  {:>8}",
            "---",
            "----------------",
            "--------------",
            "--------------",
            "--------"
        )?;

        for (i, b) in report.backups.iter().enumerate() {
            let status = chain_entry_status(report, i);
            wprintln!(
                writer,
                "  {:>3}  {:<16}  {:>14}  {:>14}  {:>8}",
                i + 1,
                b.backup_type,
                b.from_lsn,
                b.to_lsn,
                status,
            )?;
            if verbose {
                wprintln!(writer, "       Path: {}", b.path.display())?;
                if let Some(last) = b.last_lsn {
                    wprintln!(writer, "       Last LSN: {}", last)?;
                }
            }
        }
        wprintln!(writer)?;
    }

    // Chain verdict
    if report.chain_valid {
        if let Some((min, max)) = report.total_lsn_range {
            wprintln!(writer, "  Chain: VALID (LSN range {} -> {})", min, max)?;
        } else {
            wprintln!(writer, "  Chain: VALID")?;
        }
    } else {
        wprintln!(writer, "  Chain: INVALID")?;
    }

    // Anomalies
    if !report.anomalies.is_empty() {
        wprintln!(writer)?;
        wprintln!(writer, "  Anomalies:")?;
        for a in &report.anomalies {
            let kind = match a.kind {
                backup::ChainAnomalyKind::Gap => "GAP",
                backup::ChainAnomalyKind::Overlap => "OVERLAP",
                backup::ChainAnomalyKind::MissingFull => "MISSING_FULL",
            };
            wprintln!(writer, "    [{}] {}", kind, a.message)?;
        }
    }

    wprintln!(writer)?;
    Ok(())
}

/// Determine the status label for a backup entry in the chain.
fn chain_entry_status(report: &backup::BackupChainReport, index: usize) -> &'static str {
    // Check if any anomaly involves this entry
    for a in &report.anomalies {
        if a.between.0 == index || a.between.1 == index {
            return match a.kind {
                backup::ChainAnomalyKind::Gap => "GAP",
                backup::ChainAnomalyKind::Overlap => "OVERLAP",
                backup::ChainAnomalyKind::MissingFull => "WARN",
            };
        }
    }
    "OK"
}

// ---------------------------------------------------------------------------
// CSV output
// ---------------------------------------------------------------------------

fn print_diff_csv(
    writer: &mut dyn Write,
    report: &backup::BackupDiffReport,
) -> Result<(), IdbError> {
    wprintln!(
        writer,
        "page_number,status,page_type,base_lsn,current_lsn,checksum_valid"
    )?;
    for p in &report.pages {
        let status = match p.status {
            backup::PageChangeStatus::Unchanged => "unchanged",
            backup::PageChangeStatus::Modified => "modified",
            backup::PageChangeStatus::Added => "added",
            backup::PageChangeStatus::Removed => "removed",
            backup::PageChangeStatus::Regressed => "regressed",
        };
        wprintln!(
            writer,
            "{},{},{},{},{},{}",
            p.page_number,
            status,
            csv_escape(&p.page_type),
            p.base_lsn.map(|l| l.to_string()).unwrap_or_default(),
            p.current_lsn.map(|l| l.to_string()).unwrap_or_default(),
            p.checksum_valid,
        )?;
    }
    Ok(())
}

fn print_chain_csv(
    writer: &mut dyn Write,
    report: &backup::BackupChainReport,
) -> Result<(), IdbError> {
    wprintln!(writer, "index,backup_type,from_lsn,to_lsn,last_lsn,path")?;
    for (i, b) in report.backups.iter().enumerate() {
        wprintln!(
            writer,
            "{},{},{},{},{},{}",
            i + 1,
            csv_escape(&b.backup_type),
            b.from_lsn,
            b.to_lsn,
            b.last_lsn.map(|l| l.to_string()).unwrap_or_default(),
            csv_escape(&b.path.to_string_lossy()),
        )?;
    }
    Ok(())
}

fn pct(part: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        (part as f64 / total as f64) * 100.0
    }
}
