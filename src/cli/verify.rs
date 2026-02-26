//! CLI implementation for the `inno verify` subcommand.
//!
//! Runs pure structural checks on a tablespace file without requiring
//! valid checksums. Checks page number sequence, space ID consistency,
//! LSN monotonicity, B+Tree level validity, page chain bounds, and
//! trailer LSN matching.

use std::io::Write;

use colored::Colorize;
use serde::Serialize;

use crate::cli::wprintln;
use crate::innodb::page::FilHeader;
use crate::innodb::verify::{
    extract_chain_file_info, verify_backup_chain, verify_tablespace, ChainReport, VerifyConfig,
};
use crate::IdbError;

/// Options for the `inno verify` subcommand.
pub struct VerifyOptions {
    /// Path to the InnoDB tablespace file (.ibd).
    pub file: String,
    /// Show per-page findings in text output.
    pub verbose: bool,
    /// Output in JSON format.
    pub json: bool,
    /// Override the auto-detected page size.
    pub page_size: Option<u32>,
    /// Path to MySQL keyring file for decrypting encrypted tablespaces.
    pub keyring: Option<String>,
    /// Use memory-mapped I/O for file access.
    pub mmap: bool,
    /// Path to redo log file for LSN continuity check.
    pub redo: Option<String>,
    /// Paths for backup chain verification.
    pub chain: Vec<String>,
}

/// Combined JSON output for verify with redo and/or chain.
#[derive(Debug, Serialize)]
struct FullVerifyReport {
    #[serde(flatten)]
    structural: crate::innodb::verify::VerifyReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    redo: Option<crate::innodb::verify::RedoVerifyResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    chain: Option<ChainReport>,
}

/// Run structural verification on a tablespace file.
pub fn execute(opts: &VerifyOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    // Handle --chain mode: verify backup chain across multiple files
    if !opts.chain.is_empty() {
        return execute_chain(opts, writer);
    }

    let mut ts = crate::cli::open_tablespace(&opts.file, opts.page_size, opts.mmap)?;

    if let Some(ref keyring_path) = opts.keyring {
        crate::cli::setup_decryption(&mut ts, keyring_path)?;
    }

    let page_size = ts.page_size();
    let page_count = ts.page_count();

    // Read all pages into a flat buffer
    let mut all_pages = Vec::with_capacity(page_size as usize * page_count as usize);
    for i in 0..page_count {
        let page = ts.read_page(i)?;
        all_pages.extend_from_slice(&page);
    }

    // Get space_id from page 0
    let space_id = if all_pages.len() >= page_size as usize {
        FilHeader::parse(&all_pages[..page_size as usize])
            .map(|h| h.space_id)
            .unwrap_or(0)
    } else {
        0
    };

    let config = VerifyConfig::default();
    let report = verify_tablespace(&all_pages, page_size, space_id, &opts.file, &config);

    // Redo log continuity check
    let redo_result = if let Some(ref redo_path) = opts.redo {
        Some(crate::innodb::verify::verify_redo_continuity(
            redo_path, &all_pages, page_size,
        )?)
    } else {
        None
    };

    let mut overall_passed = report.passed;
    if let Some(ref redo) = redo_result {
        if !redo.covers_tablespace {
            overall_passed = false;
        }
    }

    if opts.json {
        let full = FullVerifyReport {
            structural: report,
            redo: redo_result,
            chain: None,
        };
        let json = serde_json::to_string_pretty(&full)
            .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
        wprintln!(writer, "{}", json)?;
    } else {
        // Text output
        wprintln!(writer, "Structural Verification: {}", opts.file)?;
        wprintln!(writer, "  Page size:   {} bytes", report.page_size)?;
        wprintln!(writer, "  Total pages: {}", report.total_pages)?;
        wprintln!(writer)?;

        // Summary table
        wprintln!(
            writer,
            "  {:<30} {:>8} {:>8} {:>8}",
            "Check",
            "Checked",
            "Issues",
            "Status"
        )?;
        wprintln!(writer, "  {}", "-".repeat(60))?;
        for s in &report.summary {
            let status = if s.passed {
                "PASS".green().to_string()
            } else {
                "FAIL".red().to_string()
            };
            wprintln!(
                writer,
                "  {:<30} {:>8} {:>8} {:>8}",
                s.kind,
                s.pages_checked,
                s.issues_found,
                status
            )?;
        }
        wprintln!(writer)?;

        if opts.verbose && !report.findings.is_empty() {
            wprintln!(writer, "  Findings:")?;
            for f in &report.findings {
                wprintln!(writer, "    Page {:>4}: {}", f.page_number, f.message)?;
            }
            wprintln!(writer)?;
        }

        // Redo log continuity
        if let Some(ref redo) = redo_result {
            wprintln!(writer, "  Redo Log Continuity:")?;
            wprintln!(writer, "    Redo file:        {}", redo.redo_file)?;
            wprintln!(writer, "    Checkpoint LSN:   {}", redo.checkpoint_lsn)?;
            wprintln!(writer, "    Tablespace max:   {}", redo.tablespace_max_lsn)?;
            let redo_status = if redo.covers_tablespace {
                "PASS".green().to_string()
            } else {
                format!("{} (gap: {} bytes)", "FAIL".red(), redo.lsn_gap)
            };
            wprintln!(writer, "    Covers tablespace: {}", redo_status)?;
            wprintln!(writer)?;
        }

        let overall = if overall_passed {
            "PASS".green().to_string()
        } else {
            "FAIL".red().to_string()
        };
        wprintln!(writer, "  Overall: {}", overall)?;
    }

    if !overall_passed {
        return Err(IdbError::Argument("Verification failed".to_string()));
    }

    Ok(())
}

/// Execute backup chain verification mode.
fn execute_chain(opts: &VerifyOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    if opts.chain.len() < 2 {
        return Err(IdbError::Argument(
            "--chain requires at least 2 files".to_string(),
        ));
    }

    let mut files_info = Vec::new();

    for path in &opts.chain {
        let mut ts = crate::cli::open_tablespace(path, opts.page_size, opts.mmap)?;
        if let Some(ref keyring_path) = opts.keyring {
            crate::cli::setup_decryption(&mut ts, keyring_path)?;
        }

        let page_size = ts.page_size();
        let page_count = ts.page_count();
        let mut all_pages = Vec::with_capacity(page_size as usize * page_count as usize);
        for i in 0..page_count {
            let page = ts.read_page(i)?;
            all_pages.extend_from_slice(&page);
        }

        files_info.push(extract_chain_file_info(&all_pages, page_size, path));
    }

    let chain_report = verify_backup_chain(files_info);

    if opts.json {
        let json = serde_json::to_string_pretty(&chain_report)
            .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
        wprintln!(writer, "{}", json)?;
    } else {
        wprintln!(writer, "Backup Chain Verification")?;
        wprintln!(writer)?;

        wprintln!(
            writer,
            "  {:<40} {:>12} {:>16} {:>16}",
            "File",
            "Space ID",
            "Min LSN",
            "Max LSN"
        )?;
        wprintln!(writer, "  {}", "-".repeat(88))?;
        for f in &chain_report.files {
            wprintln!(
                writer,
                "  {:<40} {:>12} {:>16} {:>16}",
                f.file,
                f.space_id,
                f.min_lsn,
                f.max_lsn
            )?;
        }
        wprintln!(writer)?;

        if !chain_report.gaps.is_empty() {
            wprintln!(writer, "  {} detected:", "Gaps".red())?;
            for gap in &chain_report.gaps {
                wprintln!(
                    writer,
                    "    {} (max LSN {}) -> {} (min LSN {}): gap of {} bytes",
                    gap.from_file,
                    gap.from_max_lsn,
                    gap.to_file,
                    gap.to_min_lsn,
                    gap.gap_size
                )?;
            }
            wprintln!(writer)?;
        }

        let space_status = if chain_report.consistent_space_id {
            "PASS".green().to_string()
        } else {
            "FAIL (mixed space IDs)".red().to_string()
        };
        wprintln!(writer, "  Space ID consistency: {}", space_status)?;

        let chain_status = if chain_report.contiguous {
            "PASS".green().to_string()
        } else {
            "FAIL".red().to_string()
        };
        wprintln!(writer, "  Chain continuity:    {}", chain_status)?;
    }

    if !chain_report.contiguous || !chain_report.consistent_space_id {
        return Err(IdbError::Argument("Chain verification failed".to_string()));
    }

    Ok(())
}
