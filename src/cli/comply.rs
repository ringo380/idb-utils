//! CLI implementation for the `inno comply` subcommand.
//!
//! Forensic deletion-verification and data-residue scanning — the inverse of
//! `inno undelete`. Three modes, exactly one per invocation:
//!
//! - `--verify-deleted --where <col>=<value>` — confirm a value has been purged
//!   from every InnoDB-retained location (live/delete-marked/free-list/undo records).
//! - `--scan-residue --pattern <needle>` — raw literal byte sweep across all pages.
//! - `--encryption-audit` — report encrypted vs plaintext pages and key availability.
//!
//! This verifies residue within the file passed in. It cannot see the OS page cache,
//! replicas, other backups, or binlog archives, and does not certify legal compliance.

use std::io::Write;

use crate::cli::wprintln;
use crate::innodb::compliance::{encryption_audit, scan_residue, verify_deleted, Pattern};
use crate::IdbError;

/// Options for the `inno comply` subcommand.
pub struct ComplyOptions {
    /// Path to the InnoDB tablespace file (.ibd).
    pub file: String,
    /// Mode: verify a value has been deleted everywhere.
    pub verify_deleted: bool,
    /// Mode: raw byte-pattern residue scan.
    pub scan_residue: bool,
    /// Mode: encryption audit.
    pub encryption_audit: bool,
    /// Optional table-name filter (verify mode); errors if it mismatches SDI.
    pub table: Option<String>,
    /// `--where col=value` (verify mode).
    pub where_clause: Option<String>,
    /// `--pattern` needle (scan mode): UTF-8 text or `hex:...`.
    pub pattern: Option<String>,
    /// Also run the raw byte pass inside `--verify-deleted`.
    pub thorough: bool,
    /// Cap on residue matches reported (scan mode).
    pub max_hits: usize,
    /// Override page size.
    pub page_size: Option<u32>,
    /// Path to MySQL keyring file.
    pub keyring: Option<String>,
    /// Use memory-mapped I/O.
    pub mmap: bool,
    /// Emit JSON.
    pub json: bool,
    /// Emit CSV.
    pub csv: bool,
    /// Show additional detail.
    pub verbose: bool,
}

/// Execute the comply subcommand.
pub fn execute(opts: &ComplyOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    // Exactly one mode.
    let mode_count =
        opts.verify_deleted as u8 + opts.scan_residue as u8 + opts.encryption_audit as u8;
    if mode_count == 0 {
        return Err(IdbError::Argument(
            "select a mode: --verify-deleted, --scan-residue, or --encryption-audit".to_string(),
        ));
    }
    if mode_count > 1 {
        return Err(IdbError::Argument(
            "modes are mutually exclusive: pick one of --verify-deleted, --scan-residue, --encryption-audit"
                .to_string(),
        ));
    }

    let mut ts = crate::cli::open_tablespace(&opts.file, opts.page_size, opts.mmap)?;
    if let Some(ref keyring_path) = opts.keyring {
        crate::cli::setup_decryption(&mut ts, keyring_path)?;
    }

    if opts.verify_deleted {
        run_verify(opts, &mut ts, writer)
    } else if opts.scan_residue {
        run_scan(opts, &mut ts, writer)
    } else {
        run_encryption_audit(opts, &mut ts, writer)
    }
}

// ---------------------------------------------------------------------------
// Mode: verify-deleted
// ---------------------------------------------------------------------------

fn run_verify(
    opts: &ComplyOptions,
    ts: &mut crate::innodb::tablespace::Tablespace,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    let where_clause = opts.where_clause.as_deref().ok_or_else(|| {
        IdbError::Argument("--verify-deleted requires --where <column>=<value>".to_string())
    })?;
    let (col, val) = where_clause.split_once('=').ok_or_else(|| {
        IdbError::Argument("--where must be of the form <column>=<value>".to_string())
    })?;
    let col = col.trim();
    if col.is_empty() {
        return Err(IdbError::Argument(
            "--where column name must not be empty".to_string(),
        ));
    }

    // Optional table-name filter.
    if let Some(ref filter) = opts.table {
        match crate::innodb::export::extract_table_name(ts) {
            Some(name) if name.eq_ignore_ascii_case(filter) => {}
            Some(name) => {
                return Err(IdbError::Argument(format!(
                    "Table name '{}' does not match filter '{}'",
                    name, filter
                )))
            }
            None => {
                return Err(IdbError::Argument(
                    "Cannot filter by table name: SDI metadata not available".to_string(),
                ))
            }
        }
    }

    let report = verify_deleted(ts, col, val, opts.thorough)?;

    if opts.json {
        let json =
            serde_json::to_string_pretty(&report).map_err(|e| IdbError::Parse(e.to_string()))?;
        wprintln!(writer, "{}", json)?;
        return Ok(());
    }

    if opts.csv {
        wprintln!(writer, "region,page,offset,delete_marked,trx_id")?;
        for s in &report.residue_sites {
            wprintln!(
                writer,
                "{},{},{},{},{}",
                s.region,
                s.page_number,
                s.offset,
                if s.delete_marked { "Y" } else { "N" },
                s.trx_id.map_or(String::new(), |t| t.to_string()),
            )?;
        }
        return Ok(());
    }

    // Human-readable.
    let table = report.table_name.as_deref().unwrap_or("(unknown table)");
    wprintln!(
        writer,
        "Deletion verification: {}.{} = '{}'",
        table,
        report.column,
        report.target_value
    )?;
    wprintln!(
        writer,
        "Scanned: {} ({} records examined){}",
        report.regions_scanned.join(", "),
        report.records_examined,
        if report.thorough {
            ""
        } else {
            " — logical structures only; pass --thorough to also sweep slack space"
        }
    )?;

    if report.fully_purged {
        wprintln!(
            writer,
            "\nRESULT: fully purged — no residue found in the scanned regions of this file."
        )?;
    } else {
        wprintln!(
            writer,
            "\nRESULT: NOT purged — {} residue site(s) found:",
            report.residue_sites.len()
        )?;
        for s in &report.residue_sites {
            let trx = s
                .trx_id
                .map_or(String::new(), |t| format!(", trx_id={}", t));
            wprintln!(
                writer,
                "  [{}] page {} offset {}{}{}",
                s.region,
                s.page_number,
                s.offset,
                if s.delete_marked {
                    ", delete-marked"
                } else {
                    ""
                },
                trx
            )?;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Mode: scan-residue
// ---------------------------------------------------------------------------

fn run_scan(
    opts: &ComplyOptions,
    ts: &mut crate::innodb::tablespace::Tablespace,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    let pattern_str = opts.pattern.as_deref().ok_or_else(|| {
        IdbError::Argument("--scan-residue requires --pattern <text|hex:...>".to_string())
    })?;
    let pattern = Pattern::parse(pattern_str)?;

    let matches = scan_residue(ts, &pattern, opts.max_hits)?;

    if opts.json {
        let json =
            serde_json::to_string_pretty(&matches).map_err(|e| IdbError::Parse(e.to_string()))?;
        wprintln!(writer, "{}", json)?;
        return Ok(());
    }

    if opts.csv {
        wprintln!(writer, "page,page_type,offset,region,context_hex")?;
        for m in &matches {
            wprintln!(
                writer,
                "{},{},{},{},{}",
                m.page_number,
                m.page_type,
                m.offset,
                m.region.name(),
                m.context_hex,
            )?;
        }
        return Ok(());
    }

    if matches.is_empty() {
        wprintln!(
            writer,
            "No residue found for pattern in the scanned pages of this file."
        )?;
        return Ok(());
    }

    wprintln!(
        writer,
        "{} match(es){}:",
        matches.len(),
        if matches.len() >= opts.max_hits {
            format!(" (capped at {})", opts.max_hits)
        } else {
            String::new()
        }
    )?;
    wprintln!(
        writer,
        "{:<8} {:<16} {:<8} {:<12} {}",
        "PAGE",
        "PAGE_TYPE",
        "OFFSET",
        "REGION",
        "CONTEXT (hex)"
    )?;
    for m in &matches {
        wprintln!(
            writer,
            "{:<8} {:<16} {:<8} {:<12} {}",
            m.page_number,
            m.page_type,
            m.offset,
            m.region.name(),
            m.context_hex,
        )?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Mode: encryption-audit
// ---------------------------------------------------------------------------

fn run_encryption_audit(
    opts: &ComplyOptions,
    ts: &mut crate::innodb::tablespace::Tablespace,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    let report = encryption_audit(ts)?;

    if opts.json {
        let json =
            serde_json::to_string_pretty(&report).map_err(|e| IdbError::Parse(e.to_string()))?;
        wprintln!(writer, "{}", json)?;
        return Ok(());
    }

    if opts.csv {
        wprintln!(
            writer,
            "tablespace_encrypted,algorithm,key_available,encrypted_pages,total_pages"
        )?;
        wprintln!(
            writer,
            "{},{},{},{},{}",
            if report.tablespace_encrypted {
                "Y"
            } else {
                "N"
            },
            report.algorithm.as_deref().unwrap_or(""),
            if report.key_available { "Y" } else { "N" },
            report.encrypted_page_count,
            report.total_pages,
        )?;
        return Ok(());
    }

    wprintln!(
        writer,
        "Encryption audit: {}",
        if report.tablespace_encrypted {
            "ENCRYPTED tablespace"
        } else {
            "plaintext tablespace"
        }
    )?;
    if let Some(ref algo) = report.algorithm {
        wprintln!(writer, "  Algorithm:       {}", algo)?;
    }
    wprintln!(
        writer,
        "  Key available:   {}",
        if report.key_available { "yes" } else { "no" }
    )?;
    wprintln!(
        writer,
        "  Encrypted pages: {} / {}",
        report.encrypted_page_count,
        report.total_pages
    )?;

    Ok(())
}
