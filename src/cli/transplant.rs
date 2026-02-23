use std::io::Write;
use std::sync::Arc;

use byteorder::{BigEndian, ByteOrder};
use colored::Colorize;
use serde::Serialize;

use crate::cli::wprintln;
use crate::innodb::checksum::validate_checksum;
use crate::innodb::constants::FIL_PAGE_SPACE_ID;
use crate::innodb::write;
use crate::util::audit::AuditLogger;
use crate::IdbError;

/// Options for the `inno transplant` subcommand.
pub struct TransplantOptions {
    /// Path to the donor tablespace file (source of pages).
    pub donor: String,
    /// Path to the target tablespace file (destination for pages).
    pub target: String,
    /// Page numbers to transplant from donor to target.
    pub pages: Vec<u64>,
    /// Skip creating a backup of the target.
    pub no_backup: bool,
    /// Allow transplanting despite space ID mismatch or corrupt donor pages.
    pub force: bool,
    /// Preview without modifying the target file.
    pub dry_run: bool,
    /// Show per-page details.
    pub verbose: bool,
    /// Emit output as JSON.
    pub json: bool,
    /// Override the auto-detected page size.
    pub page_size: Option<u32>,
    /// Path to MySQL keyring file for decrypting encrypted tablespaces.
    pub keyring: Option<String>,
    /// Use memory-mapped I/O for file access.
    pub mmap: bool,
    /// Audit logger for recording write operations.
    pub audit_logger: Option<Arc<AuditLogger>>,
}

#[derive(Serialize)]
struct TransplantReport {
    donor: String,
    target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    backup_path: Option<String>,
    dry_run: bool,
    transplanted: u64,
    skipped: u64,
    pages: Vec<PageTransplantInfo>,
}

#[derive(Serialize)]
struct PageTransplantInfo {
    page_number: u64,
    action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    donor_checksum_valid: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    post_checksum_valid: Option<bool>,
}

/// Copy specific pages from a donor tablespace into a target.
pub fn execute(opts: &TransplantOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    if opts.pages.is_empty() {
        return Err(IdbError::Argument(
            "No pages specified. Use --pages to specify page numbers.".to_string(),
        ));
    }

    // Open both tablespaces read-only to validate
    let mut donor_ts = crate::cli::open_tablespace(&opts.donor, opts.page_size, opts.mmap)?;
    let mut target_ts = crate::cli::open_tablespace(&opts.target, opts.page_size, opts.mmap)?;

    if let Some(ref keyring_path) = opts.keyring {
        crate::cli::setup_decryption(&mut donor_ts, keyring_path)?;
        crate::cli::setup_decryption(&mut target_ts, keyring_path)?;
    }

    let donor_page_size = donor_ts.page_size();
    let target_page_size = target_ts.page_size();
    let donor_count = donor_ts.page_count();
    let target_count = target_ts.page_count();
    let donor_vendor = donor_ts.vendor_info().clone();
    let target_vendor = target_ts.vendor_info().clone();

    // Validate page sizes match
    if donor_page_size != target_page_size {
        return Err(IdbError::Argument(format!(
            "Page size mismatch: donor={}, target={}",
            donor_page_size, target_page_size
        )));
    }
    let page_size = donor_page_size;

    // Check space ID match
    let donor_page0 = donor_ts.read_page(0)?;
    let target_page0 = target_ts.read_page(0)?;
    let donor_space_id = BigEndian::read_u32(&donor_page0[FIL_PAGE_SPACE_ID..]);
    let target_space_id = BigEndian::read_u32(&target_page0[FIL_PAGE_SPACE_ID..]);

    if donor_space_id != target_space_id {
        if !opts.force {
            return Err(IdbError::Argument(format!(
                "Space ID mismatch: donor={}, target={}. Use --force to override.",
                donor_space_id, target_space_id
            )));
        }
        if !opts.json {
            wprintln!(
                writer,
                "{}: Space ID mismatch (donor={}, target={}), proceeding with --force",
                "Warning".yellow(),
                donor_space_id,
                target_space_id
            )?;
        }
    }

    // Create backup unless --no-backup or --dry-run
    let backup_path = if !opts.no_backup && !opts.dry_run {
        let path = write::create_backup(&opts.target)?;
        if !opts.json {
            wprintln!(writer, "Backup created: {}", path.display())?;
        }
        if let Some(ref logger) = opts.audit_logger {
            let _ = logger.log_backup(&opts.target, &path.display().to_string());
        }
        Some(path)
    } else {
        None
    };

    if !opts.json && !opts.dry_run {
        wprintln!(
            writer,
            "Transplanting {} pages from {} to {}...",
            opts.pages.len(),
            opts.donor,
            opts.target
        )?;
    } else if !opts.json && opts.dry_run {
        wprintln!(
            writer,
            "Dry run: previewing transplant of {} pages from {} to {}...",
            opts.pages.len(),
            opts.donor,
            opts.target
        )?;
    }

    let mut transplanted = 0u64;
    let mut skipped = 0u64;
    let mut page_details: Vec<PageTransplantInfo> = Vec::new();

    for &page_num in &opts.pages {
        // Reject page 0 unless --force
        if page_num == 0 && !opts.force {
            if opts.verbose && !opts.json {
                wprintln!(
                    writer,
                    "Page {:>4}: {} (FSP_HDR — use --force to override)",
                    page_num,
                    "skipped".yellow()
                )?;
            }
            page_details.push(PageTransplantInfo {
                page_number: page_num,
                action: "skipped".to_string(),
                reason: Some("FSP_HDR page — use --force to override".to_string()),
                donor_checksum_valid: None,
                post_checksum_valid: None,
            });
            skipped += 1;
            continue;
        }

        // Check page number in range
        if page_num >= donor_count {
            if opts.verbose && !opts.json {
                wprintln!(
                    writer,
                    "Page {:>4}: {} (out of range in donor, {} pages)",
                    page_num,
                    "skipped".yellow(),
                    donor_count
                )?;
            }
            page_details.push(PageTransplantInfo {
                page_number: page_num,
                action: "skipped".to_string(),
                reason: Some(format!("Out of range in donor ({} pages)", donor_count)),
                donor_checksum_valid: None,
                post_checksum_valid: None,
            });
            skipped += 1;
            continue;
        }

        if page_num >= target_count {
            if opts.verbose && !opts.json {
                wprintln!(
                    writer,
                    "Page {:>4}: {} (out of range in target, {} pages)",
                    page_num,
                    "skipped".yellow(),
                    target_count
                )?;
            }
            page_details.push(PageTransplantInfo {
                page_number: page_num,
                action: "skipped".to_string(),
                reason: Some(format!("Out of range in target ({} pages)", target_count)),
                donor_checksum_valid: None,
                post_checksum_valid: None,
            });
            skipped += 1;
            continue;
        }

        // Read donor page and validate
        let donor_page = write::read_page_raw(&opts.donor, page_num, page_size)?;
        let donor_valid = validate_checksum(&donor_page, page_size, Some(&donor_vendor)).valid;

        if !donor_valid && !opts.force {
            if opts.verbose && !opts.json {
                wprintln!(
                    writer,
                    "Page {:>4}: {} (donor page has invalid checksum — use --force)",
                    page_num,
                    "skipped".yellow()
                )?;
            }
            page_details.push(PageTransplantInfo {
                page_number: page_num,
                action: "skipped".to_string(),
                reason: Some("Donor page has invalid checksum".to_string()),
                donor_checksum_valid: Some(false),
                post_checksum_valid: None,
            });
            skipped += 1;
            continue;
        }

        if !donor_valid && opts.force && !opts.json {
            wprintln!(
                writer,
                "{}: Donor page {} has invalid checksum, transplanting anyway (--force)",
                "Warning".yellow(),
                page_num
            )?;
        }

        // Write to target
        if !opts.dry_run {
            write::write_page(&opts.target, page_num, page_size, &donor_page)?;
            if let Some(ref logger) = opts.audit_logger {
                let _ = logger.log_page_write(&opts.target, page_num, "transplant", None, None);
            }

            // Post-validate
            let written = write::read_page_raw(&opts.target, page_num, page_size)?;
            let post_valid = validate_checksum(&written, page_size, Some(&target_vendor)).valid;

            if opts.verbose && !opts.json {
                let status = if post_valid {
                    "OK".green().to_string()
                } else {
                    "CHECKSUM INVALID".red().to_string()
                };
                wprintln!(
                    writer,
                    "Page {:>4}: transplanted (post-validate: {})",
                    page_num,
                    status
                )?;
            }

            page_details.push(PageTransplantInfo {
                page_number: page_num,
                action: "transplanted".to_string(),
                reason: None,
                donor_checksum_valid: Some(donor_valid),
                post_checksum_valid: Some(post_valid),
            });
        } else {
            if opts.verbose && !opts.json {
                wprintln!(
                    writer,
                    "Page {:>4}: would transplant (donor checksum: {})",
                    page_num,
                    if donor_valid { "OK" } else { "INVALID" }
                )?;
            }
            page_details.push(PageTransplantInfo {
                page_number: page_num,
                action: "would_transplant".to_string(),
                reason: None,
                donor_checksum_valid: Some(donor_valid),
                post_checksum_valid: None,
            });
        }

        transplanted += 1;
    }

    if opts.json {
        let report = TransplantReport {
            donor: opts.donor.clone(),
            target: opts.target.clone(),
            backup_path: backup_path.map(|p| p.display().to_string()),
            dry_run: opts.dry_run,
            transplanted,
            skipped,
            pages: page_details,
        };
        let json = serde_json::to_string_pretty(&report)
            .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
        wprintln!(writer, "{}", json)?;
    } else {
        wprintln!(writer)?;
        wprintln!(writer, "Transplant Summary:")?;
        if transplanted > 0 {
            let label = if opts.dry_run {
                "Would transplant"
            } else {
                "Transplanted"
            };
            wprintln!(
                writer,
                "  {}: {}",
                label,
                format!("{}", transplanted).green()
            )?;
        } else {
            wprintln!(writer, "  Transplanted: 0")?;
        }
        if skipped > 0 {
            wprintln!(
                writer,
                "  Skipped:      {}",
                format!("{}", skipped).yellow()
            )?;
        } else {
            wprintln!(writer, "  Skipped:      0")?;
        }
    }

    Ok(())
}
