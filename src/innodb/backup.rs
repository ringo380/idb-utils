//! Incremental backup analysis.
//!
//! Detects changed pages between two tablespace snapshots via LSN comparison,
//! and validates backup chains by parsing XtraBackup checkpoint files for
//! LSN continuity.
//!
//! # Delta detection
//!
//! [`diff_backup_lsn`] compares a base (backup) tablespace against a current
//! (live) copy page-by-page using their FIL header LSN values. Pages where
//! the current LSN exceeds the base LSN are classified as modified.
//!
//! # Chain validation
//!
//! [`scan_backup_chain`] walks a directory of XtraBackup backup sets, parses
//! each `xtrabackup_checkpoints` file, and verifies that the `to_lsn` of each
//! backup connects to the `from_lsn` of the next.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::innodb::checksum::validate_checksum;
use crate::innodb::page::FilHeader;
use crate::innodb::tablespace::Tablespace;
use crate::IdbError;

// ---------------------------------------------------------------------------
// Delta detection (Issue #152)
// ---------------------------------------------------------------------------

/// Classification of a page's change status between two snapshots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum PageChangeStatus {
    /// LSN identical in both snapshots.
    Unchanged,
    /// Current LSN is greater than base LSN.
    Modified,
    /// Page exists only in the current tablespace (tablespace grew).
    Added,
    /// Page exists only in the base tablespace (tablespace shrank).
    Removed,
    /// Current LSN is less than base LSN (unusual — possible point-in-time restore).
    Regressed,
}

/// Per-page delta detail.
#[derive(Debug, Clone, Serialize)]
pub struct PageDelta {
    /// Page number.
    pub page_number: u64,
    /// Change status.
    pub status: PageChangeStatus,
    /// Page type name from FIL header.
    pub page_type: String,
    /// LSN in the base/backup tablespace.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_lsn: Option<u64>,
    /// LSN in the current tablespace.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_lsn: Option<u64>,
    /// Whether the current page's checksum is valid.
    pub checksum_valid: bool,
}

/// Summary counts of changed pages.
#[derive(Debug, Clone, Serialize)]
pub struct BackupDiffSummary {
    pub unchanged: u64,
    pub modified: u64,
    pub added: u64,
    pub removed: u64,
    pub regressed: u64,
}

/// Full diff report between two tablespace snapshots.
#[derive(Debug, Clone, Serialize)]
pub struct BackupDiffReport {
    /// Path to the base/backup tablespace.
    pub base_file: String,
    /// Path to the current tablespace.
    pub current_file: String,
    /// Tablespace space ID.
    pub space_id: u32,
    /// Page size in bytes.
    pub page_size: u32,
    /// Number of pages in the base tablespace.
    pub base_page_count: u64,
    /// Number of pages in the current tablespace.
    pub current_page_count: u64,
    /// Maximum LSN observed across all base pages.
    pub base_max_lsn: u64,
    /// Maximum LSN observed across all current pages.
    pub current_max_lsn: u64,
    /// Summary of page change counts.
    pub summary: BackupDiffSummary,
    /// Per-page delta details (only populated in verbose mode).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub pages: Vec<PageDelta>,
    /// Distribution of modified pages by page type.
    pub modified_page_types: BTreeMap<String, u64>,
}

/// Compare two tablespace snapshots using LSN-based delta detection.
///
/// Opens both tablespaces, validates they share the same space ID, then
/// compares each page's LSN to classify it as unchanged, modified, added,
/// removed, or regressed.
pub fn diff_backup_lsn(
    base: &mut Tablespace,
    current: &mut Tablespace,
    base_path: &str,
    current_path: &str,
    verbose: bool,
) -> Result<BackupDiffReport, IdbError> {
    let page_size = current.page_size();

    // Validate same space_id
    let base_space_id = base.fsp_header().map(|f| f.space_id).unwrap_or(0);
    let current_space_id = current.fsp_header().map(|f| f.space_id).unwrap_or(0);
    if base_space_id != current_space_id {
        return Err(IdbError::Argument(format!(
            "Space ID mismatch: base has {} but current has {}",
            base_space_id, current_space_id
        )));
    }

    let base_count = base.page_count();
    let current_count = current.page_count();
    let max_pages = base_count.max(current_count);
    let vendor_info = current.vendor_info().clone();

    let mut summary = BackupDiffSummary {
        unchanged: 0,
        modified: 0,
        added: 0,
        removed: 0,
        regressed: 0,
    };
    let mut pages = Vec::new();
    let mut modified_types: BTreeMap<String, u64> = BTreeMap::new();
    let mut base_max_lsn = 0u64;
    let mut current_max_lsn = 0u64;

    for page_num in 0..max_pages {
        let base_page = if page_num < base_count {
            Some(base.read_page(page_num)?)
        } else {
            None
        };
        let current_page = if page_num < current_count {
            Some(current.read_page(page_num)?)
        } else {
            None
        };

        match (&base_page, &current_page) {
            (Some(bp), Some(cp)) => {
                let bh = FilHeader::parse(bp);
                let ch = FilHeader::parse(cp);
                let b_lsn = bh.as_ref().map(|h| h.lsn).unwrap_or(0);
                let c_lsn = ch.as_ref().map(|h| h.lsn).unwrap_or(0);
                let is_empty = cp.iter().all(|&b| b == 0);

                base_max_lsn = base_max_lsn.max(b_lsn);
                current_max_lsn = current_max_lsn.max(c_lsn);

                let status = if is_empty && bp.iter().all(|&b| b == 0) {
                    PageChangeStatus::Unchanged
                } else if c_lsn > b_lsn {
                    PageChangeStatus::Modified
                } else if c_lsn == b_lsn {
                    PageChangeStatus::Unchanged
                } else {
                    PageChangeStatus::Regressed
                };

                let page_type_name = ch
                    .as_ref()
                    .map(|h| h.page_type.name().to_string())
                    .unwrap_or_else(|| "UNKNOWN".to_string());

                if status == PageChangeStatus::Modified {
                    *modified_types.entry(page_type_name.clone()).or_insert(0) += 1;
                }

                let checksum_valid = validate_checksum(cp, page_size, Some(&vendor_info)).valid;

                match status {
                    PageChangeStatus::Unchanged => summary.unchanged += 1,
                    PageChangeStatus::Modified => summary.modified += 1,
                    PageChangeStatus::Regressed => summary.regressed += 1,
                    _ => {}
                }

                if verbose {
                    pages.push(PageDelta {
                        page_number: page_num,
                        status,
                        page_type: page_type_name,
                        base_lsn: Some(b_lsn),
                        current_lsn: Some(c_lsn),
                        checksum_valid,
                    });
                }
            }
            (None, Some(cp)) => {
                // Page only in current (tablespace grew)
                let ch = FilHeader::parse(cp);
                let c_lsn = ch.as_ref().map(|h| h.lsn).unwrap_or(0);
                current_max_lsn = current_max_lsn.max(c_lsn);
                let page_type_name = ch
                    .as_ref()
                    .map(|h| h.page_type.name().to_string())
                    .unwrap_or_else(|| "UNKNOWN".to_string());
                let checksum_valid = validate_checksum(cp, page_size, Some(&vendor_info)).valid;

                summary.added += 1;
                if verbose {
                    pages.push(PageDelta {
                        page_number: page_num,
                        status: PageChangeStatus::Added,
                        page_type: page_type_name,
                        base_lsn: None,
                        current_lsn: Some(c_lsn),
                        checksum_valid,
                    });
                }
            }
            (Some(bp), None) => {
                // Page only in base (tablespace shrank)
                let bh = FilHeader::parse(bp);
                let b_lsn = bh.as_ref().map(|h| h.lsn).unwrap_or(0);
                base_max_lsn = base_max_lsn.max(b_lsn);
                let page_type_name = bh
                    .as_ref()
                    .map(|h| h.page_type.name().to_string())
                    .unwrap_or_else(|| "UNKNOWN".to_string());

                summary.removed += 1;
                if verbose {
                    pages.push(PageDelta {
                        page_number: page_num,
                        status: PageChangeStatus::Removed,
                        page_type: page_type_name,
                        base_lsn: Some(b_lsn),
                        current_lsn: None,
                        checksum_valid: false,
                    });
                }
            }
            (None, None) => {}
        }
    }

    Ok(BackupDiffReport {
        base_file: base_path.to_string(),
        current_file: current_path.to_string(),
        space_id: current_space_id,
        page_size,
        base_page_count: base_count,
        current_page_count: current_count,
        base_max_lsn,
        current_max_lsn,
        summary,
        pages,
        modified_page_types: modified_types,
    })
}

// ---------------------------------------------------------------------------
// Backup chain validation (Issue #153)
// ---------------------------------------------------------------------------

/// Parsed XtraBackup checkpoint metadata.
#[derive(Debug, Clone, Serialize)]
pub struct BackupCheckpoint {
    /// Path to the backup directory.
    pub path: PathBuf,
    /// Backup type string (e.g., "full-backuped", "incremental").
    pub backup_type: String,
    /// Start LSN of the backup (0 for full backups).
    pub from_lsn: u64,
    /// End LSN of the backup.
    pub to_lsn: u64,
    /// Last LSN seen during the backup process.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_lsn: Option<u64>,
    /// Whether the backup was compacted.
    pub compact: bool,
}

/// Kind of chain anomaly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum ChainAnomalyKind {
    /// LSN gap between consecutive backups.
    Gap,
    /// LSN overlap between consecutive backups.
    Overlap,
    /// No full backup found as chain anchor.
    MissingFull,
}

/// A gap, overlap, or other anomaly in the backup chain.
#[derive(Debug, Clone, Serialize)]
pub struct ChainAnomaly {
    /// Kind of anomaly.
    pub kind: ChainAnomalyKind,
    /// Indices of the two backups involved (in the sorted backups array).
    pub between: (usize, usize),
    /// The LSN gap/overlap boundary values.
    pub from_lsn: u64,
    pub to_lsn: u64,
    /// Human-readable description.
    pub message: String,
}

/// Backup chain analysis report.
#[derive(Debug, Clone, Serialize)]
pub struct BackupChainReport {
    /// Path to the backup directory.
    pub chain_dir: String,
    /// Backup sets sorted by from_lsn.
    pub backups: Vec<BackupCheckpoint>,
    /// Whether the chain is valid (no gaps, has full backup).
    pub chain_valid: bool,
    /// Total LSN range covered (min from_lsn, max to_lsn).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_lsn_range: Option<(u64, u64)>,
    /// Anomalies detected in the chain.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub anomalies: Vec<ChainAnomaly>,
    /// Whether a full backup was found.
    pub has_full_backup: bool,
}

/// Parse an XtraBackup `xtrabackup_checkpoints` file.
///
/// The file contains `key = value` lines. Required fields are `backup_type`,
/// `from_lsn`, and `to_lsn`. Unknown keys are silently ignored.
pub fn parse_xtrabackup_checkpoints(path: &Path) -> Result<BackupCheckpoint, IdbError> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| IdbError::Io(format!("Failed to read {}: {}", path.display(), e)))?;

    parse_xtrabackup_checkpoints_str(&content, path)
}

/// Parse checkpoint content from a string (testable without filesystem).
fn parse_xtrabackup_checkpoints_str(
    content: &str,
    path: &Path,
) -> Result<BackupCheckpoint, IdbError> {
    let mut backup_type = None;
    let mut from_lsn = None;
    let mut to_lsn = None;
    let mut last_lsn = None;
    let mut compact = false;

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            let key = key.trim();
            let value = value.trim();
            match key {
                "backup_type" => backup_type = Some(value.to_string()),
                "from_lsn" => from_lsn = value.parse::<u64>().ok(),
                "to_lsn" => to_lsn = value.parse::<u64>().ok(),
                "last_lsn" => last_lsn = value.parse::<u64>().ok(),
                "compact" => compact = value == "1",
                _ => {} // ignore unknown keys
            }
        }
    }

    let backup_type = backup_type
        .ok_or_else(|| IdbError::Parse(format!("Missing backup_type in {}", path.display())))?;
    let from_lsn = from_lsn
        .ok_or_else(|| IdbError::Parse(format!("Missing from_lsn in {}", path.display())))?;
    let to_lsn =
        to_lsn.ok_or_else(|| IdbError::Parse(format!("Missing to_lsn in {}", path.display())))?;

    // Use parent directory as the backup path (the checkpoint file is inside it)
    let backup_dir = path.parent().unwrap_or(path).to_path_buf();

    Ok(BackupCheckpoint {
        path: backup_dir,
        backup_type,
        from_lsn,
        to_lsn,
        last_lsn,
        compact,
    })
}

/// Scan a directory for XtraBackup backup sets and validate chain continuity.
///
/// Walks immediate subdirectories looking for `xtrabackup_checkpoints` files,
/// parses each, sorts by `from_lsn`, and validates that the LSN chain is
/// continuous (each backup's `to_lsn` connects to the next's `from_lsn`).
#[cfg(not(target_arch = "wasm32"))]
pub fn scan_backup_chain(dir: &Path) -> Result<BackupChainReport, IdbError> {
    let mut checkpoints = Vec::new();

    // Check the directory itself for a checkpoint file
    let root_checkpoint = dir.join("xtrabackup_checkpoints");
    if root_checkpoint.exists() {
        if let Ok(cp) = parse_xtrabackup_checkpoints(&root_checkpoint) {
            checkpoints.push(cp);
        }
    }

    // Walk immediate subdirectories
    let entries = std::fs::read_dir(dir)
        .map_err(|e| IdbError::Io(format!("Failed to read directory {}: {}", dir.display(), e)))?;

    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        let path = entry.path();
        if path.is_dir() {
            let checkpoint_file = path.join("xtrabackup_checkpoints");
            if checkpoint_file.exists() {
                if let Ok(cp) = parse_xtrabackup_checkpoints(&checkpoint_file) {
                    checkpoints.push(cp);
                }
            }
        }
    }

    if checkpoints.is_empty() {
        return Ok(BackupChainReport {
            chain_dir: dir.to_string_lossy().to_string(),
            backups: Vec::new(),
            chain_valid: false,
            total_lsn_range: None,
            anomalies: vec![ChainAnomaly {
                kind: ChainAnomalyKind::MissingFull,
                between: (0, 0),
                from_lsn: 0,
                to_lsn: 0,
                message: "No xtrabackup_checkpoints files found".to_string(),
            }],
            has_full_backup: false,
        });
    }

    // Sort by from_lsn
    checkpoints.sort_by_key(|c| c.from_lsn);

    validate_chain(dir, checkpoints)
}

/// Validate a sorted list of backup checkpoints for chain continuity.
#[cfg(not(target_arch = "wasm32"))]
fn validate_chain(
    dir: &Path,
    backups: Vec<BackupCheckpoint>,
) -> Result<BackupChainReport, IdbError> {
    let mut anomalies = Vec::new();

    // Check for full backup
    let has_full = backups
        .iter()
        .any(|b| b.backup_type.contains("full") || b.from_lsn == 0);

    if !has_full {
        anomalies.push(ChainAnomaly {
            kind: ChainAnomalyKind::MissingFull,
            between: (0, 0),
            from_lsn: 0,
            to_lsn: 0,
            message: "No full backup found in chain".to_string(),
        });
    }

    // Check LSN continuity between consecutive backups
    for i in 0..backups.len().saturating_sub(1) {
        let prev_to = backups[i].to_lsn;
        let next_from = backups[i + 1].from_lsn;

        if prev_to < next_from {
            anomalies.push(ChainAnomaly {
                kind: ChainAnomalyKind::Gap,
                between: (i, i + 1),
                from_lsn: prev_to,
                to_lsn: next_from,
                message: format!(
                    "LSN gap between backup {} and {}: {} → {}",
                    i + 1,
                    i + 2,
                    prev_to,
                    next_from
                ),
            });
        } else if prev_to > next_from {
            anomalies.push(ChainAnomaly {
                kind: ChainAnomalyKind::Overlap,
                between: (i, i + 1),
                from_lsn: next_from,
                to_lsn: prev_to,
                message: format!(
                    "LSN overlap between backup {} and {}: {} overlaps {}",
                    i + 1,
                    i + 2,
                    prev_to,
                    next_from
                ),
            });
        }
    }

    let chain_valid = has_full
        && !anomalies
            .iter()
            .any(|a| a.kind == ChainAnomalyKind::Gap || a.kind == ChainAnomalyKind::MissingFull);

    let total_lsn_range = if !backups.is_empty() {
        Some((
            backups.first().map(|b| b.from_lsn).unwrap_or(0),
            backups.last().map(|b| b.to_lsn).unwrap_or(0),
        ))
    } else {
        None
    };

    Ok(BackupChainReport {
        chain_dir: dir.to_string_lossy().to_string(),
        backups,
        chain_valid,
        total_lsn_range,
        anomalies,
        has_full_backup: has_full,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::innodb::constants::*;
    use byteorder::{BigEndian, ByteOrder};
    use std::io::Write;
    use tempfile::NamedTempFile;

    /// Build a page with given page_number, page_type, LSN, and space_id.
    fn build_page(
        page_number: u32,
        page_type: u16,
        lsn: u64,
        space_id: u32,
        page_size: u32,
    ) -> Vec<u8> {
        let ps = page_size as usize;
        let mut page = vec![0u8; ps];

        BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], page_number);
        BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], 0xFFFFFFFF);
        BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], 0xFFFFFFFF);
        BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], lsn);
        BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], page_type);
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);

        // Trailer LSN low 32 bits
        BigEndian::write_u32(&mut page[ps - 4..], lsn as u32);

        // CRC-32C checksum
        let crc1 = crc32c::crc32c(&page[4..26]);
        let crc2 = crc32c::crc32c(&page[38..ps - 8]);
        let checksum = crc1 ^ crc2;
        BigEndian::write_u32(&mut page[0..4], checksum);
        BigEndian::write_u32(&mut page[ps - 8..ps - 4], checksum);

        page
    }

    fn build_fsp_page(space_id: u32, lsn: u64, page_size: u32) -> Vec<u8> {
        let mut page = build_page(0, 8, lsn, space_id, page_size); // FSP_HDR = 8
        let base = FIL_PAGE_DATA;
        BigEndian::write_u32(&mut page[base..], space_id);

        // Re-stamp checksum
        let ps = page_size as usize;
        let crc1 = crc32c::crc32c(&page[4..26]);
        let crc2 = crc32c::crc32c(&page[38..ps - 8]);
        let checksum = crc1 ^ crc2;
        BigEndian::write_u32(&mut page[0..4], checksum);
        BigEndian::write_u32(&mut page[ps - 8..ps - 4], checksum);

        page
    }

    fn write_tablespace(pages: &[Vec<u8>]) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        for page in pages {
            file.write_all(page).unwrap();
        }
        file.flush().unwrap();
        file
    }

    // -- Delta detection tests --

    #[test]
    fn test_diff_identical_tablespaces() {
        let ps = 16384u32;
        let fsp = build_fsp_page(1, 1000, ps);
        let idx = build_page(1, 17855, 2000, 1, ps);
        let base_file = write_tablespace(&[fsp.clone(), idx.clone()]);
        let current_file = write_tablespace(&[fsp, idx]);

        let mut base = Tablespace::open(base_file.path().to_str().unwrap()).unwrap();
        let mut current = Tablespace::open(current_file.path().to_str().unwrap()).unwrap();

        let report =
            diff_backup_lsn(&mut base, &mut current, "base.ibd", "current.ibd", false).unwrap();

        assert_eq!(report.summary.unchanged, 2);
        assert_eq!(report.summary.modified, 0);
        assert_eq!(report.summary.added, 0);
        assert_eq!(report.summary.removed, 0);
    }

    #[test]
    fn test_diff_modified_pages() {
        let ps = 16384u32;
        let fsp = build_fsp_page(1, 1000, ps);
        let idx_base = build_page(1, 17855, 2000, 1, ps);
        let idx_current = build_page(1, 17855, 3000, 1, ps); // higher LSN

        let base_file = write_tablespace(&[fsp.clone(), idx_base]);
        let current_file = write_tablespace(&[fsp, idx_current]);

        let mut base = Tablespace::open(base_file.path().to_str().unwrap()).unwrap();
        let mut current = Tablespace::open(current_file.path().to_str().unwrap()).unwrap();

        let report = diff_backup_lsn(&mut base, &mut current, "b", "c", true).unwrap();

        assert_eq!(report.summary.modified, 1);
        assert_eq!(report.summary.unchanged, 1); // FSP page same
        assert_eq!(report.current_max_lsn, 3000);

        let modified = report.pages.iter().find(|p| p.page_number == 1).unwrap();
        assert_eq!(modified.status, PageChangeStatus::Modified);
        assert_eq!(modified.base_lsn, Some(2000));
        assert_eq!(modified.current_lsn, Some(3000));
    }

    #[test]
    fn test_diff_grown_tablespace() {
        let ps = 16384u32;
        let fsp = build_fsp_page(1, 1000, ps);
        let idx = build_page(1, 17855, 2000, 1, ps);
        let extra = build_page(2, 17855, 3000, 1, ps);

        let base_file = write_tablespace(&[fsp.clone(), idx.clone()]);
        let current_file = write_tablespace(&[fsp, idx, extra]);

        let mut base = Tablespace::open(base_file.path().to_str().unwrap()).unwrap();
        let mut current = Tablespace::open(current_file.path().to_str().unwrap()).unwrap();

        let report = diff_backup_lsn(&mut base, &mut current, "b", "c", false).unwrap();

        assert_eq!(report.summary.added, 1);
        assert_eq!(report.base_page_count, 2);
        assert_eq!(report.current_page_count, 3);
    }

    #[test]
    fn test_diff_shrunk_tablespace() {
        let ps = 16384u32;
        let fsp = build_fsp_page(1, 1000, ps);
        let idx1 = build_page(1, 17855, 2000, 1, ps);
        let idx2 = build_page(2, 17855, 3000, 1, ps);

        let base_file = write_tablespace(&[fsp.clone(), idx1.clone(), idx2]);
        let current_file = write_tablespace(&[fsp, idx1]);

        let mut base = Tablespace::open(base_file.path().to_str().unwrap()).unwrap();
        let mut current = Tablespace::open(current_file.path().to_str().unwrap()).unwrap();

        let report = diff_backup_lsn(&mut base, &mut current, "b", "c", false).unwrap();

        assert_eq!(report.summary.removed, 1);
    }

    #[test]
    fn test_diff_space_id_mismatch() {
        let ps = 16384u32;
        let fsp1 = build_fsp_page(1, 1000, ps);
        let fsp2 = build_fsp_page(2, 1000, ps); // different space_id

        let base_file = write_tablespace(&[fsp1]);
        let current_file = write_tablespace(&[fsp2]);

        let mut base = Tablespace::open(base_file.path().to_str().unwrap()).unwrap();
        let mut current = Tablespace::open(current_file.path().to_str().unwrap()).unwrap();

        let result = diff_backup_lsn(&mut base, &mut current, "b", "c", false);
        match result {
            Err(IdbError::Argument(msg)) => assert!(msg.contains("Space ID mismatch")),
            _ => panic!("Expected Argument error for space_id mismatch"),
        }
    }

    #[test]
    fn test_diff_regressed_lsn() {
        let ps = 16384u32;
        let fsp = build_fsp_page(1, 1000, ps);
        let idx_base = build_page(1, 17855, 5000, 1, ps);
        let idx_current = build_page(1, 17855, 2000, 1, ps); // lower LSN

        let base_file = write_tablespace(&[fsp.clone(), idx_base]);
        let current_file = write_tablespace(&[fsp, idx_current]);

        let mut base = Tablespace::open(base_file.path().to_str().unwrap()).unwrap();
        let mut current = Tablespace::open(current_file.path().to_str().unwrap()).unwrap();

        let report = diff_backup_lsn(&mut base, &mut current, "b", "c", false).unwrap();

        assert_eq!(report.summary.regressed, 1);
    }

    // -- XtraBackup checkpoint parsing tests --

    #[test]
    fn test_parse_xtrabackup_checkpoints() {
        let content = "\
backup_type = full-backuped
from_lsn = 0
to_lsn = 12345678
last_lsn = 12345679
compact = 0
recover_binlog_info = 0
";
        let cp = parse_xtrabackup_checkpoints_str(content, Path::new("/backups/full")).unwrap();
        assert_eq!(cp.backup_type, "full-backuped");
        assert_eq!(cp.from_lsn, 0);
        assert_eq!(cp.to_lsn, 12345678);
        assert_eq!(cp.last_lsn, Some(12345679));
        assert!(!cp.compact);
    }

    #[test]
    fn test_parse_checkpoints_missing_field() {
        let content = "backup_type = incremental\nfrom_lsn = 100\n";
        let result = parse_xtrabackup_checkpoints_str(content, Path::new("/backups/inc1"));
        match result {
            Err(IdbError::Parse(msg)) => assert!(msg.contains("to_lsn")),
            _ => panic!("Expected Parse error for missing to_lsn"),
        }
    }

    // -- Chain validation tests --

    #[test]
    fn test_chain_valid() {
        let backups = vec![
            BackupCheckpoint {
                path: PathBuf::from("/backups/full"),
                backup_type: "full-backuped".to_string(),
                from_lsn: 0,
                to_lsn: 1000,
                last_lsn: Some(1001),
                compact: false,
            },
            BackupCheckpoint {
                path: PathBuf::from("/backups/inc1"),
                backup_type: "incremental".to_string(),
                from_lsn: 1000,
                to_lsn: 2000,
                last_lsn: Some(2001),
                compact: false,
            },
            BackupCheckpoint {
                path: PathBuf::from("/backups/inc2"),
                backup_type: "incremental".to_string(),
                from_lsn: 2000,
                to_lsn: 3000,
                last_lsn: Some(3001),
                compact: false,
            },
        ];

        let report = validate_chain(Path::new("/backups"), backups).unwrap();
        assert!(report.chain_valid);
        assert!(report.has_full_backup);
        assert!(report.anomalies.is_empty());
        assert_eq!(report.total_lsn_range, Some((0, 3000)));
    }

    #[test]
    fn test_chain_gap() {
        let backups = vec![
            BackupCheckpoint {
                path: PathBuf::from("/backups/full"),
                backup_type: "full-backuped".to_string(),
                from_lsn: 0,
                to_lsn: 1000,
                last_lsn: None,
                compact: false,
            },
            BackupCheckpoint {
                path: PathBuf::from("/backups/inc1"),
                backup_type: "incremental".to_string(),
                from_lsn: 2000, // gap: 1000 < 2000
                to_lsn: 3000,
                last_lsn: None,
                compact: false,
            },
        ];

        let report = validate_chain(Path::new("/backups"), backups).unwrap();
        assert!(!report.chain_valid);
        assert_eq!(report.anomalies.len(), 1);
        assert_eq!(report.anomalies[0].kind, ChainAnomalyKind::Gap);
    }

    #[test]
    fn test_chain_no_full_backup() {
        let backups = vec![BackupCheckpoint {
            path: PathBuf::from("/backups/inc1"),
            backup_type: "incremental".to_string(),
            from_lsn: 1000,
            to_lsn: 2000,
            last_lsn: None,
            compact: false,
        }];

        let report = validate_chain(Path::new("/backups"), backups).unwrap();
        assert!(!report.chain_valid);
        assert!(!report.has_full_backup);
        assert!(report
            .anomalies
            .iter()
            .any(|a| a.kind == ChainAnomalyKind::MissingFull));
    }

    #[test]
    fn test_chain_overlap() {
        let backups = vec![
            BackupCheckpoint {
                path: PathBuf::from("/backups/full"),
                backup_type: "full-backuped".to_string(),
                from_lsn: 0,
                to_lsn: 2000,
                last_lsn: None,
                compact: false,
            },
            BackupCheckpoint {
                path: PathBuf::from("/backups/inc1"),
                backup_type: "incremental".to_string(),
                from_lsn: 1500, // overlap: 2000 > 1500
                to_lsn: 3000,
                last_lsn: None,
                compact: false,
            },
        ];

        let report = validate_chain(Path::new("/backups"), backups).unwrap();
        // Overlap is a warning, not chain-breaking
        assert!(report.chain_valid);
        assert!(report
            .anomalies
            .iter()
            .any(|a| a.kind == ChainAnomalyKind::Overlap));
    }
}
