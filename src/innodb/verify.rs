//! Structural validation for InnoDB tablespace files.
//!
//! Performs pure structural checks on tablespace pages without
//! requiring checksums to be valid — useful for catching
//! logical corruption and metadata inconsistencies.

use byteorder::{BigEndian, ByteOrder};
use serde::Serialize;

use crate::innodb::constants::*;
use crate::innodb::index::IndexHeader;
use crate::innodb::page::FilHeader;
use crate::innodb::page_types::PageType;
#[cfg(not(target_arch = "wasm32"))]
use crate::IdbError;

/// Kind of structural check performed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum VerifyCheckKind {
    /// Page number at offset 4 matches expected position.
    PageNumberSequence,
    /// All pages share the same space_id as page 0.
    SpaceIdConsistency,
    /// LSNs are non-decreasing across pages (within tolerance).
    LsnMonotonicity,
    /// INDEX pages: B+Tree level is within reasonable bounds.
    BTreeLevelConsistency,
    /// prev/next page pointers within file bounds; first page has prev == FIL_NULL.
    PageChainBounds,
    /// Trailer LSN low-32 matches header LSN low-32.
    TrailerLsnMatch,
}

impl std::fmt::Display for VerifyCheckKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VerifyCheckKind::PageNumberSequence => write!(f, "page_number_sequence"),
            VerifyCheckKind::SpaceIdConsistency => write!(f, "space_id_consistency"),
            VerifyCheckKind::LsnMonotonicity => write!(f, "lsn_monotonicity"),
            VerifyCheckKind::BTreeLevelConsistency => write!(f, "btree_level_consistency"),
            VerifyCheckKind::PageChainBounds => write!(f, "page_chain_bounds"),
            VerifyCheckKind::TrailerLsnMatch => write!(f, "trailer_lsn_match"),
        }
    }
}

/// A single finding from structural verification.
#[derive(Debug, Clone, Serialize)]
pub struct VerifyFinding {
    /// Which structural check produced this finding.
    pub kind: VerifyCheckKind,
    /// The page number where the issue was found.
    pub page_number: u64,
    /// Human-readable description of the issue.
    pub message: String,
    /// Expected value (if applicable).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected: Option<String>,
    /// Actual value found (if applicable).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual: Option<String>,
}

/// Summary of a single check kind across the tablespace.
#[derive(Debug, Clone, Serialize)]
pub struct CheckSummary {
    /// Which check this summary is for.
    pub kind: VerifyCheckKind,
    /// Number of pages examined for this check.
    pub pages_checked: u64,
    /// Number of issues found.
    pub issues_found: u64,
    /// Whether the check passed (no issues found).
    pub passed: bool,
}

/// Configuration for which checks to run.
pub struct VerifyConfig {
    /// Check that page numbers in headers match their file position.
    pub check_page_numbers: bool,
    /// Check that all pages share the same space_id as page 0.
    pub check_space_ids: bool,
    /// Check that LSNs do not decrease significantly between pages.
    pub check_lsn_monotonicity: bool,
    /// Check that B+Tree levels are within reasonable bounds on INDEX pages.
    pub check_btree_levels: bool,
    /// Check that prev/next chain pointers are within file bounds.
    pub check_chain_bounds: bool,
    /// Check that trailer LSN low-32 matches header LSN low-32.
    pub check_trailer_lsn: bool,
}

impl Default for VerifyConfig {
    fn default() -> Self {
        Self {
            check_page_numbers: true,
            check_space_ids: true,
            check_lsn_monotonicity: true,
            check_btree_levels: true,
            check_chain_bounds: true,
            check_trailer_lsn: true,
        }
    }
}

/// Full verification report for a tablespace.
#[derive(Debug, Clone, Serialize)]
pub struct VerifyReport {
    /// Path to the tablespace file.
    pub file: String,
    /// Total number of pages in the file.
    pub total_pages: u64,
    /// Page size in bytes.
    pub page_size: u32,
    /// Whether all checks passed.
    pub passed: bool,
    /// Individual findings (issues found).
    pub findings: Vec<VerifyFinding>,
    /// Per-check summaries.
    pub summary: Vec<CheckSummary>,
}

/// Verify a tablespace by running all structural checks.
///
/// Takes all pages as a flat byte slice, the page size, and the space_id
/// from page 0. Returns a `VerifyReport` with findings and per-check summaries.
pub fn verify_tablespace(
    all_pages: &[u8],
    page_size: u32,
    space_id: u32,
    file: &str,
    config: &VerifyConfig,
) -> VerifyReport {
    let ps = page_size as usize;
    let total_pages = (all_pages.len() / ps) as u64;
    let mut findings = Vec::new();

    // Collect per-check counters
    let mut page_num_checked = 0u64;
    let mut page_num_issues = 0u64;
    let mut space_id_checked = 0u64;
    let mut space_id_issues = 0u64;
    let mut lsn_checked = 0u64;
    let mut lsn_issues = 0u64;
    let mut btree_checked = 0u64;
    let mut btree_issues = 0u64;
    let mut chain_checked = 0u64;
    let mut chain_issues = 0u64;
    let mut trailer_checked = 0u64;
    let mut trailer_issues = 0u64;

    let mut prev_lsn: u64 = 0;

    for page_idx in 0..total_pages {
        let offset = page_idx as usize * ps;
        let page_data = &all_pages[offset..offset + ps];

        // Skip all-zero pages
        if page_data.iter().all(|&b| b == 0) {
            continue;
        }

        let header = match FilHeader::parse(page_data) {
            Some(h) => h,
            None => continue,
        };

        // Check 1: Page number sequence
        if config.check_page_numbers {
            page_num_checked += 1;
            if header.page_number as u64 != page_idx {
                page_num_issues += 1;
                findings.push(VerifyFinding {
                    kind: VerifyCheckKind::PageNumberSequence,
                    page_number: page_idx,
                    message: format!(
                        "Page {} has page_number {} in header",
                        page_idx, header.page_number
                    ),
                    expected: Some(page_idx.to_string()),
                    actual: Some(header.page_number.to_string()),
                });
            }
        }

        // Check 2: Space ID consistency
        if config.check_space_ids {
            space_id_checked += 1;
            if header.space_id != space_id {
                space_id_issues += 1;
                findings.push(VerifyFinding {
                    kind: VerifyCheckKind::SpaceIdConsistency,
                    page_number: page_idx,
                    message: format!(
                        "Page {} has space_id {} (expected {})",
                        page_idx, header.space_id, space_id
                    ),
                    expected: Some(space_id.to_string()),
                    actual: Some(header.space_id.to_string()),
                });
            }
        }

        // Check 3: LSN monotonicity
        if config.check_lsn_monotonicity && page_idx > 0 {
            lsn_checked += 1;
            // Allow page 0 to have any LSN; subsequent pages should not decrease dramatically.
            // Minor LSN non-monotonicity is normal due to page flushing order.
            // We only flag significant drops (> 50% of previous LSN) as issues.
            if header.lsn > 0 && prev_lsn > 0 && header.lsn < prev_lsn / 2 {
                lsn_issues += 1;
                findings.push(VerifyFinding {
                    kind: VerifyCheckKind::LsnMonotonicity,
                    page_number: page_idx,
                    message: format!(
                        "Page {} LSN {} is significantly lower than previous {}",
                        page_idx, header.lsn, prev_lsn
                    ),
                    expected: Some(format!(">= {}", prev_lsn / 2)),
                    actual: Some(header.lsn.to_string()),
                });
            }
        }
        if header.lsn > 0 {
            prev_lsn = header.lsn;
        }

        // Check 4: B+Tree level consistency (INDEX pages only)
        if config.check_btree_levels && header.page_type == PageType::Index {
            if let Some(idx_header) = IndexHeader::parse(page_data) {
                btree_checked += 1;
                // Level 0 = leaf. Level > 0 = internal.
                // Max InnoDB B+Tree depth is ~64; flag unreasonable values.
                if idx_header.level > 64 {
                    btree_issues += 1;
                    findings.push(VerifyFinding {
                        kind: VerifyCheckKind::BTreeLevelConsistency,
                        page_number: page_idx,
                        message: format!(
                            "Page {} has unreasonable B+Tree level {}",
                            page_idx, idx_header.level
                        ),
                        expected: Some("<= 64".to_string()),
                        actual: Some(idx_header.level.to_string()),
                    });
                }
            }
        }

        // Check 5: Page chain bounds
        if config.check_chain_bounds {
            chain_checked += 1;
            if header.prev_page != FIL_NULL && header.prev_page as u64 >= total_pages {
                chain_issues += 1;
                findings.push(VerifyFinding {
                    kind: VerifyCheckKind::PageChainBounds,
                    page_number: page_idx,
                    message: format!(
                        "Page {} prev pointer {} is out of bounds (total: {})",
                        page_idx, header.prev_page, total_pages
                    ),
                    expected: Some(format!("< {} or FIL_NULL", total_pages)),
                    actual: Some(header.prev_page.to_string()),
                });
            }
            if header.next_page != FIL_NULL && header.next_page as u64 >= total_pages {
                chain_issues += 1;
                findings.push(VerifyFinding {
                    kind: VerifyCheckKind::PageChainBounds,
                    page_number: page_idx,
                    message: format!(
                        "Page {} next pointer {} is out of bounds (total: {})",
                        page_idx, header.next_page, total_pages
                    ),
                    expected: Some(format!("< {} or FIL_NULL", total_pages)),
                    actual: Some(header.next_page.to_string()),
                });
            }
        }

        // Check 6: Trailer LSN match
        if config.check_trailer_lsn {
            trailer_checked += 1;
            let trailer_offset = ps - SIZE_FIL_TRAILER;
            if page_data.len() >= trailer_offset + 8 {
                let trailer_lsn_low =
                    BigEndian::read_u32(&page_data[trailer_offset + 4..trailer_offset + 8]);
                let header_lsn_low = (header.lsn & 0xFFFFFFFF) as u32;
                if trailer_lsn_low != header_lsn_low {
                    trailer_issues += 1;
                    findings.push(VerifyFinding {
                        kind: VerifyCheckKind::TrailerLsnMatch,
                        page_number: page_idx,
                        message: format!(
                            "Page {} header LSN low32 0x{:08X} != trailer 0x{:08X}",
                            page_idx, header_lsn_low, trailer_lsn_low
                        ),
                        expected: Some(format!("0x{:08X}", header_lsn_low)),
                        actual: Some(format!("0x{:08X}", trailer_lsn_low)),
                    });
                }
            }
        }
    }

    // Build summaries
    let mut summary = Vec::new();
    if config.check_page_numbers {
        summary.push(CheckSummary {
            kind: VerifyCheckKind::PageNumberSequence,
            pages_checked: page_num_checked,
            issues_found: page_num_issues,
            passed: page_num_issues == 0,
        });
    }
    if config.check_space_ids {
        summary.push(CheckSummary {
            kind: VerifyCheckKind::SpaceIdConsistency,
            pages_checked: space_id_checked,
            issues_found: space_id_issues,
            passed: space_id_issues == 0,
        });
    }
    if config.check_lsn_monotonicity {
        summary.push(CheckSummary {
            kind: VerifyCheckKind::LsnMonotonicity,
            pages_checked: lsn_checked,
            issues_found: lsn_issues,
            passed: lsn_issues == 0,
        });
    }
    if config.check_btree_levels {
        summary.push(CheckSummary {
            kind: VerifyCheckKind::BTreeLevelConsistency,
            pages_checked: btree_checked,
            issues_found: btree_issues,
            passed: btree_issues == 0,
        });
    }
    if config.check_chain_bounds {
        summary.push(CheckSummary {
            kind: VerifyCheckKind::PageChainBounds,
            pages_checked: chain_checked,
            issues_found: chain_issues,
            passed: chain_issues == 0,
        });
    }
    if config.check_trailer_lsn {
        summary.push(CheckSummary {
            kind: VerifyCheckKind::TrailerLsnMatch,
            pages_checked: trailer_checked,
            issues_found: trailer_issues,
            passed: trailer_issues == 0,
        });
    }

    let passed = summary.iter().all(|s| s.passed);

    VerifyReport {
        file: file.to_string(),
        total_pages,
        page_size,
        passed,
        findings,
        summary,
    }
}

// ---------------------------------------------------------------------------
// Redo log continuity verification (#102)
// ---------------------------------------------------------------------------

/// Result of verifying redo log continuity against a tablespace.
#[derive(Debug, Clone, Serialize)]
pub struct RedoVerifyResult {
    /// Path to the redo log file.
    pub redo_file: String,
    /// Checkpoint LSN from the redo log.
    pub checkpoint_lsn: u64,
    /// Maximum LSN found across all tablespace pages.
    pub tablespace_max_lsn: u64,
    /// Whether the redo log covers the tablespace (checkpoint >= max page LSN).
    pub covers_tablespace: bool,
    /// LSN gap (tablespace_max_lsn - checkpoint_lsn) if not covered; 0 otherwise.
    pub lsn_gap: u64,
}

/// Verify redo log continuity against a tablespace.
///
/// Opens the redo log, reads the most recent checkpoint LSN (higher of the
/// two checkpoint slots), and compares it against the maximum LSN found
/// across all pages in the tablespace.
#[cfg(not(target_arch = "wasm32"))]
pub fn verify_redo_continuity(
    redo_path: &str,
    all_pages: &[u8],
    page_size: u32,
) -> Result<RedoVerifyResult, IdbError> {
    use crate::innodb::log::LogFile;

    let mut log = LogFile::open(redo_path)?;
    let cp0 = log.read_checkpoint(0)?;
    let cp1 = log.read_checkpoint(1)?;
    let checkpoint_lsn = cp0.lsn.max(cp1.lsn);

    let ps = page_size as usize;
    let total_pages = all_pages.len() / ps;
    let mut max_lsn: u64 = 0;

    for i in 0..total_pages {
        let page_data = &all_pages[i * ps..(i + 1) * ps];
        if page_data.iter().all(|&b| b == 0) {
            continue;
        }
        if let Some(header) = FilHeader::parse(page_data) {
            if header.lsn > max_lsn {
                max_lsn = header.lsn;
            }
        }
    }

    let covers_tablespace = checkpoint_lsn >= max_lsn;
    let lsn_gap = if covers_tablespace {
        0
    } else {
        max_lsn - checkpoint_lsn
    };

    Ok(RedoVerifyResult {
        redo_file: redo_path.to_string(),
        checkpoint_lsn,
        tablespace_max_lsn: max_lsn,
        covers_tablespace,
        lsn_gap,
    })
}

// ---------------------------------------------------------------------------
// Backup chain verification (#101)
// ---------------------------------------------------------------------------

/// Information extracted from a tablespace file for chain ordering.
#[derive(Debug, Clone, Serialize)]
pub struct ChainFileInfo {
    /// Path to the file.
    pub file: String,
    /// Space ID from page 0.
    pub space_id: u32,
    /// Maximum LSN found across all pages.
    pub max_lsn: u64,
    /// Minimum non-zero LSN found.
    pub min_lsn: u64,
    /// Total pages in the file.
    pub total_pages: u64,
}

/// A gap detected between consecutive files in the chain.
#[derive(Debug, Clone, Serialize)]
pub struct ChainGap {
    /// The file before the gap.
    pub from_file: String,
    /// Max LSN of the file before the gap.
    pub from_max_lsn: u64,
    /// The file after the gap.
    pub to_file: String,
    /// Min LSN of the file after the gap.
    pub to_min_lsn: u64,
    /// Size of the LSN gap.
    pub gap_size: u64,
}

/// Report of backup chain verification.
#[derive(Debug, Clone, Serialize)]
pub struct ChainReport {
    /// Ordered list of files in the chain (by max LSN).
    pub files: Vec<ChainFileInfo>,
    /// Gaps detected between consecutive files.
    pub gaps: Vec<ChainGap>,
    /// Whether the chain is contiguous (no gaps).
    pub contiguous: bool,
    /// Whether all files share the same space_id.
    pub consistent_space_id: bool,
}

/// Extract chain info from a tablespace's raw page data.
pub fn extract_chain_file_info(all_pages: &[u8], page_size: u32, file: &str) -> ChainFileInfo {
    let ps = page_size as usize;
    let total_pages = (all_pages.len() / ps) as u64;
    let mut max_lsn: u64 = 0;
    let mut min_lsn: u64 = u64::MAX;
    let mut space_id: u32 = 0;

    for i in 0..total_pages as usize {
        let page_data = &all_pages[i * ps..(i + 1) * ps];
        if page_data.iter().all(|&b| b == 0) {
            continue;
        }
        if let Some(header) = FilHeader::parse(page_data) {
            if i == 0 {
                space_id = header.space_id;
            }
            if header.lsn > max_lsn {
                max_lsn = header.lsn;
            }
            if header.lsn > 0 && header.lsn < min_lsn {
                min_lsn = header.lsn;
            }
        }
    }

    if min_lsn == u64::MAX {
        min_lsn = 0;
    }

    ChainFileInfo {
        file: file.to_string(),
        space_id,
        max_lsn,
        min_lsn,
        total_pages,
    }
}

/// Verify a backup chain given pre-extracted file info.
///
/// Orders files by max_lsn and checks for LSN gaps between consecutive files.
/// A gap exists when one file's min_lsn is greater than the previous file's max_lsn.
pub fn verify_backup_chain(mut files_info: Vec<ChainFileInfo>) -> ChainReport {
    if files_info.is_empty() {
        return ChainReport {
            files: vec![],
            gaps: vec![],
            contiguous: true,
            consistent_space_id: true,
        };
    }

    // Sort by max_lsn ascending
    files_info.sort_by_key(|f| f.max_lsn);

    // Check space_id consistency
    let first_space_id = files_info[0].space_id;
    let consistent_space_id = files_info.iter().all(|f| f.space_id == first_space_id);

    // Detect gaps
    let mut gaps = Vec::new();
    for pair in files_info.windows(2) {
        let prev = &pair[0];
        let next = &pair[1];
        // If next file's min LSN > prev file's max LSN, there's a gap
        if next.min_lsn > prev.max_lsn {
            gaps.push(ChainGap {
                from_file: prev.file.clone(),
                from_max_lsn: prev.max_lsn,
                to_file: next.file.clone(),
                to_min_lsn: next.min_lsn,
                gap_size: next.min_lsn - prev.max_lsn,
            });
        }
    }

    let contiguous = gaps.is_empty();

    ChainReport {
        files: files_info,
        gaps,
        contiguous,
        consistent_space_id,
    }
}
