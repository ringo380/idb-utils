//! Crash recovery simulation.
//!
//! Models InnoDB's `innodb_force_recovery` levels 1-6 to predict data
//! recoverability at each level without actually attempting recovery. Classifies
//! every page by the minimum recovery level needed to access it, then aggregates
//! per-index and per-table impact estimates.
//!
//! # Recovery levels
//!
//! | Level | MySQL constant | Effect |
//! |-------|---------------|--------|
//! | 0 | (normal) | Full crash recovery |
//! | 1 | SRV_FORCE_IGNORE_CORRUPT | Skip corrupt pages |
//! | 2 | SRV_FORCE_NO_BACKGROUND | Prevent background threads |
//! | 3 | SRV_FORCE_NO_TRX_UNDO | Skip transaction rollbacks |
//! | 4 | SRV_FORCE_NO_IBUF_MERGE | Skip insert buffer merge |
//! | 5 | SRV_FORCE_NO_UNDO_LOG_SCAN | Skip undo log scan |
//! | 6 | SRV_FORCE_NO_LOG_REDO | Skip redo log application |

use std::collections::{BTreeMap, HashMap};

use serde::Serialize;

use crate::innodb::checksum::{validate_checksum, validate_lsn};
use crate::innodb::corruption::{classify_corruption, CorruptionPattern};
use crate::innodb::index::IndexHeader;
use crate::innodb::page::FilHeader;
use crate::innodb::page_types::PageType;
use crate::innodb::sdi;
use crate::innodb::tablespace::Tablespace;
use crate::IdbError;

// ---------------------------------------------------------------------------
// Recovery level constants
// ---------------------------------------------------------------------------

/// Recovery level descriptions matching MySQL `innodb_force_recovery` values.
const LEVEL_INFO: [(u8, &str, &str); 7] = [
    (
        0,
        "Normal recovery",
        "Full crash recovery; all checks applied",
    ),
    (
        1,
        "SRV_FORCE_IGNORE_CORRUPT",
        "Skip corrupt pages during recovery",
    ),
    (
        2,
        "SRV_FORCE_NO_BACKGROUND",
        "Prevent background threads (purge, insert buffer merge thread)",
    ),
    (
        3,
        "SRV_FORCE_NO_TRX_UNDO",
        "Skip transaction rollbacks after recovery",
    ),
    (
        4,
        "SRV_FORCE_NO_IBUF_MERGE",
        "Skip insert buffer merge operations",
    ),
    (
        5,
        "SRV_FORCE_NO_UNDO_LOG_SCAN",
        "Skip undo log scan; treats incomplete transactions as committed",
    ),
    (
        6,
        "SRV_FORCE_NO_LOG_REDO",
        "Skip redo log application; tablespace opened as-is",
    ),
];

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/// Per-page recovery classification.
#[derive(Debug, Clone, Serialize)]
pub struct PageRecoveryStatus {
    /// Page number within the tablespace.
    pub page_number: u64,
    /// Page type string (e.g., "INDEX", "UNDO_LOG", "FSP_HDR").
    pub page_type: String,
    /// InnoDB index ID (only for INDEX pages).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_id: Option<u64>,
    /// Whether the stored checksum is valid.
    pub checksum_valid: bool,
    /// Whether header and trailer LSN values are consistent.
    pub lsn_consistent: bool,
    /// Corruption pattern classification (only for corrupt pages).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub corruption_pattern: Option<String>,
    /// Number of user records on the page (INDEX pages only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record_count: Option<u16>,
    /// B+Tree level (0 = leaf; INDEX pages only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub btree_level: Option<u16>,
    /// Minimum `innodb_force_recovery` level required to access this page.
    pub min_recovery_level: u8,
}

/// Per-index impact summary.
#[derive(Debug, Clone, Serialize)]
pub struct IndexImpact {
    /// InnoDB internal index ID.
    pub index_id: u64,
    /// Index name from SDI metadata (if available).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    /// Whether this is the clustered (primary) index.
    pub is_clustered: bool,
    /// Total pages belonging to this index.
    pub total_pages: u64,
    /// Pages with valid checksums.
    pub intact_pages: u64,
    /// Pages with invalid checksums.
    pub corrupt_pages: u64,
    /// All-zero pages belonging to this index.
    pub empty_pages: u64,
    /// Total user records across intact leaf pages.
    pub total_records: u64,
    /// Records at risk (on corrupt leaf pages) at each recovery level.
    pub lost_records_by_level: BTreeMap<u8, u64>,
}

/// Per-table impact summary.
#[derive(Debug, Clone, Serialize)]
pub struct TableImpact {
    /// Table name from SDI metadata (if available).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_name: Option<String>,
    /// Index-level impacts for this table.
    pub indexes: Vec<IndexImpact>,
    /// Data loss estimates per recovery level.
    pub data_loss_by_level: BTreeMap<u8, DataLossEstimate>,
}

/// Data loss estimate at a specific recovery level.
#[derive(Debug, Clone, Serialize)]
pub struct DataLossEstimate {
    /// Recovery level (0-6).
    pub level: u8,
    /// Whether the table is accessible at this level.
    pub accessible: bool,
    /// Number of corrupt pages that would be skipped.
    pub corrupt_pages_skipped: u64,
    /// Estimated number of records on corrupt leaf pages.
    pub records_at_risk: u64,
    /// Percentage of total records at risk (0.0-100.0).
    pub pct_data_at_risk: f64,
}

/// Assessment of a single recovery level across the entire tablespace.
#[derive(Debug, Clone, Serialize)]
pub struct LevelAssessment {
    /// Recovery level (0-6).
    pub level: u8,
    /// MySQL constant name (e.g., "SRV_FORCE_IGNORE_CORRUPT").
    pub name: &'static str,
    /// Human-readable description.
    pub description: &'static str,
    /// Number of tables accessible at this level.
    pub tables_accessible: u64,
    /// Total number of tables.
    pub total_tables: u64,
    /// Number of tables that would lose some data.
    pub tables_with_data_loss: u64,
    /// Total records at risk across all tables.
    pub total_records_at_risk: u64,
    /// Overall percentage of data at risk.
    pub pct_overall_risk: f64,
    /// Level-specific warnings.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

/// Recovery plan with recommended level.
#[derive(Debug, Clone, Serialize)]
pub struct RecoveryPlan {
    /// Recommended `innodb_force_recovery` level (0-6).
    pub recommended_level: u8,
    /// Human-readable rationale for the recommendation.
    pub rationale: String,
    /// Per-level assessment.
    pub levels: Vec<LevelAssessment>,
}

/// Page count summary.
#[derive(Debug, Clone, Serialize)]
pub struct PageSummary {
    pub intact: u64,
    pub corrupt: u64,
    pub empty: u64,
    pub unreadable: u64,
}

/// Complete simulation report.
#[derive(Debug, Clone, Serialize)]
pub struct SimulationReport {
    /// Path to the analyzed file.
    pub file: String,
    /// Page size in bytes.
    pub page_size: u32,
    /// Total number of pages.
    pub total_pages: u64,
    /// Database vendor (MySQL, Percona, MariaDB).
    pub vendor: String,
    /// Page count summary.
    pub page_summary: PageSummary,
    /// Per-page details (only populated in verbose mode).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub pages: Vec<PageRecoveryStatus>,
    /// Per-table impact analysis.
    pub tables: Vec<TableImpact>,
    /// Recovery plan with recommendation.
    pub plan: RecoveryPlan,
}

// ---------------------------------------------------------------------------
// Page-level classification
// ---------------------------------------------------------------------------

/// Classify the minimum recovery level needed to access a page.
///
/// Returns the `innodb_force_recovery` level (0-6) required for MySQL to
/// skip over or tolerate any corruption on this page.
fn classify_page_recovery_level(page_type: &PageType, checksum_valid: bool, is_empty: bool) -> u8 {
    // Valid or empty pages are accessible at level 0
    if checksum_valid || is_empty {
        return 0;
    }

    // Corrupt page — level depends on page type
    match page_type {
        // FSP_HDR (page 0) and INODE contain tablespace/segment metadata;
        // corruption here is catastrophic
        PageType::FspHdr | PageType::Inode => 6,

        // Extent descriptors — needed for space management
        PageType::Xdes => 5,

        // Insert buffer bitmap — needed for ibuf merge
        PageType::IbufBitmap => 4,

        // Undo log pages — needed for transaction rollback
        PageType::UndoLog => 3,

        // Data pages, SDI, LOB — skippable at level 1
        PageType::Index
        | PageType::Sdi
        | PageType::Blob
        | PageType::ZBlob
        | PageType::ZBlob2
        | PageType::LobFirst
        | PageType::LobData
        | PageType::LobIndex
        | PageType::ZlobFirst
        | PageType::ZlobData
        | PageType::ZlobIndex
        | PageType::ZlobFrag
        | PageType::ZlobFragEntry
        | PageType::SdiBlob
        | PageType::SdiZblob
        | PageType::Rtree => 1,

        // Anything else (system pages, unknown types) — assume level 1
        _ => 1,
    }
}

// ---------------------------------------------------------------------------
// Core simulation
// ---------------------------------------------------------------------------

/// Internal page scan result used before aggregation.
struct ScannedPage {
    page_number: u64,
    page_type_name: String,
    index_id: Option<u64>,
    checksum_valid: bool,
    lsn_consistent: bool,
    corruption_pattern: Option<CorruptionPattern>,
    record_count: Option<u16>,
    btree_level: Option<u16>,
    is_empty: bool,
    min_recovery_level: u8,
}

/// Run crash recovery simulation on a tablespace.
///
/// Scans every page, classifies recoverability at each level, aggregates
/// per-index and per-table impact, and builds a recovery plan with an
/// optimal level recommendation.
///
/// Pass `sdi_json` (from an SDI record with `sdi_type == 1`) to enable
/// index name and table name resolution. If `None`, indexes are identified
/// by ID only.
pub fn simulate_recovery(
    ts: &mut Tablespace,
    sdi_json: Option<&str>,
    file_path: &str,
    verbose: bool,
) -> Result<SimulationReport, IdbError> {
    let page_size = ts.page_size();
    let total_pages = ts.page_count();
    let vendor = ts.vendor_info().vendor.to_string();
    let vendor_info = ts.vendor_info().clone();

    // Phase 1: Page scan
    let mut scanned: Vec<ScannedPage> = Vec::with_capacity(total_pages as usize);

    ts.for_each_page(|page_num, page_data| {
        let is_empty = page_data.iter().all(|&b| b == 0);

        if is_empty {
            scanned.push(ScannedPage {
                page_number: page_num,
                page_type_name: "ALLOCATED".to_string(),
                index_id: None,
                checksum_valid: true,
                lsn_consistent: true,
                corruption_pattern: None,
                record_count: None,
                btree_level: None,
                is_empty: true,
                min_recovery_level: 0,
            });
            return Ok(());
        }

        let header = FilHeader::parse(page_data);
        let (page_type, page_type_name) = match &header {
            Some(h) => (h.page_type, h.page_type.name().to_string()),
            None => (PageType::Unknown(0), "UNKNOWN".to_string()),
        };

        let checksum_result = validate_checksum(page_data, page_size, Some(&vendor_info));
        let checksum_valid = checksum_result.valid;
        let lsn_consistent = validate_lsn(page_data, page_size);

        let corruption_pattern = if !checksum_valid {
            Some(classify_corruption(page_data, page_size))
        } else {
            None
        };

        // Parse INDEX header for index_id, n_recs, level
        let (index_id, record_count, btree_level) = if page_type == PageType::Index {
            match IndexHeader::parse(page_data) {
                Some(idx_hdr) => (
                    Some(idx_hdr.index_id),
                    Some(idx_hdr.n_recs),
                    Some(idx_hdr.level),
                ),
                None => (None, None, None),
            }
        } else {
            (None, None, None)
        };

        let min_recovery_level = classify_page_recovery_level(&page_type, checksum_valid, false);

        scanned.push(ScannedPage {
            page_number: page_num,
            page_type_name,
            index_id,
            checksum_valid,
            lsn_consistent,
            corruption_pattern,
            record_count,
            btree_level,
            is_empty: false,
            min_recovery_level,
        });

        Ok(())
    })?;

    // Build verbose page list if requested
    let pages = if verbose {
        scanned
            .iter()
            .map(|s| PageRecoveryStatus {
                page_number: s.page_number,
                page_type: s.page_type_name.clone(),
                index_id: s.index_id,
                checksum_valid: s.checksum_valid,
                lsn_consistent: s.lsn_consistent,
                corruption_pattern: s.corruption_pattern.as_ref().map(|p| p.name().to_string()),
                record_count: s.record_count,
                btree_level: s.btree_level,
                min_recovery_level: s.min_recovery_level,
            })
            .collect()
    } else {
        Vec::new()
    };

    // Page summary
    let mut intact = 0u64;
    let mut corrupt = 0u64;
    let mut empty = 0u64;
    for s in &scanned {
        if s.is_empty {
            empty += 1;
        } else if s.checksum_valid {
            intact += 1;
        } else {
            corrupt += 1;
        }
    }
    let page_summary = PageSummary {
        intact,
        corrupt,
        empty,
        unreadable: 0,
    };

    // Phase 2: Index/table aggregation
    let index_name_map = sdi_json.map(sdi::build_index_name_map).unwrap_or_default();
    let index_table_map = sdi_json.map(sdi::build_index_table_map).unwrap_or_default();

    // Group pages by index_id (only INDEX pages)
    let mut index_pages: HashMap<u64, Vec<&ScannedPage>> = HashMap::new();
    for s in &scanned {
        if let Some(idx_id) = s.index_id {
            index_pages.entry(idx_id).or_default().push(s);
        }
    }

    // Determine which index_id is the clustered index per table
    // (lowest index_id per table name is assumed to be clustered)
    let mut table_min_index: HashMap<String, u64> = HashMap::new();
    for (&idx_id, table_name) in &index_table_map {
        let entry = table_min_index.entry(table_name.clone()).or_insert(idx_id);
        if idx_id < *entry {
            *entry = idx_id;
        }
    }
    // For indexes without SDI, group them by lowest index_id
    let mut unknown_min_index: Option<u64> = None;
    for &idx_id in index_pages.keys() {
        if !index_table_map.contains_key(&idx_id) {
            match unknown_min_index {
                Some(ref mut min) => {
                    if idx_id < *min {
                        *min = idx_id;
                    }
                }
                None => unknown_min_index = Some(idx_id),
            }
        }
    }

    // Build per-index impacts
    let mut index_impacts: Vec<IndexImpact> = Vec::new();
    let mut sorted_index_ids: Vec<u64> = index_pages.keys().copied().collect();
    sorted_index_ids.sort();

    for idx_id in sorted_index_ids {
        let pages_for_index = &index_pages[&idx_id];
        let mut total = 0u64;
        let mut intact_count = 0u64;
        let mut corrupt_count = 0u64;
        let mut empty_count = 0u64;
        let mut total_records = 0u64;
        let mut corrupt_leaf_records = 0u64;

        for s in pages_for_index {
            total += 1;
            if s.is_empty {
                empty_count += 1;
            } else if s.checksum_valid {
                intact_count += 1;
                // Count records on intact leaf pages (level 0)
                if s.btree_level == Some(0) {
                    total_records += s.record_count.unwrap_or(0) as u64;
                }
            } else {
                corrupt_count += 1;
                // Estimate records on corrupt leaf pages using average from intact pages
                if s.btree_level == Some(0) || s.btree_level.is_none() {
                    // Use record_count if header was parseable, else we'll estimate later
                    corrupt_leaf_records += s.record_count.unwrap_or(0) as u64;
                }
            }
        }

        // If corrupt leaf pages have no record count (header unparseable),
        // estimate from average of intact leaf pages
        let intact_leaf_count = pages_for_index
            .iter()
            .filter(|s| s.checksum_valid && s.btree_level == Some(0))
            .count() as u64;
        let avg_records_per_leaf = if intact_leaf_count > 0 {
            total_records / intact_leaf_count
        } else {
            0
        };
        let corrupt_leaf_no_header = pages_for_index
            .iter()
            .filter(|s| {
                !s.checksum_valid
                    && !s.is_empty
                    && s.record_count.is_none()
                    && (s.btree_level == Some(0) || s.btree_level.is_none())
            })
            .count() as u64;
        corrupt_leaf_records += corrupt_leaf_no_header * avg_records_per_leaf;

        // Records at risk by level: at level 0, corrupt pages make the table
        // inaccessible (communicated via accessible=false in DataLossEstimate);
        // at level 1+, corrupt INDEX pages are skipped (data lost but DB starts).
        // The lost count is always the corrupt-leaf record estimate — the
        // accessible flag distinguishes "table won't open" from "some rows lost".
        let mut lost_by_level = BTreeMap::new();
        let records_at_risk = corrupt_leaf_records;
        for lvl in 0..=6 {
            lost_by_level.insert(lvl, records_at_risk);
        }

        let is_clustered = if let Some(table_name) = index_table_map.get(&idx_id) {
            table_min_index.get(table_name) == Some(&idx_id)
        } else {
            unknown_min_index == Some(idx_id)
        };

        index_impacts.push(IndexImpact {
            index_id: idx_id,
            index_name: index_name_map.get(&idx_id).cloned(),
            is_clustered,
            total_pages: total,
            intact_pages: intact_count,
            corrupt_pages: corrupt_count,
            empty_pages: empty_count,
            total_records,
            lost_records_by_level: lost_by_level,
        });
    }

    // Group indexes into tables
    let mut table_groups: BTreeMap<String, Vec<IndexImpact>> = BTreeMap::new();
    let mut unknown_indexes: Vec<IndexImpact> = Vec::new();

    for impact in index_impacts {
        if let Some(table_name) = index_table_map.get(&impact.index_id) {
            table_groups
                .entry(table_name.clone())
                .or_default()
                .push(impact);
        } else {
            unknown_indexes.push(impact);
        }
    }

    let mut tables: Vec<TableImpact> = Vec::new();

    for (table_name, indexes) in table_groups {
        let data_loss = build_table_data_loss(&indexes);
        tables.push(TableImpact {
            table_name: Some(table_name),
            indexes,
            data_loss_by_level: data_loss,
        });
    }

    if !unknown_indexes.is_empty() {
        let data_loss = build_table_data_loss(&unknown_indexes);
        tables.push(TableImpact {
            table_name: None,
            indexes: unknown_indexes,
            data_loss_by_level: data_loss,
        });
    }

    // Phase 3: Recovery plan
    let plan = build_recovery_plan(&tables, &scanned);

    Ok(SimulationReport {
        file: file_path.to_string(),
        page_size,
        total_pages,
        vendor,
        page_summary,
        pages,
        tables,
        plan,
    })
}

/// Build per-level data loss estimates for a table from its index impacts.
fn build_table_data_loss(indexes: &[IndexImpact]) -> BTreeMap<u8, DataLossEstimate> {
    let mut estimates = BTreeMap::new();

    // Total records across all indexes' intact leaf pages
    let total_records: u64 = indexes.iter().map(|i| i.total_records).sum();
    let total_corrupt: u64 = indexes.iter().map(|i| i.corrupt_pages).sum();

    // Check if any critical (non-INDEX) corruption blocks access
    let has_critical_corruption = indexes.iter().any(|i| i.corrupt_pages > 0);

    for level in 0..=6u8 {
        let records_at_risk: u64 = indexes
            .iter()
            .map(|i| i.lost_records_by_level.get(&level).copied().unwrap_or(0))
            .sum();

        let pct = if total_records + records_at_risk > 0 {
            (records_at_risk as f64 / (total_records + records_at_risk) as f64) * 100.0
        } else if total_corrupt > 0 {
            100.0
        } else {
            0.0
        };

        // At level 0, table is inaccessible if any pages are corrupt
        let accessible = if level == 0 {
            !has_critical_corruption
        } else {
            true
        };

        let corrupt_skipped = if level == 0 { 0 } else { total_corrupt };

        estimates.insert(
            level,
            DataLossEstimate {
                level,
                accessible,
                corrupt_pages_skipped: corrupt_skipped,
                records_at_risk,
                pct_data_at_risk: (pct * 100.0).round() / 100.0,
            },
        );
    }

    estimates
}

/// Build the recovery plan with level assessments and recommendation.
fn build_recovery_plan(tables: &[TableImpact], scanned: &[ScannedPage]) -> RecoveryPlan {
    let total_tables = tables.len() as u64;

    // Find the maximum min_recovery_level across all pages
    let max_level_needed = scanned
        .iter()
        .map(|s| s.min_recovery_level)
        .max()
        .unwrap_or(0);

    // Total records across all tables
    let total_records: u64 = tables
        .iter()
        .flat_map(|t| &t.indexes)
        .map(|i| i.total_records)
        .sum();

    let mut levels = Vec::with_capacity(7);

    for level in 0..=6u8 {
        let (name, description) = LEVEL_INFO
            .iter()
            .find(|(l, _, _)| *l == level)
            .map(|(_, n, d)| (*n, *d))
            .unwrap_or(("Unknown", "Unknown level"));

        let tables_accessible = tables
            .iter()
            .filter(|t| {
                t.data_loss_by_level
                    .get(&level)
                    .map(|e| e.accessible)
                    .unwrap_or(false)
            })
            .count() as u64;

        let tables_with_loss = tables
            .iter()
            .filter(|t| {
                t.data_loss_by_level
                    .get(&level)
                    .map(|e| e.records_at_risk > 0)
                    .unwrap_or(false)
            })
            .count() as u64;

        let records_at_risk: u64 = tables
            .iter()
            .filter_map(|t| t.data_loss_by_level.get(&level))
            .map(|e| e.records_at_risk)
            .sum();

        let total_including_corrupt = total_records + records_at_risk;
        let pct_risk = if total_including_corrupt > 0 {
            (records_at_risk as f64 / total_including_corrupt as f64) * 100.0
        } else {
            0.0
        };

        let mut warnings = Vec::new();
        if level >= 3 {
            warnings.push("Uncommitted transactions will not be rolled back".to_string());
        }
        if level >= 4 {
            warnings
                .push("Insert buffer merge skipped; secondary indexes may be stale".to_string());
        }
        if level >= 5 {
            warnings.push("Undo log scan skipped; transaction state unknown".to_string());
        }
        if level >= 6 {
            warnings.push("Redo log replay skipped; pages may reflect pre-crash state".to_string());
        }

        levels.push(LevelAssessment {
            level,
            name,
            description,
            tables_accessible,
            total_tables,
            tables_with_data_loss: tables_with_loss,
            total_records_at_risk: records_at_risk,
            pct_overall_risk: (pct_risk * 100.0).round() / 100.0,
            warnings,
        });
    }

    // Recommendation: lowest level where all tables are accessible
    let recommended = if max_level_needed == 0 {
        0
    } else {
        levels
            .iter()
            .find(|l| l.tables_accessible == total_tables && l.level >= max_level_needed)
            .map(|l| l.level)
            .unwrap_or(6)
    };

    let rationale = build_rationale(recommended, &levels, scanned);

    RecoveryPlan {
        recommended_level: recommended,
        rationale,
        levels,
    }
}

/// Generate a human-readable rationale for the recommended level.
fn build_rationale(recommended: u8, levels: &[LevelAssessment], scanned: &[ScannedPage]) -> String {
    let corrupt_count = scanned
        .iter()
        .filter(|s| !s.checksum_valid && !s.is_empty)
        .count();

    if recommended == 0 {
        return "No corrupt pages detected. Normal recovery (level 0) is sufficient.".to_string();
    }

    let level_info = &levels[recommended as usize];
    let mut parts = Vec::new();

    parts.push(format!(
        "Level {} ({}) recommended.",
        recommended, level_info.name
    ));

    if corrupt_count > 0 {
        parts.push(format!(
            "{} corrupt page{} detected.",
            corrupt_count,
            if corrupt_count == 1 { "" } else { "s" }
        ));
    }

    if level_info.tables_accessible == level_info.total_tables {
        parts.push("All tables accessible at this level.".to_string());
    } else {
        parts.push(format!(
            "{}/{} tables accessible.",
            level_info.tables_accessible, level_info.total_tables
        ));
    }

    if level_info.total_records_at_risk > 0 {
        parts.push(format!(
            "~{} records at risk ({:.1}% of data).",
            level_info.total_records_at_risk, level_info.pct_overall_risk
        ));
    }

    parts.push(
        "Based on static file analysis; redo log replay may recover additional pages.".to_string(),
    );

    parts.join(" ")
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

    /// Build a minimal valid page with correct CRC-32C checksum.
    fn build_page(page_number: u32, page_type: u16, page_size: u32) -> Vec<u8> {
        let ps = page_size as usize;
        let mut page = vec![0u8; ps];

        // FIL header
        BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], page_number);
        BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], 0xFFFFFFFF);
        BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], 0xFFFFFFFF);
        BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], 1000 + page_number as u64);
        BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], page_type);
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], 1);

        // FIL trailer — LSN low 32 bits must match header
        let lsn = 1000 + page_number as u64;
        BigEndian::write_u32(&mut page[ps - 4..], lsn as u32);

        // CRC-32C checksum
        stamp_crc32c(&mut page, ps);

        page
    }

    /// Build an INDEX page with index_id, n_recs, and level.
    fn build_index_page(
        page_number: u32,
        index_id: u64,
        n_recs: u16,
        level: u16,
        page_size: u32,
    ) -> Vec<u8> {
        let ps = page_size as usize;
        let mut page = build_page(page_number, 17855, page_size); // INDEX = 17855

        let base = FIL_PAGE_DATA;
        BigEndian::write_u16(&mut page[base + PAGE_N_RECS..], n_recs);
        BigEndian::write_u16(&mut page[base + PAGE_N_HEAP..], 0x8000 | (n_recs + 2)); // compact flag
        BigEndian::write_u16(&mut page[base + PAGE_HEAP_TOP..], 200); // arbitrary
        BigEndian::write_u16(&mut page[base + PAGE_LEVEL..], level);
        BigEndian::write_u64(&mut page[base + PAGE_INDEX_ID..], index_id);

        // Re-stamp checksum after INDEX header changes
        stamp_crc32c(&mut page, ps);

        page
    }

    /// Compute and write CRC-32C checksum for a page.
    fn stamp_crc32c(page: &mut [u8], page_size: usize) {
        let crc1 = crc32c::crc32c(&page[4..26]);
        let crc2 = crc32c::crc32c(&page[38..page_size - 8]);
        let checksum = crc1 ^ crc2;
        BigEndian::write_u32(&mut page[0..4], checksum);
        // Trailer checksum
        BigEndian::write_u32(&mut page[page_size - 8..page_size - 4], checksum);
    }

    /// Build a valid FSP_HDR page (page 0).
    fn build_fsp_page(page_size: u32, total_pages: u32) -> Vec<u8> {
        let mut page = build_page(0, 8, page_size); // FSP_HDR = 8
        let base = FIL_PAGE_DATA;
        // space_id
        BigEndian::write_u32(&mut page[base..], 1);
        // size (pages)
        BigEndian::write_u32(&mut page[base + 8..], total_pages);
        // flags — encode page size (for auto-detection to work, we set the proper flags)
        // For 16K default, flags can be 0
        BigEndian::write_u32(&mut page[base + 16..], 0);

        stamp_crc32c(&mut page, page_size as usize);
        page
    }

    /// Write a tablespace file from page buffers.
    fn write_tablespace(pages: &[Vec<u8>]) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        for page in pages {
            file.write_all(page).unwrap();
        }
        file.flush().unwrap();
        file
    }

    #[test]
    fn test_classify_level_valid_page() {
        assert_eq!(
            classify_page_recovery_level(&PageType::Index, true, false),
            0
        );
    }

    #[test]
    fn test_classify_level_empty_page() {
        assert_eq!(
            classify_page_recovery_level(&PageType::Allocated, false, true),
            0
        );
    }

    #[test]
    fn test_classify_level_corrupt_index() {
        assert_eq!(
            classify_page_recovery_level(&PageType::Index, false, false),
            1
        );
    }

    #[test]
    fn test_classify_level_corrupt_undo() {
        assert_eq!(
            classify_page_recovery_level(&PageType::UndoLog, false, false),
            3
        );
    }

    #[test]
    fn test_classify_level_corrupt_ibuf() {
        assert_eq!(
            classify_page_recovery_level(&PageType::IbufBitmap, false, false),
            4
        );
    }

    #[test]
    fn test_classify_level_corrupt_xdes() {
        assert_eq!(
            classify_page_recovery_level(&PageType::Xdes, false, false),
            5
        );
    }

    #[test]
    fn test_classify_level_corrupt_fsp_hdr() {
        assert_eq!(
            classify_page_recovery_level(&PageType::FspHdr, false, false),
            6
        );
    }

    #[test]
    fn test_classify_level_corrupt_inode() {
        assert_eq!(
            classify_page_recovery_level(&PageType::Inode, false, false),
            6
        );
    }

    #[test]
    fn test_all_valid_pages_level_zero() {
        let page_size = 16384u32;
        let fsp = build_fsp_page(page_size, 4);
        let idx1 = build_index_page(1, 100, 50, 0, page_size);
        let idx2 = build_index_page(2, 100, 45, 0, page_size);
        let idx3 = build_index_page(3, 101, 30, 0, page_size);
        let file = write_tablespace(&[fsp, idx1, idx2, idx3]);

        let mut ts = Tablespace::open(file.path().to_str().unwrap()).unwrap();
        let report =
            simulate_recovery(&mut ts, None, file.path().to_str().unwrap(), false).unwrap();

        assert_eq!(report.plan.recommended_level, 0);
        assert_eq!(report.page_summary.corrupt, 0);
        assert_eq!(report.page_summary.intact, 4);
    }

    #[test]
    fn test_corrupt_index_page_needs_level_one() {
        let page_size = 16384u32;
        let fsp = build_fsp_page(page_size, 3);
        let idx1 = build_index_page(1, 100, 50, 0, page_size);
        let mut idx2 = build_index_page(2, 100, 40, 0, page_size);
        // Corrupt the checksum
        idx2[0] ^= 0xFF;
        let file = write_tablespace(&[fsp, idx1, idx2]);

        let mut ts = Tablespace::open(file.path().to_str().unwrap()).unwrap();
        let report = simulate_recovery(&mut ts, None, file.path().to_str().unwrap(), true).unwrap();

        assert_eq!(report.plan.recommended_level, 1);
        assert_eq!(report.page_summary.corrupt, 1);
        // Verify page 2 needs level 1
        let page2 = report.pages.iter().find(|p| p.page_number == 2).unwrap();
        assert_eq!(page2.min_recovery_level, 1);
    }

    #[test]
    fn test_corrupt_fsp_hdr_needs_level_six() {
        let page_size = 16384u32;
        let mut fsp = build_fsp_page(page_size, 2);
        fsp[0] ^= 0xFF; // corrupt checksum
        let idx1 = build_index_page(1, 100, 50, 0, page_size);
        let file = write_tablespace(&[fsp, idx1]);

        let mut ts =
            Tablespace::open_with_page_size(file.path().to_str().unwrap(), page_size).unwrap();
        let report = simulate_recovery(&mut ts, None, file.path().to_str().unwrap(), true).unwrap();

        assert_eq!(report.plan.recommended_level, 6);
        let page0 = report.pages.iter().find(|p| p.page_number == 0).unwrap();
        assert_eq!(page0.min_recovery_level, 6);
    }

    #[test]
    fn test_empty_pages_no_impact() {
        let page_size = 16384u32;
        let fsp = build_fsp_page(page_size, 3);
        let empty = vec![0u8; page_size as usize];
        let idx = build_index_page(2, 100, 50, 0, page_size);
        let file = write_tablespace(&[fsp, empty, idx]);

        let mut ts = Tablespace::open(file.path().to_str().unwrap()).unwrap();
        let report =
            simulate_recovery(&mut ts, None, file.path().to_str().unwrap(), false).unwrap();

        assert_eq!(report.plan.recommended_level, 0);
        assert_eq!(report.page_summary.empty, 1);
        assert_eq!(report.page_summary.intact, 2);
    }

    #[test]
    fn test_multiple_indexes_independent() {
        let page_size = 16384u32;
        let fsp = build_fsp_page(page_size, 4);
        let idx100 = build_index_page(1, 100, 50, 0, page_size);
        let idx101 = build_index_page(2, 101, 30, 0, page_size);
        let mut idx101_corrupt = build_index_page(3, 101, 25, 0, page_size);
        idx101_corrupt[0] ^= 0xFF;
        let file = write_tablespace(&[fsp, idx100, idx101, idx101_corrupt]);

        let mut ts = Tablespace::open(file.path().to_str().unwrap()).unwrap();
        let report =
            simulate_recovery(&mut ts, None, file.path().to_str().unwrap(), false).unwrap();

        assert_eq!(report.plan.recommended_level, 1);

        // Find indexes — without SDI they'll be grouped into one unknown table
        let all_indexes: Vec<&IndexImpact> =
            report.tables.iter().flat_map(|t| &t.indexes).collect();
        let idx100_impact = all_indexes.iter().find(|i| i.index_id == 100).unwrap();
        let idx101_impact = all_indexes.iter().find(|i| i.index_id == 101).unwrap();

        assert_eq!(idx100_impact.corrupt_pages, 0);
        assert_eq!(idx101_impact.corrupt_pages, 1);
    }

    #[test]
    fn test_level_assessment_cumulative() {
        let page_size = 16384u32;
        let fsp = build_fsp_page(page_size, 3);
        let idx = build_index_page(1, 100, 50, 0, page_size);
        let mut corrupt_idx = build_index_page(2, 100, 40, 0, page_size);
        corrupt_idx[0] ^= 0xFF;
        let file = write_tablespace(&[fsp, idx, corrupt_idx]);

        let mut ts = Tablespace::open(file.path().to_str().unwrap()).unwrap();
        let report =
            simulate_recovery(&mut ts, None, file.path().to_str().unwrap(), false).unwrap();

        // Level 3 should include level 1's warnings plus its own
        let level1 = &report.plan.levels[1];
        let level3 = &report.plan.levels[3];
        assert!(level3.warnings.len() >= level1.warnings.len());
        // Records at risk should be same at all levels (just corrupt INDEX pages)
        let level0 = &report.plan.levels[0];
        assert_eq!(level0.total_records_at_risk, level1.total_records_at_risk);
        assert_eq!(level1.total_records_at_risk, level3.total_records_at_risk);
    }

    #[test]
    fn test_sdi_name_resolution() {
        let page_size = 16384u32;
        let fsp = build_fsp_page(page_size, 3);
        let idx1 = build_index_page(1, 139, 50, 0, page_size);
        let idx2 = build_index_page(2, 140, 30, 0, page_size);
        let file = write_tablespace(&[fsp, idx1, idx2]);

        let sdi_json = r#"{
            "dd_object": {
                "name": "test_table",
                "indexes": [
                    {"name": "PRIMARY", "se_private_data": "id=139;root=1;"},
                    {"name": "idx_name", "se_private_data": "id=140;root=2;"}
                ]
            }
        }"#;

        let mut ts = Tablespace::open(file.path().to_str().unwrap()).unwrap();
        let report = simulate_recovery(
            &mut ts,
            Some(sdi_json),
            file.path().to_str().unwrap(),
            false,
        )
        .unwrap();

        assert_eq!(report.tables.len(), 1);
        assert_eq!(report.tables[0].table_name.as_deref(), Some("test_table"));
        let primary = report.tables[0]
            .indexes
            .iter()
            .find(|i| i.index_id == 139)
            .unwrap();
        assert_eq!(primary.index_name.as_deref(), Some("PRIMARY"));
        assert!(primary.is_clustered);
    }
}
