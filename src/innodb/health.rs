//! B+Tree index health metrics for InnoDB tablespaces.
//!
//! Computes per-index health metrics including fill factor, fragmentation,
//! garbage ratio, and tree depth by analyzing INDEX page headers. The analysis
//! requires only a single pass over the tablespace pages.
//!
//! # Usage
//!
//! ```rust,ignore
//! use idb::innodb::tablespace::Tablespace;
//! use idb::innodb::health::{extract_index_page_snapshot, analyze_health};
//!
//! let mut ts = Tablespace::open("table.ibd").unwrap();
//! let page_size = ts.page_size();
//! let mut snapshots = Vec::new();
//! let mut empty_pages = 0u64;
//! let mut total_pages = 0u64;
//!
//! ts.for_each_page(|page_num, data| {
//!     total_pages += 1;
//!     if let Some(snap) = extract_index_page_snapshot(data, page_num) {
//!         snapshots.push(snap);
//!     } else if data.iter().all(|&b| b == 0) {
//!         empty_pages += 1;
//!     }
//!     Ok(())
//! }).unwrap();
//!
//! let report = analyze_health(snapshots, page_size, total_pages, empty_pages, "table.ibd");
//! println!("Indexes: {}", report.indexes.len());
//! ```

use serde::Serialize;
use std::collections::BTreeMap;

use crate::innodb::constants::*;
use crate::innodb::index::IndexHeader;
use crate::innodb::page::FilHeader;
use crate::innodb::page_types::PageType;

/// Intermediate per-page snapshot of an INDEX page's key metrics.
#[derive(Debug, Clone)]
pub struct IndexPageSnapshot {
    /// Page number within the tablespace.
    pub page_number: u64,
    /// Index ID this page belongs to.
    pub index_id: u64,
    /// B+Tree level (0 = leaf).
    pub level: u16,
    /// Byte offset of the heap top within the page.
    pub heap_top: u16,
    /// Bytes consumed by deleted/garbage records.
    pub garbage: u16,
    /// Number of user records on this page.
    pub n_recs: u16,
    /// Previous page pointer (FIL_NULL if none).
    pub prev: u32,
    /// Next page pointer (FIL_NULL if none).
    pub next: u32,
}

/// Aggregated health metrics for a single index.
#[derive(Debug, Clone, Serialize)]
pub struct IndexHealth {
    /// Index ID.
    pub index_id: u64,
    /// Optional index name (resolved from SDI, if available).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    /// Maximum B+Tree depth observed (leaf level is 0).
    pub tree_depth: u16,
    /// Total number of pages belonging to this index.
    pub total_pages: u64,
    /// Number of leaf-level pages (level 0).
    pub leaf_pages: u64,
    /// Number of non-leaf pages (level > 0).
    pub non_leaf_pages: u64,
    /// Total user records across all pages.
    pub total_records: u64,
    /// Average fill factor across all pages (0.0..1.0).
    pub avg_fill_factor: f64,
    /// Minimum fill factor across all pages.
    pub min_fill_factor: f64,
    /// Maximum fill factor across all pages.
    pub max_fill_factor: f64,
    /// Average garbage ratio across all pages (0.0..1.0).
    pub avg_garbage_ratio: f64,
    /// Total garbage bytes across all pages.
    pub total_garbage_bytes: u64,
    /// Fragmentation ratio for leaf pages (0.0..1.0).
    /// Measured as the fraction of non-sequential page transitions.
    pub fragmentation: f64,
    /// Number of leaf pages with zero user records.
    pub empty_leaf_pages: u64,
}

/// Tablespace-level health summary.
#[derive(Debug, Clone, Serialize)]
pub struct TablespaceHealth {
    /// Total pages in the tablespace.
    pub total_pages: u64,
    /// Number of INDEX pages.
    pub index_pages: u64,
    /// Number of non-INDEX pages (FSP, INODE, SDI, etc.).
    pub non_index_pages: u64,
    /// Number of all-zero (empty/allocated) pages.
    pub empty_pages: u64,
    /// Page size in bytes.
    pub page_size: u32,
    /// Average fill factor across all INDEX pages.
    pub avg_fill_factor: f64,
    /// Average garbage ratio across all INDEX pages.
    pub avg_garbage_ratio: f64,
    /// Average fragmentation across all indexes.
    pub avg_fragmentation: f64,
    /// Number of distinct indexes found.
    pub index_count: u64,
}

/// Top-level health report for a tablespace.
#[derive(Debug, Clone, Serialize)]
pub struct HealthReport {
    /// Path to the analyzed file.
    pub file: String,
    /// Tablespace-level summary.
    pub summary: TablespaceHealth,
    /// Per-index health metrics.
    pub indexes: Vec<IndexHealth>,
}

/// Extract an [`IndexPageSnapshot`] from raw page bytes.
///
/// Returns `None` if the page is not an INDEX page (type 17855) or if
/// the page data is too short to contain valid FIL + INDEX headers.
pub fn extract_index_page_snapshot(
    page_data: &[u8],
    page_number: u64,
) -> Option<IndexPageSnapshot> {
    let fil = FilHeader::parse(page_data)?;
    if fil.page_type != PageType::Index {
        return None;
    }

    let idx = IndexHeader::parse(page_data)?;

    Some(IndexPageSnapshot {
        page_number,
        index_id: idx.index_id,
        level: idx.level,
        heap_top: idx.heap_top,
        garbage: idx.garbage,
        n_recs: idx.n_recs,
        prev: fil.prev_page,
        next: fil.next_page,
    })
}

/// Compute the fill factor for a single INDEX page.
///
/// The usable data area is `page_size - PAGE_DATA_OFFSET - SIZE_FIL_TRAILER`.
/// The used portion is `heap_top - PAGE_DATA_OFFSET - garbage`.
/// Result is clamped to \[0.0, 1.0\].
pub fn compute_fill_factor(heap_top: u16, garbage: u16, page_size: u32) -> f64 {
    let usable = page_size as f64 - PAGE_DATA_OFFSET as f64 - SIZE_FIL_TRAILER as f64;
    if usable <= 0.0 {
        return 0.0;
    }
    let used = heap_top as f64 - PAGE_DATA_OFFSET as f64 - garbage as f64;
    (used / usable).clamp(0.0, 1.0)
}

/// Compute the garbage ratio for a single INDEX page.
///
/// Ratio of garbage bytes to the usable data area.
pub fn compute_garbage_ratio(garbage: u16, page_size: u32) -> f64 {
    let usable = page_size as f64 - PAGE_DATA_OFFSET as f64 - SIZE_FIL_TRAILER as f64;
    if usable <= 0.0 {
        return 0.0;
    }
    (garbage as f64 / usable).clamp(0.0, 1.0)
}

/// Compute leaf-level fragmentation for a set of pages sorted by page number.
///
/// Fragmentation is the ratio of non-sequential page transitions to the
/// total number of transitions. A perfectly defragmented index has all
/// leaf pages in sequential page-number order (fragmentation = 0.0).
///
/// Returns 0.0 for a single page (no transitions to measure).
pub fn compute_fragmentation(leaf_page_numbers: &mut [u64]) -> f64 {
    if leaf_page_numbers.len() <= 1 {
        return 0.0;
    }
    leaf_page_numbers.sort_unstable();
    let transitions = leaf_page_numbers.len() - 1;
    let non_sequential = leaf_page_numbers
        .windows(2)
        .filter(|w| w[1] != w[0] + 1)
        .count();
    non_sequential as f64 / transitions as f64
}

/// Analyze collected INDEX page snapshots and produce a [`HealthReport`].
///
/// Groups snapshots by `index_id`, computes per-index metrics, and builds
/// a tablespace summary. The `total_pages` and `empty_pages` counts should
/// include all pages in the tablespace, not just INDEX pages.
pub fn analyze_health(
    snapshots: Vec<IndexPageSnapshot>,
    page_size: u32,
    total_pages: u64,
    empty_pages: u64,
    file: &str,
) -> HealthReport {
    // Group by index_id
    let mut groups: BTreeMap<u64, Vec<IndexPageSnapshot>> = BTreeMap::new();
    for snap in snapshots {
        groups.entry(snap.index_id).or_default().push(snap);
    }

    let index_page_count: u64 = groups.values().map(|v| v.len() as u64).sum();
    let non_index_pages = total_pages.saturating_sub(index_page_count + empty_pages);

    let mut indexes = Vec::with_capacity(groups.len());
    let mut all_fill_factors = Vec::new();
    let mut all_garbage_ratios = Vec::new();

    for (index_id, pages) in &groups {
        let mut tree_depth: u16 = 0;
        let mut leaf_pages: u64 = 0;
        let mut non_leaf_pages: u64 = 0;
        let mut total_records: u64 = 0;
        let mut total_garbage_bytes: u64 = 0;
        let mut empty_leaf_pages: u64 = 0;
        let mut fill_factors = Vec::with_capacity(pages.len());
        let mut garbage_ratios = Vec::with_capacity(pages.len());
        let mut leaf_page_numbers = Vec::new();

        for snap in pages {
            let ff = compute_fill_factor(snap.heap_top, snap.garbage, page_size);
            let gr = compute_garbage_ratio(snap.garbage, page_size);
            fill_factors.push(ff);
            garbage_ratios.push(gr);
            all_fill_factors.push(ff);
            all_garbage_ratios.push(gr);

            if snap.level > tree_depth {
                tree_depth = snap.level;
            }
            if snap.level == 0 {
                leaf_pages += 1;
                leaf_page_numbers.push(snap.page_number);
                if snap.n_recs == 0 {
                    empty_leaf_pages += 1;
                }
            } else {
                non_leaf_pages += 1;
            }
            total_records += snap.n_recs as u64;
            total_garbage_bytes += snap.garbage as u64;
        }

        let avg_fill = if fill_factors.is_empty() {
            0.0
        } else {
            fill_factors.iter().sum::<f64>() / fill_factors.len() as f64
        };
        let min_fill = fill_factors.iter().cloned().fold(f64::INFINITY, f64::min);
        let max_fill = fill_factors
            .iter()
            .cloned()
            .fold(f64::NEG_INFINITY, f64::max);
        let avg_garbage = if garbage_ratios.is_empty() {
            0.0
        } else {
            garbage_ratios.iter().sum::<f64>() / garbage_ratios.len() as f64
        };

        let fragmentation = compute_fragmentation(&mut leaf_page_numbers);

        // tree_depth reported as max_level + 1 (number of levels including leaf)
        indexes.push(IndexHealth {
            index_id: *index_id,
            index_name: None,
            tree_depth: tree_depth + 1,
            total_pages: pages.len() as u64,
            leaf_pages,
            non_leaf_pages,
            total_records,
            avg_fill_factor: round2(avg_fill),
            min_fill_factor: round2(if min_fill.is_infinite() {
                0.0
            } else {
                min_fill
            }),
            max_fill_factor: round2(if max_fill.is_infinite() {
                0.0
            } else {
                max_fill
            }),
            avg_garbage_ratio: round2(avg_garbage),
            total_garbage_bytes,
            fragmentation: round2(fragmentation),
            empty_leaf_pages,
        });
    }

    let avg_fill = if all_fill_factors.is_empty() {
        0.0
    } else {
        round2(all_fill_factors.iter().sum::<f64>() / all_fill_factors.len() as f64)
    };
    let avg_garbage = if all_garbage_ratios.is_empty() {
        0.0
    } else {
        round2(all_garbage_ratios.iter().sum::<f64>() / all_garbage_ratios.len() as f64)
    };
    let avg_frag = if indexes.is_empty() {
        0.0
    } else {
        round2(indexes.iter().map(|i| i.fragmentation).sum::<f64>() / indexes.len() as f64)
    };

    HealthReport {
        file: file.to_string(),
        summary: TablespaceHealth {
            total_pages,
            index_pages: index_page_count,
            non_index_pages,
            empty_pages,
            page_size,
            avg_fill_factor: avg_fill,
            avg_garbage_ratio: avg_garbage,
            avg_fragmentation: avg_frag,
            index_count: indexes.len() as u64,
        },
        indexes,
    }
}

/// Round to 2 decimal places.
fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fill_factor_full_page() {
        // heap_top at end of usable area, no garbage
        let usable_end = (16384 - SIZE_FIL_TRAILER) as u16;
        let ff = compute_fill_factor(usable_end, 0, 16384);
        assert!((ff - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_fill_factor_half_page() {
        let usable = 16384.0 - PAGE_DATA_OFFSET as f64 - SIZE_FIL_TRAILER as f64;
        let half_heap_top = PAGE_DATA_OFFSET as u16 + (usable / 2.0) as u16;
        let ff = compute_fill_factor(half_heap_top, 0, 16384);
        assert!((ff - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_fill_factor_empty_page() {
        let ff = compute_fill_factor(PAGE_DATA_OFFSET as u16, 0, 16384);
        assert!(ff.abs() < 0.001);
    }

    #[test]
    fn test_fill_factor_with_garbage() {
        // heap_top at 3/4, garbage = 1/4 of usable => fill = 0.5
        let usable = 16384.0 - PAGE_DATA_OFFSET as f64 - SIZE_FIL_TRAILER as f64;
        let heap_top = PAGE_DATA_OFFSET as u16 + (usable * 0.75) as u16;
        let garbage = (usable * 0.25) as u16;
        let ff = compute_fill_factor(heap_top, garbage, 16384);
        assert!((ff - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_garbage_ratio() {
        let usable = 16384.0 - PAGE_DATA_OFFSET as f64 - SIZE_FIL_TRAILER as f64;
        let garbage = (usable * 0.25) as u16;
        let gr = compute_garbage_ratio(garbage, 16384);
        assert!((gr - 0.25).abs() < 0.01);
    }

    #[test]
    fn test_fragmentation_sequential() {
        let mut pages = vec![1, 2, 3, 4, 5];
        assert!(compute_fragmentation(&mut pages).abs() < 0.001);
    }

    #[test]
    fn test_fragmentation_scattered() {
        let mut pages = vec![1, 10, 20, 30, 40];
        let frag = compute_fragmentation(&mut pages);
        assert!((frag - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_fragmentation_single_page() {
        let mut pages = vec![5];
        assert!(compute_fragmentation(&mut pages).abs() < 0.001);
    }

    #[test]
    fn test_fragmentation_empty() {
        let mut pages: Vec<u64> = vec![];
        assert!(compute_fragmentation(&mut pages).abs() < 0.001);
    }

    #[test]
    fn test_fragmentation_partial() {
        // 1->2 sequential, 2->5 non-sequential, 5->6 sequential = 1/3
        let mut pages = vec![1, 2, 5, 6];
        let frag = compute_fragmentation(&mut pages);
        assert!((frag - 1.0 / 3.0).abs() < 0.01);
    }

    #[test]
    fn test_analyze_health_groups_by_index() {
        let snapshots = vec![
            IndexPageSnapshot {
                page_number: 3,
                index_id: 100,
                level: 0,
                heap_top: 8000,
                garbage: 0,
                n_recs: 50,
                prev: FIL_NULL,
                next: 4,
            },
            IndexPageSnapshot {
                page_number: 4,
                index_id: 100,
                level: 0,
                heap_top: 8000,
                garbage: 0,
                n_recs: 50,
                prev: 3,
                next: FIL_NULL,
            },
            IndexPageSnapshot {
                page_number: 5,
                index_id: 200,
                level: 0,
                heap_top: 4000,
                garbage: 100,
                n_recs: 20,
                prev: FIL_NULL,
                next: FIL_NULL,
            },
        ];

        let report = analyze_health(snapshots, 16384, 10, 2, "test.ibd");
        assert_eq!(report.indexes.len(), 2);
        assert_eq!(report.summary.index_count, 2);
        assert_eq!(report.summary.index_pages, 3);
        assert_eq!(report.summary.empty_pages, 2);
        assert_eq!(report.summary.total_pages, 10);

        let idx100 = report.indexes.iter().find(|i| i.index_id == 100).unwrap();
        assert_eq!(idx100.total_pages, 2);
        assert_eq!(idx100.leaf_pages, 2);
        assert_eq!(idx100.total_records, 100);
        assert_eq!(idx100.tree_depth, 1);
        // Sequential pages 3->4, fragmentation should be 0
        assert!(idx100.fragmentation.abs() < 0.001);

        let idx200 = report.indexes.iter().find(|i| i.index_id == 200).unwrap();
        assert_eq!(idx200.total_pages, 1);
        assert_eq!(idx200.total_records, 20);
        assert!(idx200.total_garbage_bytes > 0);
    }

    #[test]
    fn test_analyze_health_empty() {
        let report = analyze_health(vec![], 16384, 5, 5, "empty.ibd");
        assert!(report.indexes.is_empty());
        assert_eq!(report.summary.index_count, 0);
        assert_eq!(report.summary.total_pages, 5);
        assert_eq!(report.summary.empty_pages, 5);
    }

    #[test]
    fn test_analyze_health_multi_level() {
        let snapshots = vec![
            IndexPageSnapshot {
                page_number: 3,
                index_id: 100,
                level: 2, // root
                heap_top: 300,
                garbage: 0,
                n_recs: 2,
                prev: FIL_NULL,
                next: FIL_NULL,
            },
            IndexPageSnapshot {
                page_number: 4,
                index_id: 100,
                level: 1,
                heap_top: 500,
                garbage: 0,
                n_recs: 5,
                prev: FIL_NULL,
                next: FIL_NULL,
            },
            IndexPageSnapshot {
                page_number: 5,
                index_id: 100,
                level: 0,
                heap_top: 8000,
                garbage: 0,
                n_recs: 50,
                prev: FIL_NULL,
                next: 6,
            },
            IndexPageSnapshot {
                page_number: 6,
                index_id: 100,
                level: 0,
                heap_top: 8000,
                garbage: 0,
                n_recs: 50,
                prev: 5,
                next: FIL_NULL,
            },
        ];

        let report = analyze_health(snapshots, 16384, 10, 0, "multi.ibd");
        let idx = &report.indexes[0];
        assert_eq!(idx.tree_depth, 3); // levels 0, 1, 2 => depth = 3
        assert_eq!(idx.leaf_pages, 2);
        assert_eq!(idx.non_leaf_pages, 2);
        assert_eq!(idx.total_records, 107);
    }
}
