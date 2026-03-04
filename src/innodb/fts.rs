//! Full-text search (FTS) auxiliary table detection and metadata extraction.
//!
//! InnoDB FTS indexes create auxiliary tables with filenames following the pattern:
//! `FTS_<table_id_hex>_<type>.ibd` where type is one of:
//! - `CONFIG` — FTS configuration
//! - `<index_id_hex>_INDEX_<N>` — inverted index shards (N = 0-5)
//! - `BEING_DELETED` / `BEING_DELETED_CACHE` — deletion bookkeeping
//! - `DELETED` / `DELETED_CACHE` — deleted document IDs

use serde::Serialize;

/// Type of FTS auxiliary file.
///
/// # Examples
///
/// ```
/// use idb::innodb::fts::FtsFileType;
///
/// let ft = FtsFileType::Config;
/// assert_eq!(format!("{ft}"), "CONFIG");
///
/// let ft = FtsFileType::Index(3);
/// assert_eq!(format!("{ft}"), "INDEX_3");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum FtsFileType {
    /// FTS configuration table.
    Config,
    /// Inverted index shard (0-5).
    Index(u8),
    /// Deleted document IDs.
    Delete,
    /// Deleted document IDs (cache).
    DeleteCache,
    /// Documents being deleted.
    BeingDeleted,
    /// Documents being deleted (cache).
    BeingDeletedCache,
}

impl std::fmt::Display for FtsFileType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FtsFileType::Config => write!(f, "CONFIG"),
            FtsFileType::Index(n) => write!(f, "INDEX_{n}"),
            FtsFileType::Delete => write!(f, "DELETED"),
            FtsFileType::DeleteCache => write!(f, "DELETED_CACHE"),
            FtsFileType::BeingDeleted => write!(f, "BEING_DELETED"),
            FtsFileType::BeingDeletedCache => write!(f, "BEING_DELETED_CACHE"),
        }
    }
}

/// Parsed metadata from an FTS auxiliary filename.
///
/// # Examples
///
/// ```
/// use idb::innodb::fts::{parse_fts_filename, FtsFileType};
///
/// let info = parse_fts_filename("FTS_0000000000000437_CONFIG.ibd").unwrap();
/// assert_eq!(info.table_id_hex, "0000000000000437");
/// assert_eq!(info.file_type, FtsFileType::Config);
///
/// let info = parse_fts_filename("FTS_0000000000000437_00000000000004a2_INDEX_1.ibd").unwrap();
/// assert_eq!(info.table_id_hex, "0000000000000437");
/// assert_eq!(info.index_id_hex, Some("00000000000004a2".to_string()));
/// assert_eq!(info.file_type, FtsFileType::Index(1));
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct FtsFileInfo {
    /// Hex-encoded table ID from the filename.
    pub table_id_hex: String,
    /// Hex-encoded index ID (for INDEX files only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_id_hex: Option<String>,
    /// Type of FTS auxiliary file.
    pub file_type: FtsFileType,
}

/// Check if a filename matches the FTS auxiliary table naming pattern.
///
/// # Examples
///
/// ```
/// use idb::innodb::fts::is_fts_auxiliary;
///
/// assert!(is_fts_auxiliary("FTS_0000000000000437_00000000000004a2_INDEX_1.ibd"));
/// assert!(is_fts_auxiliary("FTS_0000000000000437_CONFIG.ibd"));
/// assert!(is_fts_auxiliary("FTS_0000000000000100_DELETED.ibd"));
/// assert!(!is_fts_auxiliary("users.ibd"));
/// assert!(!is_fts_auxiliary("FTS_bad.ibd"));
/// ```
pub fn is_fts_auxiliary(filename: &str) -> bool {
    parse_fts_filename(filename).is_some()
}

/// Parse an FTS auxiliary filename into structured metadata.
///
/// Returns `None` if the filename doesn't match the FTS naming pattern.
pub fn parse_fts_filename(filename: &str) -> Option<FtsFileInfo> {
    // Strip path prefix — only look at the filename itself
    let name = filename
        .rsplit('/')
        .next()
        .unwrap_or(filename)
        .rsplit('\\')
        .next()
        .unwrap_or(filename);

    // Must start with "FTS_" and end with ".ibd"
    let stripped = name.strip_prefix("FTS_")?.strip_suffix(".ibd")?;

    // Table ID is always the first 16 hex chars
    if stripped.len() < 16 {
        return None;
    }

    let table_id_hex = &stripped[..16];
    if !table_id_hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }

    let rest = &stripped[16..];
    if rest.is_empty() {
        return None;
    }

    // Rest starts with underscore separator
    let rest = rest.strip_prefix('_')?;

    // Try to match known suffixes
    if rest == "CONFIG" {
        return Some(FtsFileInfo {
            table_id_hex: table_id_hex.to_string(),
            index_id_hex: None,
            file_type: FtsFileType::Config,
        });
    }

    if rest == "DELETED" {
        return Some(FtsFileInfo {
            table_id_hex: table_id_hex.to_string(),
            index_id_hex: None,
            file_type: FtsFileType::Delete,
        });
    }

    if rest == "DELETED_CACHE" {
        return Some(FtsFileInfo {
            table_id_hex: table_id_hex.to_string(),
            index_id_hex: None,
            file_type: FtsFileType::DeleteCache,
        });
    }

    if rest == "BEING_DELETED" {
        return Some(FtsFileInfo {
            table_id_hex: table_id_hex.to_string(),
            index_id_hex: None,
            file_type: FtsFileType::BeingDeleted,
        });
    }

    if rest == "BEING_DELETED_CACHE" {
        return Some(FtsFileInfo {
            table_id_hex: table_id_hex.to_string(),
            index_id_hex: None,
            file_type: FtsFileType::BeingDeletedCache,
        });
    }

    // Try INDEX pattern: <16-char index_id>_INDEX_<N>
    if rest.len() >= 24 {
        let index_id_hex = &rest[..16];
        if index_id_hex.chars().all(|c| c.is_ascii_hexdigit()) {
            let suffix = &rest[16..];
            if let Some(n_str) = suffix.strip_prefix("_INDEX_") {
                if let Ok(n) = n_str.parse::<u8>() {
                    return Some(FtsFileInfo {
                        table_id_hex: table_id_hex.to_string(),
                        index_id_hex: Some(index_id_hex.to_string()),
                        file_type: FtsFileType::Index(n),
                    });
                }
            }
        }
    }

    None
}

/// Summary of FTS auxiliary files for a single table.
#[derive(Debug, Clone, Serialize)]
pub struct FtsTableSummary {
    /// Hex-encoded table ID.
    pub table_id: String,
    /// Number of index shard files found.
    pub index_count: usize,
    /// Whether a CONFIG file was found.
    pub has_config: bool,
    /// Whether DELETED/BEING_DELETED files were found.
    pub has_delete: bool,
}

/// Group a list of FTS file infos by table ID into summaries.
///
/// # Examples
///
/// ```
/// use idb::innodb::fts::{parse_fts_filename, summarize_fts_files};
///
/// let files = vec![
///     "FTS_0000000000000437_CONFIG.ibd",
///     "FTS_0000000000000437_00000000000004a2_INDEX_0.ibd",
///     "FTS_0000000000000437_00000000000004a2_INDEX_1.ibd",
///     "FTS_0000000000000437_DELETED.ibd",
/// ];
///
/// let infos: Vec<_> = files.iter().filter_map(|f| parse_fts_filename(f)).collect();
/// let summaries = summarize_fts_files(&infos);
/// assert_eq!(summaries.len(), 1);
/// assert_eq!(summaries[0].index_count, 2);
/// assert!(summaries[0].has_config);
/// assert!(summaries[0].has_delete);
/// ```
pub fn summarize_fts_files(infos: &[FtsFileInfo]) -> Vec<FtsTableSummary> {
    use std::collections::HashMap;

    let mut tables: HashMap<&str, (usize, bool, bool)> = HashMap::new();

    for info in infos {
        let entry = tables
            .entry(&info.table_id_hex)
            .or_insert((0, false, false));

        match &info.file_type {
            FtsFileType::Index(_) => entry.0 += 1,
            FtsFileType::Config => entry.1 = true,
            FtsFileType::Delete
            | FtsFileType::DeleteCache
            | FtsFileType::BeingDeleted
            | FtsFileType::BeingDeletedCache => entry.2 = true,
        }
    }

    let mut summaries: Vec<FtsTableSummary> = tables
        .into_iter()
        .map(|(tid, (idx_count, has_config, has_delete))| FtsTableSummary {
            table_id: tid.to_string(),
            index_count: idx_count,
            has_config,
            has_delete,
        })
        .collect();

    summaries.sort_by(|a, b| a.table_id.cmp(&b.table_id));
    summaries
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_fts_auxiliary() {
        assert!(is_fts_auxiliary(
            "FTS_0000000000000437_00000000000004a2_INDEX_1.ibd"
        ));
        assert!(is_fts_auxiliary("FTS_0000000000000437_CONFIG.ibd"));
        assert!(is_fts_auxiliary("FTS_0000000000000100_DELETED.ibd"));
        assert!(is_fts_auxiliary(
            "FTS_0000000000000100_BEING_DELETED.ibd"
        ));
        assert!(!is_fts_auxiliary("users.ibd"));
        assert!(!is_fts_auxiliary("FTS_bad.ibd"));
        assert!(!is_fts_auxiliary("FTS_.ibd"));
    }

    #[test]
    fn test_parse_fts_config() {
        let info = parse_fts_filename("FTS_0000000000000437_CONFIG.ibd").unwrap();
        assert_eq!(info.table_id_hex, "0000000000000437");
        assert_eq!(info.file_type, FtsFileType::Config);
        assert!(info.index_id_hex.is_none());
    }

    #[test]
    fn test_parse_fts_index() {
        let info = parse_fts_filename(
            "FTS_0000000000000437_00000000000004a2_INDEX_3.ibd",
        )
        .unwrap();
        assert_eq!(info.table_id_hex, "0000000000000437");
        assert_eq!(
            info.index_id_hex,
            Some("00000000000004a2".to_string())
        );
        assert_eq!(info.file_type, FtsFileType::Index(3));
    }

    #[test]
    fn test_parse_fts_deleted() {
        let info = parse_fts_filename("FTS_0000000000000100_DELETED.ibd").unwrap();
        assert_eq!(info.file_type, FtsFileType::Delete);

        let info =
            parse_fts_filename("FTS_0000000000000100_DELETED_CACHE.ibd").unwrap();
        assert_eq!(info.file_type, FtsFileType::DeleteCache);
    }

    #[test]
    fn test_parse_fts_being_deleted() {
        let info =
            parse_fts_filename("FTS_0000000000000100_BEING_DELETED.ibd").unwrap();
        assert_eq!(info.file_type, FtsFileType::BeingDeleted);

        let info = parse_fts_filename(
            "FTS_0000000000000100_BEING_DELETED_CACHE.ibd",
        )
        .unwrap();
        assert_eq!(info.file_type, FtsFileType::BeingDeletedCache);
    }

    #[test]
    fn test_parse_fts_invalid() {
        assert!(parse_fts_filename("users.ibd").is_none());
        assert!(parse_fts_filename("FTS_.ibd").is_none());
        assert!(parse_fts_filename("FTS_GGGG000000000437_CONFIG.ibd").is_none());
        assert!(parse_fts_filename("FTS_0000000000000437_UNKNOWN.ibd").is_none());
    }

    #[test]
    fn test_parse_fts_with_path() {
        let info = parse_fts_filename(
            "/var/lib/mysql/test/FTS_0000000000000437_CONFIG.ibd",
        )
        .unwrap();
        assert_eq!(info.table_id_hex, "0000000000000437");
        assert_eq!(info.file_type, FtsFileType::Config);
    }

    #[test]
    fn test_summarize_fts_files() {
        let files = vec![
            "FTS_0000000000000437_CONFIG.ibd",
            "FTS_0000000000000437_00000000000004a2_INDEX_0.ibd",
            "FTS_0000000000000437_00000000000004a2_INDEX_1.ibd",
            "FTS_0000000000000437_DELETED.ibd",
            "FTS_0000000000000100_CONFIG.ibd",
        ];

        let infos: Vec<_> = files
            .iter()
            .filter_map(|f| parse_fts_filename(f))
            .collect();
        let summaries = summarize_fts_files(&infos);
        assert_eq!(summaries.len(), 2);

        let s437 = summaries
            .iter()
            .find(|s| s.table_id == "0000000000000437")
            .unwrap();
        assert_eq!(s437.index_count, 2);
        assert!(s437.has_config);
        assert!(s437.has_delete);

        let s100 = summaries
            .iter()
            .find(|s| s.table_id == "0000000000000100")
            .unwrap();
        assert_eq!(s100.index_count, 0);
        assert!(s100.has_config);
        assert!(!s100.has_delete);
    }

    #[test]
    fn test_fts_file_type_display() {
        assert_eq!(format!("{}", FtsFileType::Config), "CONFIG");
        assert_eq!(format!("{}", FtsFileType::Index(3)), "INDEX_3");
        assert_eq!(format!("{}", FtsFileType::Delete), "DELETED");
        assert_eq!(
            format!("{}", FtsFileType::BeingDeletedCache),
            "BEING_DELETED_CACHE"
        );
    }
}
