//! Live MySQL cross-validation for InnoDB tablespace files.
//!
//! Compares on-disk tablespace metadata (space IDs, page counts) against
//! live MySQL `INFORMATION_SCHEMA.INNODB_TABLESPACES` data to detect
//! orphan files, missing tablespaces, and space ID mismatches.
//!
//! The library module uses a [`MysqlSource`] trait for testability —
//! no MySQL dependency at this level.

use serde::Serialize;
use std::path::Path;
use std::path::PathBuf;

use crate::innodb::constants::*;
use crate::innodb::page::FilHeader;
use crate::innodb::page_types::PageType;

/// Tablespace mapping from MySQL metadata.
#[derive(Debug, Clone, Serialize)]
pub struct TablespaceMapping {
    /// Tablespace name from MySQL (e.g., "mydb/mytable").
    pub name: String,
    /// Space ID from MySQL.
    pub space_id: u32,
    /// Row format (e.g., "Dynamic", "Compact").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_format: Option<String>,
}

/// Index info from MySQL metadata.
#[derive(Debug, Clone, Serialize)]
pub struct MysqlIndexInfo {
    /// Index name.
    pub name: String,
    /// TABLE_ID from MySQL.
    pub table_id: u64,
    /// SPACE from MySQL.
    pub space_id: u32,
    /// Root page number.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_no: Option<u64>,
}

/// Table stats from MySQL.
#[derive(Debug, Clone, Serialize)]
pub struct MysqlTableStats {
    /// Number of rows from MySQL.
    pub num_rows: u64,
    /// Auto-increment value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_increment: Option<u64>,
}

/// Trait for fetching MySQL metadata. Allows mock implementations for testing.
pub trait MysqlSource {
    /// Get all tablespace mappings.
    fn tablespace_mappings(&self) -> Result<Vec<TablespaceMapping>, crate::IdbError>;
}

/// A file found on disk but not in MySQL's tablespace registry.
#[derive(Debug, Clone, Serialize)]
pub struct OrphanFile {
    /// Path to the orphan file.
    pub path: String,
    /// Space ID from the file's page 0.
    pub space_id: u32,
}

/// A tablespace registered in MySQL but whose file is missing from disk.
#[derive(Debug, Clone, Serialize)]
pub struct MissingFile {
    /// Tablespace name from MySQL.
    pub name: String,
    /// Space ID from MySQL.
    pub space_id: u32,
}

/// A space ID mismatch between disk and MySQL.
#[derive(Debug, Clone, Serialize)]
pub struct SpaceIdMismatch {
    /// Path to the file.
    pub path: String,
    /// Space ID from the file's page 0.
    pub disk_space_id: u32,
    /// Space ID from MySQL.
    pub mysql_space_id: u32,
    /// Tablespace name from MySQL.
    pub mysql_name: String,
}

/// Full validation report.
#[derive(Debug, Clone, Serialize)]
pub struct ValidationReport {
    /// Total disk files scanned.
    pub disk_files: usize,
    /// Total MySQL tablespaces queried.
    pub mysql_tablespaces: usize,
    /// Files on disk not in MySQL.
    pub orphans: Vec<OrphanFile>,
    /// MySQL tablespaces with no disk file.
    pub missing: Vec<MissingFile>,
    /// Space ID mismatches.
    pub mismatches: Vec<SpaceIdMismatch>,
    /// Whether all checks passed.
    pub passed: bool,
}

/// Cross-validate disk files against MySQL tablespace mappings.
///
/// `disk` is a list of (file_path, space_id) pairs read from page 0 of each .ibd file.
/// `mysql` is a list of tablespace mappings from MySQL.
pub fn cross_validate(disk: &[(PathBuf, u32)], mysql: &[TablespaceMapping]) -> ValidationReport {
    use std::collections::HashMap;

    // Build maps for quick lookup
    let mysql_by_space: HashMap<u32, &TablespaceMapping> =
        mysql.iter().map(|m| (m.space_id, m)).collect();

    let mut orphans = Vec::new();
    let mut mismatches = Vec::new();
    let mut matched_space_ids: std::collections::HashSet<u32> = std::collections::HashSet::new();

    for (path, disk_space_id) in disk {
        let path_str = path.to_string_lossy().to_string();

        // Try to match by space_id
        if mysql_by_space.contains_key(disk_space_id) {
            matched_space_ids.insert(*disk_space_id);
        } else {
            // Check if the file matches by name pattern
            let mut found_by_name = false;
            for m in mysql {
                // Check if the file path ends with the tablespace name pattern
                let expected_suffix =
                    format!("{}.ibd", m.name.replace('/', std::path::MAIN_SEPARATOR_STR));
                if path_str.ends_with(&expected_suffix) {
                    // Name matches but space_id differs
                    mismatches.push(SpaceIdMismatch {
                        path: path_str.clone(),
                        disk_space_id: *disk_space_id,
                        mysql_space_id: m.space_id,
                        mysql_name: m.name.clone(),
                    });
                    matched_space_ids.insert(m.space_id);
                    found_by_name = true;
                    break;
                }
            }
            if !found_by_name {
                orphans.push(OrphanFile {
                    path: path_str,
                    space_id: *disk_space_id,
                });
            }
        }
    }

    // Find missing: MySQL entries with no matching disk file
    let missing: Vec<MissingFile> = mysql
        .iter()
        .filter(|m| !matched_space_ids.contains(&m.space_id))
        .map(|m| MissingFile {
            name: m.name.clone(),
            space_id: m.space_id,
        })
        .collect();

    let passed = orphans.is_empty() && missing.is_empty() && mismatches.is_empty();

    ValidationReport {
        disk_files: disk.len(),
        mysql_tablespaces: mysql.len(),
        orphans,
        missing,
        mismatches,
        passed,
    }
}

/// Detect orphan files and missing tablespaces (convenience wrapper).
pub fn detect_orphans(
    disk: &[(PathBuf, u32)],
    mysql: &[TablespaceMapping],
) -> (Vec<OrphanFile>, Vec<MissingFile>) {
    let report = cross_validate(disk, mysql);
    (report.orphans, report.missing)
}

/// Deep table validation report.
#[derive(Debug, Clone, Serialize)]
pub struct TableValidationReport {
    /// Table name (db/table).
    pub table_name: String,
    /// Space ID from MySQL.
    pub mysql_space_id: u32,
    /// Space ID from disk (None if file not found).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_space_id: Option<u32>,
    /// Path to the .ibd file (None if not found).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_path: Option<String>,
    /// Whether the space IDs match.
    pub space_id_match: bool,
    /// Row format from MySQL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mysql_row_format: Option<String>,
    /// Number of index root pages verified.
    pub indexes_verified: usize,
    /// Details for each index.
    pub indexes: Vec<IndexValidation>,
    /// Overall pass/fail.
    pub passed: bool,
}

/// Per-index validation result.
#[derive(Debug, Clone, Serialize)]
pub struct IndexValidation {
    /// Index name from MySQL.
    pub name: String,
    /// Root page number from MySQL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_page: Option<u64>,
    /// Whether the root page exists and is an INDEX page.
    pub root_page_valid: bool,
    /// Message about any issues found.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Perform deep validation of a single table's .ibd file against MySQL metadata.
///
/// Locates the .ibd file at `{datadir}/{table_name}.ibd` (where `table_name` is
/// in `db/table` format), reads the space ID from the FSP header on page 0,
/// compares it against MySQL's expected space ID, then verifies each index's
/// root page exists and is a valid INDEX page.
pub fn deep_validate_table(
    datadir: &Path,
    table_name: &str,
    mapping: &TablespaceMapping,
    indexes: &[MysqlIndexInfo],
    page_size: Option<u32>,
    _use_mmap: bool,
) -> TableValidationReport {
    let ps = page_size.unwrap_or(SIZE_PAGE_DEFAULT) as usize;
    let ibd_path = datadir.join(format!("{}.ibd", table_name));

    if !ibd_path.exists() {
        return TableValidationReport {
            table_name: table_name.to_string(),
            mysql_space_id: mapping.space_id,
            disk_space_id: None,
            file_path: None,
            space_id_match: false,
            mysql_row_format: mapping.row_format.clone(),
            indexes_verified: 0,
            indexes: indexes
                .iter()
                .map(|idx| IndexValidation {
                    name: idx.name.clone(),
                    root_page: idx.page_no,
                    root_page_valid: false,
                    message: Some("File not found".to_string()),
                })
                .collect(),
            passed: false,
        };
    }

    let file_path_str = ibd_path.display().to_string();

    let file_data = match std::fs::read(&ibd_path) {
        Ok(data) => data,
        Err(e) => {
            return TableValidationReport {
                table_name: table_name.to_string(),
                mysql_space_id: mapping.space_id,
                disk_space_id: None,
                file_path: Some(file_path_str),
                space_id_match: false,
                mysql_row_format: mapping.row_format.clone(),
                indexes_verified: 0,
                indexes: indexes
                    .iter()
                    .map(|idx| IndexValidation {
                        name: idx.name.clone(),
                        root_page: idx.page_no,
                        root_page_valid: false,
                        message: Some(format!("Cannot read file: {}", e)),
                    })
                    .collect(),
                passed: false,
            };
        }
    };

    if file_data.len() < ps {
        return TableValidationReport {
            table_name: table_name.to_string(),
            mysql_space_id: mapping.space_id,
            disk_space_id: None,
            file_path: Some(file_path_str),
            space_id_match: false,
            mysql_row_format: mapping.row_format.clone(),
            indexes_verified: 0,
            indexes: indexes
                .iter()
                .map(|idx| IndexValidation {
                    name: idx.name.clone(),
                    root_page: idx.page_no,
                    root_page_valid: false,
                    message: Some("File too small to contain page 0".to_string()),
                })
                .collect(),
            passed: false,
        };
    }

    // Parse space ID from page 0's FIL header
    let page0 = &file_data[..ps];
    let disk_space_id = FilHeader::parse(page0).map(|h| h.space_id);
    let space_id_match = disk_space_id == Some(mapping.space_id);

    // Validate each index's root page
    let total_pages = file_data.len() / ps;
    let mut index_validations = Vec::with_capacity(indexes.len());
    let mut all_indexes_valid = true;

    for idx in indexes {
        let page_no = match idx.page_no {
            Some(pn) => pn,
            None => {
                index_validations.push(IndexValidation {
                    name: idx.name.clone(),
                    root_page: None,
                    root_page_valid: false,
                    message: Some("No root page number from MySQL".to_string()),
                });
                all_indexes_valid = false;
                continue;
            }
        };

        if page_no as usize >= total_pages {
            index_validations.push(IndexValidation {
                name: idx.name.clone(),
                root_page: Some(page_no),
                root_page_valid: false,
                message: Some(format!(
                    "Root page {} beyond file extent ({} pages)",
                    page_no, total_pages
                )),
            });
            all_indexes_valid = false;
            continue;
        }

        let page_offset = page_no as usize * ps;
        let page_data = &file_data[page_offset..page_offset + ps];

        match FilHeader::parse(page_data) {
            Some(hdr) => {
                let is_index = hdr.page_type == PageType::Index;
                if is_index {
                    index_validations.push(IndexValidation {
                        name: idx.name.clone(),
                        root_page: Some(page_no),
                        root_page_valid: true,
                        message: None,
                    });
                } else {
                    index_validations.push(IndexValidation {
                        name: idx.name.clone(),
                        root_page: Some(page_no),
                        root_page_valid: false,
                        message: Some(format!(
                            "Root page {} has type {} (expected INDEX)",
                            page_no,
                            hdr.page_type.name()
                        )),
                    });
                    all_indexes_valid = false;
                }
            }
            None => {
                index_validations.push(IndexValidation {
                    name: idx.name.clone(),
                    root_page: Some(page_no),
                    root_page_valid: false,
                    message: Some(format!("Cannot parse FIL header on page {}", page_no)),
                });
                all_indexes_valid = false;
            }
        }
    }

    let indexes_verified = index_validations
        .iter()
        .filter(|v| v.root_page_valid)
        .count();
    let passed = space_id_match && all_indexes_valid;

    TableValidationReport {
        table_name: table_name.to_string(),
        mysql_space_id: mapping.space_id,
        disk_space_id,
        file_path: Some(file_path_str),
        space_id_match,
        mysql_row_format: mapping.row_format.clone(),
        indexes_verified,
        indexes: index_validations,
        passed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cross_validate_all_match() {
        let disk = vec![
            (PathBuf::from("/data/mydb/t1.ibd"), 10),
            (PathBuf::from("/data/mydb/t2.ibd"), 20),
        ];
        let mysql = vec![
            TablespaceMapping {
                name: "mydb/t1".into(),
                space_id: 10,
                row_format: None,
            },
            TablespaceMapping {
                name: "mydb/t2".into(),
                space_id: 20,
                row_format: None,
            },
        ];

        let report = cross_validate(&disk, &mysql);
        assert!(report.passed);
        assert!(report.orphans.is_empty());
        assert!(report.missing.is_empty());
        assert!(report.mismatches.is_empty());
    }

    #[test]
    fn test_cross_validate_orphan_detected() {
        let disk = vec![
            (PathBuf::from("/data/mydb/t1.ibd"), 10),
            (PathBuf::from("/data/mydb/old.ibd"), 99),
        ];
        let mysql = vec![TablespaceMapping {
            name: "mydb/t1".into(),
            space_id: 10,
            row_format: None,
        }];

        let report = cross_validate(&disk, &mysql);
        assert!(!report.passed);
        assert_eq!(report.orphans.len(), 1);
        assert_eq!(report.orphans[0].space_id, 99);
    }

    #[test]
    fn test_cross_validate_missing_detected() {
        let disk = vec![(PathBuf::from("/data/mydb/t1.ibd"), 10)];
        let mysql = vec![
            TablespaceMapping {
                name: "mydb/t1".into(),
                space_id: 10,
                row_format: None,
            },
            TablespaceMapping {
                name: "mydb/t2".into(),
                space_id: 20,
                row_format: None,
            },
        ];

        let report = cross_validate(&disk, &mysql);
        assert!(!report.passed);
        assert_eq!(report.missing.len(), 1);
        assert_eq!(report.missing[0].space_id, 20);
    }

    #[test]
    fn test_cross_validate_empty() {
        let report = cross_validate(&[], &[]);
        assert!(report.passed);
    }

    #[test]
    fn test_detect_orphans_convenience() {
        let disk = vec![
            (PathBuf::from("/data/mydb/t1.ibd"), 10),
            (PathBuf::from("/data/mydb/orphan.ibd"), 99),
        ];
        let mysql = vec![
            TablespaceMapping {
                name: "mydb/t1".into(),
                space_id: 10,
                row_format: None,
            },
            TablespaceMapping {
                name: "mydb/missing".into(),
                space_id: 50,
                row_format: None,
            },
        ];

        let (orphans, missing) = detect_orphans(&disk, &mysql);
        assert_eq!(orphans.len(), 1);
        assert_eq!(missing.len(), 1);
    }

    // ---- deep_validate_table tests ----

    use byteorder::{BigEndian, ByteOrder};
    use std::io::Write;
    use tempfile::TempDir;

    const PAGE_SIZE: u32 = 16384;
    const PS: usize = PAGE_SIZE as usize;

    fn write_crc32c_checksum(page: &mut [u8]) {
        let ps = page.len();
        let crc1 = crc32c::crc32c(&page[4..26]);
        let crc2 = crc32c::crc32c(&page[38..ps - 8]);
        let checksum = crc1 ^ crc2;
        BigEndian::write_u32(&mut page[0..4], checksum);
    }

    fn build_page0(space_id: u32, total_pages: u32) -> Vec<u8> {
        let mut page = vec![0u8; PS];
        BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], 0);
        BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
        BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
        BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], 1000);
        BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 8); // FSP_HDR
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);
        BigEndian::write_u32(&mut page[FIL_PAGE_DATA + FSP_SPACE_ID..], space_id);
        BigEndian::write_u32(&mut page[FIL_PAGE_DATA + FSP_SIZE..], total_pages);
        let trailer = PS - SIZE_FIL_TRAILER;
        BigEndian::write_u32(&mut page[trailer + 4..], 1000u32);
        write_crc32c_checksum(&mut page);
        page
    }

    fn build_index_page(page_num: u32, space_id: u32) -> Vec<u8> {
        let mut page = vec![0u8; PS];
        BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], page_num);
        BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
        BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
        BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], 2000);
        BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855); // INDEX
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);
        let trailer = PS - SIZE_FIL_TRAILER;
        BigEndian::write_u32(&mut page[trailer + 4..], 2000u32);
        write_crc32c_checksum(&mut page);
        page
    }

    fn build_undo_page(page_num: u32, space_id: u32) -> Vec<u8> {
        let mut page = vec![0u8; PS];
        BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], page_num);
        BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
        BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
        BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], 3000);
        BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 2); // UNDO_LOG
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);
        let trailer = PS - SIZE_FIL_TRAILER;
        BigEndian::write_u32(&mut page[trailer + 4..], 3000u32);
        write_crc32c_checksum(&mut page);
        page
    }

    fn write_ibd_file(tmpdir: &TempDir, db: &str, table: &str, pages: &[Vec<u8>]) {
        let db_dir = tmpdir.path().join(db);
        std::fs::create_dir_all(&db_dir).unwrap();
        let ibd_path = db_dir.join(format!("{}.ibd", table));
        let mut f = std::fs::File::create(ibd_path).unwrap();
        for page in pages {
            f.write_all(page).unwrap();
        }
        f.flush().unwrap();
    }

    #[test]
    fn test_deep_validate_matching_space_id() {
        let tmpdir = TempDir::new().unwrap();
        let space_id = 42u32;
        let page0 = build_page0(space_id, 4);
        let page1 = build_index_page(1, space_id);
        let page2 = build_index_page(2, space_id);
        let page3 = build_index_page(3, space_id);
        write_ibd_file(&tmpdir, "testdb", "users", &[page0, page1, page2, page3]);

        let mapping = TablespaceMapping {
            name: "testdb/users".to_string(),
            space_id,
            row_format: Some("Dynamic".to_string()),
        };
        let indexes = vec![
            MysqlIndexInfo {
                name: "PRIMARY".into(),
                table_id: 100,
                space_id,
                page_no: Some(3),
            },
            MysqlIndexInfo {
                name: "idx_email".into(),
                table_id: 100,
                space_id,
                page_no: Some(2),
            },
        ];

        let report = deep_validate_table(
            tmpdir.path(),
            "testdb/users",
            &mapping,
            &indexes,
            Some(PAGE_SIZE),
            false,
        );
        assert!(report.passed);
        assert!(report.space_id_match);
        assert_eq!(report.disk_space_id, Some(space_id));
        assert_eq!(report.indexes_verified, 2);
    }

    #[test]
    fn test_deep_validate_mismatched_space_id() {
        let tmpdir = TempDir::new().unwrap();
        let disk_sid = 42u32;
        let mysql_sid = 99u32;
        let page0 = build_page0(disk_sid, 3);
        let page1 = build_index_page(1, disk_sid);
        let page2 = build_index_page(2, disk_sid);
        write_ibd_file(&tmpdir, "testdb", "orders", &[page0, page1, page2]);

        let mapping = TablespaceMapping {
            name: "testdb/orders".into(),
            space_id: mysql_sid,
            row_format: None,
        };
        let indexes = vec![MysqlIndexInfo {
            name: "PRIMARY".into(),
            table_id: 200,
            space_id: mysql_sid,
            page_no: Some(1),
        }];

        let report = deep_validate_table(
            tmpdir.path(),
            "testdb/orders",
            &mapping,
            &indexes,
            Some(PAGE_SIZE),
            false,
        );
        assert!(!report.passed);
        assert!(!report.space_id_match);
        assert_eq!(report.disk_space_id, Some(disk_sid));
        assert!(report.indexes[0].root_page_valid);
    }

    #[test]
    fn test_deep_validate_non_index_root_page() {
        let tmpdir = TempDir::new().unwrap();
        let space_id = 55u32;
        let page0 = build_page0(space_id, 3);
        let page1 = build_index_page(1, space_id);
        let page2 = build_undo_page(2, space_id);
        write_ibd_file(&tmpdir, "testdb", "items", &[page0, page1, page2]);

        let mapping = TablespaceMapping {
            name: "testdb/items".into(),
            space_id,
            row_format: None,
        };
        let indexes = vec![
            MysqlIndexInfo {
                name: "PRIMARY".into(),
                table_id: 300,
                space_id,
                page_no: Some(1),
            },
            MysqlIndexInfo {
                name: "idx_name".into(),
                table_id: 300,
                space_id,
                page_no: Some(2),
            },
        ];

        let report = deep_validate_table(
            tmpdir.path(),
            "testdb/items",
            &mapping,
            &indexes,
            Some(PAGE_SIZE),
            false,
        );
        assert!(!report.passed);
        assert!(report.space_id_match);
        assert_eq!(report.indexes_verified, 1);
        assert!(report.indexes[0].root_page_valid);
        assert!(!report.indexes[1].root_page_valid);
        assert!(report.indexes[1]
            .message
            .as_ref()
            .unwrap()
            .contains("UNDO_LOG"));
    }

    #[test]
    fn test_deep_validate_file_not_found() {
        let tmpdir = TempDir::new().unwrap();
        let mapping = TablespaceMapping {
            name: "testdb/missing".into(),
            space_id: 10,
            row_format: None,
        };
        let indexes = vec![MysqlIndexInfo {
            name: "PRIMARY".into(),
            table_id: 400,
            space_id: 10,
            page_no: Some(3),
        }];

        let report = deep_validate_table(
            tmpdir.path(),
            "testdb/missing",
            &mapping,
            &indexes,
            Some(PAGE_SIZE),
            false,
        );
        assert!(!report.passed);
        assert!(report.disk_space_id.is_none());
        assert!(report.file_path.is_none());
    }

    #[test]
    fn test_deep_validate_root_page_beyond_file() {
        let tmpdir = TempDir::new().unwrap();
        let space_id = 77u32;
        let page0 = build_page0(space_id, 2);
        let page1 = build_index_page(1, space_id);
        write_ibd_file(&tmpdir, "testdb", "small", &[page0, page1]);

        let mapping = TablespaceMapping {
            name: "testdb/small".into(),
            space_id,
            row_format: None,
        };
        let indexes = vec![MysqlIndexInfo {
            name: "PRIMARY".into(),
            table_id: 500,
            space_id,
            page_no: Some(10),
        }];

        let report = deep_validate_table(
            tmpdir.path(),
            "testdb/small",
            &mapping,
            &indexes,
            Some(PAGE_SIZE),
            false,
        );
        assert!(!report.passed);
        assert!(report.space_id_match);
        assert!(!report.indexes[0].root_page_valid);
        assert!(report.indexes[0]
            .message
            .as_ref()
            .unwrap()
            .contains("beyond file extent"));
    }
}
