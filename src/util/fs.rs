//! Filesystem helpers for tablespace file discovery.
//!
//! Provides [`find_tablespace_files`] to recursively search a MySQL data
//! directory for `.ibd` and `.ibu` files. Used by the `find` and `tsid`
//! subcommands.

use std::path::{Path, PathBuf};

use crate::IdbError;

/// Recursively find tablespace files in a data directory, filtered by extension.
///
/// Searches the given directory and its immediate subdirectories for files
/// matching any of the provided extensions (e.g., `["ibd"]` or `["ibd", "ibu"]`).
/// Results are sorted by path.
pub fn find_tablespace_files(dir: &Path, extensions: &[&str]) -> Result<Vec<PathBuf>, IdbError> {
    let mut files = Vec::new();

    let entries = std::fs::read_dir(dir)
        .map_err(|e| IdbError::Io(format!("Cannot read directory {}: {}", dir.display(), e)))?;

    for entry in entries {
        let entry =
            entry.map_err(|e| IdbError::Io(format!("Cannot read directory entry: {}", e)))?;
        let path = entry.path();

        if path.is_dir() {
            let sub_entries = std::fs::read_dir(&path)
                .map_err(|e| IdbError::Io(format!("Cannot read {}: {}", path.display(), e)))?;

            for sub_entry in sub_entries {
                let sub_entry = sub_entry
                    .map_err(|e| IdbError::Io(format!("Cannot read directory entry: {}", e)))?;
                let sub_path = sub_entry.path();
                if has_matching_extension(&sub_path, extensions) {
                    files.push(sub_path);
                }
            }
        } else if has_matching_extension(&path, extensions) {
            files.push(path);
        }
    }

    files.sort();
    Ok(files)
}

fn has_matching_extension(path: &Path, extensions: &[&str]) -> bool {
    path.extension()
        .is_some_and(|ext| extensions.iter().any(|e| ext == *e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_find_empty_dir() {
        let dir = TempDir::new().unwrap();
        let files = find_tablespace_files(dir.path(), &["ibd"]).unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn test_find_with_ibd_files() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("table1.ibd"), b"data").unwrap();
        fs::write(dir.path().join("readme.txt"), b"text").unwrap();

        let files = find_tablespace_files(dir.path(), &["ibd"]).unwrap();
        assert_eq!(files.len(), 1);
        assert!(files[0].ends_with("table1.ibd"));
    }

    #[test]
    fn test_find_nested_dirs() {
        let dir = TempDir::new().unwrap();
        let subdir = dir.path().join("mydb");
        fs::create_dir(&subdir).unwrap();
        fs::write(subdir.join("orders.ibd"), b"data").unwrap();

        let files = find_tablespace_files(dir.path(), &["ibd"]).unwrap();
        assert_eq!(files.len(), 1);
        assert!(files[0].ends_with("orders.ibd"));
    }

    #[test]
    fn test_find_multiple_extensions() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("table.ibd"), b"data").unwrap();
        fs::write(dir.path().join("backup.ibu"), b"data").unwrap();
        fs::write(dir.path().join("notes.txt"), b"text").unwrap();

        let files = find_tablespace_files(dir.path(), &["ibd", "ibu"]).unwrap();
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn test_find_nonexistent_dir() {
        let result = find_tablespace_files(Path::new("/nonexistent/dir"), &["ibd"]);
        assert!(result.is_err());
    }
}
