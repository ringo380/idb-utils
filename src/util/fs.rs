//! Filesystem helpers for tablespace file discovery.
//!
//! Provides [`find_tablespace_files`] to recursively search a MySQL data
//! directory for `.ibd` and `.ibu` files. Used by the `find`, `tsid`,
//! `audit`, and `repair` (batch) subcommands.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::IdbError;

/// Recursively find tablespace files in a data directory, filtered by extension.
///
/// Searches the given directory up to `max_depth` levels deep for files
/// matching any of the provided extensions (e.g., `["ibd"]` or `["ibd", "ibu"]`).
/// Results are sorted by path.
///
/// # Depth semantics
///
/// - `None` — default depth 2 (root + one subdirectory level, backward compatible)
/// - `Some(0)` — unlimited recursion
/// - `Some(n)` — recurse up to `n` levels (1 = root only, 2 = root + 1 subdir, etc.)
///
/// Symlinks are resolved to canonical paths and tracked to prevent infinite loops.
pub fn find_tablespace_files(
    dir: &Path,
    extensions: &[&str],
    max_depth: Option<u32>,
) -> Result<Vec<PathBuf>, IdbError> {
    let effective_depth = max_depth.unwrap_or(2);
    let mut files = Vec::new();
    let mut visited = HashSet::new();

    // Canonicalize the root to seed the visited set
    let canonical_root = dir
        .canonicalize()
        .map_err(|e| IdbError::Io(format!("Cannot resolve {}: {}", dir.display(), e)))?;
    visited.insert(canonical_root);

    scan_dir(
        dir,
        extensions,
        effective_depth,
        1,
        &mut files,
        &mut visited,
    )?;

    files.sort();
    Ok(files)
}

/// Recursively scan a directory for tablespace files.
///
/// `current_level` starts at 1 for the root directory.
/// `max_depth` of 0 means unlimited; otherwise scanning stops when
/// `current_level > max_depth`.
fn scan_dir(
    dir: &Path,
    extensions: &[&str],
    max_depth: u32,
    current_level: u32,
    files: &mut Vec<PathBuf>,
    visited: &mut HashSet<PathBuf>,
) -> Result<(), IdbError> {
    // If max_depth is non-zero, enforce the limit
    if max_depth != 0 && current_level > max_depth {
        return Ok(());
    }

    let entries = std::fs::read_dir(dir)
        .map_err(|e| IdbError::Io(format!("Cannot read directory {}: {}", dir.display(), e)))?;

    for entry in entries {
        let entry =
            entry.map_err(|e| IdbError::Io(format!("Cannot read directory entry: {}", e)))?;
        let path = entry.path();

        if path.is_dir() {
            // Resolve symlinks to detect cycles
            let canonical = match path.canonicalize() {
                Ok(c) => c,
                Err(_) => continue, // skip unresolvable symlinks
            };

            if !visited.insert(canonical) {
                // Already visited this directory — skip to avoid infinite loop
                continue;
            }

            scan_dir(
                &path,
                extensions,
                max_depth,
                current_level + 1,
                files,
                visited,
            )?;
        } else if has_matching_extension(&path, extensions) {
            files.push(path);
        }
    }

    Ok(())
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
        let files = find_tablespace_files(dir.path(), &["ibd"], None).unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn test_find_with_ibd_files() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("table1.ibd"), b"data").unwrap();
        fs::write(dir.path().join("readme.txt"), b"text").unwrap();

        let files = find_tablespace_files(dir.path(), &["ibd"], None).unwrap();
        assert_eq!(files.len(), 1);
        assert!(files[0].ends_with("table1.ibd"));
    }

    #[test]
    fn test_find_nested_dirs() {
        let dir = TempDir::new().unwrap();
        let subdir = dir.path().join("mydb");
        fs::create_dir(&subdir).unwrap();
        fs::write(subdir.join("orders.ibd"), b"data").unwrap();

        let files = find_tablespace_files(dir.path(), &["ibd"], None).unwrap();
        assert_eq!(files.len(), 1);
        assert!(files[0].ends_with("orders.ibd"));
    }

    #[test]
    fn test_find_multiple_extensions() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("table.ibd"), b"data").unwrap();
        fs::write(dir.path().join("backup.ibu"), b"data").unwrap();
        fs::write(dir.path().join("notes.txt"), b"text").unwrap();

        let files = find_tablespace_files(dir.path(), &["ibd", "ibu"], None).unwrap();
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn test_find_nonexistent_dir() {
        let result = find_tablespace_files(Path::new("/nonexistent/dir"), &["ibd"], None);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Depth-specific tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_depth_1_root_only() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("root.ibd"), b"data").unwrap();
        let subdir = dir.path().join("sub");
        fs::create_dir(&subdir).unwrap();
        fs::write(subdir.join("nested.ibd"), b"data").unwrap();

        let files = find_tablespace_files(dir.path(), &["ibd"], Some(1)).unwrap();
        assert_eq!(files.len(), 1);
        assert!(files[0].ends_with("root.ibd"));
    }

    #[test]
    fn test_depth_2_root_plus_one_subdir() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("root.ibd"), b"data").unwrap();

        let level1 = dir.path().join("level1");
        fs::create_dir(&level1).unwrap();
        fs::write(level1.join("l1.ibd"), b"data").unwrap();

        let level2 = level1.join("level2");
        fs::create_dir(&level2).unwrap();
        fs::write(level2.join("l2.ibd"), b"data").unwrap();

        let files = find_tablespace_files(dir.path(), &["ibd"], Some(2)).unwrap();
        assert_eq!(files.len(), 2);
        // Should find root.ibd and level1/l1.ibd, but NOT level1/level2/l2.ibd
        let names: Vec<String> = files
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
            .collect();
        assert!(names.contains(&"root.ibd".to_string()));
        assert!(names.contains(&"l1.ibd".to_string()));
    }

    #[test]
    fn test_depth_3_three_levels() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("root.ibd"), b"data").unwrap();

        let level1 = dir.path().join("level1");
        fs::create_dir(&level1).unwrap();
        fs::write(level1.join("l1.ibd"), b"data").unwrap();

        let level2 = level1.join("level2");
        fs::create_dir(&level2).unwrap();
        fs::write(level2.join("l2.ibd"), b"data").unwrap();

        let level3 = level2.join("level3");
        fs::create_dir(&level3).unwrap();
        fs::write(level3.join("l3.ibd"), b"data").unwrap();

        let files = find_tablespace_files(dir.path(), &["ibd"], Some(3)).unwrap();
        assert_eq!(files.len(), 3);
        let names: Vec<String> = files
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
            .collect();
        assert!(names.contains(&"root.ibd".to_string()));
        assert!(names.contains(&"l1.ibd".to_string()));
        assert!(names.contains(&"l2.ibd".to_string()));
    }

    #[test]
    fn test_depth_0_unlimited() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("root.ibd"), b"data").unwrap();

        let level1 = dir.path().join("level1");
        fs::create_dir(&level1).unwrap();
        fs::write(level1.join("l1.ibd"), b"data").unwrap();

        let level2 = level1.join("level2");
        fs::create_dir(&level2).unwrap();
        fs::write(level2.join("l2.ibd"), b"data").unwrap();

        let level3 = level2.join("level3");
        fs::create_dir(&level3).unwrap();
        fs::write(level3.join("l3.ibd"), b"data").unwrap();

        let level4 = level3.join("level4");
        fs::create_dir(&level4).unwrap();
        fs::write(level4.join("l4.ibd"), b"data").unwrap();

        let files = find_tablespace_files(dir.path(), &["ibd"], Some(0)).unwrap();
        assert_eq!(files.len(), 5);
    }

    #[test]
    fn test_default_none_is_depth_2() {
        let dir = TempDir::new().unwrap();

        let level1 = dir.path().join("level1");
        fs::create_dir(&level1).unwrap();
        fs::write(level1.join("l1.ibd"), b"data").unwrap();

        let level2 = level1.join("level2");
        fs::create_dir(&level2).unwrap();
        fs::write(level2.join("l2.ibd"), b"data").unwrap();

        let files_none = find_tablespace_files(dir.path(), &["ibd"], None).unwrap();
        let files_two = find_tablespace_files(dir.path(), &["ibd"], Some(2)).unwrap();

        assert_eq!(files_none.len(), files_two.len());
        assert_eq!(files_none.len(), 1);
        // Only l1.ibd at depth 2, l2.ibd would be at depth 3
    }

    #[cfg(unix)]
    #[test]
    fn test_symlink_loop_safety() {
        use std::os::unix::fs::symlink;

        let dir = TempDir::new().unwrap();
        let sub = dir.path().join("sub");
        fs::create_dir(&sub).unwrap();
        fs::write(sub.join("table.ibd"), b"data").unwrap();

        // Create a symlink loop: sub/loop -> root
        symlink(dir.path(), sub.join("loop")).unwrap();

        // Unlimited depth with symlink loop should not hang
        let files = find_tablespace_files(dir.path(), &["ibd"], Some(0)).unwrap();
        assert_eq!(files.len(), 1);
        assert!(files[0].ends_with("table.ibd"));
    }

    #[cfg(unix)]
    #[test]
    fn test_symlink_self_referential() {
        use std::os::unix::fs::symlink;

        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("table.ibd"), b"data").unwrap();

        // Create a symlink to self
        let sub = dir.path().join("self");
        symlink(dir.path(), &sub).unwrap();

        let files = find_tablespace_files(dir.path(), &["ibd"], Some(0)).unwrap();
        // Should find exactly 1 file without infinite loop
        assert_eq!(files.len(), 1);
    }
}
