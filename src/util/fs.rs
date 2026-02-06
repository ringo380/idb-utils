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
