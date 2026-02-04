use std::path::Path;

use crate::innodb::page::FilHeader;
use crate::innodb::tablespace::Tablespace;
use crate::IdbError;

pub struct FindOptions {
    pub datadir: String,
    pub page: u64,
    pub checksum: Option<u32>,
    pub space_id: Option<u32>,
}

pub fn execute(opts: &FindOptions) -> Result<(), IdbError> {
    let datadir = Path::new(&opts.datadir);
    if !datadir.is_dir() {
        return Err(IdbError::Argument(format!(
            "Data directory does not exist: {}",
            opts.datadir
        )));
    }

    // Find all .ibd files in subdirectories
    let ibd_files = find_ibd_files(datadir)?;

    if ibd_files.is_empty() {
        println!("No .ibd files found in {}", opts.datadir);
        return Ok(());
    }

    for ibd_path in &ibd_files {
        let display_path = ibd_path.strip_prefix(datadir).unwrap_or(ibd_path);
        println!("Checking {}.. ", display_path.display());

        let mut ts = match Tablespace::open(ibd_path) {
            Ok(t) => t,
            Err(_) => continue,
        };

        for page_num in 0..ts.page_count() {
            let page_data = match ts.read_page(page_num) {
                Ok(d) => d,
                Err(_) => continue,
            };

            let header = match FilHeader::parse(&page_data) {
                Some(h) => h,
                None => continue,
            };

            if header.page_number as u64 == opts.page {
                // If checksum filter specified, must also match
                if let Some(expected_csum) = opts.checksum {
                    if header.checksum != expected_csum {
                        continue;
                    }
                }
                // If space_id filter specified, must also match
                if let Some(expected_sid) = opts.space_id {
                    if header.space_id != expected_sid {
                        continue;
                    }
                }

                println!(
                    "Found page {} in {} (checksum: {}, space_id: {})",
                    opts.page,
                    display_path.display(),
                    header.checksum,
                    header.space_id
                );
                return Ok(());
            }
        }
    }

    println!("Page {} not found in any .ibd file.", opts.page);
    Ok(())
}

/// Recursively find all .ibd files in subdirectories of the data directory.
fn find_ibd_files(datadir: &Path) -> Result<Vec<std::path::PathBuf>, IdbError> {
    let mut files = Vec::new();

    let entries = std::fs::read_dir(datadir)
        .map_err(|e| IdbError::Io(format!("Cannot read directory {}: {}", datadir.display(), e)))?;

    for entry in entries {
        let entry =
            entry.map_err(|e| IdbError::Io(format!("Cannot read directory entry: {}", e)))?;
        let path = entry.path();

        if path.is_dir() {
            // Look for .ibd files in subdirectories
            let sub_entries = std::fs::read_dir(&path)
                .map_err(|e| IdbError::Io(format!("Cannot read {}: {}", path.display(), e)))?;

            for sub_entry in sub_entries {
                let sub_entry = sub_entry
                    .map_err(|e| IdbError::Io(format!("Cannot read directory entry: {}", e)))?;
                let sub_path = sub_entry.path();
                if sub_path.extension().is_some_and(|ext| ext == "ibd") {
                    files.push(sub_path);
                }
            }
        } else if path.extension().is_some_and(|ext| ext == "ibd") {
            files.push(path);
        }
    }

    files.sort();
    Ok(files)
}
