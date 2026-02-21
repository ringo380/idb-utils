//! Write utilities for InnoDB page-level operations.
//!
//! Provides reusable functions for creating backups, reading/writing individual
//! pages, constructing valid FSP_HDR pages, detecting checksum algorithms, and
//! fixing page checksums. These are the building blocks for the `repair`,
//! `defrag`, `transplant`, and `recover --rebuild` subcommands.
//!
//! This module is not available on WASM targets since file I/O is not supported.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use byteorder::{BigEndian, ByteOrder};

use crate::innodb::checksum::{
    recalculate_checksum, validate_checksum, ChecksumAlgorithm,
};
use crate::innodb::constants::*;
use crate::innodb::vendor::VendorInfo;
use crate::IdbError;

/// Create a backup of a file by copying it to `<path>.bak`.
///
/// If `<path>.bak` already exists, tries `.bak.1`, `.bak.2`, etc.
/// Returns the path of the created backup file.
pub fn create_backup(path: &str) -> Result<PathBuf, IdbError> {
    let src = Path::new(path);
    if !src.exists() {
        return Err(IdbError::Io(format!("File not found: {}", path)));
    }

    let mut backup_path = PathBuf::from(format!("{}.bak", path));
    let mut counter = 1u32;
    while backup_path.exists() {
        backup_path = PathBuf::from(format!("{}.bak.{}", path, counter));
        counter += 1;
        if counter > 999 {
            return Err(IdbError::Io(format!(
                "Too many backup files for {}",
                path
            )));
        }
    }

    fs::copy(src, &backup_path)
        .map_err(|e| IdbError::Io(format!("Cannot create backup {}: {}", backup_path.display(), e)))?;

    Ok(backup_path)
}

/// Read a single page from a tablespace file.
///
/// Returns a `Vec<u8>` of exactly `page_size` bytes.
pub fn read_page_raw(path: &str, page_num: u64, page_size: u32) -> Result<Vec<u8>, IdbError> {
    let offset = page_num * page_size as u64;
    let mut f = File::open(path)
        .map_err(|e| IdbError::Io(format!("Cannot open {}: {}", path, e)))?;
    f.seek(SeekFrom::Start(offset))
        .map_err(|e| IdbError::Io(format!("Cannot seek to offset {}: {}", offset, e)))?;
    let mut buf = vec![0u8; page_size as usize];
    f.read_exact(&mut buf)
        .map_err(|e| IdbError::Io(format!("Cannot read page {}: {}", page_num, e)))?;
    Ok(buf)
}

/// Write a single page to a tablespace file at the correct offset.
///
/// The `data` slice must be exactly `page_size` bytes.
pub fn write_page(path: &str, page_num: u64, page_size: u32, data: &[u8]) -> Result<(), IdbError> {
    if data.len() != page_size as usize {
        return Err(IdbError::Argument(format!(
            "Page data is {} bytes, expected {}",
            data.len(),
            page_size
        )));
    }
    let offset = page_num * page_size as u64;
    let mut f = OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|e| IdbError::Io(format!("Cannot open {} for writing: {}", path, e)))?;
    f.seek(SeekFrom::Start(offset))
        .map_err(|e| IdbError::Io(format!("Cannot seek to offset {}: {}", offset, e)))?;
    f.write_all(data)
        .map_err(|e| IdbError::Io(format!("Cannot write page {}: {}", page_num, e)))?;
    Ok(())
}

/// Write all pages sequentially to a new file.
///
/// Creates (or truncates) the file at `path` and writes each page in order.
pub fn write_tablespace(path: &str, pages: &[Vec<u8>]) -> Result<(), IdbError> {
    let mut f = File::create(path)
        .map_err(|e| IdbError::Io(format!("Cannot create {}: {}", path, e)))?;
    for (i, page) in pages.iter().enumerate() {
        f.write_all(page)
            .map_err(|e| IdbError::Io(format!("Cannot write page {}: {}", i, e)))?;
    }
    f.flush()
        .map_err(|e| IdbError::Io(format!("Cannot flush {}: {}", path, e)))?;
    Ok(())
}

/// Build a minimal valid FSP_HDR page (page 0) with correct FIL header,
/// FSP header fields, FIL trailer, and checksum.
pub fn build_fsp_page(
    space_id: u32,
    total_pages: u32,
    flags: u32,
    lsn: u64,
    page_size: u32,
    algorithm: ChecksumAlgorithm,
) -> Vec<u8> {
    let ps = page_size as usize;
    let mut page = vec![0u8; ps];

    // FIL header
    BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], 0); // page 0
    BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], lsn);
    BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 8); // FSP_HDR
    BigEndian::write_u64(&mut page[FIL_PAGE_FILE_FLUSH_LSN..], lsn);
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);

    // FSP header at FIL_PAGE_DATA (offset 38)
    let fsp = FIL_PAGE_DATA;
    BigEndian::write_u32(&mut page[fsp + FSP_SPACE_ID..], space_id);
    BigEndian::write_u32(&mut page[fsp + FSP_SIZE..], total_pages);
    BigEndian::write_u32(&mut page[fsp + FSP_FREE_LIMIT..], total_pages);
    BigEndian::write_u32(&mut page[fsp + FSP_SPACE_FLAGS..], flags);

    // FIL trailer: low 32 bits of LSN
    let trailer = ps - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut page[trailer + 4..], (lsn & 0xFFFFFFFF) as u32);

    // Calculate and write checksum
    recalculate_checksum(&mut page, page_size, algorithm);

    page
}

/// Detect the checksum algorithm in use for a page.
///
/// Mirrors the logic of `validate_checksum()` but returns the detected
/// algorithm instead of a full validation result. Returns `None` for
/// empty/zero pages and `BUF_NO_CHECKSUM_MAGIC` pages.
pub fn detect_algorithm(
    page_data: &[u8],
    page_size: u32,
    vendor_info: Option<&VendorInfo>,
) -> ChecksumAlgorithm {
    let result = validate_checksum(page_data, page_size, vendor_info);
    result.algorithm
}

/// Fix the checksum and trailer LSN consistency for a page.
///
/// 1. Ensures the trailer LSN low-32 matches the header LSN low-32.
/// 2. Recalculates the checksum using the given algorithm.
///
/// Returns `(old_checksum, new_checksum)`.
pub fn fix_page_checksum(
    page_data: &mut [u8],
    page_size: u32,
    algorithm: ChecksumAlgorithm,
) -> (u32, u32) {
    let ps = page_size as usize;
    if page_data.len() < ps {
        return (0, 0);
    }

    // Read old checksum
    let old_checksum = match algorithm {
        ChecksumAlgorithm::MariaDbFullCrc32 => {
            BigEndian::read_u32(&page_data[ps - 4..])
        }
        _ => BigEndian::read_u32(&page_data[FIL_PAGE_SPACE_OR_CHKSUM..]),
    };

    // Fix trailer LSN consistency: low 32 bits of header LSN → trailer
    let header_lsn = BigEndian::read_u64(&page_data[FIL_PAGE_LSN..]);
    let trailer_offset = ps - SIZE_FIL_TRAILER;
    BigEndian::write_u32(
        &mut page_data[trailer_offset + 4..],
        (header_lsn & 0xFFFFFFFF) as u32,
    );

    // Recalculate checksum
    recalculate_checksum(page_data, page_size, algorithm);

    // Read new checksum
    let new_checksum = match algorithm {
        ChecksumAlgorithm::MariaDbFullCrc32 => {
            BigEndian::read_u32(&page_data[ps - 4..])
        }
        _ => BigEndian::read_u32(&page_data[FIL_PAGE_SPACE_OR_CHKSUM..]),
    };

    (old_checksum, new_checksum)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::innodb::checksum::validate_lsn;
    use crate::innodb::tablespace::Tablespace;
    use std::io::Write as IoWrite;
    use tempfile::NamedTempFile;

    const PS: u32 = 16384;

    fn make_test_page(page_num: u32, space_id: u32, lsn: u64) -> Vec<u8> {
        let ps = PS as usize;
        let mut page = vec![0u8; ps];
        BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], page_num);
        BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
        BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
        BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], lsn);
        BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855); // INDEX
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);
        let trailer = ps - SIZE_FIL_TRAILER;
        BigEndian::write_u32(&mut page[trailer + 4..], (lsn & 0xFFFFFFFF) as u32);
        recalculate_checksum(&mut page, PS, ChecksumAlgorithm::Crc32c);
        page
    }

    fn write_temp_tablespace(pages: &[Vec<u8>]) -> NamedTempFile {
        let mut tmp = NamedTempFile::new().unwrap();
        for page in pages {
            tmp.write_all(page).unwrap();
        }
        tmp.flush().unwrap();
        tmp
    }

    #[test]
    fn test_create_backup() {
        let mut tmp = NamedTempFile::new().unwrap();
        tmp.write_all(b"test data").unwrap();
        tmp.flush().unwrap();

        let path = tmp.path().to_str().unwrap();
        let backup = create_backup(path).unwrap();
        assert!(backup.exists());
        assert_eq!(fs::read(&backup).unwrap(), b"test data");

        // Second backup gets .bak.1
        let backup2 = create_backup(path).unwrap();
        assert!(backup2.exists());
        assert_ne!(backup, backup2);

        // Cleanup
        let _ = fs::remove_file(&backup);
        let _ = fs::remove_file(&backup2);
    }

    #[test]
    fn test_read_page_raw_roundtrip() {
        let page0 = build_fsp_page(42, 2, 0, 1000, PS, ChecksumAlgorithm::Crc32c);
        let page1 = make_test_page(1, 42, 2000);
        let tmp = write_temp_tablespace(&[page0.clone(), page1.clone()]);
        let path = tmp.path().to_str().unwrap();

        let read0 = read_page_raw(path, 0, PS).unwrap();
        assert_eq!(read0, page0);

        let read1 = read_page_raw(path, 1, PS).unwrap();
        assert_eq!(read1, page1);
    }

    #[test]
    fn test_write_page_modifies_correct_offset() {
        let page0 = build_fsp_page(42, 2, 0, 1000, PS, ChecksumAlgorithm::Crc32c);
        let page1 = make_test_page(1, 42, 2000);
        let tmp = write_temp_tablespace(&[page0, page1]);
        let path = tmp.path().to_str().unwrap();

        // Overwrite page 1 with a new page
        let new_page1 = make_test_page(1, 42, 9999);
        write_page(path, 1, PS, &new_page1).unwrap();

        // Verify page 1 changed
        let read1 = read_page_raw(path, 1, PS).unwrap();
        assert_eq!(read1, new_page1);

        // Verify page 0 untouched
        let read0 = read_page_raw(path, 0, PS).unwrap();
        let result = validate_checksum(&read0, PS, None);
        assert!(result.valid);
    }

    #[test]
    fn test_build_fsp_page_valid() {
        let page = build_fsp_page(42, 5, 0, 1000, PS, ChecksumAlgorithm::Crc32c);
        assert_eq!(page.len(), PS as usize);

        // Validate checksum
        let result = validate_checksum(&page, PS, None);
        assert!(result.valid);

        // Validate LSN consistency
        assert!(validate_lsn(&page, PS));

        // Check space_id in FIL header
        assert_eq!(BigEndian::read_u32(&page[FIL_PAGE_SPACE_ID..]), 42);

        // Check FSP header
        let fsp = FIL_PAGE_DATA;
        assert_eq!(BigEndian::read_u32(&page[fsp + FSP_SPACE_ID..]), 42);
        assert_eq!(BigEndian::read_u32(&page[fsp + FSP_SIZE..]), 5);
    }

    #[test]
    fn test_build_fsp_page_opens_as_tablespace() {
        let page0 = build_fsp_page(42, 3, 0, 1000, PS, ChecksumAlgorithm::Crc32c);
        let page1 = make_test_page(1, 42, 2000);
        let page2 = make_test_page(2, 42, 3000);
        let tmp = write_temp_tablespace(&[page0, page1, page2]);

        let ts = Tablespace::open(tmp.path()).unwrap();
        assert_eq!(ts.page_size(), PS);
        assert_eq!(ts.page_count(), 3);
    }

    #[test]
    fn test_fix_page_checksum_crc32c() {
        let ps = PS as usize;
        let mut page = make_test_page(1, 42, 5000);

        // Corrupt checksum
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_OR_CHKSUM..], 0xDEAD);
        assert!(!validate_checksum(&page, PS, None).valid);

        // Fix
        let (old, new) = fix_page_checksum(&mut page, PS, ChecksumAlgorithm::Crc32c);
        assert_eq!(old, 0xDEAD);
        assert_ne!(new, 0xDEAD);
        assert!(validate_checksum(&page, PS, None).valid);
        assert!(validate_lsn(&page, PS));
    }

    #[test]
    fn test_fix_page_checksum_fixes_trailer_lsn() {
        let ps = PS as usize;
        let mut page = make_test_page(1, 42, 5000);

        // Corrupt trailer LSN
        let trailer = ps - SIZE_FIL_TRAILER;
        BigEndian::write_u32(&mut page[trailer + 4..], 0xAAAA);
        assert!(!validate_lsn(&page, PS));

        // Fix
        fix_page_checksum(&mut page, PS, ChecksumAlgorithm::Crc32c);
        assert!(validate_lsn(&page, PS));
        assert!(validate_checksum(&page, PS, None).valid);
    }

    #[test]
    fn test_write_tablespace_creates_file() {
        let page0 = build_fsp_page(42, 2, 0, 1000, PS, ChecksumAlgorithm::Crc32c);
        let page1 = make_test_page(1, 42, 2000);

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        drop(tmp); // Delete so write_tablespace creates it

        write_tablespace(&path, &[page0.clone(), page1.clone()]).unwrap();

        // Verify we can open it
        let ts = Tablespace::open(&path).unwrap();
        assert_eq!(ts.page_size(), PS);
        assert_eq!(ts.page_count(), 2);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_detect_algorithm_crc32c() {
        let page = make_test_page(1, 42, 5000);
        let algo = detect_algorithm(&page, PS, None);
        assert_eq!(algo, ChecksumAlgorithm::Crc32c);
    }
}
