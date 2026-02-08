//! InnoDB page checksum validation.
//!
//! Implements the two checksum algorithms used by MySQL's InnoDB engine:
//!
//! - **CRC-32C** (default since MySQL 5.7.7): XOR of two independent CRC32c
//!   values computed over bytes `[4..26)` and `[38..page_size-8)`. These are
//!   NOT chained — each range is checksummed separately and the results XORed.
//!
//! - **Legacy InnoDB** (MySQL < 5.7.7): Uses `ut_fold_ulint_pair` with wrapping
//!   `u32` arithmetic, processing bytes one at a time over the same two ranges.
//!
//! Use [`validate_checksum`] to check a page against both algorithms.

use crate::innodb::constants::*;
use byteorder::{BigEndian, ByteOrder};

/// Checksum algorithms used by InnoDB.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumAlgorithm {
    /// CRC-32C (hardware accelerated, MySQL 5.7.7+ default)
    Crc32c,
    /// Legacy InnoDB checksum (buf_calc_page_new_checksum equivalent)
    InnoDB,
    /// No checksum (innodb_checksum_algorithm=none)
    None,
}

/// Validate a page's checksum.
///
/// Returns the detected algorithm and whether the checksum matches.
pub fn validate_checksum(page_data: &[u8], page_size: u32) -> ChecksumResult {
    let ps = page_size as usize;
    if page_data.len() < ps {
        return ChecksumResult {
            algorithm: ChecksumAlgorithm::None,
            valid: false,
            stored_checksum: 0,
            calculated_checksum: 0,
        };
    }

    let stored_checksum = BigEndian::read_u32(&page_data[FIL_PAGE_SPACE_OR_CHKSUM..]);

    // Check for "none" algorithm (stored checksum is BUF_NO_CHECKSUM_MAGIC = 0xDEADBEEF)
    if stored_checksum == 0xDEADBEEF {
        return ChecksumResult {
            algorithm: ChecksumAlgorithm::None,
            valid: true,
            stored_checksum,
            calculated_checksum: 0xDEADBEEF,
        };
    }

    // All zeros page (freshly allocated) - valid with any algorithm
    if stored_checksum == 0 {
        let all_zero = page_data[..ps].iter().all(|&b| b == 0);
        if all_zero {
            return ChecksumResult {
                algorithm: ChecksumAlgorithm::None,
                valid: true,
                stored_checksum: 0,
                calculated_checksum: 0,
            };
        }
    }

    // Try CRC-32C first (most common for MySQL 8.0+)
    let crc_checksum = calculate_crc32c(page_data, ps);
    if stored_checksum == crc_checksum {
        return ChecksumResult {
            algorithm: ChecksumAlgorithm::Crc32c,
            valid: true,
            stored_checksum,
            calculated_checksum: crc_checksum,
        };
    }

    // Try legacy InnoDB checksum
    let innodb_checksum = calculate_innodb_checksum(page_data, ps);
    if stored_checksum == innodb_checksum {
        return ChecksumResult {
            algorithm: ChecksumAlgorithm::InnoDB,
            valid: true,
            stored_checksum,
            calculated_checksum: innodb_checksum,
        };
    }

    // Neither matched - report failure with CRC-32C as expected
    ChecksumResult {
        algorithm: ChecksumAlgorithm::Crc32c,
        valid: false,
        stored_checksum,
        calculated_checksum: crc_checksum,
    }
}

/// Result of a checksum validation.
#[derive(Debug, Clone)]
pub struct ChecksumResult {
    /// The checksum algorithm that was detected or attempted.
    pub algorithm: ChecksumAlgorithm,
    /// Whether the stored checksum matches the calculated value.
    pub valid: bool,
    /// The checksum value stored in the page's FIL header (bytes 0-3).
    pub stored_checksum: u32,
    /// The checksum value calculated from the page data.
    pub calculated_checksum: u32,
}

/// Calculate CRC-32C checksum for an InnoDB page.
///
/// MySQL computes CRC-32C independently over two disjoint ranges and XORs
/// the results (see buf_calc_page_crc32 in buf0checksum.cc). Skipped regions:
/// - bytes 0-3 (stored checksum)
/// - bytes 26-37 (flush LSN + space ID, written outside buffer pool)
/// - last 8 bytes (trailer)
///
/// Range 1: bytes 4..26 (FIL_PAGE_OFFSET to FIL_PAGE_FILE_FLUSH_LSN)
/// Range 2: bytes 38..(page_size-8) (FIL_PAGE_DATA to end before trailer)
fn calculate_crc32c(page_data: &[u8], page_size: usize) -> u32 {
    let end = page_size - SIZE_FIL_TRAILER;

    // CRC-32C of range 1: bytes 4..26
    let crc1 = crc32c::crc32c(&page_data[FIL_PAGE_OFFSET..FIL_PAGE_FILE_FLUSH_LSN]);

    // CRC-32C of range 2: bytes 38..(page_size - 8)
    let crc2 = crc32c::crc32c(&page_data[FIL_PAGE_DATA..end]);

    // MySQL XORs the two CRC values (not chained/appended)
    crc1 ^ crc2
}

/// InnoDB's ut_fold_ulint_pair — the core folding function.
///
/// All arithmetic is done in u32 with wrapping, matching the effective behavior
/// of InnoDB's checksum as implemented by innodb_ruby and verified against real
/// .ibd files from MySQL 5.0 through 5.6.
#[inline]
fn ut_fold_ulint_pair(n1: u32, n2: u32) -> u32 {
    let step = n1 ^ n2 ^ UT_HASH_RANDOM_MASK2;
    let step = (step << 8).wrapping_add(n1);
    let step = step ^ UT_HASH_RANDOM_MASK;
    step.wrapping_add(n2)
}

/// Fold a byte sequence using ut_fold_ulint_pair, one byte at a time.
///
/// This matches innodb_ruby's fold_enumerator implementation, which processes
/// each byte individually through fold_pair. Verified against real .ibd files
/// from MySQL 5.0 and 5.6.
fn ut_fold_binary(data: &[u8]) -> u32 {
    let mut fold: u32 = 0;
    for &byte in data {
        fold = ut_fold_ulint_pair(fold, byte as u32);
    }
    fold
}

/// Calculate the legacy InnoDB checksum (buf_calc_page_new_checksum).
///
/// Used by MySQL < 5.7.7 (innodb_checksum_algorithm=innodb).
/// Folds two byte ranges and sums the results:
/// 1. Bytes 4..26 (FIL_PAGE_OFFSET to FIL_PAGE_FILE_FLUSH_LSN)
/// 2. Bytes 38..(page_size - 8) (FIL_PAGE_DATA to end before trailer)
fn calculate_innodb_checksum(page_data: &[u8], page_size: usize) -> u32 {
    let end = page_size - SIZE_FIL_TRAILER;

    let fold1 = ut_fold_binary(&page_data[FIL_PAGE_OFFSET..FIL_PAGE_FILE_FLUSH_LSN]);
    let fold2 = ut_fold_binary(&page_data[FIL_PAGE_DATA..end]);

    fold1.wrapping_add(fold2)
}

/// Validate the LSN consistency between header and trailer.
///
/// The low 32 bits of the header LSN should match the trailer LSN field.
pub fn validate_lsn(page_data: &[u8], page_size: u32) -> bool {
    let ps = page_size as usize;
    if page_data.len() < ps {
        return false;
    }
    let header_lsn = BigEndian::read_u64(&page_data[FIL_PAGE_LSN..]);
    let header_lsn_low32 = (header_lsn & 0xFFFFFFFF) as u32;

    let trailer_offset = ps - SIZE_FIL_TRAILER;
    let trailer_lsn_low32 = BigEndian::read_u32(&page_data[trailer_offset + 4..]);

    header_lsn_low32 == trailer_lsn_low32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_zero_page_is_valid() {
        let page = vec![0u8; 16384];
        let result = validate_checksum(&page, 16384);
        assert!(result.valid);
    }

    #[test]
    fn test_no_checksum_magic() {
        let mut page = vec![0u8; 16384];
        BigEndian::write_u32(&mut page[0..], 0xDEADBEEF);
        let result = validate_checksum(&page, 16384);
        assert!(result.valid);
        assert_eq!(result.algorithm, ChecksumAlgorithm::None);
    }

    #[test]
    fn test_lsn_validation_matching() {
        let mut page = vec![0u8; 16384];
        // Write LSN = 0x0000000012345678 at offset 16
        BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], 0x12345678);
        // Write low 32 bits at trailer + 4 (offset 16380)
        BigEndian::write_u32(&mut page[16380..], 0x12345678);
        assert!(validate_lsn(&page, 16384));
    }

    #[test]
    fn test_lsn_validation_mismatch() {
        let mut page = vec![0u8; 16384];
        BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], 0x12345678);
        BigEndian::write_u32(&mut page[16380..], 0xAAAAAAAA);
        assert!(!validate_lsn(&page, 16384));
    }
}
