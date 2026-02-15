//! InnoDB page checksum validation.
//!
//! Implements the checksum algorithms used by MySQL and MariaDB InnoDB:
//!
//! - **CRC-32C** (MySQL 5.7.7+ default): XOR of two independent CRC32c
//!   values computed over bytes `[4..26)` and `[38..page_size-8)`. These are
//!   NOT chained — each range is checksummed separately and the results XORed.
//!
//! - **Legacy InnoDB** (MySQL < 5.7.7): Uses `ut_fold_ulint_pair` with wrapping
//!   `u32` arithmetic, processing bytes one at a time over the same two ranges.
//!
//! - **MariaDB full_crc32** (MariaDB 10.5+): Single CRC-32C over bytes
//!   `[0..page_size-4)`. The checksum is stored in the last 4 bytes of the page
//!   (not in the FIL header).
//!
//! Use [`validate_checksum`] to check a page against all applicable algorithms.

use crate::innodb::constants::*;
use crate::innodb::vendor::VendorInfo;
use byteorder::{BigEndian, ByteOrder};

/// Checksum algorithms used by InnoDB.
///
/// # Examples
///
/// ```
/// use idb::innodb::checksum::ChecksumAlgorithm;
///
/// let algo = ChecksumAlgorithm::Crc32c;
/// assert_eq!(algo, ChecksumAlgorithm::Crc32c);
///
/// // All variants
/// let _crc = ChecksumAlgorithm::Crc32c;
/// let _legacy = ChecksumAlgorithm::InnoDB;
/// let _maria = ChecksumAlgorithm::MariaDbFullCrc32;
/// let _none = ChecksumAlgorithm::None;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumAlgorithm {
    /// CRC-32C (hardware accelerated, MySQL 5.7.7+ default)
    Crc32c,
    /// Legacy InnoDB checksum (buf_calc_page_new_checksum equivalent)
    InnoDB,
    /// MariaDB full_crc32 (single CRC-32C over entire page minus last 4 bytes)
    MariaDbFullCrc32,
    /// No checksum (innodb_checksum_algorithm=none)
    None,
}

/// Validate a page's checksum.
///
/// When `vendor_info` is provided and indicates MariaDB full_crc32 format,
/// the full_crc32 algorithm is tried first (checksum stored in the last 4
/// bytes of the page). Otherwise, MySQL CRC-32C and legacy InnoDB are tried.
///
/// # Examples
///
/// An all-zeros page (freshly allocated) is always considered valid:
///
/// ```
/// use idb::innodb::checksum::{validate_checksum, ChecksumAlgorithm};
///
/// let page = vec![0u8; 16384];
/// let result = validate_checksum(&page, 16384, None);
/// assert!(result.valid);
/// assert_eq!(result.algorithm, ChecksumAlgorithm::None);
/// ```
///
/// A page with the `BUF_NO_CHECKSUM_MAGIC` value (`0xDEADBEEF`) in bytes
/// 0-3 is treated as having checksums disabled:
///
/// ```
/// use idb::innodb::checksum::{validate_checksum, ChecksumAlgorithm};
/// use byteorder::{BigEndian, ByteOrder};
///
/// let mut page = vec![0u8; 16384];
/// BigEndian::write_u32(&mut page[0..], 0xDEADBEEF);
/// let result = validate_checksum(&page, 16384, None);
/// assert!(result.valid);
/// assert_eq!(result.algorithm, ChecksumAlgorithm::None);
/// ```
pub fn validate_checksum(
    page_data: &[u8],
    page_size: u32,
    vendor_info: Option<&VendorInfo>,
) -> ChecksumResult {
    let ps = page_size as usize;
    if page_data.len() < ps {
        return ChecksumResult {
            algorithm: ChecksumAlgorithm::None,
            valid: false,
            stored_checksum: 0,
            calculated_checksum: 0,
        };
    }

    // All zeros page (freshly allocated) - valid with any algorithm
    let first_u32 = BigEndian::read_u32(&page_data[FIL_PAGE_SPACE_OR_CHKSUM..]);
    if first_u32 == 0 {
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

    // MariaDB full_crc32: try this first when vendor indicates it
    if vendor_info.is_some_and(|v| v.is_full_crc32()) {
        let stored = BigEndian::read_u32(&page_data[ps - 4..ps]);
        let calculated = calculate_mariadb_full_crc32(page_data, ps);
        if stored == calculated {
            return ChecksumResult {
                algorithm: ChecksumAlgorithm::MariaDbFullCrc32,
                valid: true,
                stored_checksum: stored,
                calculated_checksum: calculated,
            };
        }
        // full_crc32 didn't match — report failure
        return ChecksumResult {
            algorithm: ChecksumAlgorithm::MariaDbFullCrc32,
            valid: false,
            stored_checksum: stored,
            calculated_checksum: calculated,
        };
    }

    let stored_checksum = first_u32;

    // Check for "none" algorithm (stored checksum is BUF_NO_CHECKSUM_MAGIC = 0xDEADBEEF)
    if stored_checksum == 0xDEADBEEF {
        return ChecksumResult {
            algorithm: ChecksumAlgorithm::None,
            valid: true,
            stored_checksum,
            calculated_checksum: 0xDEADBEEF,
        };
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
///
/// # Examples
///
/// ```
/// use idb::innodb::checksum::{validate_checksum, ChecksumResult, ChecksumAlgorithm};
///
/// let page = vec![0u8; 16384];
/// let result: ChecksumResult = validate_checksum(&page, 16384, None);
///
/// // Inspect individual fields
/// println!("Algorithm: {:?}", result.algorithm);
/// println!("Valid: {}", result.valid);
/// println!("Stored:     0x{:08X}", result.stored_checksum);
/// println!("Calculated: 0x{:08X}", result.calculated_checksum);
/// ```
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

/// Calculate MariaDB full_crc32 checksum.
///
/// MariaDB 10.5+ uses a single CRC-32C over bytes `[0..page_size-4)`.
/// The checksum is stored in the last 4 bytes of the page (NOT in the
/// FIL header at bytes 0-3 like MySQL).
fn calculate_mariadb_full_crc32(page_data: &[u8], page_size: usize) -> u32 {
    crc32c::crc32c(&page_data[0..page_size - 4])
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
///
/// # Examples
///
/// Build a 16 KiB page with a matching LSN in the header (bytes 16-23)
/// and trailer (last 4 bytes):
///
/// ```
/// use idb::innodb::checksum::validate_lsn;
/// use byteorder::{BigEndian, ByteOrder};
///
/// let mut page = vec![0u8; 16384];
///
/// // Write LSN 0x00000000_AABBCCDD into the FIL header at byte 16
/// BigEndian::write_u64(&mut page[16..], 0xAABBCCDD);
///
/// // Write the low 32 bits into the trailer (last 4 bytes of the page)
/// BigEndian::write_u32(&mut page[16380..], 0xAABBCCDD);
///
/// assert!(validate_lsn(&page, 16384));
///
/// // Corrupt the trailer — LSN no longer matches
/// BigEndian::write_u32(&mut page[16380..], 0x00000000);
/// assert!(!validate_lsn(&page, 16384));
/// ```
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
    use crate::innodb::vendor::MariaDbFormat;

    #[test]
    fn test_all_zero_page_is_valid() {
        let page = vec![0u8; 16384];
        let result = validate_checksum(&page, 16384, None);
        assert!(result.valid);
    }

    #[test]
    fn test_no_checksum_magic() {
        let mut page = vec![0u8; 16384];
        BigEndian::write_u32(&mut page[0..], 0xDEADBEEF);
        let result = validate_checksum(&page, 16384, None);
        assert!(result.valid);
        assert_eq!(result.algorithm, ChecksumAlgorithm::None);
    }

    #[test]
    fn test_mariadb_full_crc32() {
        let ps = 16384usize;
        let mut page = vec![0xABu8; ps];
        // Write some data to make it non-trivial
        BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], 1);
        BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855);

        // Calculate and store the full_crc32 checksum in last 4 bytes
        let crc = crc32c::crc32c(&page[0..ps - 4]);
        BigEndian::write_u32(&mut page[ps - 4..], crc);

        let vendor = VendorInfo::mariadb(MariaDbFormat::FullCrc32);
        let result = validate_checksum(&page, ps as u32, Some(&vendor));
        assert!(result.valid);
        assert_eq!(result.algorithm, ChecksumAlgorithm::MariaDbFullCrc32);
    }

    #[test]
    fn test_mariadb_full_crc32_invalid() {
        let ps = 16384usize;
        let mut page = vec![0xABu8; ps];
        BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], 1);
        // Wrong checksum in last 4 bytes
        BigEndian::write_u32(&mut page[ps - 4..], 0xDEADDEAD);

        let vendor = VendorInfo::mariadb(MariaDbFormat::FullCrc32);
        let result = validate_checksum(&page, ps as u32, Some(&vendor));
        assert!(!result.valid);
        assert_eq!(result.algorithm, ChecksumAlgorithm::MariaDbFullCrc32);
    }

    #[test]
    fn test_lsn_validation_matching() {
        let mut page = vec![0u8; 16384];
        BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], 0x12345678);
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
