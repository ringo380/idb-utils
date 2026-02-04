use byteorder::{BigEndian, ByteOrder};

use crate::innodb::constants::*;

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
    pub algorithm: ChecksumAlgorithm,
    pub valid: bool,
    pub stored_checksum: u32,
    pub calculated_checksum: u32,
}

/// Calculate CRC-32C checksum for an InnoDB page.
///
/// MySQL computes CRC-32C over two disjoint ranges, skipping:
/// - bytes 0-3 (stored checksum)
/// - bytes 26-37 (flush LSN + space ID, written outside buffer pool)
/// - last 8 bytes (trailer)
///
/// Range 1: bytes 4..26 (FIL_PAGE_OFFSET to FIL_PAGE_FILE_FLUSH_LSN)
/// Range 2: bytes 38..(page_size-8) (FIL_PAGE_DATA to end before trailer)
fn calculate_crc32c(page_data: &[u8], page_size: usize) -> u32 {
    let end = page_size - SIZE_FIL_TRAILER;

    // CRC-32C of range 1: bytes 4..26
    let crc = crc32c::crc32c(&page_data[FIL_PAGE_OFFSET..FIL_PAGE_FILE_FLUSH_LSN]);

    // Continue CRC with range 2: bytes 38..(page_size - 8)
    crc32c::crc32c_append(crc, &page_data[FIL_PAGE_DATA..end])
}

/// MySQL's ut_fold_ulint_pair — the core folding function.
///
/// Uses u64 to match MySQL's `ulint` (unsigned long) on LP64 platforms.
/// The final result is masked to 32 bits by the caller.
#[inline]
fn ut_fold_ulint_pair(n1: u64, n2: u64) -> u64 {
    let mask2 = UT_HASH_RANDOM_MASK2 as u64;
    let mask = UT_HASH_RANDOM_MASK as u64;
    ((((n1 ^ n2 ^ mask2) << 8).wrapping_add(n1)) ^ mask).wrapping_add(n2)
}

/// MySQL's ut_fold_binary — fold a byte sequence using ut_fold_ulint_pair.
///
/// Processes 8 bytes at a time (two u32 reads), then handles remainder.
/// Returns u64 (matching MySQL's ulint) to be truncated by caller.
fn ut_fold_binary(data: &[u8]) -> u64 {
    let mut fold: u64 = 0;
    let len = data.len();
    let aligned_len = len & !7; // round down to multiple of 8

    // Process 8 bytes at a time
    let mut i = 0;
    while i < aligned_len {
        fold = ut_fold_ulint_pair(fold, BigEndian::read_u32(&data[i..]) as u64);
        i += 4;
        fold = ut_fold_ulint_pair(fold, BigEndian::read_u32(&data[i..]) as u64);
        i += 4;
    }

    // Handle remaining bytes (matches MySQL's switch fallthrough)
    let remainder = len & 7;
    match remainder {
        7 => {
            fold = ut_fold_ulint_pair(fold, data[i] as u64);
            i += 1;
            fold = ut_fold_ulint_pair(fold, data[i] as u64);
            i += 1;
            fold = ut_fold_ulint_pair(fold, data[i] as u64);
            i += 1;
            fold = ut_fold_ulint_pair(fold, BigEndian::read_u32(&data[i..]) as u64);
        }
        6 => {
            fold = ut_fold_ulint_pair(fold, data[i] as u64);
            i += 1;
            fold = ut_fold_ulint_pair(fold, data[i] as u64);
            i += 1;
            fold = ut_fold_ulint_pair(fold, BigEndian::read_u32(&data[i..]) as u64);
        }
        5 => {
            fold = ut_fold_ulint_pair(fold, data[i] as u64);
            i += 1;
            fold = ut_fold_ulint_pair(fold, BigEndian::read_u32(&data[i..]) as u64);
        }
        4 => {
            fold = ut_fold_ulint_pair(fold, BigEndian::read_u32(&data[i..]) as u64);
        }
        3 => {
            fold = ut_fold_ulint_pair(fold, data[i] as u64);
            i += 1;
            fold = ut_fold_ulint_pair(fold, data[i] as u64);
            i += 1;
            fold = ut_fold_ulint_pair(fold, data[i] as u64);
        }
        2 => {
            fold = ut_fold_ulint_pair(fold, data[i] as u64);
            i += 1;
            fold = ut_fold_ulint_pair(fold, data[i] as u64);
        }
        1 => {
            fold = ut_fold_ulint_pair(fold, data[i] as u64);
        }
        _ => {}
    }

    fold
}

/// Calculate the legacy InnoDB checksum (buf_calc_page_new_checksum).
///
/// This matches the MySQL source exactly:
/// 1. Fold bytes from FIL_PAGE_OFFSET (4) to FIL_PAGE_FILE_FLUSH_LSN (26)
/// 2. Fold bytes from FIL_PAGE_DATA (38) to page_size - 8
/// 3. Sum both results and mask to 32 bits
fn calculate_innodb_checksum(page_data: &[u8], page_size: usize) -> u32 {
    let end = page_size - SIZE_FIL_TRAILER;

    // Range 1: bytes 4..26 (FIL_PAGE_OFFSET to FIL_PAGE_FILE_FLUSH_LSN)
    let fold1 = ut_fold_binary(&page_data[FIL_PAGE_OFFSET..FIL_PAGE_FILE_FLUSH_LSN]);

    // Range 2: bytes 38..(page_size - 8) (FIL_PAGE_DATA to end of user data)
    let fold2 = ut_fold_binary(&page_data[FIL_PAGE_DATA..end]);

    // Mask to 32 bits (matching MySQL: checksum = checksum & 0xFFFFFFFF)
    fold1.wrapping_add(fold2) as u32
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
