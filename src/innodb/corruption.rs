//! Corruption pattern classification for InnoDB pages.
//!
//! Classifies the type of damage on corrupt pages by analyzing the byte
//! patterns in the data area. This helps DBAs understand the likely cause
//! of corruption (e.g., disk bitrot, torn writes, zero-fill from firmware).

use byteorder::{BigEndian, ByteOrder};
use serde::Serialize;

use crate::innodb::checksum::calculate_crc32c;
use crate::innodb::constants::*;

/// Classification of corruption damage patterns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CorruptionPattern {
    /// >90% of data area is zero bytes — firmware bug or partial write
    ZeroFill,
    /// Shannon entropy >7.5 bits/byte — random overwrite or uninitialized memory
    RandomNoise,
    /// Valid prefix then zeros/0xFF at sector boundary — interrupted I/O
    TornWrite,
    /// Only the FIL header area has invalid data — metadata corruption
    HeaderOnly,
    /// Small Hamming distance (<8 bits) between stored and calculated checksum — single-bit errors
    Bitrot,
    /// Does not match any known pattern
    Unknown,
}

impl CorruptionPattern {
    /// Human-readable name for the corruption pattern.
    pub fn name(self) -> &'static str {
        match self {
            CorruptionPattern::ZeroFill => "zero-fill",
            CorruptionPattern::RandomNoise => "random-noise",
            CorruptionPattern::TornWrite => "torn-write",
            CorruptionPattern::HeaderOnly => "header-only",
            CorruptionPattern::Bitrot => "bitrot",
            CorruptionPattern::Unknown => "unknown",
        }
    }

    /// Short description of the likely cause.
    pub fn description(self) -> &'static str {
        match self {
            CorruptionPattern::ZeroFill => {
                "Data area mostly zeros — possible firmware bug or partial write"
            }
            CorruptionPattern::RandomNoise => {
                "High entropy data — random overwrite or uninitialized memory"
            }
            CorruptionPattern::TornWrite => {
                "Valid prefix followed by zeros at sector boundary — interrupted I/O"
            }
            CorruptionPattern::HeaderOnly => {
                "Only FIL header damaged — metadata corruption, data likely intact"
            }
            CorruptionPattern::Bitrot => {
                "Small number of bit flips — silent data corruption (bitrot)"
            }
            CorruptionPattern::Unknown => "Corruption pattern does not match any known category",
        }
    }
}

/// Compute Shannon entropy of a byte slice in bits per byte.
fn shannon_entropy(data: &[u8]) -> f64 {
    if data.is_empty() {
        return 0.0;
    }
    let mut counts = [0u64; 256];
    for &b in data {
        counts[b as usize] += 1;
    }
    let len = data.len() as f64;
    counts
        .iter()
        .filter(|&&c| c > 0)
        .map(|&c| {
            let p = c as f64 / len;
            -p * p.log2()
        })
        .sum()
}

/// Count the number of differing bits between two u32 values.
fn hamming_distance(a: u32, b: u32) -> u32 {
    (a ^ b).count_ones()
}

/// Classify the corruption pattern of a page.
///
/// The page must already be known to have an invalid checksum. This function
/// examines the byte patterns to determine the likely cause of corruption.
///
/// Detection heuristics are applied in priority order:
/// 1. **ZeroFill** -- >90% of the data area is zero bytes
/// 2. **TornWrite** -- valid prefix then all zeros/0xFF at a 512-byte sector boundary
/// 3. **Bitrot** -- Hamming distance between stored and calculated checksum is <= 8
/// 4. **HeaderOnly** -- data area CRC is valid but stored header checksum is wrong
///    (with Hamming distance > 8, ruling out simple bitrot)
/// 5. **RandomNoise** -- Shannon entropy of data area exceeds 7.5 bits/byte
/// 6. **Unknown** -- fallback
pub fn classify_corruption(page_data: &[u8], page_size: u32) -> CorruptionPattern {
    let ps = page_size as usize;
    if page_data.len() < ps || ps <= FIL_PAGE_DATA + SIZE_FIL_TRAILER {
        return CorruptionPattern::Unknown;
    }

    let data_start = FIL_PAGE_DATA;
    let data_end = ps - SIZE_FIL_TRAILER;
    let data_area = &page_data[data_start..data_end];

    // 1. ZeroFill: >90% of data area is zeros
    let zero_count = data_area.iter().filter(|&&b| b == 0).count();
    if zero_count as f64 / data_area.len() as f64 > 0.9 {
        return CorruptionPattern::ZeroFill;
    }

    // 2. TornWrite: find a 512-byte sector boundary where valid data transitions
    //    to all zeros or all 0xFF
    let sector_size = 512;
    if ps >= sector_size * 2 {
        for sector_start in (sector_size..ps).step_by(sector_size) {
            let sector_end = (sector_start + sector_size).min(ps);
            let sector = &page_data[sector_start..sector_end];
            let all_zero = sector.iter().all(|&b| b == 0);
            let all_ff = sector.iter().all(|&b| b == 0xFF);
            if all_zero || all_ff {
                // Verify that earlier sectors have some non-trivial content
                let prefix = &page_data[..sector_start];
                let non_zero = prefix.iter().filter(|&&b| b != 0).count();
                if non_zero > 10 {
                    // Also verify that all remaining sectors are blank too
                    let tail = &page_data[sector_start..ps];
                    let tail_all_zero = tail.iter().all(|&b| b == 0);
                    let tail_all_ff = tail.iter().all(|&b| b == 0xFF);
                    if tail_all_zero || tail_all_ff {
                        return CorruptionPattern::TornWrite;
                    }
                }
            }
        }
    }

    let stored = BigEndian::read_u32(&page_data[FIL_PAGE_SPACE_OR_CHKSUM..]);
    let calculated = calculate_crc32c(page_data, ps);

    // 3. Bitrot: small Hamming distance between stored and calculated checksum.
    //    Checked before HeaderOnly so that single-bit flips in the checksum
    //    field are classified as bitrot rather than header-only corruption.
    if hamming_distance(stored, calculated) <= 8 {
        return CorruptionPattern::Bitrot;
    }

    // 4. HeaderOnly: the stored checksum is wrong, but the rest of the page
    //    is self-consistent. We verify this by checking that the trailer LSN
    //    matches the header LSN (indicating the data area and trailer are
    //    intact). We already ruled out small-distance bitrot above.
    if stored != calculated {
        let header_lsn = BigEndian::read_u64(&page_data[FIL_PAGE_LSN..]);
        let header_lsn_low32 = (header_lsn & 0xFFFFFFFF) as u32;
        let trailer_offset = ps - SIZE_FIL_TRAILER;
        let trailer_lsn_low32 = BigEndian::read_u32(&page_data[trailer_offset + 4..]);
        if header_lsn_low32 == trailer_lsn_low32 && header_lsn > 0 {
            return CorruptionPattern::HeaderOnly;
        }
    }

    // 5. RandomNoise: high Shannon entropy in data area
    if shannon_entropy(data_area) > 7.5 {
        return CorruptionPattern::RandomNoise;
    }

    // 6. Fallback
    CorruptionPattern::Unknown
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a page fully populated with non-zero data in the data area,
    /// with a correct CRC-32C checksum and matching trailer LSN.
    fn build_valid_page(page_size: u32) -> Vec<u8> {
        let ps = page_size as usize;
        let mut page = vec![0u8; ps];

        // FIL header
        BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], 1);
        BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
        BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
        BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], 5000);
        BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855); // INDEX
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], 1);

        // Fill the ENTIRE data area with non-zero, varied data so ZeroFill
        // never triggers on test pages that should match other patterns
        let data_end = ps - SIZE_FIL_TRAILER;
        for i in FIL_PAGE_DATA..data_end {
            page[i] = ((i.wrapping_mul(7).wrapping_add(13)) & 0xFF) as u8;
        }

        // Trailer LSN (low 32 bits of header LSN)
        let trailer = ps - SIZE_FIL_TRAILER;
        BigEndian::write_u32(&mut page[trailer + 4..], (5000u64 & 0xFFFFFFFF) as u32);

        // Compute and store correct CRC-32C checksum
        let crc = calculate_crc32c(&page, ps);
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_OR_CHKSUM..], crc);

        page
    }

    #[test]
    fn test_classify_zero_fill() {
        let ps = 16384u32;
        let mut page = vec![0u8; ps as usize];
        // Set a non-zero header so it's not treated as empty by the caller,
        // but leave the data area as all zeros
        BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], 1);
        BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], 100);
        BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855);
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_OR_CHKSUM..], 0xBAAD);

        let pattern = classify_corruption(&page, ps);
        assert_eq!(pattern, CorruptionPattern::ZeroFill);
    }

    #[test]
    fn test_classify_random_noise() {
        let ps = 16384u32;
        let mut page = vec![0u8; ps as usize];

        // Fill entire page with pseudo-random high-entropy data
        let mut state: u64 = 0xDEADBEEF_CAFEBABE;
        for byte in page.iter_mut() {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            *byte = (state >> 33) as u8;
        }

        // Ensure the stored checksum doesn't match any valid algorithm
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_OR_CHKSUM..], 0x12345678);

        // Ensure header LSN does NOT match trailer LSN, so HeaderOnly
        // doesn't trigger before RandomNoise
        BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], 0xAAAAAAAABBBBBBBB);
        let trailer = ps as usize - SIZE_FIL_TRAILER;
        BigEndian::write_u32(&mut page[trailer + 4..], 0xCCCCCCCC);

        let pattern = classify_corruption(&page, ps);
        assert_eq!(pattern, CorruptionPattern::RandomNoise);
    }

    #[test]
    fn test_classify_torn_write() {
        let ps = 16384u32;
        let mut page = build_valid_page(ps);

        // Simulate a torn write: keep the first ~75% of the page, zero out
        // the rest at a 512-byte sector boundary. Using a late break point
        // ensures the data area is NOT >90% zeros (which would trigger
        // ZeroFill instead).
        let break_point = 12288; // 24 sectors = 75% of 16384
        for byte in page[break_point..].iter_mut() {
            *byte = 0;
        }

        // The checksum is now wrong (data changed), and the page has a valid
        // prefix followed by zeros at a sector boundary
        let pattern = classify_corruption(&page, ps);
        assert_eq!(pattern, CorruptionPattern::TornWrite);
    }

    #[test]
    fn test_classify_header_only() {
        let ps = 16384u32;
        let mut page = build_valid_page(ps);

        // Corrupt only the stored checksum in the header with a value that
        // has >8 bit Hamming distance from the correct one (so bitrot doesn't
        // trigger first)
        let correct_crc = BigEndian::read_u32(&page[FIL_PAGE_SPACE_OR_CHKSUM..]);
        // XOR with a value that flips many bits
        BigEndian::write_u32(
            &mut page[FIL_PAGE_SPACE_OR_CHKSUM..],
            correct_crc ^ 0xFF00FF00,
        );

        // Verify precondition: Hamming distance > 8
        let stored = BigEndian::read_u32(&page[FIL_PAGE_SPACE_OR_CHKSUM..]);
        let calculated = calculate_crc32c(&page, ps as usize);
        assert!(
            hamming_distance(stored, calculated) > 8,
            "Test setup: Hamming distance must be >8 to avoid bitrot classification"
        );

        let pattern = classify_corruption(&page, ps);
        assert_eq!(pattern, CorruptionPattern::HeaderOnly);
    }

    #[test]
    fn test_classify_bitrot() {
        let ps = 16384u32;
        let mut page = build_valid_page(ps);

        // Flip exactly 1 bit in the stored checksum — Hamming distance = 1
        let crc = BigEndian::read_u32(&page[FIL_PAGE_SPACE_OR_CHKSUM..]);
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_OR_CHKSUM..], crc ^ 0x01);

        let pattern = classify_corruption(&page, ps);
        assert_eq!(pattern, CorruptionPattern::Bitrot);
    }

    #[test]
    fn test_classify_bitrot_few_bits() {
        let ps = 16384u32;
        let mut page = build_valid_page(ps);

        // Flip 4 bits in the stored checksum — still within bitrot threshold (<=8)
        let crc = BigEndian::read_u32(&page[FIL_PAGE_SPACE_OR_CHKSUM..]);
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_OR_CHKSUM..], crc ^ 0x0F);

        let stored = BigEndian::read_u32(&page[FIL_PAGE_SPACE_OR_CHKSUM..]);
        let calculated = calculate_crc32c(&page, ps as usize);
        assert!(hamming_distance(stored, calculated) <= 8);

        let pattern = classify_corruption(&page, ps);
        assert_eq!(pattern, CorruptionPattern::Bitrot);
    }

    #[test]
    fn test_classify_unknown() {
        let ps = 16384u32;
        // Fill with a uniform byte value — low entropy, not zeros, not random
        let mut page = vec![0x42u8; ps as usize];
        // Set a checksum with large Hamming distance from calculated
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_OR_CHKSUM..], 0xBAADF00D);
        BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], 1);
        BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], 100);
        BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855);

        // The data area is all 0x42 — only 1 unique byte, so HeaderOnly won't
        // trigger (requires >4 unique bytes). Entropy is 0, so RandomNoise
        // won't trigger. Not mostly zeros, not torn write.
        let pattern = classify_corruption(&page, ps);
        assert_eq!(pattern, CorruptionPattern::Unknown);
    }

    #[test]
    fn test_pattern_name() {
        assert_eq!(CorruptionPattern::ZeroFill.name(), "zero-fill");
        assert_eq!(CorruptionPattern::RandomNoise.name(), "random-noise");
        assert_eq!(CorruptionPattern::TornWrite.name(), "torn-write");
        assert_eq!(CorruptionPattern::HeaderOnly.name(), "header-only");
        assert_eq!(CorruptionPattern::Bitrot.name(), "bitrot");
        assert_eq!(CorruptionPattern::Unknown.name(), "unknown");
    }

    #[test]
    fn test_pattern_description() {
        assert!(CorruptionPattern::ZeroFill.description().contains("zeros"));
        assert!(CorruptionPattern::RandomNoise
            .description()
            .contains("entropy"));
        assert!(CorruptionPattern::TornWrite
            .description()
            .contains("interrupted"));
        assert!(CorruptionPattern::HeaderOnly
            .description()
            .contains("header"));
        assert!(CorruptionPattern::Bitrot
            .description()
            .contains("bit flips"));
        assert!(CorruptionPattern::Unknown
            .description()
            .contains("does not match"));
    }

    #[test]
    fn test_shannon_entropy_uniform() {
        // All same byte — entropy should be 0
        let data = vec![0xAA; 1000];
        assert_eq!(shannon_entropy(&data), 0.0);
    }

    #[test]
    fn test_shannon_entropy_empty() {
        assert_eq!(shannon_entropy(&[]), 0.0);
    }

    #[test]
    fn test_shannon_entropy_two_values() {
        // Exactly 50/50 split between two values — entropy should be 1.0
        let mut data = vec![0u8; 1000];
        for byte in data[..500].iter_mut() {
            *byte = 1;
        }
        let entropy = shannon_entropy(&data);
        assert!((entropy - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_hamming_distance_identical() {
        assert_eq!(hamming_distance(0x12345678, 0x12345678), 0);
    }

    #[test]
    fn test_hamming_distance_one_bit() {
        assert_eq!(hamming_distance(0x00000000, 0x00000001), 1);
    }

    #[test]
    fn test_hamming_distance_all_bits() {
        assert_eq!(hamming_distance(0x00000000, 0xFFFFFFFF), 32);
    }

    #[test]
    fn test_short_page_returns_unknown() {
        let page = vec![0xFF; 10];
        assert_eq!(
            classify_corruption(&page, 16384),
            CorruptionPattern::Unknown
        );
    }
}
