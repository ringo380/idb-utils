//! Binlog event CRC-32 checksum validation.
//!
//! MySQL 5.6.6+ appends a 4-byte CRC-32C checksum to every binlog event when
//! `binlog_checksum = CRC32` (the default). The checksum covers all event
//! bytes from the common header through the end of the payload, excluding the
//! 4-byte checksum itself.
//!
//! The checksum value is stored as a **little-endian** u32 in the last 4 bytes
//! of the event.

use byteorder::{ByteOrder, LittleEndian};

use super::constants::BINLOG_CHECKSUM_LEN;

/// Validate the CRC-32C checksum of a complete binlog event.
///
/// `event_data` must include the full event bytes (common header + payload +
/// 4-byte checksum). Returns `true` if the computed CRC-32C over all bytes
/// except the last 4 matches the stored checksum.
///
/// # Examples
///
/// ```
/// use idb::binlog::checksum::validate_event_checksum;
/// use byteorder::{LittleEndian, ByteOrder};
///
/// // Build a fake 23-byte event (19 header + 0 payload + 4 checksum)
/// let mut event = vec![0u8; 23];
/// event[4] = 3; // STOP_EVENT
/// byteorder::LittleEndian::write_u32(&mut event[9..], 23); // event_length
///
/// // Compute and append the correct CRC-32C
/// let crc = crc32c::crc32c(&event[..19]);
/// byteorder::LittleEndian::write_u32(&mut event[19..], crc);
///
/// assert!(validate_event_checksum(&event));
/// ```
pub fn validate_event_checksum(event_data: &[u8]) -> bool {
    if event_data.len() < BINLOG_CHECKSUM_LEN {
        return false;
    }

    let data_end = event_data.len() - BINLOG_CHECKSUM_LEN;
    let computed = crc32c::crc32c(&event_data[..data_end]);
    let stored = LittleEndian::read_u32(&event_data[data_end..]);

    computed == stored
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_checksum() {
        let mut data = vec![0xABu8; 50];
        let crc = crc32c::crc32c(&data[..46]);
        LittleEndian::write_u32(&mut data[46..], crc);
        assert!(validate_event_checksum(&data));
    }

    #[test]
    fn invalid_checksum() {
        let mut data = vec![0xABu8; 50];
        LittleEndian::write_u32(&mut data[46..], 0xDEADBEEF);
        assert!(!validate_event_checksum(&data));
    }

    #[test]
    fn too_short() {
        let data = vec![0u8; 3];
        assert!(!validate_event_checksum(&data));
    }

    #[test]
    fn exact_checksum_size() {
        // 4 bytes = just a checksum of zero-length data
        let mut data = vec![0u8; 4];
        let crc = crc32c::crc32c(&[]);
        LittleEndian::write_u32(&mut data[0..], crc);
        assert!(validate_event_checksum(&data));
    }
}
