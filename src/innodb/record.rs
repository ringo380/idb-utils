//! Row-level record parsing for InnoDB compact format.
//!
//! InnoDB stores rows in compact record format (MySQL 5.0+), where each record
//! has a 5-byte header containing the info bits, record type, heap number, and
//! next-record pointer. This module provides [`RecordType`] classification and
//! [`walk_compact_records`] to traverse the singly-linked record chain within
//! an INDEX page, starting from the infimum record.

use byteorder::{BigEndian, ByteOrder};

use crate::innodb::constants::*;

/// Record type extracted from the info bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    /// Ordinary user record (leaf page).
    Ordinary,
    /// Node pointer record (non-leaf page).
    NodePtr,
    /// Infimum system record.
    Infimum,
    /// Supremum system record.
    Supremum,
}

impl RecordType {
    /// Convert a 3-bit status value from the record header to a `RecordType`.
    ///
    /// Only the lowest 3 bits of `val` are used.
    ///
    /// # Examples
    ///
    /// ```
    /// use idb::innodb::record::RecordType;
    ///
    /// assert_eq!(RecordType::from_u8(0), RecordType::Ordinary);
    /// assert_eq!(RecordType::from_u8(1), RecordType::NodePtr);
    /// assert_eq!(RecordType::from_u8(2), RecordType::Infimum);
    /// assert_eq!(RecordType::from_u8(3), RecordType::Supremum);
    ///
    /// // Only the lowest 3 bits are used, so 0x08 maps to Ordinary
    /// assert_eq!(RecordType::from_u8(0x08), RecordType::Ordinary);
    ///
    /// assert_eq!(RecordType::from_u8(0).name(), "REC_STATUS_ORDINARY");
    /// ```
    pub fn from_u8(val: u8) -> Self {
        match val & 0x07 {
            0 => RecordType::Ordinary,
            1 => RecordType::NodePtr,
            2 => RecordType::Infimum,
            3 => RecordType::Supremum,
            _ => RecordType::Ordinary,
        }
    }

    /// Returns the MySQL source-style name for this record type (e.g. `"REC_STATUS_ORDINARY"`).
    pub fn name(&self) -> &'static str {
        match self {
            RecordType::Ordinary => "REC_STATUS_ORDINARY",
            RecordType::NodePtr => "REC_STATUS_NODE_PTR",
            RecordType::Infimum => "REC_STATUS_INFIMUM",
            RecordType::Supremum => "REC_STATUS_SUPREMUM",
        }
    }
}

/// Parsed compact (new-style) record header.
///
/// In compact format, 5 bytes precede each record:
/// - Byte 0: info bits (delete mark, min_rec flag) + n_owned upper nibble
/// - Bytes 1-2: heap_no (13 bits) + rec_type (3 bits)
/// - Bytes 3-4: next record offset (signed, relative)
#[derive(Debug, Clone)]
pub struct CompactRecordHeader {
    /// Number of records owned by this record in the page directory.
    pub n_owned: u8,
    /// Delete mark flag.
    pub delete_mark: bool,
    /// Min-rec flag (leftmost record on a non-leaf level).
    pub min_rec: bool,
    /// Record's position in the heap.
    pub heap_no: u16,
    /// Record type.
    pub rec_type: RecordType,
    /// Relative offset to the next record (signed).
    pub next_offset: i16,
}

impl CompactRecordHeader {
    /// Parse a compact record header from the 5 bytes preceding the record origin.
    ///
    /// `data` should point to the start of the 5-byte extra header.
    ///
    /// # Examples
    ///
    /// ```
    /// use idb::innodb::record::{CompactRecordHeader, RecordType};
    /// use byteorder::{BigEndian, ByteOrder};
    ///
    /// let mut data = vec![0u8; 5];
    /// // byte 0: info_bits(4) | n_owned(4)
    /// //   delete_mark=1 (bit 5), n_owned=2 (bits 0-3) => 0x22
    /// data[0] = 0x22;
    /// // bytes 1-2: heap_no=7 (7<<3=56), rec_type=0 (Ordinary) => 56
    /// BigEndian::write_u16(&mut data[1..3], 7 << 3);
    /// // bytes 3-4: next_offset = 42
    /// BigEndian::write_i16(&mut data[3..5], 42);
    ///
    /// let hdr = CompactRecordHeader::parse(&data).unwrap();
    /// assert_eq!(hdr.n_owned, 2);
    /// assert!(hdr.delete_mark);
    /// assert!(!hdr.min_rec);
    /// assert_eq!(hdr.heap_no, 7);
    /// assert_eq!(hdr.rec_type, RecordType::Ordinary);
    /// assert_eq!(hdr.next_offset, 42);
    /// ```
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < REC_N_NEW_EXTRA_BYTES {
            return None;
        }

        // Byte 0 layout: [info_bits(4) | n_owned(4)]
        // Info bits (upper nibble): bit 5 = delete_mark, bit 4 = min_rec
        // n_owned (lower nibble): bits 0-3
        let byte0 = data[0];
        let n_owned = byte0 & 0x0F;
        let delete_mark = (byte0 & 0x20) != 0;
        let min_rec = (byte0 & 0x10) != 0;

        let two_bytes = BigEndian::read_u16(&data[1..3]);
        let rec_type = RecordType::from_u8((two_bytes & 0x07) as u8);
        let heap_no = (two_bytes >> 3) & 0x1FFF;

        let next_offset = BigEndian::read_i16(&data[3..5]);

        Some(CompactRecordHeader {
            n_owned,
            delete_mark,
            min_rec,
            heap_no,
            rec_type,
            next_offset,
        })
    }
}

/// A record position on a page, with its parsed header.
#[derive(Debug, Clone)]
pub struct RecordInfo {
    /// Absolute offset of the record origin within the page.
    pub offset: usize,
    /// Parsed record header.
    pub header: CompactRecordHeader,
}

/// Walk all user records on a compact-format INDEX page.
///
/// Starts from infimum and follows next-record offsets until reaching supremum.
/// Returns a list of record positions (excluding infimum/supremum).
///
/// # Examples
///
/// ```no_run
/// use idb::innodb::record::walk_compact_records;
/// use idb::innodb::tablespace::Tablespace;
///
/// let mut ts = Tablespace::open("table.ibd").unwrap();
/// let page = ts.read_page(3).unwrap();
/// let records = walk_compact_records(&page);
/// for rec in &records {
///     println!("Record at offset {}, type: {}", rec.offset, rec.header.rec_type.name());
/// }
/// ```
pub fn walk_compact_records(page_data: &[u8]) -> Vec<RecordInfo> {
    let mut records = Vec::new();

    // Infimum record origin is at PAGE_NEW_INFIMUM (99)
    let infimum_origin = PAGE_NEW_INFIMUM;
    if page_data.len() < infimum_origin + 2 {
        return records;
    }

    // Read infimum's next-record offset (at infimum_origin - 2, relative to origin)
    let infimum_extra_start = infimum_origin - REC_N_NEW_EXTRA_BYTES;
    if page_data.len() < infimum_extra_start + REC_N_NEW_EXTRA_BYTES {
        return records;
    }

    let infimum_hdr = match CompactRecordHeader::parse(&page_data[infimum_extra_start..]) {
        Some(h) => h,
        None => return records,
    };

    // Follow the linked list
    let mut current_offset = infimum_origin;
    let mut next_rel = infimum_hdr.next_offset;

    // Safety: limit iterations to prevent infinite loops
    let max_iter = page_data.len();
    let mut iterations = 0;

    loop {
        if iterations > max_iter {
            break;
        }
        iterations += 1;

        // Calculate next record's absolute offset
        let next_abs = (current_offset as i32 + next_rel as i32) as usize;
        if next_abs < REC_N_NEW_EXTRA_BYTES || next_abs >= page_data.len() {
            break;
        }

        // Parse the record header (5 bytes before the origin)
        let extra_start = next_abs - REC_N_NEW_EXTRA_BYTES;
        if extra_start + REC_N_NEW_EXTRA_BYTES > page_data.len() {
            break;
        }

        let hdr = match CompactRecordHeader::parse(&page_data[extra_start..]) {
            Some(h) => h,
            None => break,
        };

        // If we've reached supremum, stop
        if hdr.rec_type == RecordType::Supremum {
            break;
        }

        next_rel = hdr.next_offset;
        records.push(RecordInfo {
            offset: next_abs,
            header: hdr,
        });
        current_offset = next_abs;

        // next_offset of 0 means end of list
        if next_rel == 0 {
            break;
        }
    }

    records
}

/// Parse the variable-length field lengths from a compact record's null bitmap
/// and variable-length header. Returns the field data starting offset.
///
/// For SDI records and other known-format records, callers can use the
/// record offset directly since field positions are fixed.
pub fn read_variable_field_lengths(
    page_data: &[u8],
    record_origin: usize,
    n_nullable: usize,
    n_variable: usize,
) -> Option<(Vec<bool>, Vec<usize>)> {
    // The variable-length header grows backwards from the record origin,
    // before the 5-byte compact extra header.
    // Layout (backwards from origin - 5):
    //   - null bitmap: ceil(n_nullable / 8) bytes
    //   - variable-length field lengths: 1 or 2 bytes each

    let null_bitmap_bytes = n_nullable.div_ceil(8);
    let mut pos = record_origin - REC_N_NEW_EXTRA_BYTES;

    // Read null bitmap
    if pos < null_bitmap_bytes {
        return None;
    }
    pos -= null_bitmap_bytes;
    let mut nulls = Vec::with_capacity(n_nullable);
    for i in 0..n_nullable {
        let byte_idx = pos + (i / 8);
        let bit_idx = i % 8;
        if byte_idx >= page_data.len() {
            return None;
        }
        nulls.push((page_data[byte_idx] & (1 << bit_idx)) != 0);
    }

    // Read variable-length field lengths
    let mut var_lengths = Vec::with_capacity(n_variable);
    for _ in 0..n_variable {
        if pos == 0 {
            return None;
        }
        pos -= 1;
        if pos >= page_data.len() {
            return None;
        }
        let len_byte = page_data[pos] as usize;
        if len_byte & 0x80 != 0 {
            // 2-byte length
            if pos == 0 {
                return None;
            }
            pos -= 1;
            if pos >= page_data.len() {
                return None;
            }
            let high_byte = page_data[pos] as usize;
            let total_len = ((len_byte & 0x3F) << 8) | high_byte;
            var_lengths.push(total_len);
        } else {
            var_lengths.push(len_byte);
        }
    }

    Some((nulls, var_lengths))
}

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::ByteOrder;

    #[test]
    fn test_record_type_from_u8() {
        assert_eq!(RecordType::from_u8(0), RecordType::Ordinary);
        assert_eq!(RecordType::from_u8(1), RecordType::NodePtr);
        assert_eq!(RecordType::from_u8(2), RecordType::Infimum);
        assert_eq!(RecordType::from_u8(3), RecordType::Supremum);
    }

    #[test]
    fn test_compact_record_header_parse() {
        // Build a 5-byte compact header:
        // byte0: [info_bits(4) | n_owned(4)]
        //   n_owned=1 in lower nibble, no info bits => 0x01
        // bytes 1-2: heap_no=5 (5<<3=0x0028), rec_type=0 => 0x0028
        // bytes 3-4: next_offset = 30 => 0x001E
        let mut data = vec![0u8; 5];
        data[0] = 0x01; // n_owned=1, no delete, no min_rec
        BigEndian::write_u16(&mut data[1..3], 5 << 3); // heap_no=5, type=0
        BigEndian::write_i16(&mut data[3..5], 30); // next=30

        let hdr = CompactRecordHeader::parse(&data).unwrap();
        assert_eq!(hdr.n_owned, 1);
        assert!(!hdr.delete_mark);
        assert!(!hdr.min_rec);
        assert_eq!(hdr.heap_no, 5);
        assert_eq!(hdr.rec_type, RecordType::Ordinary);
        assert_eq!(hdr.next_offset, 30);
    }

    #[test]
    fn test_compact_record_header_with_flags() {
        let mut data = vec![0u8; 5];
        // n_owned=3 (0x30), delete_mark (0x20), min_rec (0x10)
        // => 0x30 | 0x20 | 0x10 = 0x70... wait, n_owned is bits 4-7 so n_owned=3 is 0x30
        // delete_mark is bit 5 (0x20), min_rec is bit 4 (0x10)
        // But if n_owned=3 takes bits 4-7, that's 0x30, which conflicts with bit 5 for delete.
        // Actually in InnoDB: byte0 has info_bits in upper 4 bits and... let me recheck.
        // The layout is: [info_bits(4) | n_owned(4)]
        // info_bits: bit 7=unused, bit 6=unused, bit 5=delete_mark, bit 4=min_rec
        // n_owned: bits 0-3
        // So: delete_mark=1, min_rec=0, n_owned=2 => 0x20 | 0x02 = 0x22
        data[0] = 0x22; // delete_mark=1, n_owned=2
        BigEndian::write_u16(&mut data[1..3], (10 << 3) | 1); // heap_no=10, type=node_ptr
        BigEndian::write_i16(&mut data[3..5], -50); // negative offset

        let hdr = CompactRecordHeader::parse(&data).unwrap();
        assert_eq!(hdr.n_owned, 2);
        assert!(hdr.delete_mark);
        assert!(!hdr.min_rec);
        assert_eq!(hdr.heap_no, 10);
        assert_eq!(hdr.rec_type, RecordType::NodePtr);
        assert_eq!(hdr.next_offset, -50);
    }
}
