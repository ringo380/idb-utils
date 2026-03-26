//! B+Tree search for InnoDB clustered indexes.
//!
//! Provides functions to traverse the B+Tree of a clustered index to locate
//! the leaf page containing a given primary key value. This is used by
//! timeline correlation to map binlog row events back to specific tablespace
//! pages.
//!
//! # Algorithm
//!
//! Starting from the root page (obtained from SDI metadata), the search
//! descends through non-leaf (node pointer) pages by comparing the search
//! key against record keys on each level. At each non-leaf page, the child
//! pointer of the last record whose key is <= the search key is followed.
//! The traversal terminates when a leaf page (level 0) is reached.

use byteorder::{BigEndian, ByteOrder};
use serde::Serialize;

use crate::innodb::export::extract_column_layout;
use crate::innodb::field_decode::{decode_field, ColumnStorageInfo, FieldValue};
use crate::innodb::index::IndexHeader;
use crate::innodb::page::FilHeader;
use crate::innodb::page_types::PageType;
use crate::innodb::record::{read_variable_field_lengths, walk_compact_records, RecordType};
use crate::innodb::sdi;
use crate::innodb::tablespace::Tablespace;
use crate::IdbError;

/// Maximum B+Tree depth to prevent infinite loops on corrupt data.
const MAX_BTREE_DEPTH: u16 = 10;

/// Primary key value used for B+Tree search comparisons.
///
/// This mirrors `BinlogPkValue` from the binlog row image module. When that
/// module is available, callers can convert between the two types.
#[derive(Debug, Clone, PartialEq)]
pub enum PkValue {
    /// Signed integer (TINYINT through BIGINT).
    Int(i64),
    /// Unsigned integer.
    Uint(u64),
    /// String value (VARCHAR, CHAR, etc.).
    Str(String),
    /// Raw bytes (BINARY, VARBINARY, etc.).
    Bytes(Vec<u8>),
}

/// Result of a B+Tree search.
#[derive(Debug, Clone, Serialize)]
pub struct BtreeSearchResult {
    /// The leaf page number containing (or nearest to) the search key.
    pub leaf_page_no: u32,
    /// Number of B+Tree levels traversed.
    pub levels_traversed: u16,
}

/// Extract clustered index info from a tablespace's SDI metadata.
///
/// Returns `(root_page_no, index_id, pk_columns)` where `pk_columns` contains
/// only the primary key columns (no system or non-PK columns).
///
/// Returns `None` if SDI metadata is unavailable or the clustered index
/// cannot be found.
pub fn extract_clustered_index_info(
    ts: &mut Tablespace,
) -> Option<(u32, u64, Vec<ColumnStorageInfo>)> {
    // Get full column layout and clustered index ID
    let (all_columns, index_id) = extract_column_layout(ts)?;

    // Extract only PK columns (before the first system column)
    let pk_columns: Vec<ColumnStorageInfo> = all_columns
        .into_iter()
        .take_while(|c| !c.is_system_column)
        .collect();

    if pk_columns.is_empty() {
        return None;
    }

    // Find root page from SDI se_private_data
    let root_page_no = extract_root_page(ts)?;

    Some((root_page_no, index_id, pk_columns))
}

/// Extract the root page number of the clustered (PRIMARY) index from SDI.
fn extract_root_page(ts: &mut Tablespace) -> Option<u32> {
    let sdi_pages = sdi::find_sdi_pages(ts).ok()?;
    if sdi_pages.is_empty() {
        return None;
    }
    let records = sdi::extract_sdi_from_pages(ts, &sdi_pages).ok()?;

    for rec in &records {
        if rec.sdi_type == 1 {
            let raw: serde_json::Value = serde_json::from_str(&rec.data).ok()?;
            let indexes = raw.get("dd_object")?.get("indexes")?.as_array()?;

            // Look for PRIMARY index (type == 1)
            for idx in indexes {
                let idx_type = idx.get("type")?.as_u64()?;
                if idx_type == 1 {
                    let se_data = idx.get("se_private_data")?.as_str()?;
                    for part in se_data.split(';') {
                        if let Some(root_str) = part.strip_prefix("root=") {
                            if let Ok(root) = root_str.parse::<u32>() {
                                return Some(root);
                            }
                        }
                    }
                }
            }

            // Fallback: any index with root= in se_private_data
            for idx in indexes {
                let se_data = match idx.get("se_private_data").and_then(|v| v.as_str()) {
                    Some(s) => s,
                    None => continue,
                };
                for part in se_data.split(';') {
                    if let Some(root_str) = part.strip_prefix("root=") {
                        if let Ok(root) = root_str.parse::<u32>() {
                            return Some(root);
                        }
                    }
                }
            }
        }
    }

    None
}

/// Search a B+Tree to find the leaf page containing a given primary key.
///
/// Traverses from the root page down through non-leaf levels, following
/// node pointers based on key comparisons. Returns the leaf page number
/// and the number of levels traversed.
///
/// # Errors
///
/// Returns `IdbError::Parse` if a page cannot be read or has an unexpected
/// structure (wrong page type, wrong index ID, etc.).
pub fn search_btree(
    ts: &mut Tablespace,
    root_page_no: u32,
    index_id: u64,
    pk_columns: &[ColumnStorageInfo],
    search_key: &[PkValue],
    page_size: u32,
) -> Result<BtreeSearchResult, IdbError> {
    if search_key.is_empty() || pk_columns.is_empty() {
        return Err(IdbError::Argument(
            "search key and PK columns must not be empty".to_string(),
        ));
    }

    let mut current_page = root_page_no;
    let mut levels: u16 = 0;

    loop {
        if levels > MAX_BTREE_DEPTH {
            return Err(IdbError::Parse(format!(
                "B+Tree traversal exceeded maximum depth of {} levels",
                MAX_BTREE_DEPTH
            )));
        }

        let page_data = ts.read_page(current_page as u64)?;

        // Verify this is an INDEX page
        let fil_header = FilHeader::parse(&page_data).ok_or_else(|| {
            IdbError::Parse(format!(
                "failed to parse FIL header on page {}",
                current_page
            ))
        })?;
        if fil_header.page_type != PageType::Index {
            return Err(IdbError::Parse(format!(
                "page {} is {:?}, expected INDEX",
                current_page, fil_header.page_type
            )));
        }

        let idx_header = IndexHeader::parse(&page_data).ok_or_else(|| {
            IdbError::Parse(format!(
                "failed to parse INDEX header on page {}",
                current_page
            ))
        })?;

        // Verify index ID (skip on root since it's always correct by definition)
        if levels > 0 && idx_header.index_id != index_id {
            return Err(IdbError::Parse(format!(
                "index ID mismatch on page {}: expected {}, got {}",
                current_page, index_id, idx_header.index_id
            )));
        }

        // If this is a leaf page, we've found the target
        if idx_header.level == 0 {
            return Ok(BtreeSearchResult {
                leaf_page_no: current_page,
                levels_traversed: levels,
            });
        }

        // Non-leaf page: find the child pointer to follow
        let records = walk_compact_records(&page_data);

        // Default to the leftmost child (from the min_rec record)
        let mut child_page: Option<u32> = None;

        for rec in &records {
            let rec_type = rec.header.rec_type();
            if rec_type != RecordType::NodePtr {
                continue;
            }

            // Extract child page for this record
            let this_child =
                match extract_child_page_no(&page_data, rec.offset, pk_columns, page_size as usize)
                {
                    Some(cp) => cp,
                    None => continue,
                };

            // The min_rec record is the leftmost; always a valid default
            if rec.header.min_rec() {
                child_page = Some(this_child);
                continue;
            }

            // Compare this record's key with the search key
            let cmp = compare_record_key(&page_data, rec.offset, pk_columns, search_key);

            match cmp {
                std::cmp::Ordering::Less | std::cmp::Ordering::Equal => {
                    // Record key <= search key: this child could contain our key
                    child_page = Some(this_child);
                }
                std::cmp::Ordering::Greater => {
                    // Record key > search key: the previous child is correct
                    break;
                }
            }
        }

        let next_page = child_page.ok_or_else(|| {
            IdbError::Parse(format!(
                "no valid node pointer found on non-leaf page {}",
                current_page
            ))
        })?;

        current_page = next_page;
        levels += 1;
    }
}

/// Compare the primary key stored in a record against a search key.
///
/// Decodes PK column values from the record at the given offset and
/// compares them column-by-column with the search key values.
fn compare_record_key(
    page_data: &[u8],
    record_offset: usize,
    pk_columns: &[ColumnStorageInfo],
    search_key: &[PkValue],
) -> std::cmp::Ordering {
    // Count nullable and variable-length columns among PK columns
    let n_nullable = pk_columns.iter().filter(|c| c.is_nullable).count();
    let n_variable = pk_columns.iter().filter(|c| c.is_variable).count();

    // Read variable-length field headers and null bitmap
    let (nulls, var_lengths) =
        match read_variable_field_lengths(page_data, record_offset, n_nullable, n_variable) {
            Some(v) => v,
            None => return std::cmp::Ordering::Equal, // Can't decode; treat as equal
        };

    // Decode fields starting at record_offset
    let mut pos = record_offset;
    let mut null_idx = 0;
    let mut var_idx = 0;

    for (col_idx, col) in pk_columns.iter().enumerate() {
        if col_idx >= search_key.len() {
            break;
        }

        // Check null bitmap
        if col.is_nullable {
            if null_idx < nulls.len() && nulls[null_idx] {
                // NULL in record — NULL sorts as less than any value
                return std::cmp::Ordering::Less;
            }
            null_idx += 1;
        }

        // Determine field length
        let field_len = if col.is_variable {
            if var_idx < var_lengths.len() {
                let len = var_lengths[var_idx];
                var_idx += 1;
                len
            } else {
                return std::cmp::Ordering::Equal;
            }
        } else {
            col.fixed_len
        };

        if pos + field_len > page_data.len() {
            return std::cmp::Ordering::Equal;
        }

        let field_data = &page_data[pos..pos + field_len];
        let field_value = decode_field(field_data, col);
        pos += field_len;

        let cmp = compare_field_value(&field_value, &search_key[col_idx]);
        if cmp != std::cmp::Ordering::Equal {
            return cmp;
        }
    }

    std::cmp::Ordering::Equal
}

/// Compare a decoded `FieldValue` against a `PkValue`.
fn compare_field_value(field: &FieldValue, search: &PkValue) -> std::cmp::Ordering {
    match (field, search) {
        (FieldValue::Int(a), PkValue::Int(b)) => a.cmp(b),
        (FieldValue::Uint(a), PkValue::Uint(b)) => a.cmp(b),
        // Cross-type integer comparisons
        (FieldValue::Int(a), PkValue::Uint(b)) => {
            if *a < 0 {
                std::cmp::Ordering::Less
            } else {
                (*a as u64).cmp(b)
            }
        }
        (FieldValue::Uint(a), PkValue::Int(b)) => {
            if *b < 0 {
                std::cmp::Ordering::Greater
            } else {
                a.cmp(&(*b as u64))
            }
        }
        (FieldValue::Str(a), PkValue::Str(b)) => a.as_str().cmp(b.as_str()),
        (FieldValue::Hex(a), PkValue::Bytes(b)) => {
            // Hex-encoded field vs raw bytes
            let hex_str: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
            a.cmp(&hex_str)
        }
        // For incompatible types, treat as equal (best effort)
        _ => std::cmp::Ordering::Equal,
    }
}

/// Extract the child page number from a node pointer record.
///
/// On non-leaf pages, each record stores PK column values followed by a
/// 4-byte big-endian child page number. This function calculates where
/// the PK data ends and reads the child pointer.
fn extract_child_page_no(
    page_data: &[u8],
    record_offset: usize,
    pk_columns: &[ColumnStorageInfo],
    _page_size: usize,
) -> Option<u32> {
    // Count nullable and variable-length columns among PK columns
    let n_nullable = pk_columns.iter().filter(|c| c.is_nullable).count();
    let n_variable = pk_columns.iter().filter(|c| c.is_variable).count();

    // Read variable-length field headers and null bitmap
    let (nulls, var_lengths) =
        read_variable_field_lengths(page_data, record_offset, n_nullable, n_variable)?;

    // Calculate total PK data length
    let mut pk_data_len: usize = 0;
    let mut null_idx = 0;
    let mut var_idx = 0;

    for col in pk_columns {
        if col.is_nullable {
            if null_idx < nulls.len() && nulls[null_idx] {
                // NULL column takes zero bytes
                null_idx += 1;
                continue;
            }
            null_idx += 1;
        }

        if col.is_variable {
            if var_idx < var_lengths.len() {
                pk_data_len += var_lengths[var_idx];
                var_idx += 1;
            }
        } else {
            pk_data_len += col.fixed_len;
        }
    }

    // Child page number is the 4 bytes immediately after PK data
    let child_offset = record_offset + pk_data_len;
    if child_offset + 4 > page_data.len() {
        return None;
    }

    Some(BigEndian::read_u32(
        &page_data[child_offset..child_offset + 4],
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::innodb::constants::*;
    use byteorder::ByteOrder;

    /// Build a minimal ColumnStorageInfo for an INT column.
    fn int_column(name: &str, unsigned: bool) -> ColumnStorageInfo {
        ColumnStorageInfo {
            name: name.to_string(),
            dd_type: 4, // DD_TYPE_LONG
            column_type: if unsigned {
                "int unsigned".to_string()
            } else {
                "int".to_string()
            },
            is_nullable: false,
            is_unsigned: unsigned,
            fixed_len: 4,
            is_variable: false,
            charset_max_bytes: 0,
            datetime_precision: 0,
            is_system_column: false,
            elements: Vec::new(),
            numeric_precision: 10,
            numeric_scale: 0,
        }
    }

    /// Build a synthetic INDEX page with given level and records.
    ///
    /// Creates a valid compact-format INDEX page with a FIL header, INDEX header,
    /// infimum/supremum system records, and user records linked in order.
    fn build_index_page(
        page_size: usize,
        page_number: u32,
        index_id: u64,
        level: u16,
        records: &[Vec<u8>], // Each Vec<u8> is the full record data (PK + optional child ptr)
        is_node_ptr: bool,
    ) -> Vec<u8> {
        let mut page = vec![0u8; page_size];

        // FIL header
        BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], page_number);
        BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855); // FIL_PAGE_INDEX

        // INDEX header at FIL_PAGE_DATA (byte 38)
        let base = FIL_PAGE_DATA;
        BigEndian::write_u16(
            &mut page[base + PAGE_N_HEAP..],
            0x8000 | (records.len() as u16 + 2),
        ); // compact flag + count
        BigEndian::write_u16(&mut page[base + PAGE_N_RECS..], records.len() as u16);
        BigEndian::write_u16(&mut page[base + PAGE_LEVEL..], level);
        BigEndian::write_u64(&mut page[base + PAGE_INDEX_ID..], index_id);

        // Infimum record at PAGE_NEW_INFIMUM (99)
        // Extra header at 99 - 5 = 94
        // rec_type = Infimum (2)
        let infimum_extra = PAGE_NEW_INFIMUM - REC_N_NEW_EXTRA_BYTES;
        // bytes 1-2: heap_no=0, rec_type=2 => (0 << 3) | 2 = 2
        BigEndian::write_u16(&mut page[infimum_extra + 1..], 0x0002);

        // Supremum record at PAGE_NEW_SUPREMUM (112)
        let supremum_extra = PAGE_NEW_SUPREMUM - REC_N_NEW_EXTRA_BYTES;
        // bytes 1-2: heap_no=1, rec_type=3 => (1 << 3) | 3 = 11
        BigEndian::write_u16(&mut page[supremum_extra + 1..], 0x000B);
        // Supremum next_offset = 0 (end of chain)
        BigEndian::write_i16(&mut page[supremum_extra + 3..], 0);

        if records.is_empty() {
            // Infimum points to supremum
            let next = PAGE_NEW_SUPREMUM as i16 - PAGE_NEW_INFIMUM as i16;
            BigEndian::write_i16(&mut page[infimum_extra + 3..], next);
            return page;
        }

        // Place user records starting after supremum + "supremum\0" (8 bytes)
        let mut record_start = PAGE_NEW_SUPREMUM + 8;
        let mut record_offsets = Vec::new();

        for (i, rec_data) in records.iter().enumerate() {
            // Each record needs 5 extra bytes before it + the data
            let extra_start = record_start;
            let origin = extra_start + REC_N_NEW_EXTRA_BYTES;

            // Record extra header
            let rec_type_bits: u8 = if is_node_ptr { 1 } else { 0 };
            let heap_no = (i as u16) + 2; // 0=infimum, 1=supremum, 2+=user
            let heap_status = (heap_no << 3) | (rec_type_bits as u16);
            let mut info_byte: u8 = 0;
            if i == 0 && is_node_ptr {
                info_byte |= 0x10; // min_rec flag for first non-leaf record
            }
            page[extra_start] = info_byte;
            BigEndian::write_u16(&mut page[extra_start + 1..], heap_status);

            // Copy record data at origin
            if origin + rec_data.len() <= page_size {
                page[origin..origin + rec_data.len()].copy_from_slice(rec_data);
            }

            record_offsets.push(origin);
            record_start = origin + rec_data.len();
        }

        // Link infimum -> first record
        let first_origin = record_offsets[0];
        let infimum_next = first_origin as i16 - PAGE_NEW_INFIMUM as i16;
        BigEndian::write_i16(&mut page[infimum_extra + 3..], infimum_next);

        // Link records to each other, last record -> supremum
        for i in 0..record_offsets.len() {
            let this_origin = record_offsets[i];
            let this_extra = this_origin - REC_N_NEW_EXTRA_BYTES;
            let next_target = if i + 1 < record_offsets.len() {
                record_offsets[i + 1]
            } else {
                PAGE_NEW_SUPREMUM
            };
            let next_offset = next_target as i16 - this_origin as i16;
            BigEndian::write_i16(&mut page[this_extra + 3..], next_offset);
        }

        page
    }

    #[test]
    fn test_extract_child_page_no() {
        let pk_cols = vec![int_column("id", false)];
        let page_size = SIZE_PAGE_DEFAULT as usize;

        // Build a non-leaf page with one NodePtr record containing:
        // - 4-byte INT PK value (42, InnoDB encoded: XOR high bit)
        // - 4-byte child page number (7)
        let mut rec_data = vec![0u8; 8];
        let encoded_val: u32 = (42i32 as u32) ^ 0x80000000; // InnoDB signed int encoding
        BigEndian::write_u32(&mut rec_data[0..4], encoded_val);
        BigEndian::write_u32(&mut rec_data[4..8], 7); // child page 7

        let page = build_index_page(page_size, 3, 100, 1, &[rec_data], true);

        // Find the first user record offset (after infimum/supremum)
        let records = walk_compact_records(&page);
        assert!(!records.is_empty(), "should have at least one record");

        let child = extract_child_page_no(&page, records[0].offset, &pk_cols, page_size);
        assert_eq!(child, Some(7));
    }

    #[test]
    fn test_compare_int_keys() {
        // Signed int comparison
        let val_a = FieldValue::Int(42);
        let key_a = PkValue::Int(42);
        assert_eq!(
            compare_field_value(&val_a, &key_a),
            std::cmp::Ordering::Equal
        );

        let val_b = FieldValue::Int(10);
        let key_b = PkValue::Int(20);
        assert_eq!(
            compare_field_value(&val_b, &key_b),
            std::cmp::Ordering::Less
        );

        let val_c = FieldValue::Int(30);
        let key_c = PkValue::Int(20);
        assert_eq!(
            compare_field_value(&val_c, &key_c),
            std::cmp::Ordering::Greater
        );

        // Unsigned int comparison
        let val_d = FieldValue::Uint(100);
        let key_d = PkValue::Uint(100);
        assert_eq!(
            compare_field_value(&val_d, &key_d),
            std::cmp::Ordering::Equal
        );

        let val_e = FieldValue::Uint(50);
        let key_e = PkValue::Uint(100);
        assert_eq!(
            compare_field_value(&val_e, &key_e),
            std::cmp::Ordering::Less
        );

        // Cross-type: negative signed vs unsigned
        let val_f = FieldValue::Int(-1);
        let key_f = PkValue::Uint(0);
        assert_eq!(
            compare_field_value(&val_f, &key_f),
            std::cmp::Ordering::Less
        );

        // Cross-type: positive signed vs unsigned
        let val_g = FieldValue::Uint(10);
        let key_g = PkValue::Int(-5);
        assert_eq!(
            compare_field_value(&val_g, &key_g),
            std::cmp::Ordering::Greater
        );
    }

    #[test]
    fn test_compare_string_keys() {
        let val_a = FieldValue::Str("alice".to_string());
        let key_a = PkValue::Str("alice".to_string());
        assert_eq!(
            compare_field_value(&val_a, &key_a),
            std::cmp::Ordering::Equal
        );

        let val_b = FieldValue::Str("alice".to_string());
        let key_b = PkValue::Str("bob".to_string());
        assert_eq!(
            compare_field_value(&val_b, &key_b),
            std::cmp::Ordering::Less
        );

        let val_c = FieldValue::Str("charlie".to_string());
        let key_c = PkValue::Str("bob".to_string());
        assert_eq!(
            compare_field_value(&val_c, &key_c),
            std::cmp::Ordering::Greater
        );
    }

    #[test]
    fn test_search_single_page() {
        // A root page that is also a leaf (level 0) should return immediately.
        let page_size = SIZE_PAGE_DEFAULT as usize;
        let index_id = 42u64;
        let pk_cols = vec![int_column("id", false)];

        // Build a leaf page (level 0) with one ordinary record
        let mut rec_data = vec![0u8; 4];
        let encoded_val: u32 = (1i32 as u32) ^ 0x80000000;
        BigEndian::write_u32(&mut rec_data[0..4], encoded_val);

        let page = build_index_page(page_size, 3, index_id, 0, &[rec_data], false);

        // Create a mock tablespace from the single page
        let mut ts_data = vec![0u8; page_size * 4]; // pages 0-3
                                                    // Copy our leaf page into page 3
        ts_data[page_size * 3..page_size * 4].copy_from_slice(&page);

        // Set page 0 as FSP_HDR with correct page size flags
        BigEndian::write_u16(&mut ts_data[FIL_PAGE_TYPE..], 8); // FSP_HDR type

        let mut ts = Tablespace::from_bytes(ts_data).unwrap();

        let search_key = vec![PkValue::Int(1)];
        let result = search_btree(
            &mut ts,
            3,
            index_id,
            &pk_cols,
            &search_key,
            SIZE_PAGE_DEFAULT,
        );

        assert!(result.is_ok(), "search should succeed");
        let res = result.unwrap();
        assert_eq!(res.leaf_page_no, 3);
        assert_eq!(res.levels_traversed, 0);
    }
}
