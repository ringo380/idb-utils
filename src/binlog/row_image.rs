//! Binlog row image parsing — extract PK values from row event data.
//!
//! MySQL binlog row events (WRITE/UPDATE/DELETE_ROWS) encode row data as
//! packed binary images: a null bitmap followed by column values in column
//! order. This module parses those images to extract primary key column
//! values, enabling correlation between binlog events and InnoDB tablespace
//! pages.
//!
//! All integer fields use **little-endian** byte order and standard 2's
//! complement encoding (NOT InnoDB's big-endian XOR'd sign-bit format).

use byteorder::{ByteOrder, LittleEndian};
use serde::Serialize;

// -----------------------------------------------------------------------
// MySQL protocol type codes (from mysql_com.h / field_types.h)
// -----------------------------------------------------------------------

/// TINY (1-byte integer).
const MYSQL_TYPE_TINY: u8 = 1;
/// SHORT (2-byte integer).
const MYSQL_TYPE_SHORT: u8 = 2;
/// LONG (4-byte integer).
const MYSQL_TYPE_LONG: u8 = 3;
/// FLOAT (4-byte IEEE 754).
const MYSQL_TYPE_FLOAT: u8 = 4;
/// DOUBLE (8-byte IEEE 754).
const MYSQL_TYPE_DOUBLE: u8 = 5;
/// LONGLONG (8-byte integer).
const MYSQL_TYPE_LONGLONG: u8 = 8;
/// INT24 / MEDIUMINT (3-byte integer).
const MYSQL_TYPE_INT24: u8 = 9;
/// DATETIME2 (temporal with fractional seconds).
const MYSQL_TYPE_DATETIME2: u8 = 12;
/// VARCHAR (variable-length string).
const MYSQL_TYPE_VARCHAR: u8 = 15;
/// BIT.
const MYSQL_TYPE_BIT: u8 = 16;
/// TIMESTAMP2 (temporal with fractional seconds).
const MYSQL_TYPE_TIMESTAMP2: u8 = 17;
/// TIME2 (temporal with fractional seconds).
const MYSQL_TYPE_TIME2: u8 = 18;
/// NEWDECIMAL (packed decimal).
const MYSQL_TYPE_NEWDECIMAL: u8 = 246;
/// ENUM.
const MYSQL_TYPE_ENUM: u8 = 247;
/// SET.
const MYSQL_TYPE_SET: u8 = 248;
/// BLOB / TEXT (variable-length binary).
const MYSQL_TYPE_BLOB: u8 = 252;
/// VAR_STRING.
const MYSQL_TYPE_VAR_STRING: u8 = 253;
/// STRING (fixed-length).
const MYSQL_TYPE_STRING: u8 = 254;

// -----------------------------------------------------------------------
// Public types
// -----------------------------------------------------------------------

/// Column metadata for a binlog row image column.
#[derive(Debug, Clone, Serialize)]
pub struct BinlogColumnMeta {
    /// MySQL protocol type code (1=TINY, 2=SHORT, 3=LONG, 8=LONGLONG, 15=VARCHAR, etc.).
    pub column_type: u8,
    /// Whether the column is unsigned.
    pub is_unsigned: bool,
    /// Type-specific metadata (max length for VARCHAR, display size for INT, etc.).
    pub type_metadata: u16,
    /// Whether this column is part of the primary key.
    pub is_pk: bool,
    /// Ordinal position within the PK (0-based), if `is_pk` is true.
    pub pk_ordinal: Option<usize>,
}

/// A decoded primary key value from a binlog row image.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum BinlogPkValue {
    /// Signed integer value.
    Int(i64),
    /// Unsigned integer value.
    Uint(u64),
    /// String value (VARCHAR, CHAR, etc.).
    Str(String),
    /// Raw bytes (BLOB, BINARY, etc.).
    Bytes(Vec<u8>),
}

impl std::fmt::Display for BinlogPkValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BinlogPkValue::Int(v) => write!(f, "{}", v),
            BinlogPkValue::Uint(v) => write!(f, "{}", v),
            BinlogPkValue::Str(s) => write!(f, "{}", s),
            BinlogPkValue::Bytes(b) => {
                write!(f, "0x")?;
                for byte in b {
                    write!(f, "{:02x}", byte)?;
                }
                Ok(())
            }
        }
    }
}

// -----------------------------------------------------------------------
// Metadata parsing
// -----------------------------------------------------------------------

/// Parse per-column metadata from a TABLE_MAP event's metadata section.
///
/// Each MySQL column type consumes a different number of bytes from the
/// metadata block. This function walks the metadata bytes and returns
/// one `u16` metadata value per column.
///
/// # Arguments
///
/// * `column_types` — Column type codes from the TABLE_MAP event.
/// * `metadata_bytes` — Raw metadata section bytes.
///
/// # Returns
///
/// A vector of per-column metadata values (same length as `column_types`).
pub fn parse_column_metadata(column_types: &[u8], metadata_bytes: &[u8]) -> Vec<u16> {
    let mut result = Vec::with_capacity(column_types.len());
    let mut offset = 0;

    for &col_type in column_types {
        let (value, consumed) = read_type_metadata(col_type, metadata_bytes, offset);
        result.push(value);
        offset += consumed;
    }

    result
}

/// Read one column's metadata from the metadata byte stream.
///
/// Returns `(metadata_value, bytes_consumed)`.
fn read_type_metadata(column_type: u8, data: &[u8], offset: usize) -> (u16, usize) {
    match column_type {
        // 0-byte metadata types
        MYSQL_TYPE_TINY | MYSQL_TYPE_SHORT | MYSQL_TYPE_LONG | MYSQL_TYPE_LONGLONG
        | MYSQL_TYPE_INT24 => (0, 0),

        // 1-byte metadata types
        MYSQL_TYPE_FLOAT | MYSQL_TYPE_DOUBLE => {
            let v = data.get(offset).copied().unwrap_or(0) as u16;
            (v, 1)
        }
        MYSQL_TYPE_DATETIME2 | MYSQL_TYPE_TIMESTAMP2 | MYSQL_TYPE_TIME2 => {
            let v = data.get(offset).copied().unwrap_or(0) as u16;
            (v, 1)
        }
        MYSQL_TYPE_BLOB => {
            // Pack length (1-4), stored as 1 byte.
            let v = data.get(offset).copied().unwrap_or(0) as u16;
            (v, 1)
        }

        // 2-byte metadata types (little-endian u16)
        MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING => {
            if offset + 2 <= data.len() {
                (LittleEndian::read_u16(&data[offset..]), 2)
            } else {
                (0, 2)
            }
        }
        MYSQL_TYPE_NEWDECIMAL => {
            // precision (1 byte) + scale (1 byte), packed as (precision << 8 | scale).
            let precision = data.get(offset).copied().unwrap_or(0) as u16;
            let scale = data.get(offset + 1).copied().unwrap_or(0) as u16;
            ((precision << 8) | scale, 2)
        }
        MYSQL_TYPE_STRING | MYSQL_TYPE_ENUM | MYSQL_TYPE_SET => {
            // 2 bytes: real type + length
            if offset + 2 <= data.len() {
                let b0 = data[offset] as u16;
                let b1 = data[offset + 1] as u16;
                ((b0 << 8) | b1, 2)
            } else {
                (0, 2)
            }
        }
        MYSQL_TYPE_BIT => {
            // bits (1 byte) + bytes (1 byte)
            if offset + 2 <= data.len() {
                let bits = data[offset] as u16;
                let bytes = data[offset + 1] as u16;
                ((bytes << 8) | bits, 2)
            } else {
                (0, 2)
            }
        }

        // Unknown type — consume 0 bytes (best effort).
        _ => (0, 0),
    }
}

// -----------------------------------------------------------------------
// Row image PK extraction
// -----------------------------------------------------------------------

/// Extract primary key values from a binlog row image.
///
/// Parses the first row from the packed row image data, decoding column
/// values based on the column metadata, and returns only the PK columns
/// sorted by their ordinal position within the key.
///
/// # Arguments
///
/// * `row_data` — Raw row image bytes (null bitmap + column values).
/// * `columns` — Column metadata (type, signedness, PK membership).
///
/// # Returns
///
/// `Some(Vec<BinlogPkValue>)` with PK values in key order, or `None` if
/// any PK column is NULL or cannot be decoded.
pub fn extract_pk_from_row_image(
    row_data: &[u8],
    columns: &[BinlogColumnMeta],
) -> Option<Vec<BinlogPkValue>> {
    let col_count = columns.len();
    if col_count == 0 {
        return None;
    }

    // Null bitmap: ceil(col_count / 8) bytes.
    let null_bitmap_len = col_count.div_ceil(8);
    if row_data.len() < null_bitmap_len {
        return None;
    }
    let null_bitmap = &row_data[..null_bitmap_len];
    let mut offset = null_bitmap_len;

    // Collect PK values keyed by pk_ordinal.
    let pk_count = columns.iter().filter(|c| c.is_pk).count();
    if pk_count == 0 {
        return None;
    }
    let mut pk_values: Vec<(usize, BinlogPkValue)> = Vec::with_capacity(pk_count);

    for (i, col) in columns.iter().enumerate() {
        // Check null bitmap.
        let is_null = (null_bitmap[i / 8] >> (i % 8)) & 1 == 1;

        if is_null {
            if col.is_pk {
                // PK column is NULL — cannot extract PK.
                return None;
            }
            continue;
        }

        // Decode or skip this column's value.
        let remaining = &row_data[offset..];
        let size = match column_value_size(col.column_type, col.type_metadata, remaining) {
            Some(s) => s,
            None => {
                if col.is_pk {
                    return None;
                }
                // Cannot determine size of non-PK column — abort.
                return None;
            }
        };

        if col.is_pk {
            if remaining.len() < size {
                return None;
            }
            let value = decode_column_value(
                col.column_type,
                col.is_unsigned,
                col.type_metadata,
                &remaining[..size],
            )?;
            let ordinal = col.pk_ordinal.unwrap_or(0);
            pk_values.push((ordinal, value));
        }

        offset += size;
    }

    // Sort by PK ordinal.
    pk_values.sort_by_key(|(ord, _)| *ord);
    Some(pk_values.into_iter().map(|(_, v)| v).collect())
}

// -----------------------------------------------------------------------
// Column value size computation
// -----------------------------------------------------------------------

/// Compute the byte size of a column value in the row image.
///
/// This is needed to skip over non-PK columns when parsing the row.
/// Returns `None` for column types whose size cannot be determined.
fn column_value_size(column_type: u8, metadata: u16, data: &[u8]) -> Option<usize> {
    match column_type {
        MYSQL_TYPE_TINY => Some(1),
        MYSQL_TYPE_SHORT => Some(2),
        MYSQL_TYPE_INT24 => Some(3),
        MYSQL_TYPE_LONG => Some(4),
        MYSQL_TYPE_LONGLONG => Some(8),
        MYSQL_TYPE_FLOAT => Some(4),
        MYSQL_TYPE_DOUBLE => Some(8),

        MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING => {
            // Length prefix: 1 byte if max_len < 256, else 2 bytes LE.
            let max_len = metadata as usize;
            if max_len < 256 {
                let len = *data.first()? as usize;
                Some(1 + len)
            } else {
                if data.len() < 2 {
                    return None;
                }
                let len = LittleEndian::read_u16(data) as usize;
                Some(2 + len)
            }
        }

        MYSQL_TYPE_STRING => {
            // Fixed-length string. Metadata high byte is real type, low byte is length.
            // For types that map to STRING in binlog (CHAR, ENUM, SET), the
            // metadata encodes the real type and max byte length.
            let real_type = (metadata >> 8) as u8;
            let max_len = metadata & 0xFF;

            match real_type {
                // ENUM: size is 1 or 2 bytes depending on element count.
                MYSQL_TYPE_ENUM => {
                    let size = max_len as usize;
                    Some(size)
                }
                // SET: size in bytes.
                MYSQL_TYPE_SET => {
                    let size = max_len as usize;
                    Some(size)
                }
                // Default CHAR: if max_len > 255, use 2-byte length prefix.
                _ => {
                    if max_len > 255 {
                        if data.len() < 2 {
                            return None;
                        }
                        let len = LittleEndian::read_u16(data) as usize;
                        Some(2 + len)
                    } else {
                        let len = *data.first()? as usize;
                        Some(1 + len)
                    }
                }
            }
        }

        MYSQL_TYPE_BLOB => {
            // Pack length from metadata (1-4 bytes).
            let pack_len = metadata as usize;
            if pack_len == 0 || data.len() < pack_len {
                return None;
            }
            let blob_len = match pack_len {
                1 => data[0] as usize,
                2 => LittleEndian::read_u16(data) as usize,
                3 => data[0] as usize | (data[1] as usize) << 8 | (data[2] as usize) << 16,
                4 => LittleEndian::read_u32(data) as usize,
                _ => return None,
            };
            Some(pack_len + blob_len)
        }

        MYSQL_TYPE_NEWDECIMAL => {
            let precision = (metadata >> 8) as usize;
            let scale = (metadata & 0xFF) as usize;
            Some(decimal_binary_size(precision, scale))
        }

        MYSQL_TYPE_BIT => {
            let bits = (metadata & 0xFF) as usize;
            let bytes = (metadata >> 8) as usize;
            Some(bytes + if bits > 0 { 1 } else { 0 })
        }

        MYSQL_TYPE_DATETIME2 => {
            // 5 bytes + fractional seconds part.
            let fsp = metadata as usize;
            Some(5 + fsp_storage_size(fsp))
        }

        MYSQL_TYPE_TIMESTAMP2 => {
            // 4 bytes + fractional seconds part.
            let fsp = metadata as usize;
            Some(4 + fsp_storage_size(fsp))
        }

        MYSQL_TYPE_TIME2 => {
            // 3 bytes + fractional seconds part.
            let fsp = metadata as usize;
            Some(3 + fsp_storage_size(fsp))
        }

        // Unsupported type — cannot determine size.
        _ => None,
    }
}

/// Compute storage size for fractional seconds precision.
///
/// MySQL temporal types store fractional seconds in ceil(fsp/2) bytes.
fn fsp_storage_size(fsp: usize) -> usize {
    fsp.div_ceil(2)
}

/// Compute binary storage size for a DECIMAL(precision, scale).
///
/// MySQL packs 9 digits per 4 bytes, with leftover digits using fewer bytes.
fn decimal_binary_size(precision: usize, scale: usize) -> usize {
    // Digits-to-bytes mapping for leftover digits (0-8).
    const DIG2BYTES: [usize; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];

    let intg = precision - scale;
    let intg_full = intg / 9;
    let intg_leftover = intg % 9;
    let frac_full = scale / 9;
    let frac_leftover = scale % 9;

    intg_full * 4 + DIG2BYTES[intg_leftover] + frac_full * 4 + DIG2BYTES[frac_leftover]
}

// -----------------------------------------------------------------------
// Column value decoding
// -----------------------------------------------------------------------

/// Decode a single column value from its raw bytes.
///
/// Returns `None` for unsupported types (the caller should treat this as
/// an inability to extract the PK).
fn decode_column_value(
    column_type: u8,
    is_unsigned: bool,
    metadata: u16,
    data: &[u8],
) -> Option<BinlogPkValue> {
    match column_type {
        MYSQL_TYPE_TINY => {
            if data.is_empty() {
                return None;
            }
            if is_unsigned {
                Some(BinlogPkValue::Uint(data[0] as u64))
            } else {
                Some(BinlogPkValue::Int(data[0] as i8 as i64))
            }
        }

        MYSQL_TYPE_SHORT => {
            if data.len() < 2 {
                return None;
            }
            if is_unsigned {
                Some(BinlogPkValue::Uint(LittleEndian::read_u16(data) as u64))
            } else {
                Some(BinlogPkValue::Int(LittleEndian::read_i16(data) as i64))
            }
        }

        MYSQL_TYPE_INT24 => {
            if data.len() < 3 {
                return None;
            }
            let raw = data[0] as u32 | (data[1] as u32) << 8 | (data[2] as u32) << 16;
            if is_unsigned {
                Some(BinlogPkValue::Uint(raw as u64))
            } else {
                // Sign-extend from 24 bits.
                let signed = if raw & 0x800000 != 0 {
                    (raw | 0xFF000000) as i32 as i64
                } else {
                    raw as i64
                };
                Some(BinlogPkValue::Int(signed))
            }
        }

        MYSQL_TYPE_LONG => {
            if data.len() < 4 {
                return None;
            }
            if is_unsigned {
                Some(BinlogPkValue::Uint(LittleEndian::read_u32(data) as u64))
            } else {
                Some(BinlogPkValue::Int(LittleEndian::read_i32(data) as i64))
            }
        }

        MYSQL_TYPE_LONGLONG => {
            if data.len() < 8 {
                return None;
            }
            if is_unsigned {
                Some(BinlogPkValue::Uint(LittleEndian::read_u64(data)))
            } else {
                Some(BinlogPkValue::Int(LittleEndian::read_i64(data)))
            }
        }

        MYSQL_TYPE_FLOAT => {
            if data.len() < 4 {
                return None;
            }
            // Store float as its integer bit pattern for PK purposes.
            let bits = LittleEndian::read_u32(data);
            let val = f32::from_bits(bits);
            Some(BinlogPkValue::Str(format!("{}", val)))
        }

        MYSQL_TYPE_DOUBLE => {
            if data.len() < 8 {
                return None;
            }
            let bits = LittleEndian::read_u64(data);
            let val = f64::from_bits(bits);
            Some(BinlogPkValue::Str(format!("{}", val)))
        }

        MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING => {
            let max_len = metadata as usize;
            if max_len < 256 {
                let len = *data.first()? as usize;
                if data.len() < 1 + len {
                    return None;
                }
                let s = String::from_utf8_lossy(&data[1..1 + len]).into_owned();
                Some(BinlogPkValue::Str(s))
            } else {
                if data.len() < 2 {
                    return None;
                }
                let len = LittleEndian::read_u16(data) as usize;
                if data.len() < 2 + len {
                    return None;
                }
                let s = String::from_utf8_lossy(&data[2..2 + len]).into_owned();
                Some(BinlogPkValue::Str(s))
            }
        }

        MYSQL_TYPE_STRING => {
            let real_type = (metadata >> 8) as u8;
            let max_len = metadata & 0xFF;

            match real_type {
                MYSQL_TYPE_ENUM => {
                    let size = max_len as usize;
                    if data.len() < size {
                        return None;
                    }
                    let val = match size {
                        1 => data[0] as u64,
                        2 => LittleEndian::read_u16(data) as u64,
                        _ => return None,
                    };
                    Some(BinlogPkValue::Uint(val))
                }
                MYSQL_TYPE_SET => {
                    let size = max_len as usize;
                    if data.len() < size {
                        return None;
                    }
                    Some(BinlogPkValue::Bytes(data[..size].to_vec()))
                }
                _ => {
                    // Fixed-length CHAR: length-prefixed.
                    if max_len > 255 {
                        if data.len() < 2 {
                            return None;
                        }
                        let len = LittleEndian::read_u16(data) as usize;
                        if data.len() < 2 + len {
                            return None;
                        }
                        let s = String::from_utf8_lossy(&data[2..2 + len]).into_owned();
                        Some(BinlogPkValue::Str(s))
                    } else {
                        let len = *data.first()? as usize;
                        if data.len() < 1 + len {
                            return None;
                        }
                        let s = String::from_utf8_lossy(&data[1..1 + len]).into_owned();
                        Some(BinlogPkValue::Str(s))
                    }
                }
            }
        }

        MYSQL_TYPE_BLOB => {
            let pack_len = metadata as usize;
            if pack_len == 0 || data.len() < pack_len {
                return None;
            }
            let blob_len = match pack_len {
                1 => data[0] as usize,
                2 => LittleEndian::read_u16(data) as usize,
                3 => data[0] as usize | (data[1] as usize) << 8 | (data[2] as usize) << 16,
                4 => LittleEndian::read_u32(data) as usize,
                _ => return None,
            };
            if data.len() < pack_len + blob_len {
                return None;
            }
            Some(BinlogPkValue::Bytes(
                data[pack_len..pack_len + blob_len].to_vec(),
            ))
        }

        // Other types: return None (cannot decode for PK purposes).
        _ => None,
    }
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- parse_column_metadata tests --

    #[test]
    fn parse_metadata_int_types() {
        // INT types consume 0 metadata bytes.
        let types = vec![MYSQL_TYPE_TINY, MYSQL_TYPE_LONG, MYSQL_TYPE_LONGLONG];
        let meta = parse_column_metadata(&types, &[]);
        assert_eq!(meta, vec![0, 0, 0]);
    }

    #[test]
    fn parse_metadata_varchar() {
        let types = vec![MYSQL_TYPE_VARCHAR];
        // Max length = 200 (0xC8, 0x00 LE).
        let meta_bytes = [0xC8, 0x00];
        let meta = parse_column_metadata(&types, &meta_bytes);
        assert_eq!(meta, vec![200]);
    }

    #[test]
    fn parse_metadata_blob() {
        let types = vec![MYSQL_TYPE_BLOB];
        let meta_bytes = [2]; // pack length = 2
        let meta = parse_column_metadata(&types, &meta_bytes);
        assert_eq!(meta, vec![2]);
    }

    #[test]
    fn parse_metadata_mixed() {
        // LONG (0 bytes) + VARCHAR (2 bytes) + BLOB (1 byte) + SHORT (0 bytes)
        let types = vec![
            MYSQL_TYPE_LONG,
            MYSQL_TYPE_VARCHAR,
            MYSQL_TYPE_BLOB,
            MYSQL_TYPE_SHORT,
        ];
        let meta_bytes = [0x00, 0x01, 3]; // VARCHAR max_len=256 (0x0100 LE), BLOB pack=3
        let meta = parse_column_metadata(&types, &meta_bytes);
        assert_eq!(meta, vec![0, 256, 3, 0]);
    }

    #[test]
    fn parse_metadata_newdecimal() {
        let types = vec![MYSQL_TYPE_NEWDECIMAL];
        // precision=10, scale=2
        let meta_bytes = [10, 2];
        let meta = parse_column_metadata(&types, &meta_bytes);
        // Packed as (10 << 8) | 2 = 2562.
        assert_eq!(meta, vec![2562]);
    }

    #[test]
    fn parse_metadata_datetime2() {
        let types = vec![MYSQL_TYPE_DATETIME2];
        let meta_bytes = [3]; // fsp=3
        let meta = parse_column_metadata(&types, &meta_bytes);
        assert_eq!(meta, vec![3]);
    }

    // -- INT decoding tests --

    #[test]
    fn decode_tiny_signed() {
        let cols = vec![make_pk_col(MYSQL_TYPE_TINY, false, 0, 0)];
        let mut data = vec![0u8; 1]; // null bitmap: no nulls
        data.push(0xFE); // -2 as i8
        let pk = extract_pk_from_row_image(&data, &cols).unwrap();
        assert_eq!(pk, vec![BinlogPkValue::Int(-2)]);
    }

    #[test]
    fn decode_tiny_unsigned() {
        let cols = vec![make_pk_col(MYSQL_TYPE_TINY, true, 0, 0)];
        let mut data = vec![0u8; 1];
        data.push(0xFE); // 254 unsigned
        let pk = extract_pk_from_row_image(&data, &cols).unwrap();
        assert_eq!(pk, vec![BinlogPkValue::Uint(254)]);
    }

    #[test]
    fn decode_long_signed() {
        let cols = vec![make_pk_col(MYSQL_TYPE_LONG, false, 0, 0)];
        let mut data = vec![0u8; 1];
        let mut buf = [0u8; 4];
        LittleEndian::write_i32(&mut buf, -42);
        data.extend_from_slice(&buf);
        let pk = extract_pk_from_row_image(&data, &cols).unwrap();
        assert_eq!(pk, vec![BinlogPkValue::Int(-42)]);
    }

    #[test]
    fn decode_long_unsigned() {
        let cols = vec![make_pk_col(MYSQL_TYPE_LONG, true, 0, 0)];
        let mut data = vec![0u8; 1];
        let mut buf = [0u8; 4];
        LittleEndian::write_u32(&mut buf, 3_000_000_000);
        data.extend_from_slice(&buf);
        let pk = extract_pk_from_row_image(&data, &cols).unwrap();
        assert_eq!(pk, vec![BinlogPkValue::Uint(3_000_000_000)]);
    }

    #[test]
    fn decode_longlong_signed() {
        let cols = vec![make_pk_col(MYSQL_TYPE_LONGLONG, false, 0, 0)];
        let mut data = vec![0u8; 1];
        let mut buf = [0u8; 8];
        LittleEndian::write_i64(&mut buf, -9_000_000_000);
        data.extend_from_slice(&buf);
        let pk = extract_pk_from_row_image(&data, &cols).unwrap();
        assert_eq!(pk, vec![BinlogPkValue::Int(-9_000_000_000)]);
    }

    #[test]
    fn decode_longlong_unsigned() {
        let cols = vec![make_pk_col(MYSQL_TYPE_LONGLONG, true, 0, 0)];
        let mut data = vec![0u8; 1];
        let mut buf = [0u8; 8];
        LittleEndian::write_u64(&mut buf, 18_000_000_000_000_000_000);
        data.extend_from_slice(&buf);
        let pk = extract_pk_from_row_image(&data, &cols).unwrap();
        assert_eq!(pk, vec![BinlogPkValue::Uint(18_000_000_000_000_000_000)]);
    }

    #[test]
    fn decode_short_signed() {
        let cols = vec![make_pk_col(MYSQL_TYPE_SHORT, false, 0, 0)];
        let mut data = vec![0u8; 1];
        let mut buf = [0u8; 2];
        LittleEndian::write_i16(&mut buf, -1000);
        data.extend_from_slice(&buf);
        let pk = extract_pk_from_row_image(&data, &cols).unwrap();
        assert_eq!(pk, vec![BinlogPkValue::Int(-1000)]);
    }

    #[test]
    fn decode_int24_signed() {
        let cols = vec![make_pk_col(MYSQL_TYPE_INT24, false, 0, 0)];
        let mut data = vec![0u8; 1];
        // -100 as 3-byte LE: 0xFFFF9C in LE = [0x9C, 0xFF, 0xFF]
        data.extend_from_slice(&[0x9C, 0xFF, 0xFF]);
        let pk = extract_pk_from_row_image(&data, &cols).unwrap();
        assert_eq!(pk, vec![BinlogPkValue::Int(-100)]);
    }

    #[test]
    fn decode_int24_unsigned() {
        let cols = vec![make_pk_col(MYSQL_TYPE_INT24, true, 0, 0)];
        let mut data = vec![0u8; 1];
        // 100000 = 0x0186A0, LE = [0xA0, 0x86, 0x01]
        data.extend_from_slice(&[0xA0, 0x86, 0x01]);
        let pk = extract_pk_from_row_image(&data, &cols).unwrap();
        assert_eq!(pk, vec![BinlogPkValue::Uint(100_000)]);
    }

    // -- VARCHAR decoding tests --

    #[test]
    fn decode_varchar_short_prefix() {
        // VARCHAR with max_len < 256 → 1-byte length prefix.
        let cols = vec![make_pk_col(MYSQL_TYPE_VARCHAR, false, 200, 0)];
        let mut data = vec![0u8; 1]; // null bitmap
        data.push(5); // length = 5
        data.extend_from_slice(b"hello");
        let pk = extract_pk_from_row_image(&data, &cols).unwrap();
        assert_eq!(pk, vec![BinlogPkValue::Str("hello".to_string())]);
    }

    #[test]
    fn decode_varchar_long_prefix() {
        // VARCHAR with max_len >= 256 → 2-byte length prefix.
        let cols = vec![make_pk_col(MYSQL_TYPE_VARCHAR, false, 500, 0)];
        let mut data = vec![0u8; 1]; // null bitmap
        let mut len_buf = [0u8; 2];
        LittleEndian::write_u16(&mut len_buf, 11);
        data.extend_from_slice(&len_buf);
        data.extend_from_slice(b"hello world");
        let pk = extract_pk_from_row_image(&data, &cols).unwrap();
        assert_eq!(pk, vec![BinlogPkValue::Str("hello world".to_string())]);
    }

    // -- Null bitmap tests --

    #[test]
    fn null_pk_returns_none() {
        let cols = vec![make_pk_col(MYSQL_TYPE_LONG, false, 0, 0)];
        // Null bitmap: bit 0 set → column 0 is NULL.
        let data = vec![0x01];
        assert!(extract_pk_from_row_image(&data, &cols).is_none());
    }

    #[test]
    fn null_non_pk_column_skipped() {
        // Two columns: non-PK INT (NULL) + PK INT (42).
        let cols = vec![
            BinlogColumnMeta {
                column_type: MYSQL_TYPE_LONG,
                is_unsigned: true,
                type_metadata: 0,
                is_pk: false,
                pk_ordinal: None,
            },
            make_pk_col(MYSQL_TYPE_LONG, true, 0, 0),
        ];
        let mut data = vec![0x01]; // bit 0 set → col 0 is NULL, col 1 is not
        let mut buf = [0u8; 4];
        LittleEndian::write_u32(&mut buf, 42);
        data.extend_from_slice(&buf);
        let pk = extract_pk_from_row_image(&data, &cols).unwrap();
        assert_eq!(pk, vec![BinlogPkValue::Uint(42)]);
    }

    // -- Multi-column PK tests --

    #[test]
    fn multi_column_pk_sorted_by_ordinal() {
        // 3 columns: PK ord=1 (SHORT), non-PK (LONG), PK ord=0 (TINY).
        let cols = vec![
            BinlogColumnMeta {
                column_type: MYSQL_TYPE_SHORT,
                is_unsigned: true,
                type_metadata: 0,
                is_pk: true,
                pk_ordinal: Some(1),
            },
            BinlogColumnMeta {
                column_type: MYSQL_TYPE_LONG,
                is_unsigned: true,
                type_metadata: 0,
                is_pk: false,
                pk_ordinal: None,
            },
            BinlogColumnMeta {
                column_type: MYSQL_TYPE_TINY,
                is_unsigned: true,
                type_metadata: 0,
                is_pk: true,
                pk_ordinal: Some(0),
            },
        ];
        let mut data = vec![0u8; 1]; // null bitmap: all non-null
                                     // Col 0 (SHORT): 1000
        let mut buf = [0u8; 2];
        LittleEndian::write_u16(&mut buf, 1000);
        data.extend_from_slice(&buf);
        // Col 1 (LONG): 99999
        let mut buf4 = [0u8; 4];
        LittleEndian::write_u32(&mut buf4, 99999);
        data.extend_from_slice(&buf4);
        // Col 2 (TINY): 7
        data.push(7);

        let pk = extract_pk_from_row_image(&data, &cols).unwrap();
        // Sorted by ordinal: ord=0 (TINY 7) first, ord=1 (SHORT 1000) second.
        assert_eq!(pk, vec![BinlogPkValue::Uint(7), BinlogPkValue::Uint(1000)]);
    }

    // -- column_value_size tests --

    #[test]
    fn size_varchar_short() {
        let data = [5, b'h', b'e', b'l', b'l', b'o'];
        assert_eq!(column_value_size(MYSQL_TYPE_VARCHAR, 200, &data), Some(6));
    }

    #[test]
    fn size_varchar_long() {
        let mut data = vec![0u8; 2];
        LittleEndian::write_u16(&mut data, 3);
        data.extend_from_slice(b"abc");
        assert_eq!(column_value_size(MYSQL_TYPE_VARCHAR, 500, &data), Some(5));
    }

    #[test]
    fn size_blob_pack2() {
        let mut data = vec![0u8; 2];
        LittleEndian::write_u16(&mut data, 10);
        data.extend_from_slice(&[0u8; 10]);
        assert_eq!(column_value_size(MYSQL_TYPE_BLOB, 2, &data), Some(12));
    }

    #[test]
    fn size_datetime2_fsp3() {
        // 5 base + ceil(3/2) = 5 + 2 = 7
        assert_eq!(
            column_value_size(MYSQL_TYPE_DATETIME2, 3, &[0u8; 10]),
            Some(7)
        );
    }

    // -- Display tests --

    #[test]
    fn pk_value_display() {
        assert_eq!(BinlogPkValue::Int(-42).to_string(), "-42");
        assert_eq!(BinlogPkValue::Uint(100).to_string(), "100");
        assert_eq!(BinlogPkValue::Str("hello".into()).to_string(), "hello");
        assert_eq!(BinlogPkValue::Bytes(vec![0xDE, 0xAD]).to_string(), "0xdead");
    }

    // -- decimal_binary_size tests --

    #[test]
    fn decimal_size_10_2() {
        // DECIMAL(10,2): intg=8 → 0 full + 8 leftover (4 bytes), frac=2 → 0 full + 2 leftover (1 byte)
        assert_eq!(decimal_binary_size(10, 2), 5);
    }

    #[test]
    fn decimal_size_18_0() {
        // DECIMAL(18,0): intg=18 → 2 full (8 bytes) + 0 leftover
        assert_eq!(decimal_binary_size(18, 0), 8);
    }

    // -- Empty / edge cases --

    #[test]
    fn empty_columns_returns_none() {
        assert!(extract_pk_from_row_image(&[0], &[]).is_none());
    }

    #[test]
    fn no_pk_columns_returns_none() {
        let cols = vec![BinlogColumnMeta {
            column_type: MYSQL_TYPE_LONG,
            is_unsigned: true,
            type_metadata: 0,
            is_pk: false,
            pk_ordinal: None,
        }];
        let mut data = vec![0u8; 1];
        data.extend_from_slice(&[0u8; 4]);
        assert!(extract_pk_from_row_image(&data, &cols).is_none());
    }

    #[test]
    fn truncated_data_returns_none() {
        let cols = vec![make_pk_col(MYSQL_TYPE_LONGLONG, false, 0, 0)];
        let data = vec![0u8; 1]; // null bitmap only, no value bytes
        assert!(extract_pk_from_row_image(&data, &cols).is_none());
    }

    // -- Helper --

    fn make_pk_col(col_type: u8, unsigned: bool, meta: u16, ordinal: usize) -> BinlogColumnMeta {
        BinlogColumnMeta {
            column_type: col_type,
            is_unsigned: unsigned,
            type_metadata: meta,
            is_pk: true,
            pk_ordinal: Some(ordinal),
        }
    }
}
