//! Field-level value decoding for InnoDB records.
//!
//! Decodes raw bytes from InnoDB compact-format records into typed values
//! using column metadata from SDI (MySQL 8.0+ data dictionary). Handles
//! InnoDB's internal storage encodings: big-endian integers with XOR'd
//! sign bit, IEEE 754 floats with sign-bit manipulation, packed DATE and
//! DATETIME fields, and UTF-8 string fields.
//!
//! # Supported types
//!
//! | SQL Type | InnoDB encoding | Decoder |
//! |----------|----------------|---------|
//! | TINYINT–BIGINT | Big-endian, XOR high bit for signed | `decode_int` |
//! | FLOAT | 4-byte IEEE 754 with sign handling | `decode_float` |
//! | DOUBLE | 8-byte IEEE 754 with sign handling | `decode_double` |
//! | DECIMAL | Packed BCD with sign in first nibble | `decode_decimal` |
//! | DATE | 3-byte packed year/month/day | `decode_date` |
//! | DATETIME | 5+fsp bytes packed bit-field | `decode_datetime` |
//! | TIMESTAMP | 4+fsp bytes UTC seconds | `decode_timestamp` |
//! | TIME | 3+fsp bytes packed bit-field | `decode_time` |
//! | YEAR | 1 byte + 1900 | `decode_year` |
//! | VARCHAR/CHAR | UTF-8 with lossy fallback | `decode_string` |
//! | ENUM | 1-2 byte index into element list | `decode_enum` |
//! | SET | Bitmask into element list | `decode_set` |
//! | BLOB/TEXT | Inline bytes or hex | `decode_string` |
//! | JSON | Inline bytes as hex | `decode_hex` |
//! | GEOMETRY | Inline bytes as hex (WKB) | `decode_hex` |
//! | Others | Raw hex | `decode_hex` |

use serde::Serialize;

use crate::innodb::schema::DdTable;

/// Decoded field value from an InnoDB record.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum FieldValue {
    /// SQL NULL.
    Null,
    /// Signed integer (TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT).
    Int(i64),
    /// Unsigned integer.
    Uint(u64),
    /// Single-precision float.
    Float(f32),
    /// Double-precision float.
    Double(f64),
    /// String value (VARCHAR, CHAR, DATE, DATETIME, etc.).
    Str(String),
    /// Hex-encoded bytes for unsupported types.
    Hex(String),
}

/// Physical storage information for a single column.
#[derive(Debug, Clone)]
pub struct ColumnStorageInfo {
    /// Column name.
    pub name: String,
    /// MySQL dd_type code.
    pub dd_type: u64,
    /// SQL type string (e.g., "int", "varchar(255)").
    pub column_type: String,
    /// Whether the column allows NULL.
    pub is_nullable: bool,
    /// Whether the column is unsigned.
    pub is_unsigned: bool,
    /// Fixed-length size in bytes (0 for variable-length).
    pub fixed_len: usize,
    /// Whether this is a variable-length field.
    pub is_variable: bool,
    /// Maximum bytes per character for string types.
    pub charset_max_bytes: usize,
    /// Fractional seconds precision for DATETIME/TIMESTAMP/TIME.
    pub datetime_precision: u64,
    /// Whether this is a system column (DB_TRX_ID, DB_ROLL_PTR, DB_ROW_ID).
    pub is_system_column: bool,
    /// ENUM/SET element names (from SDI metadata).
    pub elements: Vec<String>,
    /// Numeric precision for DECIMAL.
    pub numeric_precision: u64,
    /// Numeric scale for DECIMAL.
    pub numeric_scale: u64,
}

// MySQL dd_type codes (from sql/dd/types/column.h)
const DD_TYPE_TINY: u64 = 2; // TINYINT
const DD_TYPE_SHORT: u64 = 3; // SMALLINT
const DD_TYPE_INT24: u64 = 5; // MEDIUMINT
const DD_TYPE_LONG: u64 = 4; // INT
const DD_TYPE_LONGLONG: u64 = 9; // BIGINT
const DD_TYPE_FLOAT: u64 = 6; // FLOAT
const DD_TYPE_DOUBLE: u64 = 7; // DOUBLE
const DD_TYPE_NEWDECIMAL: u64 = 20; // DECIMAL
const DD_TYPE_DATE: u64 = 13; // DATE (newdate)
const DD_TYPE_DATETIME: u64 = 18; // DATETIME2
const DD_TYPE_TIMESTAMP: u64 = 17; // TIMESTAMP2
const DD_TYPE_YEAR: u64 = 14; // YEAR
const DD_TYPE_VARCHAR: u64 = 16; // VARCHAR
const DD_TYPE_STRING: u64 = 15; // CHAR
const DD_TYPE_BLOB: u64 = 19; // BLOB/TEXT
const DD_TYPE_JSON: u64 = 21; // JSON
const DD_TYPE_ENUM: u64 = 22; // ENUM
const DD_TYPE_SET: u64 = 23; // SET
const DD_TYPE_BIT: u64 = 24; // BIT
const DD_TYPE_GEOMETRY: u64 = 25; // GEOMETRY
const DD_TYPE_TIME2: u64 = 12; // TIME2

/// Build a column layout from SDI table metadata.
///
/// Maps SDI columns to physical InnoDB storage order:
/// 1. Primary key columns (from clustered index)
/// 2. DB_TRX_ID (6 bytes) — system column
/// 3. DB_ROLL_PTR (7 bytes) — system column
/// 4. Remaining user columns in ordinal order
///
/// Hidden columns (hidden == 2, i.e., SE-hidden) are included as system columns.
/// Virtual/generated columns (is_virtual) are excluded.
pub fn build_column_layout(dd_table: &DdTable) -> Vec<ColumnStorageInfo> {
    let mut layout = Vec::new();

    // Find the PRIMARY/clustered index
    let primary_idx = dd_table.indexes.iter().find(|i| i.index_type == 1);

    // Collect PK column ordinal positions
    let mut pk_col_positions: Vec<u64> = Vec::new();
    if let Some(pk) = primary_idx {
        for elem in &pk.elements {
            if !elem.hidden {
                pk_col_positions.push(elem.column_opx);
            }
        }
    }

    // Build visible user columns sorted by ordinal_position
    let mut user_columns: Vec<&crate::innodb::schema::DdColumn> = dd_table
        .columns
        .iter()
        .filter(|c| !c.is_virtual && (c.hidden == 1 || c.hidden == 4)) // HT_VISIBLE + HT_HIDDEN_USER only
        .collect();
    user_columns.sort_by_key(|c| c.ordinal_position);

    // PK columns first
    for &pk_opx in &pk_col_positions {
        if let Some(col) = dd_table.columns.get(pk_opx as usize) {
            if !col.is_virtual && (col.hidden == 1 || col.hidden == 4) {
                layout.push(column_to_storage_info(col, false));
            }
        }
    }

    // System columns
    layout.push(ColumnStorageInfo {
        name: "DB_TRX_ID".to_string(),
        dd_type: 0,
        column_type: "system".to_string(),
        is_nullable: false,
        is_unsigned: true,
        fixed_len: 6,
        is_variable: false,
        charset_max_bytes: 0,
        datetime_precision: 0,
        is_system_column: true,
        elements: Vec::new(),
        numeric_precision: 0,
        numeric_scale: 0,
    });
    layout.push(ColumnStorageInfo {
        name: "DB_ROLL_PTR".to_string(),
        dd_type: 0,
        column_type: "system".to_string(),
        is_nullable: false,
        is_unsigned: true,
        fixed_len: 7,
        is_variable: false,
        charset_max_bytes: 0,
        datetime_precision: 0,
        is_system_column: true,
        elements: Vec::new(),
        numeric_precision: 0,
        numeric_scale: 0,
    });

    // Remaining non-PK user columns
    for col in &user_columns {
        let col_opx = dd_table.columns.iter().position(|c| std::ptr::eq(c, *col));
        if let Some(opx) = col_opx {
            if !pk_col_positions.contains(&(opx as u64)) {
                layout.push(column_to_storage_info(col, false));
            }
        }
    }

    layout
}

/// Convert a DdColumn to a ColumnStorageInfo.
fn column_to_storage_info(
    col: &crate::innodb::schema::DdColumn,
    is_system: bool,
) -> ColumnStorageInfo {
    let (fixed_len, is_variable) = compute_storage_size(col);
    let charset_max_bytes = charset_max_bytes_from_collation(col.collation_id);
    let elements: Vec<String> = col.elements.iter().map(|e| e.name.clone()).collect();

    ColumnStorageInfo {
        name: col.name.clone(),
        dd_type: col.dd_type,
        column_type: col.column_type_utf8.clone(),
        is_nullable: col.is_nullable,
        is_unsigned: col.is_unsigned,
        fixed_len,
        is_variable,
        charset_max_bytes,
        datetime_precision: col.datetime_precision,
        is_system_column: is_system,
        elements,
        numeric_precision: col.numeric_precision,
        numeric_scale: col.numeric_scale,
    }
}

/// Compute the fixed storage size and variable-length flag for a column.
fn compute_storage_size(col: &crate::innodb::schema::DdColumn) -> (usize, bool) {
    match col.dd_type {
        DD_TYPE_TINY => (1, false),
        DD_TYPE_SHORT => (2, false),
        DD_TYPE_INT24 => (3, false),
        DD_TYPE_LONG => (4, false),
        DD_TYPE_LONGLONG => (8, false),
        DD_TYPE_FLOAT => (4, false),
        DD_TYPE_DOUBLE => (8, false),
        DD_TYPE_YEAR => (1, false),
        DD_TYPE_DATE => (3, false),
        DD_TYPE_DATETIME | DD_TYPE_TIMESTAMP | DD_TYPE_TIME2 => {
            // Base size + fractional seconds storage
            let base = match col.dd_type {
                DD_TYPE_DATETIME => 5,
                DD_TYPE_TIMESTAMP => 4,
                DD_TYPE_TIME2 => 3,
                _ => unreachable!(),
            };
            let fsp_bytes = fsp_storage_bytes(col.datetime_precision);
            (base + fsp_bytes, false)
        }
        DD_TYPE_VARCHAR | DD_TYPE_BLOB | DD_TYPE_JSON | DD_TYPE_GEOMETRY => {
            (0, true) // variable-length
        }
        DD_TYPE_STRING => {
            // CHAR: fixed-length = char_length * charset_max_bytes
            let max_bytes = charset_max_bytes_from_collation(col.collation_id);
            if max_bytes > 1 {
                // Multi-byte CHAR is stored as variable-length in compact format
                (0, true)
            } else {
                (col.char_length as usize, false)
            }
        }
        DD_TYPE_ENUM => {
            // ENUM: 1 byte for <=255 values, 2 bytes otherwise
            let n = col.elements.len();
            if n <= 255 {
                (1, false)
            } else {
                (2, false)
            }
        }
        DD_TYPE_SET => {
            // SET: ceil(n_elements / 8) bytes
            let n = col.elements.len();
            let bytes = n.div_ceil(8).max(1);
            (bytes, false)
        }
        DD_TYPE_BIT => {
            // BIT(M): ceil(M / 8) bytes
            let bits = col.char_length as usize;
            let bytes = bits.div_ceil(8).max(1);
            (bytes, false)
        }
        DD_TYPE_NEWDECIMAL => {
            // DECIMAL: complex packed BCD, approximate
            let precision = col.numeric_precision as usize;
            let scale = col.numeric_scale as usize;
            let intg = precision - scale;
            let intg_bytes = (intg / 9) * 4 + decimal_leftover_bytes(intg % 9);
            let frac_bytes = (scale / 9) * 4 + decimal_leftover_bytes(scale % 9);
            (intg_bytes + frac_bytes, false)
        }
        _ => (0, true), // Unknown: treat as variable
    }
}

/// Bytes needed for leftover digits in DECIMAL packed BCD.
fn decimal_leftover_bytes(digits: usize) -> usize {
    match digits {
        0 => 0,
        1..=2 => 1,
        3..=4 => 2,
        5..=6 => 3,
        7..=9 => 4,
        _ => 4,
    }
}

/// Storage bytes for fractional seconds precision.
fn fsp_storage_bytes(fsp: u64) -> usize {
    match fsp {
        0 => 0,
        1 | 2 => 1,
        3 | 4 => 2,
        5 | 6 => 3,
        _ => 0,
    }
}

/// Determine max bytes per character from collation ID.
/// Common collation IDs from MySQL:
///   - 33 (utf8_general_ci): 3 bytes
///   - 45 (utf8mb4_general_ci): 4 bytes
///   - 255 (utf8mb4_0900_ai_ci): 4 bytes
///   - 8 (latin1_swedish_ci): 1 byte
///   - 63 (binary): 1 byte
fn charset_max_bytes_from_collation(collation_id: u64) -> usize {
    match collation_id {
        // latin1 collations
        5 | 8 | 15 | 31 | 47 | 48 | 49 | 94 => 1,
        // binary
        63 => 1,
        // ascii
        11 | 65 => 1,
        // utf8 (3-byte)
        33 | 83 | 192..=215 => 3,
        // utf8mb4 (4-byte) — most common in MySQL 8.0+
        45 | 46 | 224..=247 | 255 | 256..=310 => 4,
        // Default: assume 4-byte (safe upper bound)
        _ => 4,
    }
}

/// Decode a field value from raw bytes based on column storage info.
pub fn decode_field(data: &[u8], col: &ColumnStorageInfo) -> FieldValue {
    if data.is_empty() {
        return FieldValue::Null;
    }

    match col.dd_type {
        DD_TYPE_TINY => decode_int(data, 1, col.is_unsigned),
        DD_TYPE_SHORT => decode_int(data, 2, col.is_unsigned),
        DD_TYPE_INT24 => decode_int(data, 3, col.is_unsigned),
        DD_TYPE_LONG => decode_int(data, 4, col.is_unsigned),
        DD_TYPE_LONGLONG => decode_int(data, 8, col.is_unsigned),
        DD_TYPE_FLOAT => decode_float(data),
        DD_TYPE_DOUBLE => decode_double(data),
        DD_TYPE_NEWDECIMAL => decode_decimal(data, col.numeric_precision, col.numeric_scale),
        DD_TYPE_DATE => decode_date(data),
        DD_TYPE_DATETIME => decode_datetime(data, col.datetime_precision),
        DD_TYPE_TIMESTAMP => decode_timestamp(data, col.datetime_precision),
        DD_TYPE_TIME2 => decode_time(data, col.datetime_precision),
        DD_TYPE_YEAR => decode_year(data),
        DD_TYPE_VARCHAR | DD_TYPE_STRING => decode_string(data),
        DD_TYPE_BLOB => decode_string(data),
        DD_TYPE_ENUM => decode_enum(data, &col.elements),
        DD_TYPE_SET => decode_set(data, &col.elements),
        DD_TYPE_JSON | DD_TYPE_GEOMETRY => decode_hex(data),
        // System columns: decode as unsigned int
        0 if col.is_system_column => decode_int(data, data.len(), true),
        // Everything else: hex fallback
        _ => decode_hex(data),
    }
}

/// Decode a big-endian integer with XOR'd sign bit.
///
/// InnoDB stores signed integers with the high bit XOR'd so that
/// memcmp ordering matches numeric ordering.
fn decode_int(data: &[u8], size: usize, unsigned: bool) -> FieldValue {
    if data.len() < size {
        return decode_hex(data);
    }

    // Read big-endian unsigned value
    let mut val: u64 = 0;
    for &b in &data[..size] {
        val = (val << 8) | b as u64;
    }

    // XOR the high bit (InnoDB encoding for memcmp ordering)
    let sign_bit: u64 = 1 << (size * 8 - 1);
    val ^= sign_bit;

    if unsigned {
        FieldValue::Uint(val)
    } else {
        // Convert to signed: if original high bit was 0 (now 1 after XOR),
        // value is positive. If original was 1 (now 0), value is negative.
        let max_unsigned: u64 = if size == 8 {
            u64::MAX
        } else {
            (1u64 << (size * 8)) - 1
        };
        if val > (max_unsigned >> 1) {
            // Negative: val is in [sign_bit..max], map to negative range
            // For size bytes, negative range is [-(sign_bit)..-1]
            let signed = (val as i64).wrapping_sub(1i64.wrapping_shl((size * 8) as u32));
            FieldValue::Int(signed)
        } else {
            FieldValue::Int(val as i64)
        }
    }
}

/// Decode a 4-byte InnoDB float.
///
/// InnoDB float encoding: if high bit is set, XOR all bits (positive).
/// If high bit is clear, XOR only high bit (negative, to flip sign for ordering).
fn decode_float(data: &[u8]) -> FieldValue {
    if data.len() < 4 {
        return decode_hex(data);
    }

    let mut bytes = [data[0], data[1], data[2], data[3]];
    if bytes[0] & 0x80 != 0 {
        // Positive: XOR all bits
        for b in &mut bytes {
            *b ^= 0xFF;
        }
    } else {
        // Negative: XOR high bit only
        bytes[0] ^= 0x80;
    }
    // Now reverse the byte order — InnoDB stores float in big-endian
    bytes.reverse();
    let f = f32::from_le_bytes(bytes);
    FieldValue::Float(f)
}

/// Decode an 8-byte InnoDB double.
///
/// Same encoding as float but 8 bytes.
fn decode_double(data: &[u8]) -> FieldValue {
    if data.len() < 8 {
        return decode_hex(data);
    }

    let mut bytes = [
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ];
    if bytes[0] & 0x80 != 0 {
        for b in &mut bytes {
            *b ^= 0xFF;
        }
    } else {
        bytes[0] ^= 0x80;
    }
    bytes.reverse();
    let d = f64::from_le_bytes(bytes);
    FieldValue::Double(d)
}

/// Decode a 3-byte InnoDB DATE (newdate format).
///
/// Stored as: day(5) | month(4) | year(15) packed into 3 bytes big-endian.
/// Actually stored as little-endian 3-byte integer in InnoDB.
fn decode_date(data: &[u8]) -> FieldValue {
    if data.len() < 3 {
        return decode_hex(data);
    }

    // 3-byte big-endian packed date
    let val = ((data[0] as u32) << 16) | ((data[1] as u32) << 8) | data[2] as u32;

    // XOR high bit for signed ordering
    let val = val ^ (1 << 23);

    let day = val & 0x1F;
    let month = (val >> 5) & 0x0F;
    let year = val >> 9;

    if year == 0 && month == 0 && day == 0 {
        FieldValue::Str("0000-00-00".to_string())
    } else {
        FieldValue::Str(format!("{:04}-{:02}-{:02}", year, month, day))
    }
}

/// Decode a DATETIME2 (5 + fsp bytes).
///
/// Packed as big-endian integer with XOR'd sign bit:
/// - year_month (17 bits): year * 13 + month
/// - day (5 bits)
/// - hour (5 bits)
/// - minute (6 bits)
/// - second (6 bits)
///
/// Total: 40 bits = 5 bytes
fn decode_datetime(data: &[u8], fsp: u64) -> FieldValue {
    let base_len = 5;
    let fsp_bytes = fsp_storage_bytes(fsp);
    if data.len() < base_len + fsp_bytes {
        return decode_hex(data);
    }

    // Read 5-byte big-endian
    let mut val: u64 = 0;
    for &b in &data[..5] {
        val = (val << 8) | b as u64;
    }

    // XOR high bit
    val ^= 1 << 39;

    let second = val & 0x3F;
    let minute = (val >> 6) & 0x3F;
    let hour = (val >> 12) & 0x1F;
    let day = (val >> 17) & 0x1F;
    let year_month = val >> 22;

    let year = year_month / 13;
    let month = year_month % 13;

    if fsp > 0 && fsp_bytes > 0 {
        // Read fractional seconds
        let mut frac: u32 = 0;
        for &b in &data[5..5 + fsp_bytes] {
            frac = (frac << 8) | b as u32;
        }
        // fsp 1-2: 1 byte, fsp 3-4: 2 bytes, fsp 5-6: 3 bytes
        // Adjust to microseconds
        let micros = match fsp {
            1 | 2 => frac as u64 * 10000,
            3 | 4 => frac as u64 * 100,
            5 | 6 => frac as u64,
            _ => 0,
        };
        let frac_str = format!("{:06}", micros);
        let frac_trimmed = &frac_str[..fsp as usize];
        FieldValue::Str(format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{}",
            year, month, day, hour, minute, second, frac_trimmed
        ))
    } else {
        FieldValue::Str(format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            year, month, day, hour, minute, second
        ))
    }
}

/// Decode a TIMESTAMP2 (4 + fsp bytes).
///
/// 4-byte big-endian UTC seconds since epoch.
fn decode_timestamp(data: &[u8], fsp: u64) -> FieldValue {
    if data.len() < 4 {
        return decode_hex(data);
    }

    let secs = ((data[0] as u32) << 24)
        | ((data[1] as u32) << 16)
        | ((data[2] as u32) << 8)
        | data[3] as u32;

    if secs == 0 {
        return FieldValue::Str("0000-00-00 00:00:00".to_string());
    }

    // Convert to date/time components (UTC)
    // Simple UTC conversion without timezone support
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hour = time_of_day / 3600;
    let minute = (time_of_day % 3600) / 60;
    let second = time_of_day % 60;

    // Days since 1970-01-01 to Y-M-D
    let (year, month, day) = days_to_ymd(days_since_epoch);

    let fsp_bytes = fsp_storage_bytes(fsp);
    if fsp > 0 && fsp_bytes > 0 && data.len() >= 4 + fsp_bytes {
        let mut frac: u32 = 0;
        for &b in &data[4..4 + fsp_bytes] {
            frac = (frac << 8) | b as u32;
        }
        let micros = match fsp {
            1 | 2 => frac as u64 * 10000,
            3 | 4 => frac as u64 * 100,
            5 | 6 => frac as u64,
            _ => 0,
        };
        let frac_str = format!("{:06}", micros);
        let frac_trimmed = &frac_str[..fsp as usize];
        FieldValue::Str(format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{}",
            year, month, day, hour, minute, second, frac_trimmed
        ))
    } else {
        FieldValue::Str(format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            year, month, day, hour, minute, second
        ))
    }
}

/// Convert days since 1970-01-01 to (year, month, day).
fn days_to_ymd(days: u32) -> (u32, u32, u32) {
    // Algorithm from https://howardhinnant.github.io/date_algorithms.html
    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Decode a 1-byte YEAR field.
fn decode_year(data: &[u8]) -> FieldValue {
    if data.is_empty() {
        return FieldValue::Null;
    }
    let val = data[0];
    if val == 0 {
        FieldValue::Str("0000".to_string())
    } else {
        FieldValue::Uint(1900 + val as u64)
    }
}

/// Decode a string field (VARCHAR or CHAR).
fn decode_string(data: &[u8]) -> FieldValue {
    // CHAR fields have trailing padding (0x20 for ASCII/UTF-8)
    let trimmed = trim_trailing_spaces(data);
    FieldValue::Str(String::from_utf8_lossy(trimmed).to_string())
}

/// Trim trailing 0x20 (space) bytes.
fn trim_trailing_spaces(data: &[u8]) -> &[u8] {
    let mut end = data.len();
    while end > 0 && data[end - 1] == 0x20 {
        end -= 1;
    }
    &data[..end]
}

/// Decode InnoDB packed BCD DECIMAL.
///
/// InnoDB DECIMAL storage (packed BCD):
/// - Digits are grouped into groups of 9, stored as 4-byte big-endian ints
/// - Leftover digits (< 9) use 1-4 bytes depending on count
/// - The first byte has the sign bit XOR'd for memcmp ordering
/// - Positive values: high bit set in stored form
/// - Negative values: all bytes XOR'd with 0xFF
fn decode_decimal(data: &[u8], precision: u64, scale: u64) -> FieldValue {
    if precision == 0 {
        return decode_hex(data);
    }

    let intg = (precision - scale) as usize;
    let frac = scale as usize;
    let intg_full = intg / 9;
    let intg_left = intg % 9;
    let frac_full = frac / 9;
    let frac_left = frac % 9;

    let expected_len = intg_full * 4
        + decimal_leftover_bytes(intg_left)
        + frac_full * 4
        + decimal_leftover_bytes(frac_left);

    if data.len() < expected_len {
        return decode_hex(data);
    }

    // Copy and handle sign
    let mut buf = data[..expected_len].to_vec();
    let negative = buf[0] & 0x80 == 0;

    // For negative values, XOR all bytes with 0xFF to restore original BCD.
    if negative {
        for b in &mut buf {
            *b ^= 0xFF;
        }
    }
    // Clear sign bit (high bit of first byte) in all cases
    buf[0] &= 0x7F;

    let mut result = String::new();
    if negative {
        result.push('-');
    }

    let mut pos = 0;

    // Decode integer leftover digits
    if intg_left > 0 {
        let bytes = decimal_leftover_bytes(intg_left);
        let mut val: u32 = 0;
        for &b in &buf[pos..pos + bytes] {
            val = (val << 8) | b as u32;
        }
        result.push_str(&val.to_string());
        pos += bytes;
    }

    // Decode full integer groups
    for i in 0..intg_full {
        let val = ((buf[pos] as u32) << 24)
            | ((buf[pos + 1] as u32) << 16)
            | ((buf[pos + 2] as u32) << 8)
            | buf[pos + 3] as u32;
        if i == 0 && intg_left == 0 {
            result.push_str(&val.to_string());
        } else {
            result.push_str(&format!("{:09}", val));
        }
        pos += 4;
    }

    // If no integer part was written, write "0"
    if intg == 0 || (intg_full == 0 && intg_left == 0) {
        result.push('0');
    }

    // Decode fractional part
    if frac > 0 {
        result.push('.');

        for _ in 0..frac_full {
            let val = ((buf[pos] as u32) << 24)
                | ((buf[pos + 1] as u32) << 16)
                | ((buf[pos + 2] as u32) << 8)
                | buf[pos + 3] as u32;
            result.push_str(&format!("{:09}", val));
            pos += 4;
        }

        if frac_left > 0 {
            let bytes = decimal_leftover_bytes(frac_left);
            let mut val: u32 = 0;
            for &b in &buf[pos..pos + bytes] {
                val = (val << 8) | b as u32;
            }
            let formatted = format!("{:0width$}", val, width = frac_left);
            result.push_str(&formatted);
        }
    }

    FieldValue::Str(result)
}

/// Decode a TIME2 field (3 + fsp bytes).
///
/// Stored as big-endian 3-byte integer with offset encoding:
/// stored_value = signed_value + 0x800000.
/// The signed value packs:
/// - hours (10 bits)
/// - minutes (6 bits)
/// - seconds (6 bits)
///
/// Total: 24 bits = 3 bytes, plus FSP bytes for fractional seconds.
fn decode_time(data: &[u8], fsp: u64) -> FieldValue {
    let base_len = 3;
    let fsp_bytes = fsp_storage_bytes(fsp);
    if data.len() < base_len + fsp_bytes {
        return decode_hex(data);
    }

    // Read 3-byte big-endian unsigned value
    let stored: u32 = ((data[0] as u32) << 16) | ((data[1] as u32) << 8) | data[2] as u32;

    // InnoDB TIME2 uses offset encoding: stored = signed_value + 0x800000
    let signed_val: i32 = stored as i32 - 0x800000;
    let negative = signed_val < 0;
    let abs_val = signed_val.unsigned_abs();

    let second = abs_val & 0x3F;
    let minute = (abs_val >> 6) & 0x3F;
    let hour = (abs_val >> 12) & 0x3FF;

    let sign = if negative { "-" } else { "" };

    if fsp > 0 && fsp_bytes > 0 {
        let mut frac: u32 = 0;
        for &b in &data[3..3 + fsp_bytes] {
            frac = (frac << 8) | b as u32;
        }
        let micros = match fsp {
            1 | 2 => frac as u64 * 10000,
            3 | 4 => frac as u64 * 100,
            5 | 6 => frac as u64,
            _ => 0,
        };
        let frac_str = format!("{:06}", micros);
        let frac_trimmed = &frac_str[..fsp as usize];
        FieldValue::Str(format!(
            "{}{:02}:{:02}:{:02}.{}",
            sign, hour, minute, second, frac_trimmed
        ))
    } else {
        FieldValue::Str(format!("{}{:02}:{:02}:{:02}", sign, hour, minute, second))
    }
}

/// Decode an ENUM field (1 or 2 byte index).
///
/// ENUM stores a 1-based index into the element list.
/// Index 0 means empty string ('').
fn decode_enum(data: &[u8], elements: &[String]) -> FieldValue {
    let idx = match data.len() {
        1 => data[0] as usize,
        2 => ((data[0] as usize) << 8) | data[1] as usize,
        _ => return decode_hex(data),
    };

    if idx == 0 {
        return FieldValue::Str(String::new());
    }

    // ENUM index is 1-based
    if idx <= elements.len() {
        FieldValue::Str(elements[idx - 1].clone())
    } else {
        FieldValue::Uint(idx as u64)
    }
}

/// Decode a SET field (1-8 byte bitmask).
///
/// SET stores a bitmask where each bit corresponds to an element.
fn decode_set(data: &[u8], elements: &[String]) -> FieldValue {
    let mut bitmask: u64 = 0;
    for (i, &b) in data.iter().enumerate() {
        bitmask |= (b as u64) << (i * 8);
    }

    if bitmask == 0 {
        return FieldValue::Str(String::new());
    }

    let mut selected = Vec::new();
    for (i, elem) in elements.iter().enumerate() {
        if bitmask & (1 << i) != 0 {
            selected.push(elem.as_str());
        }
    }

    FieldValue::Str(selected.join(","))
}

/// Hex-encode bytes as a fallback.
fn decode_hex(data: &[u8]) -> FieldValue {
    let hex: String = data.iter().map(|b| format!("{:02x}", b)).collect();
    FieldValue::Hex(format!("0x{}", hex))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_col(dd_type: u64, unsigned: bool) -> ColumnStorageInfo {
        ColumnStorageInfo {
            name: "test".to_string(),
            dd_type,
            column_type: "test".to_string(),
            is_nullable: false,
            is_unsigned: unsigned,
            fixed_len: 4,
            is_variable: false,
            charset_max_bytes: 1,
            datetime_precision: 0,
            is_system_column: false,
            elements: Vec::new(),
            numeric_precision: 0,
            numeric_scale: 0,
        }
    }

    #[test]
    fn test_decode_int_unsigned_zero() {
        // InnoDB stores unsigned 0 as 0x00000000 with XOR: 0x80000000
        let data = [0x80, 0x00, 0x00, 0x00];
        let col = make_col(DD_TYPE_LONG, true);
        match decode_field(&data, &col) {
            FieldValue::Uint(v) => assert_eq!(v, 0),
            other => panic!("Expected Uint(0), got {:?}", other),
        }
    }

    #[test]
    fn test_decode_int_unsigned_one() {
        let data = [0x80, 0x00, 0x00, 0x01];
        let col = make_col(DD_TYPE_LONG, true);
        match decode_field(&data, &col) {
            FieldValue::Uint(v) => assert_eq!(v, 1),
            other => panic!("Expected Uint(1), got {:?}", other),
        }
    }

    #[test]
    fn test_decode_int_signed_zero() {
        let data = [0x80, 0x00, 0x00, 0x00];
        let col = make_col(DD_TYPE_LONG, false);
        match decode_field(&data, &col) {
            FieldValue::Int(v) => assert_eq!(v, 0),
            other => panic!("Expected Int(0), got {:?}", other),
        }
    }

    #[test]
    fn test_decode_int_signed_positive() {
        // Signed 42: stored as 0x8000002A (XOR high bit)
        let data = [0x80, 0x00, 0x00, 0x2A];
        let col = make_col(DD_TYPE_LONG, false);
        match decode_field(&data, &col) {
            FieldValue::Int(v) => assert_eq!(v, 42),
            other => panic!("Expected Int(42), got {:?}", other),
        }
    }

    #[test]
    fn test_decode_int_signed_negative() {
        // Signed -1: two's complement is 0xFFFFFFFF, XOR high bit => 0x7FFFFFFF
        let data = [0x7F, 0xFF, 0xFF, 0xFF];
        let col = make_col(DD_TYPE_LONG, false);
        match decode_field(&data, &col) {
            FieldValue::Int(v) => assert_eq!(v, -1),
            other => panic!("Expected Int(-1), got {:?}", other),
        }
    }

    #[test]
    fn test_decode_int_signed_min() {
        // INT minimum (-2147483648): stored as 0x00000000 (all zeros after XOR)
        let data = [0x00, 0x00, 0x00, 0x00];
        let col = make_col(DD_TYPE_LONG, false);
        match decode_field(&data, &col) {
            FieldValue::Int(v) => assert_eq!(v, -2147483648),
            other => panic!("Expected Int(-2147483648), got {:?}", other),
        }
    }

    #[test]
    fn test_decode_tinyint() {
        // Unsigned TINYINT 255: stored as 0xFF XOR 0x80 = 0x7F... wait
        // TINYINT(1 byte): 255 unsigned stored as 0x80|0xFF = 0xFF XOR sign_bit
        // sign_bit for 1 byte = 0x80, so 255 stored as 255 XOR 128 = 127?
        // No: InnoDB XORs sign bit on store: store(v) = v XOR sign_bit
        // So unsigned 0 -> 0x80, unsigned 255 -> 255^128 = 127? That seems wrong.
        // Actually for UNSIGNED, high bit is XOR'd: stored = val ^ 0x80
        // 0 -> 0x80, 127 -> 0xFF, 128 -> 0x00, 255 -> 0x7F
        // On decode: val = stored ^ 0x80
        // 0x80 -> 0, 0xFF -> 127, 0x00 -> 128, 0x7F -> 255
        let data = [0x7F]; // stored value for unsigned 255
        let col = make_col(DD_TYPE_TINY, true);
        match decode_field(&data, &col) {
            FieldValue::Uint(v) => assert_eq!(v, 255),
            other => panic!("Expected Uint(255), got {:?}", other),
        }
    }

    #[test]
    fn test_decode_bigint() {
        // Unsigned BIGINT 1: stored as 0x8000000000000001
        let data = [0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01];
        let col = make_col(DD_TYPE_LONGLONG, true);
        match decode_field(&data, &col) {
            FieldValue::Uint(v) => assert_eq!(v, 1),
            other => panic!("Expected Uint(1), got {:?}", other),
        }
    }

    #[test]
    fn test_decode_year() {
        let data = [126]; // 1900 + 126 = 2026
        let col = make_col(DD_TYPE_YEAR, false);
        match decode_field(&data, &col) {
            FieldValue::Uint(v) => assert_eq!(v, 2026),
            other => panic!("Expected Uint(2026), got {:?}", other),
        }
    }

    #[test]
    fn test_decode_year_zero() {
        let data = [0];
        let col = make_col(DD_TYPE_YEAR, false);
        match decode_field(&data, &col) {
            FieldValue::Str(s) => assert_eq!(s, "0000"),
            other => panic!("Expected Str(0000), got {:?}", other),
        }
    }

    #[test]
    fn test_decode_string_varchar() {
        let data = b"hello";
        let col = ColumnStorageInfo {
            name: "test".to_string(),
            dd_type: DD_TYPE_VARCHAR,
            column_type: "varchar(255)".to_string(),
            is_nullable: false,
            is_unsigned: false,
            fixed_len: 0,
            is_variable: true,
            charset_max_bytes: 4,
            datetime_precision: 0,
            is_system_column: false,
            elements: vec![],
            numeric_precision: 0,
            numeric_scale: 0,
        };
        match decode_field(data, &col) {
            FieldValue::Str(s) => assert_eq!(s, "hello"),
            other => panic!("Expected Str(hello), got {:?}", other),
        }
    }

    #[test]
    fn test_decode_string_char_padded() {
        // CHAR(10) with trailing spaces
        let data = b"hi        "; // "hi" + 8 spaces
        let col = ColumnStorageInfo {
            name: "test".to_string(),
            dd_type: DD_TYPE_STRING,
            column_type: "char(10)".to_string(),
            is_nullable: false,
            is_unsigned: false,
            fixed_len: 10,
            is_variable: false,
            charset_max_bytes: 1,
            datetime_precision: 0,
            is_system_column: false,
            elements: vec![],
            numeric_precision: 0,
            numeric_scale: 0,
        };
        match decode_field(data, &col) {
            FieldValue::Str(s) => assert_eq!(s, "hi"),
            other => panic!("Expected Str(hi), got {:?}", other),
        }
    }

    #[test]
    fn test_decode_hex_fallback() {
        let data = [0xDE, 0xAD, 0xBE, 0xEF];
        // Use an unrecognized dd_type so it falls through to hex
        let col = make_col(255, false);
        match decode_field(&data, &col) {
            FieldValue::Hex(h) => assert_eq!(h, "0xdeadbeef"),
            other => panic!("Expected Hex, got {:?}", other),
        }
    }

    #[test]
    fn test_decode_empty_data() {
        let data: &[u8] = &[];
        let col = make_col(DD_TYPE_LONG, false);
        match decode_field(data, &col) {
            FieldValue::Null => {}
            other => panic!("Expected Null, got {:?}", other),
        }
    }

    #[test]
    fn test_build_column_layout_ordering() {
        // Minimal DdTable with 3 columns and a PRIMARY KEY on col 0
        let dd_table = DdTable {
            name: "test".to_string(),
            columns: vec![
                crate::innodb::schema::DdColumn {
                    name: "id".to_string(),
                    dd_type: DD_TYPE_LONG,
                    ordinal_position: 1,
                    is_unsigned: true,
                    hidden: 1, // HT_VISIBLE
                    ..Default::default()
                },
                crate::innodb::schema::DdColumn {
                    name: "name".to_string(),
                    dd_type: DD_TYPE_VARCHAR,
                    ordinal_position: 2,
                    column_type_utf8: "varchar(100)".to_string(),
                    hidden: 1, // HT_VISIBLE
                    ..Default::default()
                },
                crate::innodb::schema::DdColumn {
                    name: "age".to_string(),
                    dd_type: DD_TYPE_LONG,
                    ordinal_position: 3,
                    hidden: 1, // HT_VISIBLE
                    ..Default::default()
                },
            ],
            indexes: vec![crate::innodb::schema::DdIndex {
                name: "PRIMARY".to_string(),
                index_type: 1,
                elements: vec![crate::innodb::schema::DdIndexElement {
                    column_opx: 0,
                    length: 4294967295,
                    order: 2,
                    hidden: false,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let layout = build_column_layout(&dd_table);
        // Expected order: id (PK), DB_TRX_ID, DB_ROLL_PTR, name, age
        assert_eq!(layout.len(), 5);
        assert_eq!(layout[0].name, "id");
        assert_eq!(layout[1].name, "DB_TRX_ID");
        assert!(layout[1].is_system_column);
        assert_eq!(layout[2].name, "DB_ROLL_PTR");
        assert!(layout[2].is_system_column);
        assert_eq!(layout[3].name, "name");
        assert_eq!(layout[4].name, "age");
    }

    #[test]
    fn test_fsp_storage_bytes() {
        assert_eq!(fsp_storage_bytes(0), 0);
        assert_eq!(fsp_storage_bytes(1), 1);
        assert_eq!(fsp_storage_bytes(2), 1);
        assert_eq!(fsp_storage_bytes(3), 2);
        assert_eq!(fsp_storage_bytes(4), 2);
        assert_eq!(fsp_storage_bytes(5), 3);
        assert_eq!(fsp_storage_bytes(6), 3);
    }

    #[test]
    fn test_charset_max_bytes() {
        assert_eq!(charset_max_bytes_from_collation(8), 1); // latin1
        assert_eq!(charset_max_bytes_from_collation(63), 1); // binary
        assert_eq!(charset_max_bytes_from_collation(33), 3); // utf8
        assert_eq!(charset_max_bytes_from_collation(255), 4); // utf8mb4
    }

    // -----------------------------------------------------------------------
    // DECIMAL decoder tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_decode_decimal_positive() {
        // DECIMAL(10,2) value 12345.67
        // precision=10, scale=2 → intg=8, frac=2
        // intg: 8/9=0 full groups, 8 leftover → 4 bytes
        // frac: 2/9=0 full groups, 2 leftover → 1 byte
        // Total: 5 bytes
        // Positive: high bit set
        // intg_left(8 digits) = 12345 → stored as 4-byte BE with sign bit
        // frac_left(2 digits) = 67 → stored as 1 byte
        let mut col = make_col(DD_TYPE_NEWDECIMAL, false);
        col.numeric_precision = 10;
        col.numeric_scale = 2;

        // Build: 0x80 | (12345 >> 24) ...
        // 12345 = 0x00003039
        // With sign bit: 0x80003039
        let data = [0x80, 0x00, 0x30, 0x39, 0x43]; // 12345 + 67
        match decode_field(&data, &col) {
            FieldValue::Str(s) => assert_eq!(s, "12345.67"),
            other => panic!("Expected Str(12345.67), got {:?}", other),
        }
    }

    #[test]
    fn test_decode_decimal_zero() {
        // DECIMAL(5,2) value 0.00
        // intg=3 leftover → 2 bytes, frac=2 leftover → 1 byte = 3 bytes
        let mut col = make_col(DD_TYPE_NEWDECIMAL, false);
        col.numeric_precision = 5;
        col.numeric_scale = 2;

        let data = [0x80, 0x00, 0x00]; // positive zero
        match decode_field(&data, &col) {
            FieldValue::Str(s) => assert_eq!(s, "0.00"),
            other => panic!("Expected Str(0.00), got {:?}", other),
        }
    }

    #[test]
    fn test_decode_decimal_negative() {
        // DECIMAL(5,2) value -1.23
        // intg=3 leftover → 2 bytes, frac=2 leftover → 1 byte = 3 bytes
        // Negative: high bit clear, all other bytes XOR'd with 0xFF
        // Positive 1.23 would be: [0x80, 0x01, 0x17]  (1 in 2 bytes, 23 in 1 byte)
        // Negative -1.23: XOR high bit → [0x00, ...], XOR rest → [0x00, 0xFE, 0xE8]
        // Actually: positive is [0x80, 0x01, 0x17], negative: first byte 0x80 XOR = 0x00,
        // then remaining bytes XOR 0xFF: 0x01 ^ 0xFF = 0xFE, 0x17 ^ 0xFF = 0xE8
        let mut col = make_col(DD_TYPE_NEWDECIMAL, false);
        col.numeric_precision = 5;
        col.numeric_scale = 2;

        // Negative storage: sign bit clear, rest inverted
        let data = [0x7F, 0xFE, 0xE8]; // -1.23
        match decode_field(&data, &col) {
            FieldValue::Str(s) => assert_eq!(s, "-1.23"),
            other => panic!("Expected Str(-1.23), got {:?}", other),
        }
    }

    #[test]
    fn test_decode_decimal_multi_group() {
        // DECIMAL(20,4) value 1234567890.1234
        // intg = 16: 1 full group (9 digits) + 7 leftover (4 bytes)
        // frac = 4: 0 full groups + 4 leftover (2 bytes)
        // Total: 4 + 4 + 2 = 10 bytes
        //
        // Integer part: 1234567890 (10 digits in 16-digit field)
        //   leftover 7 digits: 0000001 → value 1 (stored in 4 bytes)
        //   full group: 234567890 (stored in 4 bytes)
        // Fractional part: .1234
        //   leftover 4 digits: 1234 (stored in 2 bytes)
        //
        // Positive: XOR sign bit on first byte
        // leftover value 1 → 4 bytes: [0x00, 0x00, 0x00, 0x01] → with sign: [0x80, 0x00, 0x00, 0x01]
        // full group: 234567890 = 0x0DFB38D2 → [0x0D, 0xFB, 0x38, 0xD2]
        // frac leftover: 1234 = 0x04D2 → [0x04, 0xD2]
        let mut col = make_col(DD_TYPE_NEWDECIMAL, false);
        col.numeric_precision = 20;
        col.numeric_scale = 4;

        let data = [0x80, 0x00, 0x00, 0x01, 0x0D, 0xFB, 0x38, 0xD2, 0x04, 0xD2];
        match decode_field(&data, &col) {
            FieldValue::Str(s) => assert_eq!(s, "1234567890.1234"),
            other => panic!("Expected Str(1234567890.1234), got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // TIME decoder tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_decode_time_positive() {
        // TIME value 12:30:45
        // Packed: hours=12, minutes=30, seconds=45
        // signed_val = (12 << 12) | (30 << 6) | 45 = 0xC7AD = 51117
        // Stored with offset: signed_val + 0x800000 = 0x80C7AD
        let mut col = make_col(DD_TYPE_TIME2, false);
        col.datetime_precision = 0;
        let data = [0x80, 0xC7, 0xAD]; // 12:30:45 with offset encoding
        match decode_field(&data, &col) {
            FieldValue::Str(s) => assert_eq!(s, "12:30:45"),
            other => panic!("Expected Str(12:30:45), got {:?}", other),
        }
    }

    #[test]
    fn test_decode_time_zero() {
        // TIME value 00:00:00
        // signed_val = 0, stored = 0 + 0x800000 = 0x800000
        let mut col = make_col(DD_TYPE_TIME2, false);
        col.datetime_precision = 0;
        let data = [0x80, 0x00, 0x00]; // positive zero stored
        match decode_field(&data, &col) {
            FieldValue::Str(s) => assert_eq!(s, "00:00:00"),
            other => panic!("Expected Str(00:00:00), got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // ENUM decoder tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_decode_enum_valid_index() {
        let mut col = make_col(DD_TYPE_ENUM, false);
        col.elements = vec!["red".to_string(), "green".to_string(), "blue".to_string()];
        col.fixed_len = 1;

        // Index 2 = "green" (1-based)
        let data = [0x02];
        match decode_field(&data, &col) {
            FieldValue::Str(s) => assert_eq!(s, "green"),
            other => panic!("Expected Str(green), got {:?}", other),
        }
    }

    #[test]
    fn test_decode_enum_zero_index() {
        let mut col = make_col(DD_TYPE_ENUM, false);
        col.elements = vec!["red".to_string(), "green".to_string()];
        col.fixed_len = 1;

        // Index 0 = empty string
        let data = [0x00];
        match decode_field(&data, &col) {
            FieldValue::Str(s) => assert_eq!(s, ""),
            other => panic!("Expected Str(), got {:?}", other),
        }
    }

    #[test]
    fn test_decode_enum_two_byte() {
        let mut col = make_col(DD_TYPE_ENUM, false);
        col.elements = vec!["a".to_string(); 300]; // > 255 elements
        col.elements[255] = "found_it".to_string();
        col.fixed_len = 2;

        // Index 256 (0x0100) = 256th element (0-based index 255)
        let data = [0x01, 0x00];
        match decode_field(&data, &col) {
            FieldValue::Str(s) => assert_eq!(s, "found_it"),
            other => panic!("Expected Str(found_it), got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // SET decoder tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_decode_set_single() {
        let mut col = make_col(DD_TYPE_SET, false);
        col.elements = vec![
            "read".to_string(),
            "write".to_string(),
            "execute".to_string(),
        ];
        col.fixed_len = 1;

        // Bitmask 0x05 = bits 0 and 2 → "read,execute"
        let data = [0x05];
        match decode_field(&data, &col) {
            FieldValue::Str(s) => assert_eq!(s, "read,execute"),
            other => panic!("Expected Str(read,execute), got {:?}", other),
        }
    }

    #[test]
    fn test_decode_set_all() {
        let mut col = make_col(DD_TYPE_SET, false);
        col.elements = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        col.fixed_len = 1;

        // Bitmask 0x07 = all three
        let data = [0x07];
        match decode_field(&data, &col) {
            FieldValue::Str(s) => assert_eq!(s, "a,b,c"),
            other => panic!("Expected Str(a,b,c), got {:?}", other),
        }
    }

    #[test]
    fn test_decode_set_empty() {
        let mut col = make_col(DD_TYPE_SET, false);
        col.elements = vec!["a".to_string(), "b".to_string()];
        col.fixed_len = 1;

        let data = [0x00];
        match decode_field(&data, &col) {
            FieldValue::Str(s) => assert_eq!(s, ""),
            other => panic!("Expected empty Str, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // BLOB/TEXT decoder test
    // -----------------------------------------------------------------------

    #[test]
    fn test_decode_blob_inline() {
        let col = ColumnStorageInfo {
            name: "data".to_string(),
            dd_type: DD_TYPE_BLOB,
            column_type: "text".to_string(),
            is_nullable: true,
            is_unsigned: false,
            fixed_len: 0,
            is_variable: true,
            charset_max_bytes: 4,
            datetime_precision: 0,
            is_system_column: false,
            elements: Vec::new(),
            numeric_precision: 0,
            numeric_scale: 0,
        };

        let data = b"hello world";
        match decode_field(data, &col) {
            FieldValue::Str(s) => assert_eq!(s, "hello world"),
            other => panic!("Expected Str, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // JSON/GEOMETRY decoder tests (hex fallback)
    // -----------------------------------------------------------------------

    #[test]
    fn test_decode_json_hex() {
        let col = make_col(DD_TYPE_JSON, false);
        let data = [0x01, 0x02, 0x03];
        match decode_field(&data, &col) {
            FieldValue::Hex(h) => assert_eq!(h, "0x010203"),
            other => panic!("Expected Hex, got {:?}", other),
        }
    }

    #[test]
    fn test_decode_geometry_hex() {
        let col = make_col(DD_TYPE_GEOMETRY, false);
        let data = [0xAB, 0xCD];
        match decode_field(&data, &col) {
            FieldValue::Hex(h) => assert_eq!(h, "0xabcd"),
            other => panic!("Expected Hex, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // DECIMAL storage size tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_decimal_storage_size() {
        assert_eq!(decimal_leftover_bytes(0), 0);
        assert_eq!(decimal_leftover_bytes(1), 1);
        assert_eq!(decimal_leftover_bytes(2), 1);
        assert_eq!(decimal_leftover_bytes(4), 2);
        assert_eq!(decimal_leftover_bytes(9), 4);
    }

    // -----------------------------------------------------------------------
    // TIME2 storage size tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_time2_storage_size() {
        let col = crate::innodb::schema::DdColumn {
            dd_type: DD_TYPE_TIME2,
            datetime_precision: 0,
            ..Default::default()
        };
        assert_eq!(compute_storage_size(&col), (3, false));

        let col3 = crate::innodb::schema::DdColumn {
            dd_type: DD_TYPE_TIME2,
            datetime_precision: 3,
            ..Default::default()
        };
        assert_eq!(compute_storage_size(&col3), (5, false));

        let col6 = crate::innodb::schema::DdColumn {
            dd_type: DD_TYPE_TIME2,
            datetime_precision: 6,
            ..Default::default()
        };
        assert_eq!(compute_storage_size(&col6), (6, false));
    }
}
