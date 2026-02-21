//! Schema extraction and DDL reconstruction from SDI metadata.
//!
//! MySQL 8.0+ embeds complete table definitions (columns, indexes, foreign keys)
//! as SDI JSON inside every `.ibd` file. This module parses that JSON into typed
//! Rust structs and reconstructs human-readable `CREATE TABLE` DDL.
//!
//! For pre-8.0 tablespaces without SDI, a best-effort inference from INDEX page
//! structure provides basic information about detected indexes.
//!
//! # Usage
//!
//! ```no_run
//! use idb::innodb::tablespace::Tablespace;
//! use idb::innodb::sdi::{find_sdi_pages, extract_sdi_from_pages};
//! use idb::innodb::schema::extract_schema_from_sdi;
//!
//! let mut ts = Tablespace::open("table.ibd").unwrap();
//! let sdi_pages = find_sdi_pages(&mut ts).unwrap();
//! let records = extract_sdi_from_pages(&mut ts, &sdi_pages).unwrap();
//! for rec in &records {
//!     if rec.sdi_type == 1 {
//!         let schema = extract_schema_from_sdi(&rec.data).unwrap();
//!         println!("{}", schema.ddl);
//!     }
//! }
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::IdbError;

// ---------------------------------------------------------------------------
// SDI JSON deserialization structs
// ---------------------------------------------------------------------------

/// Top-level SDI envelope wrapping a dd_object.
#[derive(Debug, Deserialize)]
pub struct SdiEnvelope {
    /// MySQL server version ID (e.g., 90001 for 9.0.1).
    #[serde(default)]
    pub mysqld_version_id: u64,
    /// Object type: "Table" or "Tablespace".
    #[serde(default)]
    pub dd_object_type: String,
    /// The data dictionary object.
    #[serde(default)]
    pub dd_object: DdTable,
}

/// Data dictionary table definition.
#[derive(Debug, Default, Deserialize)]
pub struct DdTable {
    /// Table name.
    #[serde(default)]
    pub name: String,
    /// Schema (database) name.
    #[serde(default)]
    pub schema_ref: String,
    /// Storage engine name.
    #[serde(default)]
    pub engine: String,
    /// Default collation ID.
    #[serde(default)]
    pub collation_id: u64,
    /// Row format code (1=FIXED, 2=DYNAMIC, 3=COMPRESSED, etc.).
    #[serde(default)]
    pub row_format: u64,
    /// Table comment.
    #[serde(default)]
    pub comment: String,
    /// Column definitions.
    #[serde(default)]
    pub columns: Vec<DdColumn>,
    /// Index definitions.
    #[serde(default)]
    pub indexes: Vec<DdIndex>,
    /// Foreign key definitions.
    #[serde(default)]
    pub foreign_keys: Vec<DdForeignKey>,
    /// MySQL server version ID.
    #[serde(default)]
    pub mysql_version_id: u64,
    /// Table hidden flag (1=visible for tables).
    #[serde(default)]
    pub hidden: u64,
}

/// Data dictionary column definition.
#[derive(Debug, Default, Deserialize)]
pub struct DdColumn {
    /// Column name.
    #[serde(default)]
    pub name: String,
    /// dd_type code (internal MySQL type enumeration).
    #[serde(rename = "type", default)]
    pub dd_type: u64,
    /// SQL type string from MySQL (e.g., "varchar(255)", "int unsigned").
    #[serde(default)]
    pub column_type_utf8: String,
    /// Position in the column list (1-based).
    #[serde(default)]
    pub ordinal_position: u64,
    /// Hidden flag: 1=visible, 2=SE-hidden (DB_TRX_ID, DB_ROLL_PTR, DB_ROW_ID).
    #[serde(default)]
    pub hidden: u64,
    /// Whether the column allows NULL.
    #[serde(default)]
    pub is_nullable: bool,
    /// Whether the column is unsigned.
    #[serde(default)]
    pub is_unsigned: bool,
    /// Whether the column is AUTO_INCREMENT.
    #[serde(default)]
    pub is_auto_increment: bool,
    /// Whether the column is virtual (generated).
    #[serde(default)]
    pub is_virtual: bool,
    /// Character length.
    #[serde(default)]
    pub char_length: u64,
    /// Numeric precision.
    #[serde(default)]
    pub numeric_precision: u64,
    /// Numeric scale.
    #[serde(default)]
    pub numeric_scale: u64,
    /// Datetime fractional seconds precision.
    #[serde(default)]
    pub datetime_precision: u64,
    /// Collation ID for this column.
    #[serde(default)]
    pub collation_id: u64,
    /// Default value as UTF-8 string.
    #[serde(default)]
    pub default_value_utf8: String,
    /// Whether default_value_utf8 is NULL.
    #[serde(default)]
    pub default_value_utf8_null: bool,
    /// Whether the column has no default.
    #[serde(default)]
    pub has_no_default: bool,
    /// Default option (e.g., "CURRENT_TIMESTAMP").
    #[serde(default)]
    pub default_option: String,
    /// Update option (e.g., "CURRENT_TIMESTAMP").
    #[serde(default)]
    pub update_option: String,
    /// Generation expression (raw).
    #[serde(default)]
    pub generation_expression: String,
    /// Generation expression as UTF-8.
    #[serde(default)]
    pub generation_expression_utf8: String,
    /// ENUM/SET value elements.
    #[serde(default)]
    pub elements: Vec<DdColumnElement>,
    /// Column comment.
    #[serde(default)]
    pub comment: String,
    /// Whether the column is zerofill.
    #[serde(default)]
    pub is_zerofill: bool,
}

/// ENUM or SET value element.
#[derive(Debug, Default, Deserialize)]
pub struct DdColumnElement {
    /// The element name (value string).
    #[serde(default)]
    pub name: String,
}

/// Data dictionary index definition.
#[derive(Debug, Default, Deserialize)]
pub struct DdIndex {
    /// Index name.
    #[serde(default)]
    pub name: String,
    /// Index type: 1=PRIMARY, 2=UNIQUE, 3=MULTIPLE (non-unique), 4=FULLTEXT, 5=SPATIAL.
    #[serde(rename = "type", default)]
    pub index_type: u64,
    /// Algorithm code (1=BTREE default, 2=BTREE explicit, 3=HASH, 4=RTREE, 5=FULLTEXT).
    #[serde(default)]
    pub algorithm: u64,
    /// Whether the index is hidden.
    #[serde(default)]
    pub hidden: bool,
    /// Index elements (columns).
    #[serde(default)]
    pub elements: Vec<DdIndexElement>,
    /// Index comment.
    #[serde(default)]
    pub comment: String,
    /// Whether the index is visible.
    #[serde(default)]
    pub is_visible: bool,
}

/// Data dictionary index element (column reference).
#[derive(Debug, Default, Deserialize)]
pub struct DdIndexElement {
    /// 0-based index into the columns array.
    #[serde(default)]
    pub column_opx: u64,
    /// Prefix length (4294967295 = full column).
    #[serde(default)]
    pub length: u64,
    /// Sort order: 2=ASC, 1=DESC.
    #[serde(default)]
    pub order: u64,
    /// Whether this element is hidden (internal).
    #[serde(default)]
    pub hidden: bool,
}

/// Data dictionary foreign key definition.
#[derive(Debug, Default, Deserialize)]
pub struct DdForeignKey {
    /// Constraint name.
    #[serde(default)]
    pub name: String,
    /// Referenced table's schema name.
    #[serde(default)]
    pub referenced_table_schema_name: String,
    /// Referenced table name.
    #[serde(default)]
    pub referenced_table_name: String,
    /// ON UPDATE rule (0=NO ACTION, 1=RESTRICT, 2=CASCADE, 3=SET NULL, 4=SET DEFAULT).
    #[serde(default)]
    pub update_rule: u64,
    /// ON DELETE rule (same codes as update_rule).
    #[serde(default)]
    pub delete_rule: u64,
    /// Foreign key elements (column mappings).
    #[serde(default)]
    pub elements: Vec<DdForeignKeyElement>,
}

/// Data dictionary foreign key element (column mapping).
#[derive(Debug, Default, Deserialize)]
pub struct DdForeignKeyElement {
    /// 0-based index into the table's columns array.
    #[serde(default)]
    pub column_opx: u64,
    /// Name of the referenced column.
    #[serde(default)]
    pub referenced_column_name: String,
}

// ---------------------------------------------------------------------------
// Output structs (for text rendering and JSON serialization)
// ---------------------------------------------------------------------------

/// Reconstructed table schema from SDI or page inference.
#[derive(Debug, Clone, Serialize)]
pub struct TableSchema {
    /// Database/schema name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_name: Option<String>,
    /// Table name.
    pub table_name: String,
    /// Storage engine.
    pub engine: String,
    /// Row format (DYNAMIC, COMPRESSED, etc.).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_format: Option<String>,
    /// Table collation name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
    /// Character set name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub charset: Option<String>,
    /// Table comment.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// MySQL version string.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mysql_version: Option<String>,
    /// Schema source: "sdi" or "inferred".
    pub source: String,
    /// Column definitions.
    pub columns: Vec<ColumnDef>,
    /// Index definitions.
    pub indexes: Vec<IndexDef>,
    /// Foreign key definitions.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub foreign_keys: Vec<ForeignKeyDef>,
    /// Reconstructed CREATE TABLE DDL.
    pub ddl: String,
}

/// Column definition for output.
#[derive(Debug, Clone, Serialize)]
pub struct ColumnDef {
    /// Column name.
    pub name: String,
    /// SQL type string (e.g., "varchar(100)").
    pub column_type: String,
    /// Whether the column allows NULL.
    pub is_nullable: bool,
    /// Default value expression (None if no default).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<String>,
    /// Whether the column is AUTO_INCREMENT.
    #[serde(skip_serializing_if = "is_false")]
    pub is_auto_increment: bool,
    /// Generation expression for virtual/stored generated columns.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation_expression: Option<String>,
    /// Whether the generated column is virtual (true) or stored (false).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_virtual: Option<bool>,
    /// Column comment.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

fn is_false(v: &bool) -> bool {
    !v
}

/// Index definition for output.
#[derive(Debug, Clone, Serialize)]
pub struct IndexDef {
    /// Index name.
    pub name: String,
    /// Index type: "PRIMARY KEY", "UNIQUE KEY", "KEY", "FULLTEXT KEY", "SPATIAL KEY".
    pub index_type: String,
    /// Index columns with optional prefix length and sort order.
    pub columns: Vec<IndexColumnDef>,
    /// Index comment.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// Whether the index is visible.
    #[serde(skip_serializing_if = "is_true")]
    pub is_visible: bool,
}

fn is_true(v: &bool) -> bool {
    *v
}

/// Column reference within an index.
#[derive(Debug, Clone, Serialize)]
pub struct IndexColumnDef {
    /// Column name.
    pub name: String,
    /// Prefix length (None for full column).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix_length: Option<u64>,
    /// Sort order ("ASC" or "DESC"). Omitted if ASC (default).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<String>,
}

/// Foreign key definition for output.
#[derive(Debug, Clone, Serialize)]
pub struct ForeignKeyDef {
    /// Constraint name.
    pub name: String,
    /// Columns in this table.
    pub columns: Vec<String>,
    /// Referenced schema.table.
    pub referenced_table: String,
    /// Referenced columns.
    pub referenced_columns: Vec<String>,
    /// ON UPDATE action.
    pub on_update: String,
    /// ON DELETE action.
    pub on_delete: String,
}

/// Inferred schema from pre-8.0 tablespaces (no SDI).
#[derive(Debug, Clone, Serialize)]
pub struct InferredSchema {
    /// Source description.
    pub source: String,
    /// Record format: "COMPACT" or "REDUNDANT".
    pub record_format: String,
    /// Detected indexes.
    pub indexes: Vec<InferredIndex>,
}

/// Inferred index from page scanning.
#[derive(Debug, Clone, Serialize)]
pub struct InferredIndex {
    /// Index ID from the INDEX page header.
    pub index_id: u64,
    /// Number of leaf pages.
    pub leaf_pages: u64,
    /// Maximum non-leaf level seen.
    pub max_level: u16,
}

// ---------------------------------------------------------------------------
// Collation and type mapping
// ---------------------------------------------------------------------------

/// Map common MySQL collation IDs to collation names.
///
/// Only covers the most widely-used collations. Returns None for unknown IDs.
///
/// # Examples
///
/// ```
/// use idb::innodb::schema::collation_name;
///
/// assert_eq!(collation_name(255), Some("utf8mb4_0900_ai_ci"));
/// assert_eq!(collation_name(63), Some("binary"));
/// assert_eq!(collation_name(45), Some("utf8mb4_general_ci"));
/// assert_eq!(collation_name(99999), None);
/// ```
pub fn collation_name(id: u64) -> Option<&'static str> {
    match id {
        2 => Some("latin1_swedish_ci"),
        8 => Some("latin1_swedish_ci"),
        11 => Some("ascii_general_ci"),
        33 => Some("utf8mb3_general_ci"),
        45 => Some("utf8mb4_general_ci"),
        46 => Some("utf8mb4_bin"),
        47 => Some("latin1_bin"),
        48 => Some("latin1_general_ci"),
        63 => Some("binary"),
        83 => Some("utf8mb3_bin"),
        224 => Some("utf8mb4_unicode_ci"),
        255 => Some("utf8mb4_0900_ai_ci"),
        _ => None,
    }
}

/// Map collation ID to character set name.
///
/// # Examples
///
/// ```
/// use idb::innodb::schema::charset_from_collation;
///
/// assert_eq!(charset_from_collation(255), Some("utf8mb4"));
/// assert_eq!(charset_from_collation(63), Some("binary"));
/// assert_eq!(charset_from_collation(8), Some("latin1"));
/// assert_eq!(charset_from_collation(99999), None);
/// ```
pub fn charset_from_collation(id: u64) -> Option<&'static str> {
    match id {
        2 | 8 | 47 | 48 => Some("latin1"),
        11 => Some("ascii"),
        33 | 83 => Some("utf8mb3"),
        45 | 46 | 224 | 255 => Some("utf8mb4"),
        63 => Some("binary"),
        _ => None,
    }
}

/// Map row_format code to name.
///
/// # Examples
///
/// ```
/// use idb::innodb::schema::row_format_name;
///
/// assert_eq!(row_format_name(1), "FIXED");
/// assert_eq!(row_format_name(2), "DYNAMIC");
/// assert_eq!(row_format_name(3), "COMPRESSED");
/// ```
pub fn row_format_name(id: u64) -> &'static str {
    match id {
        1 => "FIXED",
        2 => "DYNAMIC",
        3 => "COMPRESSED",
        4 => "REDUNDANT",
        5 => "COMPACT",
        _ => "UNKNOWN",
    }
}

/// Map foreign key rule code to SQL action string.
///
/// # Examples
///
/// ```
/// use idb::innodb::schema::fk_rule_name;
///
/// assert_eq!(fk_rule_name(0), "NO ACTION");
/// assert_eq!(fk_rule_name(2), "CASCADE");
/// ```
pub fn fk_rule_name(rule: u64) -> &'static str {
    match rule {
        0 => "NO ACTION",
        1 => "RESTRICT",
        2 => "CASCADE",
        3 => "SET NULL",
        4 => "SET DEFAULT",
        _ => "NO ACTION",
    }
}

/// Fallback type mapping from dd_type code when `column_type_utf8` is empty.
///
/// This should rarely be needed — `column_type_utf8` is the authoritative source.
///
/// # Examples
///
/// ```
/// use idb::innodb::schema::{DdColumn, dd_type_to_sql};
///
/// let col = DdColumn { dd_type: 4, numeric_precision: 10, ..Default::default() };
/// assert_eq!(dd_type_to_sql(&col), "int");
///
/// let col = DdColumn { dd_type: 16, char_length: 400, collation_id: 255, ..Default::default() };
/// assert_eq!(dd_type_to_sql(&col), "varchar(100)");
/// ```
pub fn dd_type_to_sql(col: &DdColumn) -> String {
    match col.dd_type {
        1 => "tinyint".to_string(),
        2 => "smallint".to_string(),
        3 => "mediumint".to_string(),
        4 => "int".to_string(),
        5 => "bigint".to_string(),
        6 => format_decimal(col),
        7 => "float".to_string(),
        8 => "double".to_string(),
        9 | 10 => "binary".to_string(), // internal types (ROLL_PTR, TRX_ID)
        11 => "year".to_string(),
        12 => "date".to_string(),
        13 => "time".to_string(),
        14 => "datetime".to_string(),
        15 => "timestamp".to_string(),
        16 => format_varchar(col),
        17 => format_char(col),
        18 => "bit".to_string(),
        19 => "enum".to_string(),
        20 => "set".to_string(),
        23 => "tinyblob".to_string(),
        24 => "mediumblob".to_string(),
        25 => "longblob".to_string(),
        26 => "blob".to_string(),
        27 => format_text(col),
        28 => "varbinary".to_string(),
        29 => "binary".to_string(),
        30 => "geometry".to_string(),
        31 => "json".to_string(),
        _ => format!("unknown_type({})", col.dd_type),
    }
}

fn format_decimal(col: &DdColumn) -> String {
    if col.numeric_precision > 0 {
        if col.numeric_scale > 0 {
            format!("decimal({},{})", col.numeric_precision, col.numeric_scale)
        } else {
            format!("decimal({})", col.numeric_precision)
        }
    } else {
        "decimal".to_string()
    }
}

fn format_varchar(col: &DdColumn) -> String {
    // char_length is in bytes; divide by max bytes per char for the charset
    let max_bytes_per_char = charset_max_bytes(col.collation_id);
    let char_len = if max_bytes_per_char > 0 {
        col.char_length / max_bytes_per_char
    } else {
        col.char_length
    };
    format!("varchar({})", char_len)
}

fn format_char(col: &DdColumn) -> String {
    let max_bytes_per_char = charset_max_bytes(col.collation_id);
    let char_len = if max_bytes_per_char > 0 {
        col.char_length / max_bytes_per_char
    } else {
        col.char_length
    };
    if char_len > 1 {
        format!("char({})", char_len)
    } else {
        "char".to_string()
    }
}

fn format_text(col: &DdColumn) -> String {
    match col.char_length {
        0..=255 => "tinytext".to_string(),
        256..=65535 => "text".to_string(),
        65536..=16777215 => "mediumtext".to_string(),
        _ => "longtext".to_string(),
    }
}

/// Returns max bytes per character for a collation ID.
fn charset_max_bytes(collation_id: u64) -> u64 {
    match collation_id {
        2 | 8 | 11 | 47 | 48 => 1,   // latin1, ascii
        33 | 83 => 3,                   // utf8mb3
        45 | 46 | 224 | 255 => 4,      // utf8mb4
        63 => 1,                        // binary
        _ => 4, // default to utf8mb4
    }
}

/// Format MySQL version from version_id (e.g., 90001 -> "9.0.1").
fn format_mysql_version(version_id: u64) -> String {
    if version_id == 0 {
        return "unknown".to_string();
    }
    let major = version_id / 10000;
    let minor = (version_id % 10000) / 100;
    let patch = version_id % 100;
    format!("{}.{}.{}", major, minor, patch)
}

// ---------------------------------------------------------------------------
// Schema extraction
// ---------------------------------------------------------------------------

/// Extract a [`TableSchema`] from raw SDI JSON (type=1 "Table" record).
///
/// Parses the JSON into typed structs, filters out hidden columns, builds
/// column/index/FK definitions, and generates `CREATE TABLE` DDL.
///
/// # Examples
///
/// ```
/// use idb::innodb::schema::extract_schema_from_sdi;
///
/// let json = r#"{
///   "mysqld_version_id": 90001,
///   "dd_object_type": "Table",
///   "dd_object": {
///     "name": "test_table",
///     "schema_ref": "mydb",
///     "engine": "InnoDB",
///     "collation_id": 255,
///     "row_format": 2,
///     "columns": [
///       {
///         "name": "id",
///         "type": 4,
///         "column_type_utf8": "int",
///         "ordinal_position": 1,
///         "hidden": 1,
///         "is_nullable": false,
///         "is_auto_increment": true
///       },
///       {
///         "name": "DB_TRX_ID",
///         "type": 10,
///         "ordinal_position": 2,
///         "hidden": 2
///       },
///       {
///         "name": "DB_ROLL_PTR",
///         "type": 9,
///         "ordinal_position": 3,
///         "hidden": 2
///       }
///     ],
///     "indexes": [
///       {
///         "name": "PRIMARY",
///         "type": 1,
///         "hidden": false,
///         "is_visible": true,
///         "elements": [
///           { "column_opx": 0, "hidden": false, "length": 4, "order": 2 }
///         ]
///       }
///     ],
///     "foreign_keys": []
///   }
/// }"#;
///
/// let schema = extract_schema_from_sdi(json).unwrap();
/// assert_eq!(schema.table_name, "test_table");
/// assert_eq!(schema.columns.len(), 1); // DB_TRX_ID and DB_ROLL_PTR filtered
/// assert!(schema.ddl.contains("CREATE TABLE"));
/// ```
pub fn extract_schema_from_sdi(sdi_json: &str) -> Result<TableSchema, IdbError> {
    let envelope: SdiEnvelope = serde_json::from_str(sdi_json)
        .map_err(|e| IdbError::Parse(format!("Failed to parse SDI JSON: {}", e)))?;

    let dd = &envelope.dd_object;

    // Build column name lookup (all columns, including hidden)
    let all_columns: Vec<&DdColumn> = {
        let mut cols: Vec<&DdColumn> = dd.columns.iter().collect();
        cols.sort_by_key(|c| c.ordinal_position);
        cols
    };

    // Build index from ordinal_position to column (for column_opx lookups)
    // column_opx is a 0-based index into the columns array as ordered in the JSON
    let column_by_index: HashMap<u64, &DdColumn> = dd
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| (i as u64, c))
        .collect();

    // Filter visible columns (hidden == 1 means visible in MySQL's dd enum)
    let visible_columns: Vec<&DdColumn> = {
        let mut cols: Vec<&DdColumn> = all_columns.iter().copied().filter(|c| c.hidden == 1).collect();
        cols.sort_by_key(|c| c.ordinal_position);
        cols
    };

    // Build ColumnDef list
    let columns: Vec<ColumnDef> = visible_columns
        .iter()
        .map(|c| build_column_def(c))
        .collect();

    // Build IndexDef list
    let indexes: Vec<IndexDef> = dd
        .indexes
        .iter()
        .filter(|idx| !idx.hidden)
        .map(|idx| build_index_def(idx, &column_by_index))
        .collect();

    // Build ForeignKeyDef list
    let foreign_keys: Vec<ForeignKeyDef> = dd
        .foreign_keys
        .iter()
        .map(|fk| build_fk_def(fk, &column_by_index))
        .collect();

    let row_fmt = row_format_name(dd.row_format);
    let coll = collation_name(dd.collation_id);
    let cs = charset_from_collation(dd.collation_id);
    let mysql_ver = format_mysql_version(envelope.mysqld_version_id);

    let schema_name = if dd.schema_ref.is_empty() {
        None
    } else {
        Some(dd.schema_ref.clone())
    };

    let comment = if dd.comment.is_empty() {
        None
    } else {
        Some(dd.comment.clone())
    };

    let mut schema = TableSchema {
        schema_name,
        table_name: dd.name.clone(),
        engine: dd.engine.clone(),
        row_format: Some(row_fmt.to_string()),
        collation: coll.map(|s| s.to_string()),
        charset: cs.map(|s| s.to_string()),
        comment,
        mysql_version: Some(mysql_ver),
        source: "sdi".to_string(),
        columns,
        indexes,
        foreign_keys,
        ddl: String::new(),
    };

    schema.ddl = generate_ddl(&schema);
    Ok(schema)
}

fn build_column_def(col: &DdColumn) -> ColumnDef {
    let column_type = if !col.column_type_utf8.is_empty() {
        col.column_type_utf8.clone()
    } else {
        dd_type_to_sql(col)
    };

    let default_value = if !col.default_option.is_empty() {
        // default_option has expressions like "CURRENT_TIMESTAMP"
        Some(col.default_option.clone())
    } else if !col.has_no_default && !col.default_value_utf8_null && !col.default_value_utf8.is_empty() {
        Some(format!("'{}'", col.default_value_utf8.replace('\'', "''")))
    } else if !col.has_no_default && col.is_nullable && col.default_value_utf8_null {
        Some("NULL".to_string())
    } else {
        None
    };

    let generation_expression = if !col.generation_expression_utf8.is_empty() {
        Some(col.generation_expression_utf8.clone())
    } else {
        None
    };

    let is_virtual = if generation_expression.is_some() {
        Some(col.is_virtual)
    } else {
        None
    };

    let comment = if col.comment.is_empty() {
        None
    } else {
        Some(col.comment.clone())
    };

    ColumnDef {
        name: col.name.clone(),
        column_type,
        is_nullable: col.is_nullable,
        default_value,
        is_auto_increment: col.is_auto_increment,
        generation_expression,
        is_virtual,
        comment,
    }
}

fn build_index_def(idx: &DdIndex, columns: &HashMap<u64, &DdColumn>) -> IndexDef {
    let index_type = match idx.index_type {
        1 => "PRIMARY KEY",
        2 => "UNIQUE KEY",
        3 => "KEY",
        4 => "FULLTEXT KEY",
        5 => "SPATIAL KEY",
        _ => "KEY",
    };

    let idx_columns: Vec<IndexColumnDef> = idx
        .elements
        .iter()
        .filter(|e| !e.hidden)
        .map(|e| {
            let col_name = columns
                .get(&e.column_opx)
                .map(|c| c.name.clone())
                .unwrap_or_else(|| format!("col_{}", e.column_opx));

            let prefix_length = if e.length < 4294967295 {
                // Check if this is a prefix index (length < full column length)
                let full_len = columns.get(&e.column_opx).map(|c| c.char_length).unwrap_or(0);
                let max_bytes = columns
                    .get(&e.column_opx)
                    .map(|c| charset_max_bytes(c.collation_id))
                    .unwrap_or(4);
                let full_char_len = if max_bytes > 0 { full_len / max_bytes } else { full_len };
                if e.length < full_char_len {
                    Some(e.length)
                } else {
                    None
                }
            } else {
                None
            };

            let order = if e.order == 1 {
                Some("DESC".to_string())
            } else {
                None
            };

            IndexColumnDef {
                name: col_name,
                prefix_length,
                order,
            }
        })
        .collect();

    let comment = if idx.comment.is_empty() {
        None
    } else {
        Some(idx.comment.clone())
    };

    IndexDef {
        name: idx.name.clone(),
        index_type: index_type.to_string(),
        columns: idx_columns,
        comment,
        is_visible: idx.is_visible,
    }
}

fn build_fk_def(fk: &DdForeignKey, columns: &HashMap<u64, &DdColumn>) -> ForeignKeyDef {
    let fk_columns: Vec<String> = fk
        .elements
        .iter()
        .map(|e| {
            columns
                .get(&e.column_opx)
                .map(|c| c.name.clone())
                .unwrap_or_else(|| format!("col_{}", e.column_opx))
        })
        .collect();

    let ref_columns: Vec<String> = fk
        .elements
        .iter()
        .map(|e| e.referenced_column_name.clone())
        .collect();

    let ref_table = if fk.referenced_table_schema_name.is_empty() {
        format!("`{}`", fk.referenced_table_name)
    } else {
        format!(
            "`{}`.`{}`",
            fk.referenced_table_schema_name, fk.referenced_table_name
        )
    };

    ForeignKeyDef {
        name: fk.name.clone(),
        columns: fk_columns,
        referenced_table: ref_table,
        referenced_columns: ref_columns,
        on_update: fk_rule_name(fk.update_rule).to_string(),
        on_delete: fk_rule_name(fk.delete_rule).to_string(),
    }
}

// ---------------------------------------------------------------------------
// DDL generation
// ---------------------------------------------------------------------------

/// Generate `CREATE TABLE` DDL from a [`TableSchema`].
///
/// Produces MySQL-compatible DDL including column definitions, primary key,
/// secondary indexes, foreign keys, and table options.
pub fn generate_ddl(schema: &TableSchema) -> String {
    let mut ddl = format!("CREATE TABLE `{}` (\n", schema.table_name);
    let mut parts: Vec<String> = Vec::new();

    // Columns
    for col in &schema.columns {
        parts.push(format_column_ddl(col));
    }

    // Indexes
    for idx in &schema.indexes {
        parts.push(format_index_ddl(idx));
    }

    // Foreign keys
    for fk in &schema.foreign_keys {
        parts.push(format_fk_ddl(fk));
    }

    ddl.push_str(&parts.join(",\n"));
    ddl.push_str("\n)");

    // Table options
    let mut options = Vec::new();
    options.push(format!("ENGINE={}", schema.engine));
    if let Some(ref cs) = schema.charset {
        options.push(format!("DEFAULT CHARSET={}", cs));
    }
    if let Some(ref coll) = schema.collation {
        options.push(format!("COLLATE={}", coll));
    }
    if let Some(ref fmt) = schema.row_format {
        if fmt != "DYNAMIC" {
            // DYNAMIC is the default, only show non-default
            options.push(format!("ROW_FORMAT={}", fmt));
        }
    }
    if let Some(ref comment) = schema.comment {
        options.push(format!("COMMENT='{}'", comment.replace('\'', "''")));
    }

    if !options.is_empty() {
        ddl.push(' ');
        ddl.push_str(&options.join(" "));
    }
    ddl.push(';');

    ddl
}

fn format_column_ddl(col: &ColumnDef) -> String {
    let mut parts = vec![format!("  `{}` {}", col.name, col.column_type)];

    if !col.is_nullable {
        parts.push("NOT NULL".to_string());
    }

    if let Some(ref default) = col.default_value {
        parts.push(format!("DEFAULT {}", default));
    }

    if col.is_auto_increment {
        parts.push("AUTO_INCREMENT".to_string());
    }

    if let Some(ref expr) = col.generation_expression {
        let stored_or_virtual = if col.is_virtual == Some(true) {
            "VIRTUAL"
        } else {
            "STORED"
        };
        parts.push(format!("GENERATED ALWAYS AS ({}) {}", expr, stored_or_virtual));
    }

    if let Some(ref comment) = col.comment {
        parts.push(format!("COMMENT '{}'", comment.replace('\'', "''")));
    }

    parts.join(" ")
}

fn format_index_ddl(idx: &IndexDef) -> String {
    let cols = format_index_columns(&idx.columns);

    let visibility = if !idx.is_visible {
        " /*!80000 INVISIBLE */"
    } else {
        ""
    };

    let comment = if let Some(ref c) = idx.comment {
        format!(" COMMENT '{}'", c.replace('\'', "''"))
    } else {
        String::new()
    };

    match idx.index_type.as_str() {
        "PRIMARY KEY" => format!("  PRIMARY KEY ({}){}{}", cols, comment, visibility),
        _ => format!(
            "  {} `{}` ({}){}{}", idx.index_type, idx.name, cols, comment, visibility
        ),
    }
}

fn format_index_columns(columns: &[IndexColumnDef]) -> String {
    columns
        .iter()
        .map(|c| {
            let mut s = format!("`{}`", c.name);
            if let Some(len) = c.prefix_length {
                s.push_str(&format!("({})", len));
            }
            if let Some(ref ord) = c.order {
                s.push(' ');
                s.push_str(ord);
            }
            s
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_fk_ddl(fk: &ForeignKeyDef) -> String {
    let cols = fk
        .columns
        .iter()
        .map(|c| format!("`{}`", c))
        .collect::<Vec<_>>()
        .join(", ");
    let ref_cols = fk
        .referenced_columns
        .iter()
        .map(|c| format!("`{}`", c))
        .collect::<Vec<_>>()
        .join(", ");

    let mut s = format!(
        "  CONSTRAINT `{}` FOREIGN KEY ({}) REFERENCES {} ({})",
        fk.name, cols, fk.referenced_table, ref_cols
    );

    if fk.on_delete != "NO ACTION" {
        s.push_str(&format!(" ON DELETE {}", fk.on_delete));
    }
    if fk.on_update != "NO ACTION" {
        s.push_str(&format!(" ON UPDATE {}", fk.on_update));
    }

    s
}

// ---------------------------------------------------------------------------
// Pre-8.0 inference
// ---------------------------------------------------------------------------

/// Infer basic schema information from INDEX page structure (pre-8.0 fallback).
///
/// Scans all pages in the tablespace, collects INDEX page metadata (index_id,
/// level, compact/redundant format), and returns a summary of detected indexes.
pub fn infer_schema_from_pages(
    ts: &mut crate::innodb::tablespace::Tablespace,
) -> Result<InferredSchema, IdbError> {
    use crate::innodb::index::IndexHeader;
    use crate::innodb::page::FilHeader;
    use crate::innodb::page_types::PageType;
    use std::collections::BTreeMap;

    let page_count = ts.page_count();
    let mut is_compact = true;
    let mut index_stats: BTreeMap<u64, (u64, u16)> = BTreeMap::new(); // index_id -> (leaf_count, max_level)

    for page_num in 0..page_count {
        let page_data = match ts.read_page(page_num) {
            Ok(d) => d,
            Err(_) => continue,
        };

        let header = match FilHeader::parse(&page_data) {
            Some(h) => h,
            None => continue,
        };

        if header.page_type != PageType::Index {
            continue;
        }

        let idx = match IndexHeader::parse(&page_data) {
            Some(h) => h,
            None => continue,
        };

        if !idx.is_compact() {
            is_compact = false;
        }

        let entry = index_stats.entry(idx.index_id).or_insert((0, 0));
        if idx.is_leaf() {
            entry.0 += 1;
        }
        if idx.level > entry.1 {
            entry.1 = idx.level;
        }
    }

    let indexes = index_stats
        .into_iter()
        .map(|(index_id, (leaf_pages, max_level))| InferredIndex {
            index_id,
            leaf_pages,
            max_level,
        })
        .collect();

    Ok(InferredSchema {
        source: "Inferred (no SDI metadata available)".to_string(),
        record_format: if is_compact { "COMPACT" } else { "REDUNDANT" }.to_string(),
        indexes,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collation_name() {
        assert_eq!(collation_name(255), Some("utf8mb4_0900_ai_ci"));
        assert_eq!(collation_name(63), Some("binary"));
        assert_eq!(collation_name(45), Some("utf8mb4_general_ci"));
        assert_eq!(collation_name(46), Some("utf8mb4_bin"));
        assert_eq!(collation_name(33), Some("utf8mb3_general_ci"));
        assert_eq!(collation_name(0), None);
    }

    #[test]
    fn test_charset_from_collation() {
        assert_eq!(charset_from_collation(255), Some("utf8mb4"));
        assert_eq!(charset_from_collation(63), Some("binary"));
        assert_eq!(charset_from_collation(8), Some("latin1"));
        assert_eq!(charset_from_collation(33), Some("utf8mb3"));
        assert_eq!(charset_from_collation(0), None);
    }

    #[test]
    fn test_row_format_name() {
        assert_eq!(row_format_name(1), "FIXED");
        assert_eq!(row_format_name(2), "DYNAMIC");
        assert_eq!(row_format_name(3), "COMPRESSED");
        assert_eq!(row_format_name(99), "UNKNOWN");
    }

    #[test]
    fn test_fk_rule_name() {
        assert_eq!(fk_rule_name(0), "NO ACTION");
        assert_eq!(fk_rule_name(1), "RESTRICT");
        assert_eq!(fk_rule_name(2), "CASCADE");
        assert_eq!(fk_rule_name(3), "SET NULL");
        assert_eq!(fk_rule_name(4), "SET DEFAULT");
    }

    #[test]
    fn test_dd_type_to_sql_int() {
        let col = DdColumn {
            dd_type: 4,
            numeric_precision: 10,
            ..Default::default()
        };
        assert_eq!(dd_type_to_sql(&col), "int");
    }

    #[test]
    fn test_dd_type_to_sql_varchar() {
        let col = DdColumn {
            dd_type: 16,
            char_length: 400,
            collation_id: 255, // utf8mb4 = 4 bytes/char
            ..Default::default()
        };
        assert_eq!(dd_type_to_sql(&col), "varchar(100)");
    }

    #[test]
    fn test_dd_type_to_sql_decimal() {
        let col = DdColumn {
            dd_type: 6,
            numeric_precision: 10,
            numeric_scale: 2,
            ..Default::default()
        };
        assert_eq!(dd_type_to_sql(&col), "decimal(10,2)");
    }

    #[test]
    fn test_dd_type_to_sql_text() {
        let col = DdColumn {
            dd_type: 27,
            char_length: 65535,
            ..Default::default()
        };
        assert_eq!(dd_type_to_sql(&col), "text");

        let col = DdColumn {
            dd_type: 27,
            char_length: 16777215,
            ..Default::default()
        };
        assert_eq!(dd_type_to_sql(&col), "mediumtext");
    }

    #[test]
    fn test_format_mysql_version() {
        assert_eq!(format_mysql_version(90001), "9.0.1");
        assert_eq!(format_mysql_version(80040), "8.0.40");
        assert_eq!(format_mysql_version(0), "unknown");
    }

    #[test]
    fn test_extract_schema_from_sdi_minimal() {
        let json = r#"{
            "mysqld_version_id": 90001,
            "dd_object_type": "Table",
            "dd_object": {
                "name": "users",
                "schema_ref": "myapp",
                "engine": "InnoDB",
                "collation_id": 255,
                "row_format": 2,
                "columns": [
                    {
                        "name": "id",
                        "type": 4,
                        "column_type_utf8": "int unsigned",
                        "ordinal_position": 1,
                        "hidden": 1,
                        "is_nullable": false,
                        "is_auto_increment": true
                    },
                    {
                        "name": "email",
                        "type": 16,
                        "column_type_utf8": "varchar(255)",
                        "ordinal_position": 2,
                        "hidden": 1,
                        "is_nullable": false
                    },
                    {
                        "name": "DB_TRX_ID",
                        "type": 10,
                        "ordinal_position": 3,
                        "hidden": 2
                    },
                    {
                        "name": "DB_ROLL_PTR",
                        "type": 9,
                        "ordinal_position": 4,
                        "hidden": 2
                    }
                ],
                "indexes": [
                    {
                        "name": "PRIMARY",
                        "type": 1,
                        "hidden": false,
                        "is_visible": true,
                        "elements": [
                            { "column_opx": 0, "hidden": false, "length": 4, "order": 2 },
                            { "column_opx": 2, "hidden": true, "length": 4294967295, "order": 2 },
                            { "column_opx": 3, "hidden": true, "length": 4294967295, "order": 2 }
                        ]
                    },
                    {
                        "name": "idx_email",
                        "type": 2,
                        "hidden": false,
                        "is_visible": true,
                        "elements": [
                            { "column_opx": 1, "hidden": false, "length": 4294967295, "order": 2 },
                            { "column_opx": 0, "hidden": true, "length": 4294967295, "order": 2 }
                        ]
                    }
                ],
                "foreign_keys": []
            }
        }"#;

        let schema = extract_schema_from_sdi(json).unwrap();
        assert_eq!(schema.table_name, "users");
        assert_eq!(schema.schema_name, Some("myapp".to_string()));
        assert_eq!(schema.engine, "InnoDB");
        assert_eq!(schema.source, "sdi");
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.columns[0].name, "id");
        assert_eq!(schema.columns[0].column_type, "int unsigned");
        assert!(schema.columns[0].is_auto_increment);
        assert_eq!(schema.columns[1].name, "email");
        assert_eq!(schema.columns[1].column_type, "varchar(255)");
        assert_eq!(schema.indexes.len(), 2);
        assert_eq!(schema.indexes[0].index_type, "PRIMARY KEY");
        assert_eq!(schema.indexes[0].columns.len(), 1); // hidden elements filtered
        assert_eq!(schema.indexes[1].index_type, "UNIQUE KEY");
        assert_eq!(schema.indexes[1].name, "idx_email");
        assert!(schema.ddl.contains("CREATE TABLE `users`"));
        assert!(schema.ddl.contains("`id` int unsigned NOT NULL AUTO_INCREMENT"));
        assert!(schema.ddl.contains("PRIMARY KEY (`id`)"));
        assert!(schema.ddl.contains("UNIQUE KEY `idx_email` (`email`)"));
    }

    #[test]
    fn test_extract_schema_with_fk() {
        let json = r#"{
            "mysqld_version_id": 80040,
            "dd_object_type": "Table",
            "dd_object": {
                "name": "orders",
                "schema_ref": "shop",
                "engine": "InnoDB",
                "collation_id": 255,
                "row_format": 2,
                "columns": [
                    {
                        "name": "id",
                        "type": 4,
                        "column_type_utf8": "int",
                        "ordinal_position": 1,
                        "hidden": 1,
                        "is_nullable": false,
                        "is_auto_increment": true
                    },
                    {
                        "name": "user_id",
                        "type": 4,
                        "column_type_utf8": "int",
                        "ordinal_position": 2,
                        "hidden": 1,
                        "is_nullable": false
                    }
                ],
                "indexes": [
                    {
                        "name": "PRIMARY",
                        "type": 1,
                        "hidden": false,
                        "is_visible": true,
                        "elements": [
                            { "column_opx": 0, "hidden": false, "length": 4, "order": 2 }
                        ]
                    }
                ],
                "foreign_keys": [
                    {
                        "name": "fk_orders_user",
                        "referenced_table_schema_name": "shop",
                        "referenced_table_name": "users",
                        "update_rule": 0,
                        "delete_rule": 2,
                        "elements": [
                            { "column_opx": 1, "referenced_column_name": "id" }
                        ]
                    }
                ]
            }
        }"#;

        let schema = extract_schema_from_sdi(json).unwrap();
        assert_eq!(schema.foreign_keys.len(), 1);
        let fk = &schema.foreign_keys[0];
        assert_eq!(fk.name, "fk_orders_user");
        assert_eq!(fk.columns, vec!["user_id"]);
        assert_eq!(fk.referenced_table, "`shop`.`users`");
        assert_eq!(fk.referenced_columns, vec!["id"]);
        assert_eq!(fk.on_delete, "CASCADE");
        assert_eq!(fk.on_update, "NO ACTION");
        assert!(schema.ddl.contains("CONSTRAINT `fk_orders_user` FOREIGN KEY (`user_id`) REFERENCES `shop`.`users` (`id`) ON DELETE CASCADE"));
    }

    #[test]
    fn test_extract_schema_with_generated_column() {
        let json = r#"{
            "mysqld_version_id": 80040,
            "dd_object_type": "Table",
            "dd_object": {
                "name": "products",
                "schema_ref": "shop",
                "engine": "InnoDB",
                "collation_id": 255,
                "row_format": 2,
                "columns": [
                    {
                        "name": "price",
                        "type": 6,
                        "column_type_utf8": "decimal(10,2)",
                        "ordinal_position": 1,
                        "hidden": 1,
                        "is_nullable": false
                    },
                    {
                        "name": "tax",
                        "type": 6,
                        "column_type_utf8": "decimal(10,2)",
                        "ordinal_position": 2,
                        "hidden": 1,
                        "is_nullable": true,
                        "is_virtual": true,
                        "generation_expression_utf8": "`price` * 0.1"
                    }
                ],
                "indexes": [],
                "foreign_keys": []
            }
        }"#;

        let schema = extract_schema_from_sdi(json).unwrap();
        assert_eq!(schema.columns.len(), 2);
        let tax = &schema.columns[1];
        assert_eq!(tax.generation_expression, Some("`price` * 0.1".to_string()));
        assert_eq!(tax.is_virtual, Some(true));
        assert!(schema.ddl.contains("GENERATED ALWAYS AS (`price` * 0.1) VIRTUAL"));
    }

    #[test]
    fn test_ddl_generation_table_options() {
        let schema = TableSchema {
            schema_name: Some("mydb".to_string()),
            table_name: "test".to_string(),
            engine: "InnoDB".to_string(),
            row_format: Some("COMPRESSED".to_string()),
            collation: Some("utf8mb4_0900_ai_ci".to_string()),
            charset: Some("utf8mb4".to_string()),
            comment: None,
            mysql_version: Some("8.0.40".to_string()),
            source: "sdi".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                column_type: "int".to_string(),
                is_nullable: false,
                default_value: None,
                is_auto_increment: true,
                generation_expression: None,
                is_virtual: None,
                comment: None,
            }],
            indexes: vec![IndexDef {
                name: "PRIMARY".to_string(),
                index_type: "PRIMARY KEY".to_string(),
                columns: vec![IndexColumnDef {
                    name: "id".to_string(),
                    prefix_length: None,
                    order: None,
                }],
                comment: None,
                is_visible: true,
            }],
            foreign_keys: vec![],
            ddl: String::new(),
        };

        let ddl = generate_ddl(&schema);
        assert!(ddl.contains("ENGINE=InnoDB"));
        assert!(ddl.contains("DEFAULT CHARSET=utf8mb4"));
        assert!(ddl.contains("COLLATE=utf8mb4_0900_ai_ci"));
        assert!(ddl.contains("ROW_FORMAT=COMPRESSED"));
    }

    #[test]
    fn test_hidden_column_filtering() {
        let json = r#"{
            "mysqld_version_id": 90001,
            "dd_object_type": "Table",
            "dd_object": {
                "name": "t",
                "engine": "InnoDB",
                "collation_id": 255,
                "row_format": 2,
                "columns": [
                    { "name": "a", "type": 4, "column_type_utf8": "int", "ordinal_position": 1, "hidden": 1 },
                    { "name": "b", "type": 4, "column_type_utf8": "int", "ordinal_position": 2, "hidden": 1 },
                    { "name": "DB_TRX_ID", "type": 10, "ordinal_position": 3, "hidden": 2 },
                    { "name": "DB_ROLL_PTR", "type": 9, "ordinal_position": 4, "hidden": 2 },
                    { "name": "DB_ROW_ID", "type": 10, "ordinal_position": 5, "hidden": 2 }
                ],
                "indexes": [],
                "foreign_keys": []
            }
        }"#;

        let schema = extract_schema_from_sdi(json).unwrap();
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.columns[0].name, "a");
        assert_eq!(schema.columns[1].name, "b");
    }

    #[test]
    fn test_nullable_column_default_null() {
        let json = r#"{
            "mysqld_version_id": 90001,
            "dd_object_type": "Table",
            "dd_object": {
                "name": "t",
                "engine": "InnoDB",
                "collation_id": 255,
                "row_format": 2,
                "columns": [
                    {
                        "name": "notes",
                        "type": 16,
                        "column_type_utf8": "varchar(255)",
                        "ordinal_position": 1,
                        "hidden": 1,
                        "is_nullable": true,
                        "has_no_default": false,
                        "default_value_utf8": "",
                        "default_value_utf8_null": true
                    }
                ],
                "indexes": [],
                "foreign_keys": []
            }
        }"#;

        let schema = extract_schema_from_sdi(json).unwrap();
        assert_eq!(schema.columns[0].default_value, Some("NULL".to_string()));
        assert!(schema.ddl.contains("DEFAULT NULL"));
    }

    #[test]
    fn test_index_desc_order() {
        let json = r#"{
            "mysqld_version_id": 80040,
            "dd_object_type": "Table",
            "dd_object": {
                "name": "t",
                "engine": "InnoDB",
                "collation_id": 255,
                "row_format": 2,
                "columns": [
                    { "name": "a", "type": 4, "column_type_utf8": "int", "ordinal_position": 1, "hidden": 1 },
                    { "name": "b", "type": 4, "column_type_utf8": "int", "ordinal_position": 2, "hidden": 1 }
                ],
                "indexes": [
                    {
                        "name": "idx_b_desc",
                        "type": 3,
                        "hidden": false,
                        "is_visible": true,
                        "elements": [
                            { "column_opx": 1, "hidden": false, "length": 4294967295, "order": 1 }
                        ]
                    }
                ],
                "foreign_keys": []
            }
        }"#;

        let schema = extract_schema_from_sdi(json).unwrap();
        assert_eq!(schema.indexes[0].columns[0].order, Some("DESC".to_string()));
        assert!(schema.ddl.contains("`b` DESC"));
    }
}
