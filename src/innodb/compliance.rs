//! GDPR / compliance verification: deletion verification and data-residue scanning.
//!
//! This is the inverse of [`crate::innodb::undelete`]. Instead of recovering data
//! that still lingers, `compliance` answers "has this value been purged from every
//! InnoDB-retained location in this file?" and reports every place it still appears.
//!
//! Two strategies, deliberately kept separate:
//!
//! 1. **Deletion verification** ([`verify_deleted`]) — decode-and-compare over real
//!    record structures (live clustered records, delete-marked records, free-list
//!    records, and undo `DEL_MARK` entries). Type-aware and authoritative for "is
//!    this value still reachable as a record field". Blind to torn/overwritten bytes
//!    in slack space.
//! 2. **Residue scanning** ([`scan_residue`]) — a raw literal byte-pattern sweep over
//!    every page region including free/slack space. Catches residue a record-structure
//!    scan cannot see, but cannot attribute a match to a column/row.
//!
//! `verify_deleted` runs the logical pass by default; passing `thorough=true` adds the
//! raw byte pass over the encoded form of the target value.
//!
//! # Scope honesty
//!
//! This verifies residue within the tablespace file(s) passed in. It cannot see the OS
//! page cache, replicas, other backups, or binary-log archives. It reports byte- and
//! record-level residue; it does not certify legal compliance.

use serde::Serialize;

use crate::innodb::export::{decode_page_records, extract_column_layout, extract_table_name};
use crate::innodb::field_decode::{ColumnStorageInfo, FieldValue};
use crate::innodb::index::IndexHeader;
use crate::innodb::page::FilHeader;
use crate::innodb::page_types::PageType;
use crate::innodb::tablespace::Tablespace;
use crate::innodb::undelete::scan_free_list_records;
use crate::IdbError;

// ---------------------------------------------------------------------------
// Pattern
// ---------------------------------------------------------------------------

/// A literal byte needle to search for across page bytes.
///
/// No regex: the core library stays dependency-free and WASM-lean. `--pattern`
/// accepts UTF-8 text (matched as its UTF-8 bytes) or a `hex:` prefix for raw bytes.
#[derive(Debug, Clone)]
pub enum Pattern {
    /// Literal byte sequence to match.
    Bytes(Vec<u8>),
}

impl Pattern {
    /// Parse a user-supplied `--pattern` string.
    ///
    /// A `hex:` prefix decodes the remainder as hex (even number of hex digits);
    /// anything else is taken as UTF-8 text and matched as its raw bytes.
    pub fn parse(s: &str) -> Result<Self, IdbError> {
        if let Some(hex) = s.strip_prefix("hex:") {
            let hex: String = hex.chars().filter(|c| !c.is_whitespace()).collect();
            if hex.is_empty() || (hex.len() & 1) != 0 {
                return Err(IdbError::Argument(
                    "hex: pattern must have an even number of hex digits".to_string(),
                ));
            }
            let mut bytes = Vec::with_capacity(hex.len() / 2);
            let hb = hex.as_bytes();
            let mut i = 0;
            while i < hb.len() {
                let pair = std::str::from_utf8(&hb[i..i + 2])
                    .ok()
                    .and_then(|p| u8::from_str_radix(p, 16).ok());
                match pair {
                    Some(b) => bytes.push(b),
                    None => {
                        return Err(IdbError::Argument(format!(
                            "invalid hex byte '{}' in pattern",
                            &hex[i..i + 2]
                        )))
                    }
                }
                i += 2;
            }
            Ok(Pattern::Bytes(bytes))
        } else if s.is_empty() {
            Err(IdbError::Argument("pattern must not be empty".to_string()))
        } else {
            Ok(Pattern::Bytes(s.as_bytes().to_vec()))
        }
    }

    fn needle(&self) -> &[u8] {
        match self {
            Pattern::Bytes(b) => b,
        }
    }
}

/// Find every start offset of `needle` within `haystack` (overlapping matches).
fn find_all(haystack: &[u8], needle: &[u8], out: &mut Vec<usize>, cap: usize) {
    if needle.is_empty() || needle.len() > haystack.len() {
        return;
    }
    let mut i = 0;
    let last = haystack.len() - needle.len();
    while i <= last {
        if &haystack[i..i + needle.len()] == needle {
            out.push(i);
            if out.len() >= cap {
                return;
            }
            i += 1;
        } else {
            i += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// Residue scanning (#175)
// ---------------------------------------------------------------------------

/// Where within a page a residue match landed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Region {
    /// FIL header (first 38 bytes).
    FilHeader,
    /// FIL trailer (last 8 bytes).
    FilTrailer,
    /// Live record heap of an INDEX page (below `heap_top`).
    RecordHeap,
    /// Free / slack space of an INDEX page (at or above `heap_top`).
    FreeSpace,
    /// Page body of a non-INDEX page (unclassified).
    Body,
}

impl Region {
    /// Stable lowercase name for CSV / text output.
    pub fn name(self) -> &'static str {
        match self {
            Region::FilHeader => "fil_header",
            Region::FilTrailer => "fil_trailer",
            Region::RecordHeap => "record_heap",
            Region::FreeSpace => "free_space",
            Region::Body => "body",
        }
    }
}

/// A single raw byte-pattern match.
#[derive(Debug, Clone, Serialize)]
pub struct ResidueMatch {
    /// Page number the match was found in.
    pub page_number: u64,
    /// Page type name (e.g. "INDEX", "UNDO_LOG").
    pub page_type: String,
    /// Byte offset of the match within the page.
    pub offset: usize,
    /// Region classification within the page.
    pub region: Region,
    /// Up to 32 bytes of surrounding context, hex-encoded.
    pub context_hex: String,
}

/// Classify which region of a page an offset falls in.
fn classify_region(
    page_data: &[u8],
    offset: usize,
    page_type: PageType,
    page_size: usize,
) -> Region {
    if offset < crate::innodb::constants::FIL_PAGE_DATA {
        return Region::FilHeader;
    }
    if page_size >= crate::innodb::constants::SIZE_FIL_TRAILER
        && offset >= page_size - crate::innodb::constants::SIZE_FIL_TRAILER
    {
        return Region::FilTrailer;
    }
    if page_type == PageType::Index {
        if let Some(idx) = IndexHeader::parse(page_data) {
            if idx.heap_top != 0 && offset >= idx.heap_top as usize {
                return Region::FreeSpace;
            }
            return Region::RecordHeap;
        }
    }
    Region::Body
}

/// Scan every page of a tablespace for a literal byte pattern.
///
/// Returns up to `max_hits` matches across all page regions. This is the raw pass:
/// it sees slack space and non-record bytes a decode-based scan cannot.
pub fn scan_residue(
    ts: &mut Tablespace,
    pattern: &Pattern,
    max_hits: usize,
) -> Result<Vec<ResidueMatch>, IdbError> {
    let needle = pattern.needle().to_vec();
    let page_size = ts.page_size() as usize;
    let mut matches = Vec::new();

    ts.for_each_page(|page_num, page_data| {
        if matches.len() >= max_hits {
            return Ok(());
        }
        let page_type = FilHeader::parse(page_data)
            .map(|h| h.page_type)
            .unwrap_or(PageType::Unknown(0));

        let mut offsets = Vec::new();
        let remaining = max_hits - matches.len();
        find_all(page_data, &needle, &mut offsets, remaining);

        for off in offsets {
            let region = classify_region(page_data, off, page_type, page_size);
            let ctx_end = (off + 32).min(page_data.len());
            let context_hex = page_data[off..ctx_end]
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>();
            matches.push(ResidueMatch {
                page_number: page_num,
                page_type: page_type.name().to_string(),
                offset: off,
                region,
                context_hex,
            });
        }
        Ok(())
    })?;

    Ok(matches)
}

// ---------------------------------------------------------------------------
// Deletion verification (#176)
// ---------------------------------------------------------------------------

/// A single place a target value still appears.
#[derive(Debug, Clone, Serialize)]
pub struct ResidueSite {
    /// How the value was found: `live_record`, `delete_marked`, `free_list`,
    /// `undo_del_mark`, or `raw_slack`.
    pub region: String,
    /// Page number.
    pub page_number: u64,
    /// Byte offset within the page (0 when the source does not expose one).
    pub offset: usize,
    /// Whether the containing record is delete-marked.
    pub delete_marked: bool,
    /// Approximate transaction id, when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trx_id: Option<u64>,
}

/// Result of a deletion-verification scan.
#[derive(Debug, Clone, Serialize)]
pub struct DeletionReport {
    /// Table name from SDI, if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_name: Option<String>,
    /// Column that was checked.
    pub column: String,
    /// Target value (as supplied).
    pub target_value: String,
    /// True when no residue site was found anywhere scanned.
    pub fully_purged: bool,
    /// Every place the value still appears.
    pub residue_sites: Vec<ResidueSite>,
    /// Logical regions that were scanned (for honesty about coverage).
    pub regions_scanned: Vec<String>,
    /// Whether the raw byte pass ran.
    pub thorough: bool,
    /// Total records examined during the logical pass.
    pub records_examined: usize,
}

/// Render a decoded field value as a canonical comparison string.
fn field_display(val: &FieldValue) -> String {
    match val {
        FieldValue::Null => String::new(),
        FieldValue::Int(n) => n.to_string(),
        FieldValue::Uint(n) => n.to_string(),
        FieldValue::Float(f) => f.to_string(),
        FieldValue::Double(d) => d.to_string(),
        FieldValue::Str(s) => s.clone(),
        FieldValue::Hex(h) => h.clone(),
    }
}

/// Compare a decoded value against a target string (trailing-whitespace-insensitive,
/// to absorb CHAR space padding). NULL never matches a non-empty target.
fn value_matches(val: &FieldValue, target: &str) -> bool {
    if matches!(val, FieldValue::Null) {
        return target.is_empty();
    }
    field_display(val).trim_end() == target.trim_end()
}

/// Return the PK columns (leading user columns that precede the first system
/// column in the physical layout produced by `build_column_layout`).
fn pk_columns(columns: &[ColumnStorageInfo]) -> Vec<ColumnStorageInfo> {
    let mut pk = Vec::new();
    for col in columns {
        if col.is_system_column {
            break;
        }
        pk.push(col.clone());
    }
    pk
}

/// Produce literal byte needles for the raw pass, given a column and target value.
///
/// Always includes the UTF-8 bytes of the target (covers string/text columns). For
/// integer columns, additionally encodes the value the way InnoDB stores it
/// (big-endian, high bit XOR'd for signed) so numeric residue is found in slack space.
fn encode_value_needles(col: &ColumnStorageInfo, target: &str) -> Vec<Vec<u8>> {
    let mut needles: Vec<Vec<u8>> = Vec::new();
    if !target.is_empty() {
        needles.push(target.as_bytes().to_vec());
    }

    let width = col.fixed_len;
    let is_int = matches!(col.dd_type, 2 | 3 | 4 | 5 | 9); // TINY/SHORT/LONG/INT24/LONGLONG
    if is_int && (1..=8).contains(&width) {
        if col.is_unsigned {
            if let Ok(v) = target.parse::<u64>() {
                let be = v.to_be_bytes();
                needles.push(be[8 - width..].to_vec());
            }
        } else if let Ok(v) = target.parse::<i64>() {
            // InnoDB flips the sign bit for memcmp ordering.
            let be = v.to_be_bytes();
            let mut enc = be[8 - width..].to_vec();
            if let Some(first) = enc.first_mut() {
                *first ^= 0x80;
            }
            needles.push(enc);
        }
    }

    // Deduplicate identical needles.
    needles.sort();
    needles.dedup();
    needles
}

/// Verify that `target_value` in column `column` has been purged from every
/// InnoDB-retained location in `ts`.
///
/// Runs the logical decode-and-compare pass over clustered-index leaf pages
/// (live + delete-marked + free-list records) and undo `DEL_MARK` entries. When
/// `thorough` is set, also runs a raw byte pass over the encoded value.
pub fn verify_deleted(
    ts: &mut Tablespace,
    column: &str,
    target_value: &str,
    thorough: bool,
) -> Result<DeletionReport, IdbError> {
    let table_name = extract_table_name(ts);

    let (columns, clustered_index_id) = extract_column_layout(ts).ok_or_else(|| {
        IdbError::Parse(
            "Cannot extract column layout from SDI (pre-8.0 tablespace or missing SDI)".to_string(),
        )
    })?;

    // Resolve and validate the target column.
    let target_col = columns
        .iter()
        .find(|c| !c.is_system_column && c.name.eq_ignore_ascii_case(column))
        .cloned()
        .ok_or_else(|| {
            IdbError::Argument(format!(
                "Column '{}' not found in table (available: {})",
                column,
                columns
                    .iter()
                    .filter(|c| !c.is_system_column)
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })?;

    let page_size = ts.page_size();
    let pk_cols = pk_columns(&columns);
    let target_is_pk = pk_cols.iter().any(|c| c.name.eq_ignore_ascii_case(column));

    let mut sites: Vec<ResidueSite> = Vec::new();
    let mut records_examined = 0usize;

    // Collect clustered leaf pages first (the callback borrows ts immutably).
    let mut leaf_pages: Vec<(u64, Vec<u8>)> = Vec::new();
    ts.for_each_page(|pn, pdata| {
        let hdr = match FilHeader::parse(pdata) {
            Some(h) => h,
            None => return Ok(()),
        };
        if hdr.page_type != PageType::Index {
            return Ok(());
        }
        let idx = match IndexHeader::parse(pdata) {
            Some(h) => h,
            None => return Ok(()),
        };
        if idx.index_id != clustered_index_id || !idx.is_leaf() {
            return Ok(());
        }
        leaf_pages.push((pn, pdata.to_vec()));
        Ok(())
    })?;

    for (pn, pdata) in &leaf_pages {
        // Live records.
        for row in decode_page_records(pdata, &columns, false, false, page_size) {
            records_examined += 1;
            if let Some((_, val)) = row.iter().find(|(n, _)| n.eq_ignore_ascii_case(column)) {
                if value_matches(val, target_value) {
                    sites.push(ResidueSite {
                        region: "live_record".to_string(),
                        page_number: *pn,
                        offset: 0,
                        delete_marked: false,
                        trx_id: None,
                    });
                }
            }
        }

        // Delete-marked records (include system cols to recover the trx id).
        for row in decode_page_records(pdata, &columns, true, true, page_size) {
            records_examined += 1;
            let matched = row
                .iter()
                .find(|(n, _)| n.eq_ignore_ascii_case(column))
                .map(|(_, v)| value_matches(v, target_value))
                .unwrap_or(false);
            if matched {
                let trx_id = row.iter().find_map(|(n, v)| {
                    if n == "DB_TRX_ID" {
                        match v {
                            FieldValue::Uint(x) => Some(*x),
                            FieldValue::Int(x) => Some(*x as u64),
                            _ => None,
                        }
                    } else {
                        None
                    }
                });
                sites.push(ResidueSite {
                    region: "delete_marked".to_string(),
                    page_number: *pn,
                    offset: 0,
                    delete_marked: true,
                    trx_id,
                });
            }
        }

        // Free-list records (purged from the active chain but not overwritten).
        for rec in scan_free_list_records(pdata, *pn, &columns, page_size) {
            records_examined += 1;
            if rec
                .columns
                .iter()
                .find(|(n, _)| n.eq_ignore_ascii_case(column))
                .map(|(_, v)| value_matches(v, target_value))
                .unwrap_or(false)
            {
                sites.push(ResidueSite {
                    region: "free_list".to_string(),
                    page_number: *pn,
                    offset: rec.offset,
                    delete_marked: false,
                    trx_id: rec.trx_id,
                });
            }
        }
    }

    let mut regions_scanned = vec![
        "clustered_leaf_live".to_string(),
        "clustered_leaf_delete_marked".to_string(),
        "free_list".to_string(),
    ];

    // Undo DEL_MARK scan — only meaningful when the target is a PK column (undo
    // stores PK fields for deletes). Undo pages live in ibdata1/undo tablespaces;
    // on a bare .ibd this simply finds nothing.
    if target_is_pk {
        regions_scanned.push("undo_del_mark".to_string());
        if let Some(table_id) = crate::innodb::undelete::extract_table_id(ts) {
            let undo_recs = crate::innodb::undelete::scan_undo_for_deletes(ts, table_id, &pk_cols)?;
            for rec in undo_recs {
                if rec
                    .columns
                    .iter()
                    .find(|(n, _)| n.eq_ignore_ascii_case(column))
                    .map(|(_, v)| value_matches(v, target_value))
                    .unwrap_or(false)
                {
                    sites.push(ResidueSite {
                        region: "undo_del_mark".to_string(),
                        page_number: rec.page_number,
                        offset: rec.offset,
                        delete_marked: true,
                        trx_id: rec.trx_id,
                    });
                }
            }
        }
    }

    // Thorough: raw byte pass over the encoded value across all page regions.
    if thorough {
        regions_scanned.push("raw_page_bytes".to_string());
        for needle in encode_value_needles(&target_col, target_value) {
            let pat = Pattern::Bytes(needle);
            for m in scan_residue(ts, &pat, 10_000)? {
                sites.push(ResidueSite {
                    region: "raw_slack".to_string(),
                    page_number: m.page_number,
                    offset: m.offset,
                    delete_marked: false,
                    trx_id: None,
                });
            }
        }
    }

    Ok(DeletionReport {
        table_name,
        column: target_col.name,
        target_value: target_value.to_string(),
        fully_purged: sites.is_empty(),
        residue_sites: sites,
        regions_scanned,
        thorough,
        records_examined,
    })
}

// ---------------------------------------------------------------------------
// Encryption audit (#177 --encryption-audit)
// ---------------------------------------------------------------------------

/// Result of an encryption audit.
#[derive(Debug, Clone, Serialize)]
pub struct EncryptionAuditReport {
    /// Whether the tablespace declares encryption in its FSP flags.
    pub tablespace_encrypted: bool,
    /// Encryption algorithm string (from the FSP encryption info), if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub algorithm: Option<String>,
    /// Whether a decryption key/context is available for this tablespace.
    pub key_available: bool,
    /// Count of pages whose FIL page type indicates encrypted content.
    pub encrypted_page_count: u64,
    /// Total pages inspected.
    pub total_pages: u64,
}

/// Audit which pages of a tablespace are encrypted and whether a key is available.
pub fn encryption_audit(ts: &mut Tablespace) -> Result<EncryptionAuditReport, IdbError> {
    let tablespace_encrypted = ts.is_encrypted();
    let key_available = ts.encryption_info().is_some();
    let algorithm = if tablespace_encrypted {
        let flags = ts.fsp_header().map(|h| h.flags).unwrap_or(0);
        match crate::innodb::encryption::detect_encryption(flags, Some(ts.vendor_info())) {
            crate::innodb::encryption::EncryptionAlgorithm::Aes => Some("AES".to_string()),
            crate::innodb::encryption::EncryptionAlgorithm::None => Some("unknown".to_string()),
        }
    } else {
        None
    };

    let mut encrypted_page_count = 0u64;
    let mut total_pages = 0u64;

    ts.for_each_page(|_pn, page_data| {
        total_pages += 1;
        if let Some(hdr) = FilHeader::parse(page_data) {
            if matches!(
                hdr.page_type,
                PageType::Encrypted | PageType::CompressedEncrypted | PageType::EncryptedRtree
            ) {
                encrypted_page_count += 1;
            }
        }
        Ok(())
    })?;

    Ok(EncryptionAuditReport {
        tablespace_encrypted,
        algorithm,
        key_available,
        encrypted_page_count,
        total_pages,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::innodb::field_decode::FieldValue;

    #[test]
    fn pattern_parse_utf8() {
        let p = Pattern::parse("alice@example.com").unwrap();
        assert_eq!(p.needle(), b"alice@example.com");
    }

    #[test]
    fn pattern_parse_hex() {
        let p = Pattern::parse("hex:00ffca").unwrap();
        assert_eq!(p.needle(), &[0x00, 0xff, 0xca]);
    }

    #[test]
    fn pattern_parse_hex_odd_rejected() {
        assert!(Pattern::parse("hex:0ff").is_err());
    }

    #[test]
    fn pattern_parse_empty_rejected() {
        assert!(Pattern::parse("").is_err());
    }

    #[test]
    fn find_all_overlapping() {
        let mut out = Vec::new();
        find_all(b"aaaa", b"aa", &mut out, 100);
        assert_eq!(out, vec![0, 1, 2]);
    }

    #[test]
    fn find_all_respects_cap() {
        let mut out = Vec::new();
        find_all(b"aaaa", b"a", &mut out, 2);
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn find_all_no_match() {
        let mut out = Vec::new();
        find_all(b"abcdef", b"xyz", &mut out, 100);
        assert!(out.is_empty());
    }

    #[test]
    fn value_matches_trailing_ws() {
        assert!(value_matches(&FieldValue::Str("bob   ".into()), "bob"));
        assert!(value_matches(&FieldValue::Int(42), "42"));
        assert!(!value_matches(&FieldValue::Int(42), "43"));
    }

    #[test]
    fn value_matches_null() {
        assert!(value_matches(&FieldValue::Null, ""));
        assert!(!value_matches(&FieldValue::Null, "x"));
    }

    #[test]
    fn region_names_stable() {
        assert_eq!(Region::FreeSpace.name(), "free_space");
        assert_eq!(Region::RecordHeap.name(), "record_heap");
    }

    #[test]
    fn encode_needles_signed_int_xor() {
        // A 4-byte signed INT column, value 1: BE 0x00000001, sign bit flipped -> 0x80000001.
        let col = ColumnStorageInfo {
            name: "id".into(),
            dd_type: 4,
            column_type: "int".into(),
            is_nullable: false,
            is_unsigned: false,
            fixed_len: 4,
            is_variable: false,
            charset_max_bytes: 0,
            datetime_precision: 0,
            is_system_column: false,
            elements: vec![],
            numeric_precision: 0,
            numeric_scale: 0,
        };
        let needles = encode_value_needles(&col, "1");
        assert!(needles.contains(&vec![0x80, 0x00, 0x00, 0x01]));
        assert!(needles.contains(&b"1".to_vec())); // utf8 form always present
    }

    #[test]
    fn encode_needles_unsigned_int() {
        let col = ColumnStorageInfo {
            name: "n".into(),
            dd_type: 9,
            column_type: "bigint unsigned".into(),
            is_nullable: false,
            is_unsigned: true,
            fixed_len: 8,
            is_variable: false,
            charset_max_bytes: 0,
            datetime_precision: 0,
            is_system_column: false,
            elements: vec![],
            numeric_precision: 0,
            numeric_scale: 0,
        };
        let needles = encode_value_needles(&col, "255");
        assert!(needles.contains(&vec![0, 0, 0, 0, 0, 0, 0, 0xff]));
    }

    #[test]
    fn pk_columns_stop_at_system() {
        let mk = |name: &str, sys: bool| ColumnStorageInfo {
            name: name.into(),
            dd_type: 4,
            column_type: "int".into(),
            is_nullable: false,
            is_unsigned: false,
            fixed_len: 4,
            is_variable: false,
            charset_max_bytes: 0,
            datetime_precision: 0,
            is_system_column: sys,
            elements: vec![],
            numeric_precision: 0,
            numeric_scale: 0,
        };
        let cols = vec![
            mk("id", false),
            mk("DB_TRX_ID", true),
            mk("DB_ROLL_PTR", true),
            mk("email", false),
        ];
        let pk = pk_columns(&cols);
        assert_eq!(pk.len(), 1);
        assert_eq!(pk[0].name, "id");
    }
}
