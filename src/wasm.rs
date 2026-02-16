//! WebAssembly bindings for InnoDB file analysis.
//!
//! Each exported function accepts raw file bytes as `&[u8]` (via wasm-bindgen)
//! and returns a JSON string with the analysis results. These are thin wrappers
//! over the same library code used by the CLI subcommands.

use serde::Serialize;
use wasm_bindgen::prelude::*;

use crate::innodb::checksum::{validate_checksum, validate_lsn, ChecksumAlgorithm};
use crate::innodb::index::IndexHeader;
use crate::innodb::lob::{BlobPageHeader, LobFirstPageHeader};
use crate::innodb::log::{
    validate_log_block_checksum, LogBlockHeader, LogCheckpoint, LogFile, LogFileHeader,
    MlogRecordType, LOG_BLOCK_HDR_SIZE, LOG_BLOCK_SIZE, LOG_FILE_HDR_BLOCKS,
};
use crate::innodb::page::{FilHeader, FspHeader};
use crate::innodb::page_types::PageType;
use crate::innodb::record::{walk_compact_records, walk_redundant_records};
use crate::innodb::sdi;
use crate::innodb::tablespace::Tablespace;
use crate::innodb::undo::{UndoPageHeader, UndoSegmentHeader};
use crate::util::hex::hex_dump;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn to_js_err(e: crate::IdbError) -> JsValue {
    JsValue::from_str(&e.to_string())
}

fn to_json<T: Serialize>(val: &T) -> Result<String, JsValue> {
    serde_json::to_string(val).map_err(|e| JsValue::from_str(&e.to_string()))
}

// ---------------------------------------------------------------------------
// get_tablespace_info — quick metadata extraction
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct TablespaceInfo {
    page_size: u32,
    page_count: u64,
    file_size: u64,
    space_id: Option<u32>,
    vendor: String,
    is_encrypted: bool,
    fsp_flags: Option<u32>,
}

/// Returns basic tablespace metadata as JSON.
///
/// Takes raw `.ibd` file bytes and returns a JSON string containing a single
/// object with fields: `page_size` (u32), `page_count` (u64), `file_size`
/// (u64), `space_id` (u32 or null), `vendor` (string, e.g. "MySQL",
/// "Percona XtraDB", or "MariaDB"), `is_encrypted` (bool), and `fsp_flags`
/// (u32 or null). This is the lightest-weight analysis function, suitable
/// for populating file summary panels without scanning every page.
///
/// Returns an error string if the input cannot be parsed as a valid InnoDB
/// tablespace (e.g. the byte array is too short to contain a page 0 header).
#[wasm_bindgen]
pub fn get_tablespace_info(data: &[u8]) -> Result<String, JsValue> {
    let ts = Tablespace::from_bytes(data.to_vec()).map_err(to_js_err)?;
    let info = TablespaceInfo {
        page_size: ts.page_size(),
        page_count: ts.page_count(),
        file_size: ts.file_size(),
        space_id: ts.fsp_header().map(|f| f.space_id),
        vendor: ts.vendor_info().vendor.to_string(),
        is_encrypted: ts.is_encrypted(),
        fsp_flags: ts.fsp_header().map(|f| f.flags),
    };
    to_json(&info)
}

// ---------------------------------------------------------------------------
// parse_tablespace — mirrors `inno parse`
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct ParseResult {
    page_size: u32,
    page_count: u64,
    file_size: u64,
    vendor: String,
    pages: Vec<ParsedPage>,
    type_summary: Vec<TypeCount>,
}

#[derive(Serialize)]
struct ParsedPage {
    page_number: u64,
    checksum: u32,
    page_type: u16,
    page_type_name: String,
    lsn: u64,
    space_id: u32,
    prev_page: Option<u32>,
    next_page: Option<u32>,
}

#[derive(Serialize)]
struct TypeCount {
    page_type: String,
    count: u64,
}

/// Parses all pages in a tablespace and returns page headers with a type summary as JSON.
///
/// Takes raw `.ibd` file bytes and iterates over every page, extracting the
/// 38-byte FIL header from each. Returns a JSON string containing an object
/// with fields: `page_size` (u32), `page_count` (u64), `file_size` (u64),
/// `vendor` (string), `pages` (array of page header objects), and
/// `type_summary` (array of `{page_type, count}` objects sorted by frequency).
///
/// Each element in the `pages` array contains: `page_number` (u64),
/// `checksum` (u32), `page_type` (u16 raw code), `page_type_name` (string),
/// `lsn` (u64), `space_id` (u32), `prev_page` (u32 or null), and
/// `next_page` (u32 or null). Sentinel values (`0xFFFFFFFF`) for prev/next
/// page pointers are converted to null.
///
/// Returns an error string if the input is not a valid InnoDB tablespace.
#[wasm_bindgen]
pub fn parse_tablespace(data: &[u8]) -> Result<String, JsValue> {
    let mut ts = Tablespace::from_bytes(data.to_vec()).map_err(to_js_err)?;
    let mut pages = Vec::new();
    let mut type_counts = std::collections::HashMap::new();

    ts.for_each_page(|page_num, page_data| {
        if let Some(hdr) = FilHeader::parse(page_data) {
            let name = hdr.page_type.name();
            *type_counts.entry(name.to_string()).or_insert(0u64) += 1;
            pages.push(ParsedPage {
                page_number: page_num,
                checksum: hdr.checksum,
                page_type: hdr.page_type.as_u16(),
                page_type_name: name.to_string(),
                lsn: hdr.lsn,
                space_id: hdr.space_id,
                prev_page: if hdr.prev_page == 0xFFFFFFFF {
                    None
                } else {
                    Some(hdr.prev_page)
                },
                next_page: if hdr.next_page == 0xFFFFFFFF {
                    None
                } else {
                    Some(hdr.next_page)
                },
            });
        }
        Ok(())
    })
    .map_err(to_js_err)?;

    let mut type_summary: Vec<TypeCount> = type_counts
        .into_iter()
        .map(|(page_type, count)| TypeCount { page_type, count })
        .collect();
    type_summary.sort_by(|a, b| b.count.cmp(&a.count));

    let result = ParseResult {
        page_size: ts.page_size(),
        page_count: ts.page_count(),
        file_size: ts.file_size(),
        vendor: ts.vendor_info().vendor.to_string(),
        pages,
        type_summary,
    };
    to_json(&result)
}

// ---------------------------------------------------------------------------
// analyze_pages — mirrors `inno pages`
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct PageAnalysis {
    page_number: u64,
    header: PageHeaderJson,
    page_type_name: String,
    page_type_description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    fsp_header: Option<FspHeader>,
    #[serde(skip_serializing_if = "Option::is_none")]
    index_header: Option<IndexHeader>,
    #[serde(skip_serializing_if = "Option::is_none")]
    undo_page_header: Option<UndoPageHeader>,
    #[serde(skip_serializing_if = "Option::is_none")]
    undo_segment_header: Option<UndoSegmentHeader>,
    #[serde(skip_serializing_if = "Option::is_none")]
    blob_header: Option<BlobPageHeader>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lob_header: Option<LobFirstPageHeader>,
}

#[derive(Serialize)]
struct PageHeaderJson {
    checksum: u32,
    page_number: u32,
    prev_page: u32,
    next_page: u32,
    lsn: u64,
    page_type: u16,
    flush_lsn: u64,
    space_id: u32,
}

fn header_to_json(h: &FilHeader) -> PageHeaderJson {
    PageHeaderJson {
        checksum: h.checksum,
        page_number: h.page_number,
        prev_page: h.prev_page,
        next_page: h.next_page,
        lsn: h.lsn,
        page_type: h.page_type.as_u16(),
        flush_lsn: h.flush_lsn,
        space_id: h.space_id,
    }
}

/// Performs deep structural analysis on one or all pages, decoding type-specific headers.
///
/// Takes raw `.ibd` file bytes and a page selector. If `page_num` is >= 0,
/// only that single page is analyzed; if `page_num` is -1, all pages in the
/// tablespace are analyzed. Returns a JSON array of page analysis objects.
///
/// Each page analysis object contains: `page_number` (u64), `header`
/// (object with `checksum`, `page_number`, `prev_page`, `next_page`, `lsn`,
/// `page_type`, `flush_lsn`, `space_id`), `page_type_name` (string), and
/// `page_type_description` (human-readable string). Depending on the page
/// type, the following optional sub-objects are included when applicable:
/// `fsp_header` (page 0 FSP header with tablespace flags and extent info),
/// `index_header` (B+Tree index page internals), `undo_page_header` and
/// `undo_segment_header` (undo log page structures), `blob_header` (BLOB
/// page metadata), and `lob_header` (LOB first-page metadata).
///
/// Returns an error string if the input is not a valid InnoDB tablespace or
/// the requested page number is out of range.
#[wasm_bindgen]
pub fn analyze_pages(data: &[u8], page_num: i64) -> Result<String, JsValue> {
    let mut ts = Tablespace::from_bytes(data.to_vec()).map_err(to_js_err)?;
    let mut results = Vec::new();

    let range: Box<dyn Iterator<Item = u64>> = if page_num >= 0 {
        Box::new(std::iter::once(page_num as u64))
    } else {
        Box::new(0..ts.page_count())
    };

    for pn in range {
        let page_data = ts.read_page(pn).map_err(to_js_err)?;
        let hdr = match FilHeader::parse(&page_data) {
            Some(h) => h,
            None => continue,
        };

        let fsp_header = if pn == 0 {
            FspHeader::parse(&page_data)
        } else {
            None
        };
        let index_header = if hdr.page_type == PageType::Index {
            IndexHeader::parse(&page_data)
        } else {
            None
        };
        let undo_page_header = if hdr.page_type == PageType::UndoLog {
            UndoPageHeader::parse(&page_data)
        } else {
            None
        };
        let undo_segment_header = if hdr.page_type == PageType::UndoLog {
            UndoSegmentHeader::parse(&page_data)
        } else {
            None
        };
        let blob_header = if hdr.page_type == PageType::Blob {
            BlobPageHeader::parse(&page_data)
        } else {
            None
        };
        let lob_header = if matches!(
            hdr.page_type,
            PageType::ZBlob | PageType::ZBlob2 | PageType::Unknown
        ) {
            LobFirstPageHeader::parse(&page_data)
        } else {
            None
        };

        results.push(PageAnalysis {
            page_number: pn,
            header: header_to_json(&hdr),
            page_type_name: hdr.page_type.name().to_string(),
            page_type_description: hdr.page_type.description().to_string(),
            fsp_header,
            index_header,
            undo_page_header,
            undo_segment_header,
            blob_header,
            lob_header,
        });
    }

    to_json(&results)
}

// ---------------------------------------------------------------------------
// validate_checksums — mirrors `inno checksum`
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct ChecksumReport {
    page_size: u32,
    total_pages: u64,
    empty_pages: u64,
    valid_pages: u64,
    invalid_pages: u64,
    lsn_mismatches: u64,
    pages: Vec<PageChecksum>,
}

#[derive(Serialize)]
struct PageChecksum {
    page_number: u64,
    status: String,
    algorithm: String,
    stored_checksum: u32,
    calculated_checksum: u32,
    lsn_valid: bool,
}

/// Validates checksums for every page in a tablespace and returns a detailed report as JSON.
///
/// Takes raw `.ibd` file bytes, auto-detects the checksum algorithm
/// (CRC-32C, legacy InnoDB, or MariaDB full_crc32), and validates each
/// non-empty page. Also checks that the LSN in the FIL header matches the
/// LSN low-32 bits stored in the FIL trailer.
///
/// Returns a JSON string containing an object with fields: `page_size`
/// (u32), `total_pages` (u64), `empty_pages` (u64, all-zero pages skipped),
/// `valid_pages` (u64), `invalid_pages` (u64), `lsn_mismatches` (u64), and
/// `pages` (array of per-page results). Each element in `pages` contains:
/// `page_number` (u64), `status` ("valid" or "invalid"), `algorithm`
/// ("crc32c", "innodb", "mariadb_full_crc32", or "none"),
/// `stored_checksum` (u32), `calculated_checksum` (u32), and `lsn_valid`
/// (bool).
///
/// Returns an error string if the input is not a valid InnoDB tablespace.
#[wasm_bindgen]
pub fn validate_checksums(data: &[u8]) -> Result<String, JsValue> {
    let mut ts = Tablespace::from_bytes(data.to_vec()).map_err(to_js_err)?;
    let page_size = ts.page_size();
    let vendor_info = ts.vendor_info().clone();
    let mut pages = Vec::new();
    let mut empty = 0u64;
    let mut valid = 0u64;
    let mut invalid = 0u64;
    let mut lsn_mismatches = 0u64;

    ts.for_each_page(|page_num, page_data| {
        if page_data.iter().all(|&b| b == 0) {
            empty += 1;
            return Ok(());
        }

        let result = validate_checksum(page_data, page_size, Some(&vendor_info));
        let lsn_ok = validate_lsn(page_data, page_size);
        if !lsn_ok {
            lsn_mismatches += 1;
        }

        let algo_str = match result.algorithm {
            ChecksumAlgorithm::Crc32c => "crc32c",
            ChecksumAlgorithm::InnoDB => "innodb",
            ChecksumAlgorithm::MariaDbFullCrc32 => "mariadb_full_crc32",
            ChecksumAlgorithm::None => "none",
        };

        if result.valid {
            valid += 1;
        } else {
            invalid += 1;
        }

        pages.push(PageChecksum {
            page_number: page_num,
            status: if result.valid { "valid" } else { "invalid" }.to_string(),
            algorithm: algo_str.to_string(),
            stored_checksum: result.stored_checksum,
            calculated_checksum: result.calculated_checksum,
            lsn_valid: lsn_ok,
        });
        Ok(())
    })
    .map_err(to_js_err)?;

    let report = ChecksumReport {
        page_size,
        total_pages: ts.page_count(),
        empty_pages: empty,
        valid_pages: valid,
        invalid_pages: invalid,
        lsn_mismatches,
        pages,
    };
    to_json(&report)
}

// ---------------------------------------------------------------------------
// extract_sdi — mirrors `inno sdi`
// ---------------------------------------------------------------------------

/// Extracts Serialized Dictionary Information (SDI) metadata from a MySQL 8.0+ tablespace.
///
/// Takes raw `.ibd` file bytes, locates all SDI pages (page type 17853),
/// reassembles multi-page zlib-compressed SDI records by following the page
/// chain, decompresses them, and returns the parsed JSON metadata. SDI
/// records contain the full MySQL data dictionary definition for the table
/// stored in the tablespace, including column definitions, indexes, and
/// table options.
///
/// Returns a JSON array of SDI record objects. Each object is the native
/// MySQL SDI JSON structure (typically containing `dd_object` with table
/// and column metadata). Returns an empty array (`[]`) if no SDI pages are
/// found (e.g. pre-8.0 tablespaces or system tablespace files).
///
/// Returns an error string if the input is not a valid InnoDB tablespace or
/// if SDI decompression fails.
#[wasm_bindgen]
pub fn extract_sdi(data: &[u8]) -> Result<String, JsValue> {
    let mut ts = Tablespace::from_bytes(data.to_vec()).map_err(to_js_err)?;
    let sdi_pages = sdi::find_sdi_pages(&mut ts).map_err(to_js_err)?;
    if sdi_pages.is_empty() {
        return Ok("[]".to_string());
    }
    let records = sdi::extract_sdi_from_pages(&mut ts, &sdi_pages).map_err(to_js_err)?;
    let sdi_data: Vec<serde_json::Value> = records
        .iter()
        .filter_map(|r| serde_json::from_str(&r.data).ok())
        .collect();
    to_json(&sdi_data)
}

// ---------------------------------------------------------------------------
// diff_tablespaces — mirrors `inno diff`
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct DiffResult {
    page_size_1: u32,
    page_size_2: u32,
    page_count_1: u64,
    page_count_2: u64,
    identical: u64,
    modified: u64,
    only_in_first: u64,
    only_in_second: u64,
    modified_pages: Vec<ModifiedPage>,
}

#[derive(Serialize)]
struct ModifiedPage {
    page_number: u64,
    header_1: PageHeaderJson,
    header_2: PageHeaderJson,
    bytes_changed: usize,
}

/// Compares two tablespace files page-by-page and returns a detailed diff report as JSON.
///
/// Takes two raw `.ibd` file byte arrays and compares them page-by-page.
/// Both files must have the same page size or an error is returned. Pages
/// are compared byte-for-byte; when differences exist, the FIL headers
/// from both files are included along with a count of differing bytes.
///
/// Returns a JSON string containing an object with fields: `page_size_1`
/// and `page_size_2` (u32), `page_count_1` and `page_count_2` (u64),
/// `identical` (u64, number of byte-identical pages), `modified` (u64),
/// `only_in_first` (u64, pages beyond the second file's range),
/// `only_in_second` (u64), and `modified_pages` (array of diff details).
/// Each element in `modified_pages` contains: `page_number` (u64),
/// `header_1` and `header_2` (FIL header objects from each file), and
/// `bytes_changed` (usize, total number of differing bytes in the page).
///
/// Returns an error string if either input is not a valid InnoDB tablespace
/// or if the two files have different page sizes.
#[wasm_bindgen]
pub fn diff_tablespaces(data1: &[u8], data2: &[u8]) -> Result<String, JsValue> {
    let mut ts1 = Tablespace::from_bytes(data1.to_vec()).map_err(to_js_err)?;
    let mut ts2 = Tablespace::from_bytes(data2.to_vec()).map_err(to_js_err)?;

    if ts1.page_size() != ts2.page_size() {
        return Err(JsValue::from_str(&format!(
            "Page size mismatch: file 1 has {} byte pages, file 2 has {} byte pages",
            ts1.page_size(),
            ts2.page_size()
        )));
    }

    let max_pages = std::cmp::max(ts1.page_count(), ts2.page_count());
    let mut identical = 0u64;
    let mut modified = 0u64;
    let mut only_in_first = 0u64;
    let mut only_in_second = 0u64;
    let mut modified_pages = Vec::new();

    for pn in 0..max_pages {
        let p1 = if pn < ts1.page_count() {
            Some(ts1.read_page(pn).map_err(to_js_err)?)
        } else {
            None
        };
        let p2 = if pn < ts2.page_count() {
            Some(ts2.read_page(pn).map_err(to_js_err)?)
        } else {
            None
        };

        match (p1, p2) {
            (Some(a), Some(b)) => {
                if a == b {
                    identical += 1;
                } else {
                    modified += 1;
                    let h1 = FilHeader::parse(&a);
                    let h2 = FilHeader::parse(&b);
                    let bytes_diff = a.iter().zip(b.iter()).filter(|(x, y)| x != y).count();
                    if let (Some(h1), Some(h2)) = (h1, h2) {
                        modified_pages.push(ModifiedPage {
                            page_number: pn,
                            header_1: header_to_json(&h1),
                            header_2: header_to_json(&h2),
                            bytes_changed: bytes_diff,
                        });
                    }
                }
            }
            (Some(_), None) => only_in_first += 1,
            (None, Some(_)) => only_in_second += 1,
            (None, None) => {}
        }
    }

    let result = DiffResult {
        page_size_1: ts1.page_size(),
        page_size_2: ts2.page_size(),
        page_count_1: ts1.page_count(),
        page_count_2: ts2.page_count(),
        identical,
        modified,
        only_in_first,
        only_in_second,
        modified_pages,
    };
    to_json(&result)
}

// ---------------------------------------------------------------------------
// hex_dump_page — mirrors `inno dump`
// ---------------------------------------------------------------------------

/// Returns a formatted hex dump of a single page's raw bytes as a plain-text string.
///
/// Takes raw `.ibd` file bytes, a page selector, and optional byte range
/// parameters. If `page_num` is negative, page 0 is used. The `offset`
/// parameter specifies the starting byte within the page (0-based), and
/// `length` specifies how many bytes to dump (0 means dump to end of page).
///
/// Returns a plain-text string (not JSON) formatted as a traditional hex
/// dump with file offsets, hex byte values, and ASCII representation.
/// Each line shows 16 bytes in the standard `xxd`-style layout.
///
/// Returns an error string if the input is not a valid InnoDB tablespace,
/// the page number is out of range, or the offset exceeds the page boundary.
#[wasm_bindgen]
pub fn hex_dump_page(
    data: &[u8],
    page_num: i64,
    offset: u32,
    length: u32,
) -> Result<String, JsValue> {
    let mut ts = Tablespace::from_bytes(data.to_vec()).map_err(to_js_err)?;
    let pn = if page_num < 0 { 0 } else { page_num as u64 };
    let page_data = ts.read_page(pn).map_err(to_js_err)?;

    let start = offset as usize;
    let end = if length == 0 {
        page_data.len()
    } else {
        std::cmp::min(start + length as usize, page_data.len())
    };

    if start >= page_data.len() {
        return Err(JsValue::from_str("Offset beyond page boundary"));
    }

    let file_offset = pn * ts.page_size() as u64 + start as u64;
    Ok(hex_dump(&page_data[start..end], file_offset))
}

// ---------------------------------------------------------------------------
// assess_recovery — mirrors `inno recover`
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct RecoveryReport {
    page_size: u32,
    page_count: u64,
    summary: RecoverySummary,
    recoverable_records: u64,
    pages: Vec<PageRecovery>,
}

#[derive(Serialize)]
struct RecoverySummary {
    intact: u64,
    corrupt: u64,
    empty: u64,
}

#[derive(Serialize)]
struct PageRecovery {
    page_number: u64,
    status: String,
    page_type: String,
    checksum_valid: bool,
    lsn_valid: bool,
    lsn: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    record_count: Option<usize>,
}

/// Assesses page-level recoverability and counts salvageable records as JSON.
///
/// Takes raw `.ibd` file bytes and evaluates every page for data recovery
/// potential. Each page is classified as "intact" (valid checksum and LSN
/// match), "corrupt" (checksum or LSN mismatch), or "empty" (all-zero
/// bytes). For INDEX pages (B+Tree leaf and internal nodes), the function
/// walks the compact record list to count individual data records that
/// could be recovered.
///
/// Returns a JSON string containing an object with fields: `page_size`
/// (u32), `page_count` (u64), `summary` (object with `intact`, `corrupt`,
/// and `empty` counts as u64), `recoverable_records` (u64, total records
/// found in intact INDEX pages), and `pages` (array of per-page results).
/// Each element in `pages` contains: `page_number` (u64), `status`
/// ("intact", "corrupt", or "empty"), `page_type` (string name),
/// `checksum_valid` (bool), `lsn_valid` (bool), `lsn` (u64), and
/// `record_count` (usize or null, present only for INDEX pages).
///
/// Returns an error string if the input is not a valid InnoDB tablespace.
#[wasm_bindgen]
pub fn assess_recovery(data: &[u8]) -> Result<String, JsValue> {
    let mut ts = Tablespace::from_bytes(data.to_vec()).map_err(to_js_err)?;
    let page_size = ts.page_size();
    let vendor_info = ts.vendor_info().clone();
    let mut pages = Vec::new();
    let mut intact = 0u64;
    let mut corrupt = 0u64;
    let mut empty = 0u64;
    let mut total_records = 0u64;

    ts.for_each_page(|page_num, page_data| {
        if page_data.iter().all(|&b| b == 0) {
            empty += 1;
            pages.push(PageRecovery {
                page_number: page_num,
                status: "empty".to_string(),
                page_type: "EMPTY".to_string(),
                checksum_valid: false,
                lsn_valid: false,
                lsn: 0,
                record_count: None,
            });
            return Ok(());
        }

        let hdr = FilHeader::parse(page_data);
        let cksum = validate_checksum(page_data, page_size, Some(&vendor_info));
        let lsn_ok = validate_lsn(page_data, page_size);

        let (status, pt_name, lsn_val, rec_count) = match hdr {
            Some(h) => {
                let name = h.page_type.name();
                let rec = if h.page_type == PageType::Index {
                    let recs = walk_compact_records(page_data);
                    Some(recs.len())
                } else {
                    None
                };
                if cksum.valid && lsn_ok {
                    intact += 1;
                    if let Some(n) = rec {
                        total_records += n as u64;
                    }
                    ("intact", name.to_string(), h.lsn, rec)
                } else {
                    corrupt += 1;
                    ("corrupt", name.to_string(), h.lsn, rec)
                }
            }
            None => {
                corrupt += 1;
                ("corrupt", "UNKNOWN".to_string(), 0, None)
            }
        };

        pages.push(PageRecovery {
            page_number: page_num,
            status: status.to_string(),
            page_type: pt_name,
            checksum_valid: cksum.valid,
            lsn_valid: lsn_ok,
            lsn: lsn_val,
            record_count: rec_count,
        });
        Ok(())
    })
    .map_err(to_js_err)?;

    let report = RecoveryReport {
        page_size,
        page_count: ts.page_count(),
        summary: RecoverySummary {
            intact,
            corrupt,
            empty,
        },
        recoverable_records: total_records,
        pages,
    };
    to_json(&report)
}

// ---------------------------------------------------------------------------
// get_encryption_info — inspect encryption metadata from page 0
// ---------------------------------------------------------------------------

/// Returns encryption info from page 0 of an encrypted tablespace as JSON.
///
/// Takes raw `.ibd` file bytes, reads the FSP header to determine encryption
/// status, and if encrypted, parses the encryption info structure to extract
/// the magic version, master key ID, server UUID, and CRC32 checksum.
///
/// Returns a JSON string containing an object with fields: `is_encrypted`
/// (bool), `server_uuid` (string or null), `master_key_id` (u32 or null),
/// and `magic_version` (u8 or null). Fields are null when the tablespace
/// is not encrypted.
///
/// Returns an error string if the input is not a valid InnoDB tablespace.
#[wasm_bindgen]
pub fn get_encryption_info(data: &[u8]) -> Result<String, JsValue> {
    let mut ts = Tablespace::from_bytes(data.to_vec()).map_err(to_js_err)?;
    let page0 = ts.read_page(0).map_err(to_js_err)?;
    let page_size = ts.page_size();

    #[derive(Serialize)]
    struct EncInfo {
        is_encrypted: bool,
        server_uuid: Option<String>,
        master_key_id: Option<u32>,
        magic_version: Option<u8>,
    }

    if !ts.is_encrypted() {
        return to_json(&EncInfo {
            is_encrypted: false,
            server_uuid: None,
            master_key_id: None,
            magic_version: None,
        });
    }

    let info = crate::innodb::encryption::parse_encryption_info(&page0, page_size);
    match info {
        Some(ei) => to_json(&EncInfo {
            is_encrypted: true,
            server_uuid: Some(ei.server_uuid),
            master_key_id: Some(ei.master_key_id),
            magic_version: Some(ei.magic_version),
        }),
        None => to_json(&EncInfo {
            is_encrypted: true,
            server_uuid: None,
            master_key_id: None,
            magic_version: None,
        }),
    }
}

// ---------------------------------------------------------------------------
// decrypt_tablespace — decrypt all pages with a keyring
// ---------------------------------------------------------------------------

/// Decrypts an encrypted tablespace using a keyring file, returning the decrypted bytes.
///
/// Takes raw `.ibd` file bytes and raw keyring file bytes. Parses the
/// encryption info from page 0, looks up the master key in the keyring,
/// derives the per-tablespace key and IV, then decrypts all encrypted pages.
/// Returns the fully decrypted tablespace as a `Vec<u8>` (converted to
/// `Uint8Array` by wasm-bindgen).
///
/// The caller can then pass the returned bytes to any other analysis function
/// (e.g. `parse_tablespace`, `validate_checksums`) as if the tablespace were
/// unencrypted.
///
/// Returns an error string if the input is not a valid encrypted tablespace,
/// the keyring cannot be parsed, or the master key is not found.
#[wasm_bindgen]
pub fn decrypt_tablespace(data: &[u8], keyring_data: &[u8]) -> Result<Vec<u8>, JsValue> {
    use crate::innodb::decryption::DecryptionContext;
    use crate::innodb::encryption::parse_encryption_info;
    use crate::innodb::keyring::Keyring;

    let keyring = Keyring::from_bytes(keyring_data).map_err(to_js_err)?;
    let mut ts = Tablespace::from_bytes(data.to_vec()).map_err(to_js_err)?;
    let page_size = ts.page_size();
    let page0 = ts.read_page(0).map_err(to_js_err)?;

    let enc_info = parse_encryption_info(&page0, page_size)
        .ok_or_else(|| JsValue::from_str("No encryption info found on page 0"))?;

    let ctx = DecryptionContext::from_encryption_info(&enc_info, &keyring).map_err(to_js_err)?;
    ts.set_decryption_context(ctx);

    // Read all pages back (now transparently decrypted) into a contiguous buffer
    let page_count = ts.page_count();
    let mut result = Vec::with_capacity(page_count as usize * page_size as usize);
    for pn in 0..page_count {
        let page_data = ts.read_page(pn).map_err(to_js_err)?;
        result.extend_from_slice(&page_data);
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// inspect_index_records — record-level INDEX page inspection
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct RecordDetail {
    offset: usize,
    rec_type: String,
    heap_no: u16,
    n_owned: u8,
    delete_mark: bool,
    min_rec: bool,
    next_offset: i16,
    raw_hex: String,
}

#[derive(Serialize)]
struct IndexRecordReport {
    page_number: u64,
    index_id: u64,
    level: u16,
    n_recs: u16,
    is_compact: bool,
    records: Vec<RecordDetail>,
}

/// Inspects records on an INDEX page, returning record headers and raw hex snippets.
///
/// Takes raw `.ibd` file bytes and a page number. The page must be an INDEX
/// page (type 17855). Returns a JSON string containing the page's index metadata
/// and an array of record details from walking the compact record chain.
///
/// Each record in the `records` array contains: `offset` (usize, absolute byte
/// offset within the page), `rec_type` (string, e.g. "REC_STATUS_ORDINARY"),
/// `heap_no` (u16), `n_owned` (u8), `delete_mark` (bool), `min_rec` (bool),
/// `next_offset` (i16, relative), and `raw_hex` (string, hex encoding of the
/// first 20 bytes at the record origin).
///
/// Returns an error string if the page is not an INDEX page or the input is
/// not a valid InnoDB tablespace.
#[wasm_bindgen]
pub fn inspect_index_records(data: &[u8], page_num: u64) -> Result<String, JsValue> {
    let mut ts = Tablespace::from_bytes(data.to_vec()).map_err(to_js_err)?;
    let page_data = ts.read_page(page_num).map_err(to_js_err)?;

    let hdr = FilHeader::parse(&page_data)
        .ok_or_else(|| JsValue::from_str("Cannot parse FIL header"))?;

    if hdr.page_type != PageType::Index {
        return Err(JsValue::from_str(&format!(
            "Page {} is not an INDEX page (type: {})",
            page_num,
            hdr.page_type.name()
        )));
    }

    let idx_hdr = IndexHeader::parse(&page_data)
        .ok_or_else(|| JsValue::from_str("Cannot parse INDEX header"))?;

    let recs = if idx_hdr.is_compact() {
        walk_compact_records(&page_data)
    } else {
        walk_redundant_records(&page_data)
    };
    let records: Vec<RecordDetail> = recs
        .iter()
        .map(|r| {
            let end = std::cmp::min(r.offset + 20, page_data.len());
            let raw_bytes = &page_data[r.offset..end];
            let raw_hex = raw_bytes
                .iter()
                .map(|b| format!("{:02X}", b))
                .collect::<Vec<_>>()
                .join(" ");

            RecordDetail {
                offset: r.offset,
                rec_type: r.header.rec_type().name().to_string(),
                heap_no: r.header.heap_no(),
                n_owned: r.header.n_owned(),
                delete_mark: r.header.delete_mark(),
                min_rec: r.header.min_rec(),
                next_offset: r.header.next_offset_raw(),
                raw_hex,
            }
        })
        .collect();

    let report = IndexRecordReport {
        page_number: page_num,
        index_id: idx_hdr.index_id,
        level: idx_hdr.level,
        n_recs: idx_hdr.n_recs,
        is_compact: idx_hdr.is_compact(),
        records,
    };
    to_json(&report)
}

// ---------------------------------------------------------------------------
// parse_redo_log — mirrors `inno log`
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct RedoLogReport {
    file_size: u64,
    total_blocks: u64,
    data_blocks: u64,
    header: Option<LogFileHeader>,
    checkpoint_1: Option<LogCheckpoint>,
    checkpoint_2: Option<LogCheckpoint>,
    blocks: Vec<RedoBlock>,
}

#[derive(Serialize)]
struct RedoBlock {
    block_index: u64,
    block_no: u32,
    flush_flag: bool,
    data_len: u16,
    first_rec_group: u16,
    checkpoint_no: u32,
    checksum_valid: bool,
    has_data: bool,
    record_types: Vec<String>,
}

/// Parses an InnoDB redo log file and returns header, checkpoint, and block details as JSON.
///
/// Takes raw redo log file bytes (typically `ib_logfile0` or `#ib_redo*`
/// files) and parses the file header, both checkpoint slots, and every
/// 512-byte log block. For each block that contains data, the first
/// mini-transaction record type is identified and included.
///
/// Returns a JSON string containing an object with fields: `file_size`
/// (u64), `total_blocks` (u64, including header blocks), `data_blocks`
/// (u64, excluding the file header blocks), `header` (log file header
/// object or null), `checkpoint_1` and `checkpoint_2` (checkpoint objects
/// or null, each containing `checkpoint_no`, `checkpoint_lsn`,
/// `checkpoint_offset`, etc.), and `blocks` (array of block details).
/// Each block element contains: `block_index` (u64), `block_no` (u32),
/// `flush_flag` (bool), `data_len` (u16), `first_rec_group` (u16),
/// `checkpoint_no` (u32), `checksum_valid` (bool), `has_data` (bool),
/// and `record_types` (array of mlog record type name strings).
///
/// Returns an error string if the input is not a valid InnoDB redo log
/// file (e.g. missing the log file header magic or too short).
#[wasm_bindgen]
pub fn parse_redo_log(data: &[u8]) -> Result<String, JsValue> {
    let mut log = LogFile::from_bytes(data.to_vec()).map_err(to_js_err)?;

    let header = log.read_header().ok();
    let cp1 = log.read_checkpoint(0).ok();
    let cp2 = log.read_checkpoint(1).ok();

    let mut blocks = Vec::new();
    for i in 0..log.data_block_count() {
        let block_index = LOG_FILE_HDR_BLOCKS + i;
        let block_data = log.read_block(block_index).map_err(to_js_err)?;
        let bhdr = match LogBlockHeader::parse(&block_data) {
            Some(h) => h,
            None => continue,
        };
        let cksum_ok = validate_log_block_checksum(&block_data);

        let mut record_types = Vec::new();
        if bhdr.has_data() {
            let data_end = std::cmp::min(bhdr.data_len as usize, LOG_BLOCK_SIZE - 4);
            if LOG_BLOCK_HDR_SIZE < data_end {
                let rec_type = MlogRecordType::from_u8(block_data[LOG_BLOCK_HDR_SIZE]);
                record_types.push(rec_type.to_string());
            }
        }

        blocks.push(RedoBlock {
            block_index,
            block_no: bhdr.block_no,
            flush_flag: bhdr.flush_flag,
            data_len: bhdr.data_len,
            first_rec_group: bhdr.first_rec_group,
            checkpoint_no: bhdr.checkpoint_no,
            checksum_valid: cksum_ok,
            has_data: bhdr.has_data(),
            record_types,
        });
    }

    let report = RedoLogReport {
        file_size: log.file_size(),
        total_blocks: log.block_count(),
        data_blocks: log.data_block_count(),
        header,
        checkpoint_1: cp1,
        checkpoint_2: cp2,
        blocks,
    };
    to_json(&report)
}
