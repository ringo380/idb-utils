//! WebAssembly bindings for InnoDB file analysis.
//!
//! Each exported function accepts raw file bytes as `&[u8]` (via wasm-bindgen)
//! and returns a JSON string with the analysis results. These are thin wrappers
//! over the same library code used by the CLI subcommands.

use serde::Serialize;
use wasm_bindgen::prelude::*;

use crate::innodb::checksum::{validate_checksum, validate_lsn, ChecksumAlgorithm};
use crate::innodb::index::IndexHeader;
use crate::innodb::log::{
    validate_log_block_checksum, LogBlockHeader, LogCheckpoint, LogFile, LogFileHeader,
    MlogRecordType, LOG_BLOCK_HDR_SIZE, LOG_BLOCK_SIZE, LOG_FILE_HDR_BLOCKS,
};
use crate::innodb::lob::{BlobPageHeader, LobFirstPageHeader};
use crate::innodb::page::{FilHeader, FspHeader};
use crate::innodb::page_types::PageType;
use crate::innodb::record::walk_compact_records;
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

/// Return basic tablespace metadata (page size, page count, space ID, vendor, encryption).
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

/// Parse all pages and return a summary with page headers and type counts.
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

/// Analyze one or all pages with deep structure decoding.
/// If `page_num` is >= 0, analyze only that page.
/// If `page_num` is -1, analyze all pages.
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

/// Validate all page checksums and return per-page results.
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

/// Extract SDI metadata records from a MySQL 8.0+ tablespace.
/// Returns the raw SDI JSON records as a JSON array.
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

/// Compare two tablespace files page-by-page and return differences.
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

/// Return a hex dump of the specified page (or page 0 if page_num is -1).
/// The `offset` and `length` parameters allow partial dumps within the page.
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

/// Assess page-level recoverability and count salvageable records.
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

/// Parse an InnoDB redo log file and return header, checkpoints, and block details.
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
