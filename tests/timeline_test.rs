//! Integration tests for the `inno timeline` subcommand and timeline library.

use byteorder::{BigEndian, ByteOrder};

use idb::innodb::log::{
    compute_record_lsn, parse_mlog_records, LogBlockHeader, LogFile, MlogRecordType,
    LOG_BLOCK_CHECKSUM_OFFSET, LOG_BLOCK_HDR_SIZE, LOG_BLOCK_SIZE, LOG_FILE_HDR_BLOCKS,
};
use idb::innodb::timeline::{merge_timeline, TimelineAction, TimelineEntry, TimelineSource};

// ── Helpers ─────────────────────────────────────────────────────────────

/// Build a minimal valid redo log with header + checkpoint blocks + N data blocks.
fn make_redo_log(data_blocks: &[Vec<u8>]) -> Vec<u8> {
    let total_blocks = LOG_FILE_HDR_BLOCKS as usize + data_blocks.len();
    let mut buf = vec![0u8; total_blocks * LOG_BLOCK_SIZE];

    // Block 0: header — format_version=6, start_lsn=2048
    BigEndian::write_u32(&mut buf[0..], 6); // format_version
    BigEndian::write_u64(&mut buf[8..], 2048); // start_lsn
    BigEndian::write_u32(&mut buf[16..], 0); // log_uuid placeholder
                                             // created_by: "MySQL 8.0.35\0" at offset 48
    buf[48..60].copy_from_slice(b"MySQL 8.0.35");

    // Block 1 (checkpoint 0): checkpoint_lsn = 2048
    let cp_base = LOG_BLOCK_SIZE;
    BigEndian::write_u64(&mut buf[cp_base + 8..], 2048);

    // Block 3 (checkpoint 1): checkpoint_lsn = 2048
    let cp_base2 = 3 * LOG_BLOCK_SIZE;
    BigEndian::write_u64(&mut buf[cp_base2 + 8..], 2048);

    // Data blocks
    for (i, block_data) in data_blocks.iter().enumerate() {
        let base = (LOG_FILE_HDR_BLOCKS as usize + i) * LOG_BLOCK_SIZE;
        let len = block_data.len().min(LOG_BLOCK_SIZE);
        buf[base..base + len].copy_from_slice(&block_data[..len]);

        // Write CRC-32C checksum
        let crc = crc32c::crc32c(&buf[base..base + LOG_BLOCK_CHECKSUM_OFFSET]);
        BigEndian::write_u32(&mut buf[base + LOG_BLOCK_CHECKSUM_OFFSET..], crc);
    }

    buf
}

/// Build a data block with a valid header and the given payload bytes.
fn make_data_block(block_no: u32, payload: &[u8]) -> Vec<u8> {
    let mut block = vec![0u8; LOG_BLOCK_SIZE];

    // Block header
    BigEndian::write_u32(&mut block[0..], block_no);
    let data_len = (LOG_BLOCK_HDR_SIZE + payload.len()).min(LOG_BLOCK_SIZE - 4) as u16;
    BigEndian::write_u16(&mut block[4..], data_len);
    BigEndian::write_u16(&mut block[6..], LOG_BLOCK_HDR_SIZE as u16); // first_rec_group
    BigEndian::write_u32(&mut block[8..], 1); // epoch_no

    // Payload
    let end = LOG_BLOCK_HDR_SIZE + payload.len().min(LOG_BLOCK_SIZE - LOG_BLOCK_HDR_SIZE - 4);
    block[LOG_BLOCK_HDR_SIZE..end].copy_from_slice(&payload[..end - LOG_BLOCK_HDR_SIZE]);

    block
}

/// Encode a u32 as an InnoDB compressed integer (1-5 bytes).
fn compress_u32(val: u32) -> Vec<u8> {
    if val < 0x80 {
        vec![val as u8]
    } else if val < 0x4000 {
        vec![((val >> 8) as u8) | 0x80, (val & 0xFF) as u8]
    } else if val < 0x200000 {
        vec![
            ((val >> 16) as u8) | 0xC0,
            ((val >> 8) & 0xFF) as u8,
            (val & 0xFF) as u8,
        ]
    } else if val < 0x10000000 {
        vec![
            ((val >> 24) as u8) | 0xE0,
            ((val >> 16) & 0xFF) as u8,
            ((val >> 8) & 0xFF) as u8,
            (val & 0xFF) as u8,
        ]
    } else {
        vec![
            0xF0,
            ((val >> 24) & 0xFF) as u8,
            ((val >> 16) & 0xFF) as u8,
            ((val >> 8) & 0xFF) as u8,
            (val & 0xFF) as u8,
        ]
    }
}

// ── MLOG record parsing tests ───────────────────────────────────────────

#[test]
fn test_parse_mlog_single_byte_space_and_page() {
    // Type 1 (MLOG_1BYTE), space_id=5, page_no=3
    let mut payload = vec![1u8]; // type byte
    payload.extend_from_slice(&compress_u32(5)); // space_id
    payload.extend_from_slice(&compress_u32(3)); // page_no

    let block = make_data_block(4, &payload);
    let hdr = LogBlockHeader::parse(&block).unwrap();
    let records = parse_mlog_records(&block, &hdr);

    assert!(!records.is_empty());
    let rec = &records[0];
    assert_eq!(rec.record_type, MlogRecordType::Mlog1Byte);
    assert!(!rec.single_rec);
    assert_eq!(rec.space_id, Some(5));
    assert_eq!(rec.page_no, Some(3));
}

#[test]
fn test_parse_mlog_single_rec_flag() {
    // Type byte with single-rec flag: 0x80 | 30 (MLOG_WRITE_STRING)
    let mut payload = vec![0x80 | 30];
    payload.extend_from_slice(&compress_u32(10)); // space_id
    payload.extend_from_slice(&compress_u32(42)); // page_no

    let block = make_data_block(4, &payload);
    let hdr = LogBlockHeader::parse(&block).unwrap();
    let records = parse_mlog_records(&block, &hdr);

    assert!(!records.is_empty());
    let rec = &records[0];
    assert_eq!(rec.record_type, MlogRecordType::MlogWriteString);
    assert!(rec.single_rec);
    assert_eq!(rec.space_id, Some(10));
    assert_eq!(rec.page_no, Some(42));
}

#[test]
fn test_parse_mlog_non_page_type() {
    // Type 31 = MLOG_MULTI_REC_END — no space_id/page_no
    let payload = vec![31u8];
    let block = make_data_block(4, &payload);
    let hdr = LogBlockHeader::parse(&block).unwrap();
    let records = parse_mlog_records(&block, &hdr);

    assert!(!records.is_empty());
    let rec = &records[0];
    assert_eq!(rec.record_type, MlogRecordType::MlogMultiRecEnd);
    assert_eq!(rec.space_id, None);
    assert_eq!(rec.page_no, None);
}

#[test]
fn test_parse_mlog_multi_byte_compressed() {
    // Type 1, space_id=300 (2-byte compressed), page_no=65000 (3-byte compressed)
    let mut payload = vec![1u8];
    payload.extend_from_slice(&compress_u32(300));
    payload.extend_from_slice(&compress_u32(65000));

    let block = make_data_block(4, &payload);
    let hdr = LogBlockHeader::parse(&block).unwrap();
    let records = parse_mlog_records(&block, &hdr);

    assert!(!records.is_empty());
    assert_eq!(records[0].space_id, Some(300));
    assert_eq!(records[0].page_no, Some(65000));
}

#[test]
fn test_parse_mlog_empty_block() {
    // Block with data_len = LOG_BLOCK_HDR_SIZE (no payload)
    let block = make_data_block(4, &[]);
    let mut modified = block.clone();
    BigEndian::write_u16(&mut modified[4..], LOG_BLOCK_HDR_SIZE as u16);
    let hdr = LogBlockHeader::parse(&modified).unwrap();
    let records = parse_mlog_records(&modified, &hdr);
    assert!(records.is_empty());
}

#[test]
fn test_parse_mlog_multiple_records() {
    // Two records: MLOG_1BYTE + MLOG_UNDO_INSERT
    let mut payload = Vec::new();
    // Record 1: type=1, space=1, page=0
    payload.push(1u8);
    payload.extend_from_slice(&compress_u32(1));
    payload.extend_from_slice(&compress_u32(0));
    // Record 2: type=20, space=2, page=100
    payload.push(20u8);
    payload.extend_from_slice(&compress_u32(2));
    payload.extend_from_slice(&compress_u32(100));

    let block = make_data_block(4, &payload);
    let hdr = LogBlockHeader::parse(&block).unwrap();
    let records = parse_mlog_records(&block, &hdr);

    assert!(records.len() >= 2);
    assert_eq!(records[0].record_type, MlogRecordType::Mlog1Byte);
    assert_eq!(records[0].space_id, Some(1));
    // Find the MLOG_UNDO_INSERT record
    let undo_rec = records
        .iter()
        .find(|r| r.record_type == MlogRecordType::MlogUndoInsert);
    assert!(undo_rec.is_some());
    assert_eq!(undo_rec.unwrap().space_id, Some(2));
    assert_eq!(undo_rec.unwrap().page_no, Some(100));
}

// ── compute_record_lsn tests ────────────────────────────────────────────

#[test]
fn test_compute_record_lsn_first_block() {
    // start_lsn=2048, block_idx=0, offset at start of payload
    let lsn = compute_record_lsn(2048, 0, LOG_BLOCK_HDR_SIZE);
    assert_eq!(lsn, 2048); // offset - HDR_SIZE = 0
}

#[test]
fn test_compute_record_lsn_second_block() {
    let payload_per_block = LOG_BLOCK_SIZE - LOG_BLOCK_HDR_SIZE - 4; // 494
    let lsn = compute_record_lsn(2048, 1, LOG_BLOCK_HDR_SIZE);
    assert_eq!(lsn, 2048 + payload_per_block as u64);
}

// ── merge_timeline tests ────────────────────────────────────────────────

#[test]
fn test_merge_single_source() {
    let redo = vec![TimelineEntry {
        seq: 0,
        source: TimelineSource::RedoLog,
        lsn: Some(1000),
        timestamp: None,
        space_id: Some(5),
        page_no: Some(3),
        action: TimelineAction::Redo {
            mlog_type: "MLOG_REC_INSERT".to_string(),
            single_rec: true,
        },
    }];

    let report = merge_timeline(redo, vec![], vec![]);
    assert_eq!(report.redo_count, 1);
    assert_eq!(report.undo_count, 0);
    assert_eq!(report.binlog_count, 0);
    assert_eq!(report.entries.len(), 1);
    assert_eq!(report.entries[0].seq, 1);
    assert_eq!(report.page_summaries.len(), 1);
}

#[test]
fn test_merge_preserves_all_sources() {
    let redo = vec![TimelineEntry {
        seq: 0,
        source: TimelineSource::RedoLog,
        lsn: Some(100),
        timestamp: None,
        space_id: Some(1),
        page_no: Some(1),
        action: TimelineAction::Redo {
            mlog_type: "MLOG_1BYTE".to_string(),
            single_rec: false,
        },
    }];
    let undo = vec![TimelineEntry {
        seq: 0,
        source: TimelineSource::UndoLog,
        lsn: Some(200),
        timestamp: None,
        space_id: None,
        page_no: Some(7),
        action: TimelineAction::Undo {
            record_type: "DELETE_MARK".to_string(),
            trx_id: 99,
            undo_no: 1,
            table_id: 5,
        },
    }];
    let binlog = vec![TimelineEntry {
        seq: 0,
        source: TimelineSource::Binlog,
        lsn: None,
        timestamp: Some(1700000000),
        space_id: None,
        page_no: None,
        action: TimelineAction::Binlog {
            event_type: "WRITE_ROWS_EVENT_V2".to_string(),
            database: Some("test".to_string()),
            table: Some("users".to_string()),
            xid: None,
            pk_values: None,
        },
    }];

    let report = merge_timeline(redo, undo, binlog);
    assert_eq!(report.entries.len(), 3);
    assert_eq!(report.redo_count, 1);
    assert_eq!(report.undo_count, 1);
    assert_eq!(report.binlog_count, 1);
}

// ── extract_redo_timeline integration test ──────────────────────────────

#[test]
fn test_extract_redo_timeline_from_synthetic_log() {
    use idb::innodb::timeline::extract_redo_timeline;

    // Build a redo log with one data block containing two MLOG records
    let mut payload = Vec::new();
    payload.push(1u8); // MLOG_1BYTE
    payload.extend_from_slice(&compress_u32(5)); // space_id
    payload.extend_from_slice(&compress_u32(3)); // page_no
    payload.push(20u8); // MLOG_UNDO_INSERT
    payload.extend_from_slice(&compress_u32(5)); // space_id
    payload.extend_from_slice(&compress_u32(7)); // page_no

    let data_block = make_data_block(4, &payload);
    let log_data = make_redo_log(&[data_block]);

    let mut log = LogFile::from_bytes(log_data).unwrap();
    let entries = extract_redo_timeline(&mut log).unwrap();

    assert!(entries.len() >= 2);
    assert!(entries.iter().all(|e| e.source == TimelineSource::RedoLog));
    assert!(entries.iter().all(|e| e.lsn.is_some()));
}

// ── CLI validation test ─────────────────────────────────────────────────

#[test]
fn test_timeline_cli_requires_at_least_one_source() {
    use idb::cli::timeline::{execute, TimelineOptions};

    let opts = TimelineOptions {
        redo_log: None,
        undo_file: None,
        binlog: None,
        file: None,
        datadir: None,
        space_id: None,
        page: None,
        table: None,
        limit: None,
        verbose: false,
        json: false,
        page_size: None,
        keyring: None,
    };

    let mut buf = Vec::new();
    let result = execute(&opts, &mut buf);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("At least one"));
}

// ── JSON serialization test ─────────────────────────────────────────────

#[test]
fn test_timeline_report_json_serializes() {
    let report = merge_timeline(
        vec![TimelineEntry {
            seq: 0,
            source: TimelineSource::RedoLog,
            lsn: Some(500),
            timestamp: None,
            space_id: Some(1),
            page_no: Some(2),
            action: TimelineAction::Redo {
                mlog_type: "MLOG_PAGE_CREATE".to_string(),
                single_rec: false,
            },
        }],
        vec![],
        vec![],
    );

    let json = serde_json::to_string_pretty(&report).unwrap();
    assert!(json.contains("\"redo_count\": 1"));
    assert!(json.contains("MLOG_PAGE_CREATE"));
    assert!(json.contains("\"seq\": 1"));
    // Verify page_summaries present
    let val: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(val["page_summaries"][0]["space_id"], 1);
    assert_eq!(val["page_summaries"][0]["page_no"], 2);
}
