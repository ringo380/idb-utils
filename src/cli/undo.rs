//! CLI implementation for the `inno undo` subcommand.
//!
//! Analyzes undo tablespace files (`.ibu` and `.ibd`) by reading rollback
//! segment arrays, rollback segment headers, undo segment headers, and
//! undo log header chains to produce a comprehensive transaction history
//! and segment state report.

use std::io::Write;

use crate::cli::{csv_escape, wprintln};
use crate::innodb::undo;
use crate::IdbError;

/// Options for the `inno undo` subcommand.
pub struct UndoOptions {
    /// Path to the InnoDB undo tablespace file (.ibu or .ibd).
    pub file: String,
    /// Show a specific page only.
    pub page: Option<u64>,
    /// Show additional detail including undo records.
    pub verbose: bool,
    /// Output in JSON format.
    pub json: bool,
    /// Output as CSV.
    pub csv: bool,
    /// Override the auto-detected page size.
    pub page_size: Option<u32>,
    /// Path to MySQL keyring file for decrypting encrypted tablespaces.
    pub keyring: Option<String>,
    /// Use memory-mapped I/O for file access.
    pub mmap: bool,
}

/// Analyze undo tablespace and display results.
pub fn execute(opts: &UndoOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    let mut ts = crate::cli::open_tablespace(&opts.file, opts.page_size, opts.mmap)?;

    if let Some(ref keyring_path) = opts.keyring {
        crate::cli::setup_decryption(&mut ts, keyring_path)?;
    }

    // Single-page mode: just dump undo headers for one page
    if let Some(page_no) = opts.page {
        return execute_single_page(&mut ts, page_no, opts, writer);
    }

    let analysis = undo::analyze_undo_tablespace(&mut ts)?;

    if opts.json {
        let json = serde_json::to_string_pretty(&analysis)
            .map_err(|e| IdbError::Parse(e.to_string()))?;
        wprintln!(writer, "{}", json)?;
        return Ok(());
    }

    if opts.csv {
        return write_csv(&analysis, writer);
    }

    write_text(&analysis, opts.verbose, writer)
}

/// Display undo headers for a single page.
fn execute_single_page(
    ts: &mut crate::innodb::tablespace::Tablespace,
    page_no: u64,
    opts: &UndoOptions,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    let page_data = ts.read_page(page_no)?;

    let page_header = undo::UndoPageHeader::parse(&page_data).ok_or_else(|| {
        IdbError::Parse(format!("Page {} is not an undo log page", page_no))
    })?;

    let segment_header = undo::UndoSegmentHeader::parse(&page_data);

    if opts.json {
        #[derive(serde::Serialize)]
        struct SinglePageOutput {
            page_no: u64,
            page_header: undo::UndoPageHeader,
            #[serde(skip_serializing_if = "Option::is_none")]
            segment_header: Option<undo::UndoSegmentHeader>,
            log_headers: Vec<undo::UndoLogHeader>,
            record_count: usize,
        }

        let log_headers = if let Some(ref seg) = segment_header {
            if seg.last_log > 0 {
                undo::walk_undo_log_headers(&page_data, seg.last_log)
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        let records = undo::walk_undo_records(
            &page_data,
            page_header.start,
            page_header.free,
            10000,
        );

        let output = SinglePageOutput {
            page_no,
            page_header,
            segment_header,
            log_headers,
            record_count: records.len(),
        };

        let json = serde_json::to_string_pretty(&output)
            .map_err(|e| IdbError::Parse(e.to_string()))?;
        wprintln!(writer, "{}", json)?;
        return Ok(());
    }

    wprintln!(writer, "Undo Page {}", page_no)?;
    wprintln!(writer, "  Type:          {}", page_header.page_type.name())?;
    wprintln!(writer, "  Start offset:  {}", page_header.start)?;
    wprintln!(writer, "  Free offset:   {}", page_header.free)?;

    if let Some(ref seg) = segment_header {
        wprintln!(writer, "  Segment state: {}", seg.state.name())?;
        wprintln!(writer, "  Last log:      {}", seg.last_log)?;

        if seg.last_log > 0 {
            let log_headers = undo::walk_undo_log_headers(&page_data, seg.last_log);
            wprintln!(writer)?;
            wprintln!(writer, "  Undo Log Headers ({}):", log_headers.len())?;
            for (i, hdr) in log_headers.iter().enumerate() {
                wprintln!(
                    writer,
                    "    [{}] trx_id={} trx_no={} del_marks={} dict_trans={}",
                    i, hdr.trx_id, hdr.trx_no, hdr.del_marks, hdr.dict_trans
                )?;
            }
        }
    }

    if opts.verbose {
        let records = undo::walk_undo_records(
            &page_data,
            page_header.start,
            page_header.free,
            10000,
        );
        wprintln!(writer)?;
        wprintln!(writer, "  Undo Records ({}):", records.len())?;
        for rec in &records {
            wprintln!(
                writer,
                "    offset={} type={} info_bits={} data_len={}",
                rec.offset, rec.record_type, rec.info_bits, rec.data_len
            )?;
        }
    }

    Ok(())
}

/// Write full text output for undo analysis.
fn write_text(
    analysis: &undo::UndoAnalysis,
    verbose: bool,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    // RSEG array overview
    if !analysis.rseg_slots.is_empty() {
        wprintln!(
            writer,
            "Rollback Segment Array ({} slots)",
            analysis.rseg_slots.len()
        )?;
        wprintln!(
            writer,
            "{:<6} {:<12} {:<12} {:<12}",
            "Slot", "Page", "History", "Active Slots"
        )?;
        wprintln!(writer, "{}", "-".repeat(44))?;

        for (i, rseg) in analysis.rseg_headers.iter().enumerate() {
            wprintln!(
                writer,
                "{:<6} {:<12} {:<12} {:<12}",
                i, rseg.page_no, rseg.history_size, rseg.active_slot_count
            )?;
        }
        wprintln!(writer)?;
    }

    // Segment summary
    wprintln!(
        writer,
        "Undo Segments ({} total, {} active)",
        analysis.segments.len(),
        analysis.active_transactions
    )?;
    wprintln!(
        writer,
        "{:<8} {:<10} {:<8} {:<8} {:<8} {:<8}",
        "Page", "State", "Type", "Logs", "Records", "Free"
    )?;
    wprintln!(writer, "{}", "-".repeat(52))?;

    for seg in &analysis.segments {
        wprintln!(
            writer,
            "{:<8} {:<10} {:<8} {:<8} {:<8} {:<8}",
            seg.page_no,
            seg.segment_header.state.name(),
            seg.page_header.page_type.name(),
            seg.log_headers.len(),
            seg.record_count,
            seg.page_header.free
        )?;
    }

    // Verbose: undo log header details
    if verbose && !analysis.segments.is_empty() {
        wprintln!(writer)?;
        wprintln!(writer, "Undo Log Headers ({} total)", analysis.total_transactions)?;
        wprintln!(
            writer,
            "{:<8} {:<16} {:<16} {:<10} {:<6} {:<6}",
            "Page", "TRX ID", "TRX No", "Del Marks", "XID", "DDL"
        )?;
        wprintln!(writer, "{}", "-".repeat(64))?;

        for seg in &analysis.segments {
            for hdr in &seg.log_headers {
                wprintln!(
                    writer,
                    "{:<8} {:<16} {:<16} {:<10} {:<6} {:<6}",
                    seg.page_no,
                    hdr.trx_id,
                    hdr.trx_no,
                    if hdr.del_marks { "yes" } else { "no" },
                    if hdr.xid_exists { "yes" } else { "no" },
                    if hdr.dict_trans { "yes" } else { "no" }
                )?;
            }
        }
    }

    // Summary line
    wprintln!(writer)?;
    wprintln!(
        writer,
        "Total: {} segments, {} transactions, {} active",
        analysis.segments.len(),
        analysis.total_transactions,
        analysis.active_transactions
    )?;

    Ok(())
}

/// Write CSV output for undo analysis (transaction listing).
fn write_csv(analysis: &undo::UndoAnalysis, writer: &mut dyn Write) -> Result<(), IdbError> {
    wprintln!(
        writer,
        "page_no,state,type,trx_id,trx_no,del_marks,xid_exists,dict_trans,table_id"
    )?;

    for seg in &analysis.segments {
        if seg.log_headers.is_empty() {
            // Segment with no log headers — still report the segment
            wprintln!(
                writer,
                "{},{},{},,,,,",
                seg.page_no,
                csv_escape(seg.segment_header.state.name()),
                csv_escape(seg.page_header.page_type.name())
            )?;
        }
        for hdr in &seg.log_headers {
            wprintln!(
                writer,
                "{},{},{},{},{},{},{},{},{}",
                seg.page_no,
                csv_escape(seg.segment_header.state.name()),
                csv_escape(seg.page_header.page_type.name()),
                hdr.trx_id,
                hdr.trx_no,
                hdr.del_marks,
                hdr.xid_exists,
                hdr.dict_trans,
                hdr.table_id
            )?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::{BigEndian, ByteOrder};
    use crate::innodb::constants::FIL_PAGE_DATA;

    /// Build a minimal synthetic undo page for testing.
    fn build_undo_page() -> Vec<u8> {
        let mut page = vec![0u8; 16384];
        let base = FIL_PAGE_DATA;

        // FIL header: page type = FIL_PAGE_UNDO_LOG (2)
        BigEndian::write_u16(&mut page[24..], 2);

        // Undo page header
        BigEndian::write_u16(&mut page[base..], 2); // UPDATE type
        BigEndian::write_u16(&mut page[base + 2..], 120); // start
        BigEndian::write_u16(&mut page[base + 4..], 200); // free

        // Undo segment header (at base + 18)
        let seg_base = base + 18;
        BigEndian::write_u16(&mut page[seg_base..], 1); // ACTIVE state
        BigEndian::write_u16(&mut page[seg_base + 2..], 90); // last_log offset

        // Undo log header at offset 90
        let log_offset = 90;
        BigEndian::write_u64(&mut page[log_offset..], 1001); // trx_id
        BigEndian::write_u64(&mut page[log_offset + 8..], 500); // trx_no
        BigEndian::write_u16(&mut page[log_offset + 16..], 1); // del_marks
        BigEndian::write_u16(&mut page[log_offset + 18..], 120); // log_start
        page[log_offset + 20] = 0; // xid_exists
        page[log_offset + 21] = 0; // dict_trans
        BigEndian::write_u64(&mut page[log_offset + 22..], 42); // table_id
        BigEndian::write_u16(&mut page[log_offset + 30..], 0); // next_log
        BigEndian::write_u16(&mut page[log_offset + 32..], 0); // prev_log

        page
    }

    #[test]
    fn test_execute_single_page_json() {
        use crate::innodb::tablespace::Tablespace;

        let page = build_undo_page();
        let mut ts = Tablespace::from_bytes(page).unwrap();

        let opts = UndoOptions {
            file: "test.ibu".to_string(),
            page: Some(0),
            verbose: false,
            json: true,
            csv: false,
            page_size: None,
            keyring: None,
            mmap: false,
        };

        let mut buf = Vec::new();
        execute_single_page(&mut ts, 0, &opts, &mut buf).unwrap();
        let output = String::from_utf8(buf).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed["page_no"], 0);
        assert_eq!(parsed["page_header"]["page_type"], "Update");
        assert_eq!(parsed["log_headers"][0]["trx_id"], 1001);
    }

    #[test]
    fn test_execute_single_page_text() {
        use crate::innodb::tablespace::Tablespace;

        let page = build_undo_page();
        let mut ts = Tablespace::from_bytes(page).unwrap();

        let opts = UndoOptions {
            file: "test.ibu".to_string(),
            page: Some(0),
            verbose: false,
            json: false,
            csv: false,
            page_size: None,
            keyring: None,
            mmap: false,
        };

        let mut buf = Vec::new();
        execute_single_page(&mut ts, 0, &opts, &mut buf).unwrap();
        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("Undo Page 0"));
        assert!(output.contains("UPDATE"));
        assert!(output.contains("ACTIVE"));
        assert!(output.contains("trx_id=1001"));
    }

    #[test]
    fn test_write_text_empty_analysis() {
        let analysis = undo::UndoAnalysis {
            rseg_slots: Vec::new(),
            rseg_headers: Vec::new(),
            segments: Vec::new(),
            total_transactions: 0,
            active_transactions: 0,
        };

        let mut buf = Vec::new();
        write_text(&analysis, false, &mut buf).unwrap();
        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("0 total, 0 active"));
    }

    #[test]
    fn test_write_csv_header() {
        let analysis = undo::UndoAnalysis {
            rseg_slots: Vec::new(),
            rseg_headers: Vec::new(),
            segments: Vec::new(),
            total_transactions: 0,
            active_transactions: 0,
        };

        let mut buf = Vec::new();
        write_csv(&analysis, &mut buf).unwrap();
        let output = String::from_utf8(buf).unwrap();
        assert!(output.starts_with("page_no,state,type,trx_id,"));
    }
}
