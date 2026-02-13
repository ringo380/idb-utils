use std::io::Write;

use colored::Colorize;
use serde::Serialize;

use crate::cli::{create_progress_bar, wprintln};
use crate::innodb::constants::SIZE_FIL_HEAD;
use crate::innodb::page::FilHeader;
use crate::innodb::tablespace::Tablespace;
use crate::IdbError;

/// Options for the `inno diff` subcommand.
pub struct DiffOptions {
    /// Path to the first InnoDB tablespace file.
    pub file1: String,
    /// Path to the second InnoDB tablespace file.
    pub file2: String,
    /// Show per-page header field diffs.
    pub verbose: bool,
    /// Show byte-range diffs (requires verbose).
    pub byte_ranges: bool,
    /// Compare a single page only.
    pub page: Option<u64>,
    /// Emit output as JSON.
    pub json: bool,
    /// Override the auto-detected page size.
    pub page_size: Option<u32>,
    /// Path to MySQL keyring file for decrypting encrypted tablespaces.
    pub keyring: Option<String>,
}

// ── JSON output structs ─────────────────────────────────────────────

#[derive(Serialize)]
struct DiffReport {
    file1: FileInfo,
    file2: FileInfo,
    page_size_mismatch: bool,
    summary: DiffSummary,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    modified_pages: Vec<PageDiff>,
}

#[derive(Serialize)]
struct FileInfo {
    path: String,
    page_count: u64,
    page_size: u32,
}

#[derive(Serialize)]
struct DiffSummary {
    identical: u64,
    modified: u64,
    only_in_file1: u64,
    only_in_file2: u64,
}

#[derive(Serialize)]
struct PageDiff {
    page_number: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    file1_header: Option<HeaderFields>,
    #[serde(skip_serializing_if = "Option::is_none")]
    file2_header: Option<HeaderFields>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    changed_fields: Vec<FieldChange>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    byte_ranges: Vec<ByteRange>,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_bytes_changed: Option<usize>,
}

#[derive(Serialize)]
struct HeaderFields {
    checksum: String,
    page_number: u32,
    prev_page: String,
    next_page: String,
    lsn: u64,
    page_type: String,
    flush_lsn: u64,
    space_id: u32,
}

#[derive(Serialize)]
struct FieldChange {
    field: String,
    old_value: String,
    new_value: String,
}

#[derive(Serialize)]
struct ByteRange {
    start: usize,
    end: usize,
    length: usize,
}

// ── Helpers ─────────────────────────────────────────────────────────

fn header_to_fields(h: &FilHeader) -> HeaderFields {
    HeaderFields {
        checksum: format!("0x{:08X}", h.checksum),
        page_number: h.page_number,
        prev_page: format!("0x{:08X}", h.prev_page),
        next_page: format!("0x{:08X}", h.next_page),
        lsn: h.lsn,
        page_type: h.page_type.name().to_string(),
        flush_lsn: h.flush_lsn,
        space_id: h.space_id,
    }
}

fn compare_headers(h1: &FilHeader, h2: &FilHeader) -> Vec<FieldChange> {
    let mut changes = Vec::new();

    if h1.checksum != h2.checksum {
        changes.push(FieldChange {
            field: "Checksum".to_string(),
            old_value: format!("0x{:08X}", h1.checksum),
            new_value: format!("0x{:08X}", h2.checksum),
        });
    }
    if h1.page_number != h2.page_number {
        changes.push(FieldChange {
            field: "Page Number".to_string(),
            old_value: h1.page_number.to_string(),
            new_value: h2.page_number.to_string(),
        });
    }
    if h1.prev_page != h2.prev_page {
        changes.push(FieldChange {
            field: "Prev Page".to_string(),
            old_value: format!("0x{:08X}", h1.prev_page),
            new_value: format!("0x{:08X}", h2.prev_page),
        });
    }
    if h1.next_page != h2.next_page {
        changes.push(FieldChange {
            field: "Next Page".to_string(),
            old_value: format!("0x{:08X}", h1.next_page),
            new_value: format!("0x{:08X}", h2.next_page),
        });
    }
    if h1.lsn != h2.lsn {
        changes.push(FieldChange {
            field: "LSN".to_string(),
            old_value: h1.lsn.to_string(),
            new_value: h2.lsn.to_string(),
        });
    }
    if h1.page_type != h2.page_type {
        changes.push(FieldChange {
            field: "Page Type".to_string(),
            old_value: h1.page_type.name().to_string(),
            new_value: h2.page_type.name().to_string(),
        });
    }
    if h1.flush_lsn != h2.flush_lsn {
        changes.push(FieldChange {
            field: "Flush LSN".to_string(),
            old_value: h1.flush_lsn.to_string(),
            new_value: h2.flush_lsn.to_string(),
        });
    }
    if h1.space_id != h2.space_id {
        changes.push(FieldChange {
            field: "Space ID".to_string(),
            old_value: h1.space_id.to_string(),
            new_value: h2.space_id.to_string(),
        });
    }

    changes
}

fn find_diff_ranges(data1: &[u8], data2: &[u8]) -> Vec<ByteRange> {
    let len = data1.len().min(data2.len());
    let mut ranges = Vec::new();
    let mut in_diff = false;
    let mut start = 0;

    for i in 0..len {
        if data1[i] != data2[i] {
            if !in_diff {
                in_diff = true;
                start = i;
            }
        } else if in_diff {
            in_diff = false;
            ranges.push(ByteRange {
                start,
                end: i,
                length: i - start,
            });
        }
    }
    if in_diff {
        ranges.push(ByteRange {
            start,
            end: len,
            length: len - start,
        });
    }

    ranges
}

/// Compare two InnoDB tablespace files page-by-page.
pub fn execute(opts: &DiffOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    let mut ts1 = match opts.page_size {
        Some(ps) => Tablespace::open_with_page_size(&opts.file1, ps)?,
        None => Tablespace::open(&opts.file1)?,
    };
    let mut ts2 = match opts.page_size {
        Some(ps) => Tablespace::open_with_page_size(&opts.file2, ps)?,
        None => Tablespace::open(&opts.file2)?,
    };

    if let Some(ref keyring_path) = opts.keyring {
        crate::cli::setup_decryption(&mut ts1, keyring_path)?;
        crate::cli::setup_decryption(&mut ts2, keyring_path)?;
    }

    let ps1 = ts1.page_size();
    let ps2 = ts2.page_size();
    let pc1 = ts1.page_count();
    let pc2 = ts2.page_count();

    let page_size_mismatch = ps1 != ps2;

    if opts.json {
        return execute_json(opts, &mut ts1, &mut ts2, page_size_mismatch, writer);
    }

    // Text output
    wprintln!(writer, "Comparing:")?;
    wprintln!(
        writer,
        "  File 1: {} ({} pages, {} bytes/page)",
        opts.file1, pc1, ps1
    )?;
    wprintln!(
        writer,
        "  File 2: {} ({} pages, {} bytes/page)",
        opts.file2, pc2, ps2
    )?;
    wprintln!(writer)?;

    if page_size_mismatch {
        wprintln!(
            writer,
            "{}",
            format!(
                "WARNING: Page size mismatch ({} vs {}). Comparing FIL headers only.",
                ps1, ps2
            )
            .yellow()
        )?;
        wprintln!(writer)?;
    }

    // Determine comparison range
    let (start_page, end_page) = match opts.page {
        Some(p) => {
            if p >= pc1 && p >= pc2 {
                return Err(IdbError::Argument(format!(
                    "Page {} out of range (file1 has {} pages, file2 has {} pages)",
                    p, pc1, pc2
                )));
            }
            (p, p + 1)
        }
        None => (0, pc1.max(pc2)),
    };

    let common_pages = pc1.min(pc2);
    let mut identical = 0u64;
    let mut modified = 0u64;
    let mut only_in_file1 = 0u64;
    let mut only_in_file2 = 0u64;
    let mut modified_page_nums: Vec<u64> = Vec::new();

    let total = end_page - start_page;
    let pb = create_progress_bar(total, "pages");

    for page_num in start_page..end_page {
        pb.inc(1);

        // Pages only in one file
        if page_num >= pc1 {
            only_in_file2 += 1;
            continue;
        }
        if page_num >= pc2 {
            only_in_file1 += 1;
            continue;
        }

        let data1 = ts1.read_page(page_num)?;
        let data2 = ts2.read_page(page_num)?;

        if page_size_mismatch {
            // Compare only FIL headers (first 38 bytes)
            let cmp_len = SIZE_FIL_HEAD.min(data1.len()).min(data2.len());
            if data1[..cmp_len] == data2[..cmp_len] {
                identical += 1;
            } else {
                modified += 1;
                modified_page_nums.push(page_num);

                if opts.verbose {
                    print_page_diff(writer, page_num, &data1, &data2, opts.byte_ranges, true)?;
                }
            }
        } else {
            // Full page comparison
            if data1 == data2 {
                identical += 1;
            } else {
                modified += 1;
                modified_page_nums.push(page_num);

                if opts.verbose {
                    print_page_diff(writer, page_num, &data1, &data2, opts.byte_ranges, false)?;
                }
            }
        }
    }

    pb.finish_and_clear();

    // Count pages beyond common range for non-single-page mode
    if opts.page.is_none() {
        if pc1 > common_pages {
            only_in_file1 = pc1 - common_pages;
        }
        if pc2 > common_pages {
            only_in_file2 = pc2 - common_pages;
        }
    }

    // Print summary
    wprintln!(writer, "Summary:")?;
    wprintln!(writer, "  Identical pages:  {}", identical)?;
    if modified > 0 {
        wprintln!(
            writer,
            "  Modified pages:   {}",
            format!("{}", modified).red()
        )?;
    } else {
        wprintln!(writer, "  Modified pages:   {}", modified)?;
    }
    wprintln!(writer, "  Only in file 1:   {}", only_in_file1)?;
    wprintln!(writer, "  Only in file 2:   {}", only_in_file2)?;

    if !modified_page_nums.is_empty() {
        wprintln!(writer)?;
        let nums: Vec<String> = modified_page_nums.iter().map(|n| n.to_string()).collect();
        wprintln!(writer, "Modified pages: {}", nums.join(", "))?;
    }

    Ok(())
}

fn print_page_diff(
    writer: &mut dyn Write,
    page_num: u64,
    data1: &[u8],
    data2: &[u8],
    show_byte_ranges: bool,
    header_only: bool,
) -> Result<(), IdbError> {
    wprintln!(writer, "Page {}: {}", page_num, "MODIFIED".red())?;

    let h1 = FilHeader::parse(data1);
    let h2 = FilHeader::parse(data2);

    match (h1, h2) {
        (Some(h1), Some(h2)) => {
            let changes = compare_headers(&h1, &h2);
            if changes.is_empty() {
                wprintln!(writer, "  FIL header: identical (data content differs)")?;
            } else {
                for c in &changes {
                    wprintln!(writer, "  {}: {} -> {}", c.field, c.old_value, c.new_value)?;
                }
            }

            // Report unchanged page type for context
            if h1.page_type == h2.page_type
                && !changes.iter().any(|c| c.field == "Page Type")
            {
                wprintln!(writer, "  Page Type: {} (unchanged)", h1.page_type.name())?;
            }
        }
        _ => {
            wprintln!(writer, "  (could not parse one or both FIL headers)")?;
        }
    }

    if show_byte_ranges && !header_only {
        let ranges = find_diff_ranges(data1, data2);
        if !ranges.is_empty() {
            wprintln!(writer, "  Byte diff ranges:")?;
            for r in &ranges {
                wprintln!(writer, "    {}-{} ({} bytes)", r.start, r.end, r.length)?;
            }
            let total_changed: usize = ranges.iter().map(|r| r.length).sum();
            let page_size = data1.len();
            let pct = (total_changed as f64 / page_size as f64) * 100.0;
            wprintln!(
                writer,
                "  Total: {} bytes changed ({:.1}% of page)",
                total_changed,
                pct
            )?;
        }
    }

    wprintln!(writer)?;
    Ok(())
}

fn execute_json(
    opts: &DiffOptions,
    ts1: &mut Tablespace,
    ts2: &mut Tablespace,
    page_size_mismatch: bool,
    writer: &mut dyn Write,
) -> Result<(), IdbError> {
    let ps1 = ts1.page_size();
    let ps2 = ts2.page_size();
    let pc1 = ts1.page_count();
    let pc2 = ts2.page_count();

    let (start_page, end_page) = match opts.page {
        Some(p) => {
            if p >= pc1 && p >= pc2 {
                return Err(IdbError::Argument(format!(
                    "Page {} out of range (file1 has {} pages, file2 has {} pages)",
                    p, pc1, pc2
                )));
            }
            (p, p + 1)
        }
        None => (0, pc1.max(pc2)),
    };

    let mut identical = 0u64;
    let mut modified = 0u64;
    let mut only_in_file1 = 0u64;
    let mut only_in_file2 = 0u64;
    let mut modified_pages: Vec<PageDiff> = Vec::new();

    for page_num in start_page..end_page {
        if page_num >= pc1 {
            only_in_file2 += 1;
            continue;
        }
        if page_num >= pc2 {
            only_in_file1 += 1;
            continue;
        }

        let data1 = ts1.read_page(page_num)?;
        let data2 = ts2.read_page(page_num)?;

        let is_equal = if page_size_mismatch {
            let cmp_len = SIZE_FIL_HEAD.min(data1.len()).min(data2.len());
            data1[..cmp_len] == data2[..cmp_len]
        } else {
            data1 == data2
        };

        if is_equal {
            identical += 1;
        } else {
            modified += 1;

            let h1 = FilHeader::parse(&data1);
            let h2 = FilHeader::parse(&data2);

            let (file1_header, file2_header, changed_fields) = match (&h1, &h2) {
                (Some(h1), Some(h2)) => {
                    let changes = compare_headers(h1, h2);
                    (
                        Some(header_to_fields(h1)),
                        Some(header_to_fields(h2)),
                        changes,
                    )
                }
                _ => (
                    h1.as_ref().map(header_to_fields),
                    h2.as_ref().map(header_to_fields),
                    Vec::new(),
                ),
            };

            let (byte_ranges, total_bytes_changed) =
                if opts.byte_ranges && !page_size_mismatch {
                    let ranges = find_diff_ranges(&data1, &data2);
                    let total: usize = ranges.iter().map(|r| r.length).sum();
                    (ranges, Some(total))
                } else {
                    (Vec::new(), None)
                };

            modified_pages.push(PageDiff {
                page_number: page_num,
                file1_header,
                file2_header,
                changed_fields,
                byte_ranges,
                total_bytes_changed,
            });
        }
    }

    // For non-single-page mode, count pages beyond common range
    if opts.page.is_none() {
        let common = pc1.min(pc2);
        if pc1 > common {
            only_in_file1 = pc1 - common;
        }
        if pc2 > common {
            only_in_file2 = pc2 - common;
        }
    }

    let report = DiffReport {
        file1: FileInfo {
            path: opts.file1.clone(),
            page_count: pc1,
            page_size: ps1,
        },
        file2: FileInfo {
            path: opts.file2.clone(),
            page_count: pc2,
            page_size: ps2,
        },
        page_size_mismatch,
        summary: DiffSummary {
            identical,
            modified,
            only_in_file1,
            only_in_file2,
        },
        modified_pages,
    };

    let json = serde_json::to_string_pretty(&report)
        .map_err(|e| IdbError::Parse(format!("JSON serialization error: {}", e)))?;
    wprintln!(writer, "{}", json)?;

    Ok(())
}
