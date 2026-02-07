use std::io::Write;

use crate::cli::wprintln;
use crate::innodb::sdi;
use crate::innodb::tablespace::Tablespace;
use crate::IdbError;

/// Options for the `inno sdi` subcommand.
pub struct SdiOptions {
    /// Path to the InnoDB tablespace file (.ibd).
    pub file: String,
    /// Pretty-print the extracted JSON metadata.
    pub pretty: bool,
    /// Override the auto-detected page size.
    pub page_size: Option<u32>,
}

/// Execute the `inno sdi` subcommand.
///
/// Finds SDI (Serialized Dictionary Information) pages in a MySQL 8.0+
/// tablespace, extracts and decompresses the embedded JSON metadata, and
/// prints each SDI record.
pub fn execute(opts: &SdiOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    let mut ts = match opts.page_size {
        Some(ps) => Tablespace::open_with_page_size(&opts.file, ps)?,
        None => Tablespace::open(&opts.file)?,
    };

    // Find SDI pages
    let sdi_pages = sdi::find_sdi_pages(&mut ts)?;

    if sdi_pages.is_empty() {
        wprintln!(writer, "No SDI pages found in {}.", opts.file)?;
        wprintln!(writer, "SDI is only available in MySQL 8.0+ tablespaces.")?;
        return Ok(());
    }

    wprintln!(writer, "Found {} SDI page(s): {:?}", sdi_pages.len(), sdi_pages)?;

    // Use multi-page reassembly to extract records
    let records = sdi::extract_sdi_from_pages(&mut ts, &sdi_pages)?;

    if records.is_empty() {
        wprintln!(writer, "No SDI records found (pages may be non-leaf or empty).")?;
        return Ok(());
    }

    for rec in &records {
        wprintln!(writer)?;
        wprintln!(
            writer,
            "=== SDI Record: type={} ({}), id={}",
            rec.sdi_type,
            sdi::sdi_type_name(rec.sdi_type),
            rec.sdi_id
        )?;
        wprintln!(
            writer,
            "Compressed: {} bytes, Uncompressed: {} bytes",
            rec.compressed_len, rec.uncompressed_len
        )?;

        if rec.data.is_empty() {
            wprintln!(writer, "(Data could not be decompressed - may span multiple pages)")?;
            continue;
        }

        if opts.pretty {
            // Pretty-print JSON
            match serde_json::from_str::<serde_json::Value>(&rec.data) {
                Ok(json) => {
                    wprintln!(
                        writer,
                        "{}",
                        serde_json::to_string_pretty(&json).unwrap_or(rec.data.clone())
                    )?;
                }
                Err(_) => {
                    wprintln!(writer, "{}", rec.data)?;
                }
            }
        } else {
            wprintln!(writer, "{}", rec.data)?;
        }
    }

    wprintln!(writer)?;
    wprintln!(writer, "Total SDI records: {}", records.len())?;

    Ok(())
}
