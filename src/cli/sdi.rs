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
    /// Path to MySQL keyring file for decrypting encrypted tablespaces.
    pub keyring: Option<String>,
}

/// Extract SDI metadata from a MySQL 8.0+ tablespace.
///
/// SDI (Serialized Dictionary Information) is MySQL 8.0's mechanism for
/// embedding the data dictionary (table definitions, column metadata, index
/// descriptions) directly inside each `.ibd` file, replacing the `.frm` files
/// used in MySQL 5.x. SDI data is stored in pages of type 17853
/// (`FIL_PAGE_SDI`). The SDI binary format and JSON schema are identical
/// across MySQL 8.0, 9.0, and 9.1.
///
/// This command first scans the tablespace for SDI pages by checking page
/// types, then uses [`sdi::extract_sdi_from_pages`]
/// to reassemble records that may span multiple pages via the next-page chain.
/// Each record's zlib-compressed payload is decompressed into a JSON string
/// containing the full table/column/index definition.
///
/// With `--pretty`, the JSON is re-parsed and re-serialized with indentation
/// for readability. If a tablespace has no SDI pages (e.g., pre-8.0 files),
/// a message is printed indicating that SDI is unavailable.
pub fn execute(opts: &SdiOptions, writer: &mut dyn Write) -> Result<(), IdbError> {
    let mut ts = match opts.page_size {
        Some(ps) => Tablespace::open_with_page_size(&opts.file, ps)?,
        None => Tablespace::open(&opts.file)?,
    };

    if let Some(ref keyring_path) = opts.keyring {
        crate::cli::setup_decryption(&mut ts, keyring_path)?;
    }

    // MariaDB does not use SDI — return a clear error
    if ts.vendor_info().vendor == crate::innodb::vendor::InnoDbVendor::MariaDB {
        return Err(IdbError::Argument(
            "SDI is not available for MariaDB tablespaces. MariaDB does not use \
             Serialized Dictionary Information (SDI); table metadata is stored \
             in the data dictionary (mysql.* tables) or .frm files."
                .to_string(),
        ));
    }

    // Find SDI pages
    let sdi_pages = sdi::find_sdi_pages(&mut ts)?;

    if sdi_pages.is_empty() {
        wprintln!(writer, "No SDI pages found in {}.", opts.file)?;
        wprintln!(writer, "SDI is only available in MySQL 8.0+ tablespaces.")?;
        return Ok(());
    }

    wprintln!(
        writer,
        "Found {} SDI page(s): {:?}",
        sdi_pages.len(),
        sdi_pages
    )?;

    // Use multi-page reassembly to extract records
    let records = sdi::extract_sdi_from_pages(&mut ts, &sdi_pages)?;

    if records.is_empty() {
        wprintln!(
            writer,
            "No SDI records found (pages may be non-leaf or empty)."
        )?;
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
            rec.compressed_len,
            rec.uncompressed_len
        )?;

        if rec.data.is_empty() {
            wprintln!(
                writer,
                "(Data could not be decompressed - may span multiple pages)"
            )?;
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
