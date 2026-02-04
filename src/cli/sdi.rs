use crate::innodb::sdi;
use crate::innodb::tablespace::Tablespace;
use crate::IdbError;

pub struct SdiOptions {
    pub file: String,
    pub pretty: bool,
    pub page_size: Option<u32>,
}

pub fn execute(opts: &SdiOptions) -> Result<(), IdbError> {
    let mut ts = match opts.page_size {
        Some(ps) => Tablespace::open_with_page_size(&opts.file, ps)?,
        None => Tablespace::open(&opts.file)?,
    };

    // Find SDI pages
    let sdi_pages = sdi::find_sdi_pages(&mut ts)?;

    if sdi_pages.is_empty() {
        println!("No SDI pages found in {}.", opts.file);
        println!("SDI is only available in MySQL 8.0+ tablespaces.");
        return Ok(());
    }

    println!("Found {} SDI page(s): {:?}", sdi_pages.len(), sdi_pages);

    let mut total_records = 0;

    for page_num in &sdi_pages {
        let page_data = ts.read_page(*page_num)?;

        if let Some(records) = sdi::extract_sdi_from_page(&page_data) {
            for rec in &records {
                total_records += 1;
                println!();
                println!(
                    "=== SDI Record: type={} ({}), id={}",
                    rec.sdi_type,
                    sdi::sdi_type_name(rec.sdi_type),
                    rec.sdi_id
                );
                println!(
                    "Compressed: {} bytes, Uncompressed: {} bytes",
                    rec.compressed_len, rec.uncompressed_len
                );

                if rec.data.is_empty() {
                    println!("(Data could not be decompressed - may span multiple pages)");
                    continue;
                }

                if opts.pretty {
                    // Pretty-print JSON
                    match serde_json::from_str::<serde_json::Value>(&rec.data) {
                        Ok(json) => {
                            println!(
                                "{}",
                                serde_json::to_string_pretty(&json).unwrap_or(rec.data.clone())
                            );
                        }
                        Err(_) => {
                            println!("{}", rec.data);
                        }
                    }
                } else {
                    println!("{}", rec.data);
                }
            }
        }
    }

    if total_records == 0 {
        println!("No SDI records found (pages may be non-leaf or empty).");
    } else {
        println!();
        println!("Total SDI records: {}", total_records);
    }

    Ok(())
}
