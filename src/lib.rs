//! InnoDB file analysis toolkit.
//!
//! `idb` provides Rust types and functions for parsing, inspecting, and
//! manipulating InnoDB tablespace files (`.ibd`), redo log files, and
//! system tablespace data (`ibdata1`).
//!
//! # Two ways to use this crate
//!
//! 1. **As a CLI tool** — install the `inno` binary via `cargo install innodb-utils`
//!    and use its subcommands (`inno parse`, `inno checksum`, `inno sdi`, etc.).
//! 2. **As a library** — depend on `idb` in your `Cargo.toml` and use the types
//!    in [`innodb`] directly.
//!
//! # Feature flags
//!
//! | Feature | Default | Description |
//! |---------|---------|-------------|
//! | `mysql` | off | Enables live MySQL queries via `mysql_async` + `tokio` (used by `inno info`). |
//!
//! # Quick example
//!
//! ```no_run
//! use idb::innodb::tablespace::Tablespace;
//! use idb::innodb::checksum::validate_checksum;
//! use idb::innodb::page::FilHeader;
//!
//! // Open a tablespace (page size is auto-detected from page 0)
//! let mut ts = Tablespace::open("table.ibd").unwrap();
//!
//! // Read and inspect a page
//! let page = ts.read_page(0).unwrap();
//! let header = FilHeader::parse(&page).unwrap();
//! println!("Page type: {}", header.page_type);
//!
//! // Validate the page checksum
//! let result = validate_checksum(&page, ts.page_size());
//! println!("Checksum valid: {}", result.valid);
//! ```
//!
//! # Key entry points
//!
//! - [`innodb::tablespace::Tablespace`] — open and read `.ibd` files
//! - [`innodb::page::FilHeader`] — the 38-byte header on every InnoDB page
//! - [`innodb::page_types::PageType`] — page type enum with names and descriptions
//! - [`innodb::checksum::validate_checksum`] — CRC-32C and legacy checksum validation
//! - [`innodb::sdi`] — SDI metadata extraction (MySQL 8.0+)
//! - [`innodb::log::LogFile`] — redo log file reader

pub mod cli;
pub mod innodb;
pub mod util;

use thiserror::Error;

/// Errors returned by `idb` operations.
#[derive(Error, Debug)]
pub enum IdbError {
    /// An I/O error occurred (file open, read, seek, or write failure).
    #[error("I/O error: {0}")]
    Io(String),

    /// A parse error occurred (malformed binary data or unexpected values).
    #[error("Parse error: {0}")]
    Parse(String),

    /// An invalid argument was supplied (out-of-range page number, bad option, etc.).
    #[error("Invalid argument: {0}")]
    Argument(String),
}
