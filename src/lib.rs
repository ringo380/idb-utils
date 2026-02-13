//! InnoDB file analysis toolkit.
//!
//! The `innodb-utils` crate (library name `idb`) provides Rust types and
//! functions for parsing, inspecting, and manipulating InnoDB tablespace
//! files (`.ibd`), redo log files, and system tablespace data (`ibdata1`).
//!
//! # CLI Reference
//!
//! Install the `inno` binary and use its subcommands to work with InnoDB
//! files from the command line.
//!
//! ## Installation
//!
//! ```text
//! cargo install innodb-utils          # crates.io
//! brew install ringo380/tap/inno      # Homebrew (macOS/Linux)
//! ```
//!
//! Pre-built binaries for Linux and macOS (x86_64 + aarch64) are available
//! on the [GitHub releases page](https://github.com/ringo380/idb-utils/releases).
//!
//! ## Subcommands
//!
//! | Command | Purpose |
//! |---------|---------|
//! | [`inno parse`](cli::app::Commands::Parse) | Parse `.ibd` file and display page summary |
//! | [`inno pages`](cli::app::Commands::Pages) | Detailed page structure analysis (INDEX, UNDO, LOB, SDI) |
//! | [`inno dump`](cli::app::Commands::Dump) | Hex dump of raw page bytes |
//! | [`inno checksum`](cli::app::Commands::Checksum) | Validate page checksums (CRC-32C, legacy, MariaDB full\_crc32) |
//! | [`inno diff`](cli::app::Commands::Diff) | Compare two tablespace files page-by-page |
//! | [`inno corrupt`](cli::app::Commands::Corrupt) | Intentionally corrupt pages for testing |
//! | [`inno recover`](cli::app::Commands::Recover) | Assess page-level recoverability and count salvageable records |
//! | [`inno find`](cli::app::Commands::Find) | Search data directory for pages by number |
//! | [`inno tsid`](cli::app::Commands::Tsid) | List or find tablespace IDs |
//! | [`inno sdi`](cli::app::Commands::Sdi) | Extract SDI metadata (MySQL 8.0+) |
//! | [`inno log`](cli::app::Commands::Log) | Analyze InnoDB redo log files |
//! | [`inno info`](cli::app::Commands::Info) | Inspect ibdata1, compare LSNs, query MySQL |
//!
//! ## Global options
//!
//! All subcommands accept `--color <auto|always|never>` and `--output <file>`.
//! Most subcommands also accept `--json` for machine-readable output and
//! `--page-size` to override auto-detection.
//!
//! See the [`cli`] module for full details.
//!
//! # Library API
//!
//! Add `idb` as a dependency to use the parsing library directly:
//!
//! ```toml
//! [dependencies]
//! idb = { package = "innodb-utils", version = "1" }
//! ```
//!
//! ## Quick example
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
//! let result = validate_checksum(&page, ts.page_size(), None);
//! println!("Checksum valid: {}", result.valid);
//! ```
//!
//! ## Key entry points
//!
//! | Type / Function | Purpose |
//! |-----------------|---------|
//! | [`Tablespace`](innodb::tablespace::Tablespace) | Open `.ibd` files, read pages, iterate |
//! | [`FilHeader`](innodb::page::FilHeader) | Parse the 38-byte header on every InnoDB page |
//! | [`PageType`](innodb::page_types::PageType) | Map page type codes to names and descriptions |
//! | [`validate_checksum`](innodb::checksum::validate_checksum) | CRC-32C, legacy, and MariaDB full\_crc32 validation |
//! | [`extract_sdi_from_pages`](innodb::sdi::extract_sdi_from_pages) | SDI metadata extraction (MySQL 8.0+) |
//! | [`LogFile`](innodb::log::LogFile) | Read and inspect redo log files |
//! | [`VendorInfo`](innodb::vendor::VendorInfo) | Detected vendor (MySQL / Percona / MariaDB) and format details |
//!
//! ## Module overview
//!
//! | Module | Purpose |
//! |--------|---------|
//! | [`innodb::tablespace`] | File I/O, page size detection, page iteration |
//! | [`innodb::page`] | FIL header/trailer, FSP header parsing |
//! | [`innodb::page_types`] | Page type enum with names and descriptions |
//! | [`innodb::checksum`] | CRC-32C and legacy InnoDB checksum algorithms |
//! | [`innodb::index`] | INDEX page internals (B+Tree header, FSEG) |
//! | [`innodb::record`] | Row-level record parsing (compact format) |
//! | [`innodb::sdi`] | SDI metadata extraction and decompression |
//! | [`innodb::log`] | Redo log file structure and block parsing |
//! | [`innodb::undo`] | UNDO log page structures |
//! | [`innodb::lob`] | Large object (BLOB/LOB) page headers |
//! | [`innodb::compression`] | Compression detection and decompression |
//! | [`innodb::encryption`] | Encryption detection and encryption info parsing |
//! | [`innodb::decryption`] | AES-256-CBC page decryption with keyring support |
//! | [`innodb::keyring`] | MySQL `keyring_file` plugin binary format reader |
//! | [`innodb::vendor`] | Vendor detection (MySQL, Percona, MariaDB) and format info |
//! | [`innodb::constants`] | InnoDB page/file structure constants |
//!
//! ## Feature flags
//!
//! | Feature | Default | Description |
//! |---------|---------|-------------|
//! | `mysql` | off | Enables live MySQL queries via `mysql_async` + `tokio` (used by `inno info`). |

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
