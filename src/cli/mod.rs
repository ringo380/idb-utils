//! CLI subcommand implementations for the `inno` binary.
//!
//! The `inno` binary provides thirteen subcommands for analyzing InnoDB data files,
//! redo logs, and system tablespaces. CLI argument parsing uses clap derive macros,
//! with the top-level [`app::Cli`] struct and [`app::Commands`] enum defined in
//! [`app`] and shared between `main.rs` and `build.rs` (for man page generation)
//! via `include!()`.
//!
//! Each subcommand module follows the same pattern: an `Options` struct holding
//! the parsed arguments and a `pub fn execute(opts, writer) -> Result<(), IdbError>`
//! entry point. The `writer: &mut dyn Write` parameter allows output to be
//! captured in tests or redirected to a file via the global `--output` flag.
//!
//! # Subcommands
//!
//! | Command | Module | Purpose |
//! |---------|--------|---------|
//! | `inno parse` | [`parse`] | Parse FIL headers for every page and show a page-type summary table |
//! | `inno pages` | [`pages`] | Deep structure analysis of INDEX, UNDO, BLOB/LOB, and SDI pages |
//! | `inno dump` | [`dump`] | Hex dump of raw bytes by page number or absolute file offset |
//! | `inno checksum` | [`checksum`] | Validate CRC-32C and legacy InnoDB checksums for every page |
//! | `inno diff` | [`diff`] | Compare two tablespace files page-by-page and report differences |
//! | `inno watch` | [`watch`] | Monitor a tablespace file for page-level changes in real time |
//! | `inno corrupt` | [`corrupt`] | Inject random bytes into a page for testing recovery workflows |
//! | `inno recover` | [`recover`] | Assess page-level recoverability and count salvageable records |
//! | `inno find` | [`find`] | Search a MySQL data directory for pages matching a page number |
//! | `inno tsid` | [`tsid`] | List or look up tablespace (space) IDs across `.ibd`/`.ibu` files |
//! | `inno sdi` | [`sdi`] | Extract SDI metadata (MySQL 8.0+ serialized data dictionary) |
//! | `inno log` | [`log`] | Analyze redo log file headers, checkpoints, and data blocks |
//! | `inno info` | [`info`] | Inspect `ibdata1`, compare LSNs, or query a live MySQL instance |
//!
//! # Common patterns
//!
//! - **`--json`** — Every subcommand supports structured JSON output via
//!   `#[derive(Serialize)]` structs and `serde_json`.
//! - **`--page-size`** — Override auto-detected page size (useful for non-standard
//!   4K, 8K, 32K, or 64K tablespaces).
//! - **`--verbose` / `-v`** — Show additional detail such as per-page checksum
//!   status, FSEG internals, or MLOG record types.
//! - **`--color`** (global) — Control colored terminal output (`auto`, `always`,
//!   `never`).
//! - **`--output` / `-o`** (global) — Redirect output to a file instead of stdout.
//!
//! Progress bars (via [`indicatif`]) are displayed for long-running operations
//! in `parse`, `checksum`, and `find`. The `wprintln!` and `wprint!` macros
//! wrap `writeln!`/`write!` to convert `io::Error` into `IdbError`.

pub mod app;
pub mod checksum;
pub mod corrupt;
pub mod diff;
pub mod dump;
pub mod find;
pub mod info;
pub mod log;
pub mod pages;
pub mod parse;
pub mod recover;
pub mod sdi;
pub mod tsid;
pub mod watch;

/// Write a line to the given writer, converting io::Error to IdbError.
macro_rules! wprintln {
    ($w:expr) => {
        writeln!($w).map_err(|e| $crate::IdbError::Io(e.to_string()))
    };
    ($w:expr, $($arg:tt)*) => {
        writeln!($w, $($arg)*).map_err(|e| $crate::IdbError::Io(e.to_string()))
    };
}

/// Write (without newline) to the given writer, converting io::Error to IdbError.
macro_rules! wprint {
    ($w:expr, $($arg:tt)*) => {
        write!($w, $($arg)*).map_err(|e| $crate::IdbError::Io(e.to_string()))
    };
}

pub(crate) use wprint;
pub(crate) use wprintln;

use crate::innodb::decryption::DecryptionContext;
use crate::innodb::keyring::Keyring;
use crate::innodb::tablespace::Tablespace;
use crate::IdbError;
use indicatif::{ProgressBar, ProgressStyle};

/// Open a tablespace file, selecting mmap or buffered I/O based on the flag.
///
/// When `use_mmap` is true, the file is memory-mapped via `mmap(2)` for
/// potentially better performance on large files (especially with parallel
/// processing). When `page_size` is `Some`, auto-detection is bypassed.
pub(crate) fn open_tablespace(
    path: &str,
    page_size: Option<u32>,
    use_mmap: bool,
) -> Result<Tablespace, IdbError> {
    match (use_mmap, page_size) {
        (true, Some(ps)) => Tablespace::open_mmap_with_page_size(path, ps),
        (true, None) => Tablespace::open_mmap(path),
        (false, Some(ps)) => Tablespace::open_with_page_size(path, ps),
        (false, None) => Tablespace::open(path),
    }
}

/// Set up decryption on a tablespace if a keyring path is provided.
///
/// Loads the keyring file, reads the encryption info from page 0,
/// decrypts the tablespace key, and installs the decryption context
/// on the tablespace for transparent page decryption.
pub(crate) fn setup_decryption(ts: &mut Tablespace, keyring_path: &str) -> Result<(), IdbError> {
    let keyring = Keyring::load(keyring_path)?;
    let enc_info = ts.encryption_info().ok_or_else(|| {
        IdbError::Parse(
            "Keyring provided but tablespace has no encryption info on page 0".to_string(),
        )
    })?;
    let ctx = DecryptionContext::from_encryption_info(enc_info, &keyring)?;
    ts.set_decryption_context(ctx);
    Ok(())
}

/// Create a styled progress bar for iterating over pages or files.
pub(crate) fn create_progress_bar(count: u64, unit: &str) -> ProgressBar {
    let pb = ProgressBar::new(count);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(&format!(
                "{{spinner:.green}} [{{bar:40.cyan/blue}}] {{pos}}/{{len}} {} ({{eta}})",
                unit
            ))
            .unwrap()
            .progress_chars("#>-"),
    );
    pb
}
