//! CLI subcommand implementations.
//!
//! Each subcommand has an `Options` struct (clap derive) and a
//! `pub fn execute(opts, writer) -> Result<(), IdbError>` entry point.
//! The `writer: &mut dyn Write` parameter allows output to be captured
//! in tests or redirected as needed.

pub mod app;
pub mod checksum;
pub mod corrupt;
pub mod dump;
pub mod find;
pub mod info;
pub mod log;
pub mod pages;
pub mod parse;
pub mod sdi;
pub mod tsid;

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

pub(crate) use wprintln;
pub(crate) use wprint;

use indicatif::{ProgressBar, ProgressStyle};

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
