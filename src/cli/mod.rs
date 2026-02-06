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
