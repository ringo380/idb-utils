//! MySQL binary log file parsing.
//!
//! Provides structures and functions for reading MySQL binary log files,
//! including the file header, format description event, and row-based events.
//!
//! Binary logs use **LittleEndian** byte order (unlike InnoDB which uses BigEndian).

pub mod events;
pub mod header;

pub use events::*;
pub use header::*;
