//! MySQL binary log file parsing.
//!
//! Provides structures and functions for reading MySQL binary log files,
//! including the file header, format description event, and row-based events.
//!
//! Binary logs use **LittleEndian** byte order (unlike InnoDB which uses BigEndian).

pub mod checksum;
pub mod constants;
pub mod event;
pub mod events;
pub mod file;
pub mod header;

pub use checksum::validate_event_checksum;
pub use event::{BinlogEvent, BinlogEventType, CommonEventHeader};
pub use events::{analyze_binlog, BinlogAnalysis, BinlogEventSummary, RowsEvent, TableMapEvent};
pub use file::BinlogFile;
pub use header::{FormatDescriptionEvent, RotateEvent};
