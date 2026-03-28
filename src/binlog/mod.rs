//! MySQL binary log file parsing.
//!
//! Provides structures and functions for reading MySQL binary log files,
//! including the file header, format description event, and row-based events.
//!
//! Binary logs use **LittleEndian** byte order (unlike InnoDB which uses BigEndian).

pub mod checksum;
pub mod constants;
pub mod correlate;
pub mod event;
pub mod events;
pub mod file;
pub mod header;
pub mod row_image;

pub use checksum::validate_event_checksum;
pub use event::{BinlogEvent, BinlogEventType, CommonEventHeader};
pub use events::{analyze_binlog, BinlogAnalysis, BinlogEventSummary, RowsEvent, TableMapEvent};
pub use file::BinlogFile;
pub use header::{FormatDescriptionEvent, RotateEvent};
pub use correlate::{correlate_events, CorrelatedEvent, RowEventType};
pub use row_image::{
    extract_pk_from_row_image, parse_column_metadata, BinlogColumnMeta, BinlogPkValue,
};
