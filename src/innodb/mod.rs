//! InnoDB binary format parsing.
//!
//! This module contains types and functions for reading the on-disk structures
//! used by MySQL's InnoDB storage engine, including page headers/trailers,
//! B+Tree index pages, checksum validation, SDI metadata, redo log records,
//! undo log pages, LOB (large object) pages, and tablespace-level metadata.
//!
//! Start with [`tablespace::Tablespace`] to open a `.ibd` file, then use
//! [`page::FilHeader`] to inspect individual pages.

pub mod checksum;
pub mod compression;
pub mod constants;
pub mod encryption;
pub mod index;
pub mod lob;
pub mod log;
pub mod page;
pub mod page_types;
pub mod record;
pub mod sdi;
pub mod tablespace;
pub mod undo;
