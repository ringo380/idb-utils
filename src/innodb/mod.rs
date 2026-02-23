//! InnoDB binary format parsing.
//!
//! This module contains types and functions for reading the on-disk structures
//! used by MySQL's InnoDB storage engine, including page headers/trailers,
//! B+Tree index pages, checksum validation, SDI metadata, redo log records,
//! undo log pages, LOB (large object) pages, and tablespace-level metadata.
//!
//! Start with [`tablespace::Tablespace`] to open a `.ibd` file, then use
//! [`page::FilHeader`] to inspect individual pages.
//!
//! # Modules
//!
//! | Module | Purpose |
//! |--------|---------|
//! | [`tablespace`] | File I/O abstraction, page size auto-detection, page iteration |
//! | [`page`] | FIL header (38 bytes), FIL trailer (8 bytes), FSP header parsing |
//! | [`page_types`] | Page type enum mapping `u16` codes to names and descriptions |
//! | [`checksum`] | CRC-32C and legacy InnoDB checksum validation |
//! | [`index`] | INDEX page internals — B+Tree header, FSEG, system records |
//! | [`record`] | Row-level record parsing — compact format, variable-length fields |
//! | [`schema`] | Schema extraction and DDL reconstruction from SDI metadata |
//! | [`sdi`] | SDI metadata extraction from MySQL 8.0+ tablespaces |
//! | [`log`] | Redo log file header, checkpoints, and data block parsing |
//! | [`undo`] | UNDO log page header and segment header parsing |
//! | [`lob`] | Large object page headers (old-style BLOB and MySQL 8.0+ LOB) |
//! | [`compression`] | Compression algorithm detection and decompression (zlib, LZ4) |
//! | [`encryption`] | Encryption detection from FSP flags, encryption info parsing |
//! | [`keyring`] | MySQL `keyring_file` plugin format reader |
//! | [`decryption`] | AES-256-CBC page decryption using tablespace keys |
//! | [`vendor`] | Vendor detection (MySQL, Percona, MariaDB) and format variants |
//! | [`constants`] | InnoDB page/file structure constants from MySQL source headers |

pub mod checksum;
pub mod compression;
pub mod constants;
pub mod decryption;
pub mod encryption;
pub mod field_decode;
pub mod health;
pub mod index;
pub mod keyring;
pub mod lob;
pub mod log;
pub mod page;
pub mod page_types;
pub mod record;
pub mod schema;
pub mod sdi;
pub mod tablespace;
pub mod undo;
pub mod vendor;
#[cfg(not(target_arch = "wasm32"))]
pub mod write;
