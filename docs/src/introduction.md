# Introduction

IDB Utils is a command-line toolkit and Rust library for inspecting, validating, and manipulating InnoDB database files. The CLI binary is called `inno`, and the library crate is `idb`.

IDB Utils operates directly on `.ibd` tablespace files, redo logs, and system tablespaces without requiring a running MySQL instance. It is written in Rust for performance and safety, parsing binary InnoDB page structures at the byte level.

## Who Is This For?

IDB Utils serves three audiences:

- **CLI users (DBAs and sysadmins)** -- Use the `inno` command to inspect tablespace files, validate checksums, compare backups, monitor live changes, extract metadata, analyze redo logs, and assess data recoverability from the terminal.
- **Library users (Rust developers)** -- Import the `idb` crate to build custom tooling on top of InnoDB page parsing, checksum validation, SDI extraction, and redo log analysis.
- **Web users** -- Use the browser-based analyzer to drag and drop `.ibd` files for instant visual inspection without installing anything.

## Capabilities

- **Inspect tablespace files** -- Parse FIL headers, page types, INDEX structures, UNDO logs, LOB pages, and SDI records.
- **Validate checksums** -- Verify page integrity using CRC-32C, legacy InnoDB, and MariaDB full\_crc32 algorithms.
- **Compare files** -- Diff two tablespace files page-by-page to identify changes between backups or replicas.
- **Monitor changes** -- Watch a tablespace file for live modifications using LSN-based change detection.
- **Extract SDI metadata** -- Read MySQL 8.0+ Serialized Dictionary Information, including table definitions and column metadata.
- **Analyze redo logs** -- Parse redo log file headers, checkpoint blocks, and log record blocks.
- **Assess recoverability** -- Evaluate page-level damage and estimate salvageable records for data recovery planning.
- **Read encrypted tablespaces** -- Decrypt tablespace pages using keyring files for inspection and validation.
- **Audit data directories** -- Search for specific pages across a data directory, list tablespace IDs, and cross-check LSN consistency between ibdata1 and redo logs.
- **Intentional corruption** -- Corrupt pages in a controlled manner for testing backup and recovery workflows.

## Vendor Support

IDB Utils supports tablespace files from:

- MySQL 5.7 and later (including MySQL 8.0+ SDI metadata)
- Percona XtraDB (Percona Server for MySQL)
- MariaDB 10.1 and later (including MariaDB full\_crc32 checksums)

Vendor detection is automatic based on FSP flags and redo log headers.

## Links

- [GitHub Repository](https://github.com/ringo380/idb-utils) -- Source code, issue tracker, and releases.
- [API Documentation](https://docs.rs/innodb-utils) -- Rust library API reference on docs.rs.
- [Web Analyzer](https://ringo380.github.io/idb-utils/) -- Browser-based tablespace analysis tool.
