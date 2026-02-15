# Library API Overview

The `idb` crate provides Rust types and functions for parsing InnoDB tablespace files, redo logs, and related binary structures programmatically. It is published as the `innodb-utils` package on crates.io.

## Adding as a Dependency

```toml
[dependencies]
idb = { package = "innodb-utils", version = "2" }
```

For library-only usage (no CLI modules), disable default features:

```toml
[dependencies]
idb = { package = "innodb-utils", version = "2", default-features = false }
```

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `cli` | Yes | CLI-specific modules (`clap`, `colored`, `indicatif`, etc.). Disable with `default-features = false` for library-only usage. |
| `mysql` | No | Live MySQL queries via `mysql_async` + `tokio`. Implies `cli`. |

## Key Entry Points

| Type / Function | Module | Purpose |
|-----------------|--------|---------|
| `Tablespace` | `idb::innodb::tablespace` | Open `.ibd` files, read pages, iterate over all pages |
| `FilHeader` | `idb::innodb::page` | Parse the 38-byte FIL header from any InnoDB page |
| `FilTrailer` | `idb::innodb::page` | Parse the 8-byte FIL trailer from any InnoDB page |
| `FspHeader` | `idb::innodb::page` | Parse the FSP header from page 0 (space ID, size, flags) |
| `PageType` | `idb::innodb::page_types` | Map page type codes to names, descriptions, and usage notes |
| `validate_checksum` | `idb::innodb::checksum` | CRC-32C, legacy InnoDB, and MariaDB full\_crc32 validation |
| `validate_lsn` | `idb::innodb::checksum` | LSN consistency check between header and trailer |
| `find_sdi_pages` | `idb::innodb::sdi` | Locate SDI pages in a tablespace |
| `extract_sdi_from_pages` | `idb::innodb::sdi` | Extract and decompress SDI metadata records |
| `LogFile` | `idb::innodb::log` | Open and parse redo log files |
| `VendorInfo` | `idb::innodb::vendor` | Vendor detection (MySQL, Percona, MariaDB) |

## Module Overview

The library is organized under `idb::innodb`:

| Module | Purpose |
|--------|---------|
| `tablespace` | File I/O abstraction, page size auto-detection, page iteration |
| `page` | FIL header (38 bytes), FIL trailer (8 bytes), FSP header parsing |
| `page_types` | Page type enum mapping `u16` codes to names and descriptions |
| `checksum` | CRC-32C and legacy InnoDB checksum validation |
| `index` | INDEX page internals -- B+Tree header, FSEG, system records |
| `record` | Row-level record parsing -- compact format, variable-length fields |
| `sdi` | SDI metadata extraction from MySQL 8.0+ tablespaces |
| `log` | Redo log file header, checkpoints, and data block parsing |
| `undo` | UNDO log page header and segment header parsing |
| `lob` | Large object page headers (old-style BLOB and MySQL 8.0+ LOB) |
| `compression` | Compression algorithm detection and decompression (zlib, LZ4) |
| `encryption` | Encryption detection from FSP flags, encryption info parsing |
| `keyring` | MySQL `keyring_file` plugin format reader |
| `decryption` | AES-256-CBC page decryption using tablespace keys |
| `vendor` | Vendor detection (MySQL, Percona, MariaDB) and format variants |
| `constants` | InnoDB page/file structure constants from MySQL source headers |

## Quick Example

```rust,no_run
use idb::innodb::tablespace::Tablespace;
use idb::innodb::page::FilHeader;
use idb::innodb::checksum::validate_checksum;

let mut ts = Tablespace::open("employees.ibd").unwrap();
println!("Page size: {} bytes", ts.page_size());
println!("Total pages: {}", ts.page_count());
println!("Vendor: {}", ts.vendor_info());

// Read and inspect page 0
let page = ts.read_page(0).unwrap();
let header = FilHeader::parse(&page).unwrap();
println!("Page type: {}", header.page_type);

// Validate checksum
let result = validate_checksum(&page, ts.page_size(), Some(ts.vendor_info()));
println!("Checksum valid: {}", result.valid);
```

## API Reference

Full API documentation is available on docs.rs: <https://docs.rs/innodb-utils>
