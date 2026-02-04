# IDB Utils

A command-line toolkit for analyzing, parsing, and manipulating InnoDB database files. Written in Rust for performance and reliability, `idb` operates directly on `.ibd` tablespace files, redo logs, and system tablespaces without requiring a running MySQL instance.

## Installation

### From source

```bash
git clone https://github.com/ringo380/idb-utils.git
cd idb-utils
cargo build --release
```

The binary will be at `target/release/idb`.

### With MySQL support

To enable live MySQL instance queries (`idb info -D <db> -t <table>`):

```bash
cargo build --release --features mysql
```

## Quick Start

```bash
# Parse an InnoDB tablespace and show page summary
idb parse -f /var/lib/mysql/mydb/users.ibd

# Validate all page checksums
idb checksum -f users.ibd

# Hex dump page 3
idb dump -f users.ibd -p 3

# Extract MySQL 8.0+ SDI metadata
idb sdi -f users.ibd --pretty

# Search a data directory for a specific page number
idb find -d /var/lib/mysql -p 42

# Compare ibdata1 and redo log LSNs
idb info --lsn-check -d /var/lib/mysql
```

## Subcommands

| Command | Description |
|---------|-------------|
| `idb parse` | Parse .ibd file and display page headers with type summary |
| `idb pages` | Detailed page structure analysis (INDEX, UNDO, LOB, SDI) |
| `idb dump` | Hex dump of raw page bytes |
| `idb checksum` | Validate page checksums (CRC-32C and legacy InnoDB) |
| `idb corrupt` | Intentionally corrupt pages for testing |
| `idb find` | Search for pages across a MySQL data directory |
| `idb tsid` | List or find tablespace IDs |
| `idb sdi` | Extract SDI metadata from MySQL 8.0+ tablespaces |
| `idb log` | Analyze InnoDB redo log files |
| `idb info` | Inspect ibdata1 headers, compare LSNs, query table info |

All subcommands support `--help` for full option details.

## Subcommand Reference

### idb parse

Parse an `.ibd` file and display page-level information including FIL headers, FSP data, checksums, and a page type summary.

```bash
idb parse -f <file> [-p <page>] [-v] [-e] [--json] [--page-size <size>]
```

| Flag | Description |
|------|-------------|
| `-f, --file` | Path to .ibd file |
| `-p, --page` | Display a specific page number |
| `-v, --verbose` | Show additional details |
| `-e, --no-empty` | Skip empty/allocated pages |
| `--json` | Output in JSON format |
| `--page-size` | Override page size (default: auto-detect) |

### idb pages

Detailed analysis of page internals: INDEX page record headers, UNDO segments, LOB/BLOB pages, and SDI content.

```bash
idb pages -f <file> [-p <page>] [-v] [-e] [-l] [-t <type>] [--json] [--page-size <size>]
```

| Flag | Description |
|------|-------------|
| `-l, --list` | Compact one-line-per-page mode |
| `-t, --type` | Filter by page type (e.g., `INDEX`) |
| `-e, --show-empty` | Include empty/allocated pages |

### idb dump

Hex dump of raw page bytes in standard offset/hex/ASCII format.

```bash
idb dump -f <file> [-p <page>] [--offset <byte>] [-l <length>] [--raw] [--page-size <size>]
```

| Flag | Description |
|------|-------------|
| `-p, --page` | Page number to dump (default: 0) |
| `--offset` | Absolute byte offset (bypasses page mode) |
| `-l, --length` | Number of bytes to dump |
| `--raw` | Output raw binary bytes (no formatting) |

### idb checksum

Validate page checksums using both CRC-32C and legacy InnoDB algorithms.

```bash
idb checksum -f <file> [-v] [--json] [--page-size <size>]
```

| Flag | Description |
|------|-------------|
| `-v, --verbose` | Show per-page checksum details |
| `--json` | Output summary and per-page results as JSON |

### idb corrupt

Intentionally corrupt page bytes for testing InnoDB recovery scenarios.

```bash
idb corrupt -f <file> [-p <page>] [-b <bytes>] [-k] [-r] [--offset <byte>] [--json] [--page-size <size>]
```

| Flag | Description |
|------|-------------|
| `-p, --page` | Page to corrupt (random if not specified) |
| `-b, --bytes` | Number of bytes to corrupt (default: 1) |
| `-k, --header` | Corrupt the FIL header area |
| `-r, --records` | Corrupt the record data area |
| `--offset` | Absolute byte offset (bypasses page calculation) |
| `--json` | Output corruption details as JSON |

Works on any file type (`.ibd`, `ibdata1`, redo logs).

### idb find

Search recursively through a MySQL data directory for pages matching criteria.

```bash
idb find -d <datadir> -p <page> [-c <checksum>] [-s <space_id>] [--first] [--json]
```

| Flag | Description |
|------|-------------|
| `-d, --datadir` | MySQL data directory path |
| `-p, --page` | Page number to search for |
| `-c, --checksum` | Filter by checksum value |
| `-s, --space-id` | Filter by space ID |
| `--first` | Stop at first match |
| `--json` | Output matches as JSON |

### idb tsid

List tablespace IDs across all `.ibd` and `.ibu` files in a data directory.

```bash
idb tsid -d <datadir> [-l] [-t <tsid>] [--json]
```

| Flag | Description |
|------|-------------|
| `-l, --list` | List all tablespace IDs |
| `-t, --tsid` | Find file by tablespace ID |
| `--json` | Output as JSON |

### idb sdi

Extract and decompress SDI (Serialized Dictionary Information) metadata from MySQL 8.0+ tablespaces.

```bash
idb sdi -f <file> [--pretty] [--page-size <size>]
```

| Flag | Description |
|------|-------------|
| `--pretty` | Pretty-print JSON output |

Supports multi-page SDI records that span across linked pages.

### idb log

Analyze InnoDB redo log files (both legacy `ib_logfile*` and MySQL 8.0.30+ `#ib_redo*`).

```bash
idb log -f <file> [-b <blocks>] [--no-empty] [-v] [--json]
```

| Flag | Description |
|------|-------------|
| `-b, --blocks` | Limit to first N data blocks |
| `--no-empty` | Skip empty blocks |
| `-v, --verbose` | Show additional block details |
| `--json` | Output as JSON |

### idb info

Inspect InnoDB system files and optionally query a live MySQL instance for table metadata.

```bash
# Read ibdata1 page 0 header
idb info --ibdata -d /var/lib/mysql

# Compare ibdata1 LSN with redo log checkpoint LSN
idb info --lsn-check -d /var/lib/mysql

# Query table and index info from MySQL (requires --features mysql)
idb info -D mydb -t users --host 127.0.0.1 --user root
```

| Flag | Description |
|------|-------------|
| `--ibdata` | Inspect ibdata1 page 0 header |
| `--lsn-check` | Compare ibdata1 and redo log LSNs |
| `-d, --datadir` | MySQL data directory path |
| `-D, --database` | Database name (MySQL query mode) |
| `-t, --table` | Table name (MySQL query mode) |
| `--host` | MySQL host |
| `--port` | MySQL port |
| `--user` | MySQL user |
| `--password` | MySQL password |
| `--defaults-file` | Path to .my.cnf |
| `--json` | Output as JSON |

## Supported MySQL Versions

- MySQL 5.7 (legacy system tables, `ib_logfile*` redo logs)
- MySQL 8.0 (`innodb_tables`/`innodb_indexes`, SDI metadata)
- MySQL 8.0.30+ (`#innodb_redo/` directory format)
- MySQL 8.4 / 9.x

## Building from Source

Requires Rust 1.70+.

```bash
# Standard build
cargo build --release

# With MySQL query support
cargo build --release --features mysql

# Run tests
cargo test

# Run linter
cargo clippy -- -D warnings
```

## Legacy Perl Scripts

The original Perl scripts (`idb-parse.pl`, `idb-corrupter.pl`, etc.) remain in the repository root. The Rust `idb` binary replaces all of them with a unified interface and additional capabilities.
