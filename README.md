# IDB Utils

A command-line toolkit for inspecting, validating, and manipulating InnoDB database files. Written in Rust for performance and reliability, the `inno` binary operates directly on `.ibd` tablespace files, redo logs, and system tablespaces — no running MySQL instance required.

## What It Does

IDB Utils gives you low-level visibility into InnoDB's on-disk structures. Use it to:

- **Inspect tablespace files** — parse page headers, examine B+Tree index structures, walk undo logs, inspect BLOB/LOB chains
- **Validate data integrity** — verify CRC-32C and legacy InnoDB checksums, detect LSN mismatches between headers and trailers
- **Compare tablespace files** — diff two `.ibd` files page-by-page to detect changes between backups, find corruption drift, or analyze before/after differences
- **Extract metadata** — read MySQL 8.0+ SDI (Serialized Dictionary Information) directly from `.ibd` files without a running server
- **Analyze redo logs** — parse log file headers, checkpoints, and data blocks from both legacy and MySQL 8.0.30+ formats
- **Assess data recoverability** — scan damaged tablespaces, classify page integrity, count salvageable records on INDEX pages
- **Test recovery scenarios** — intentionally corrupt specific pages or byte ranges to exercise InnoDB recovery mechanisms
- **Audit a data directory** — search for pages across files, list tablespace IDs, compare LSNs between ibdata1 and redo logs

## Installation

### From Source

Requires Rust 1.70+.

```bash
git clone https://github.com/ringo380/idb-utils.git
cd idb-utils
cargo build --release
```

The binary will be at `target/release/inno`. Copy it to a directory in your `$PATH`:

```bash
cp target/release/inno /usr/local/bin/
```

### With MySQL Query Support

To enable live MySQL instance queries via `inno info`:

```bash
cargo build --release --features mysql
```

This adds the `mysql_async` and `tokio` dependencies for connecting to a running MySQL server.

### Verify Installation

```bash
inno --help
inno --version
```

## Quick Start

```bash
# Parse a tablespace and show a page type summary
inno parse -f /var/lib/mysql/mydb/users.ibd

# Validate every page checksum in a tablespace
inno checksum -f users.ibd

# Get detailed INDEX page structure (record counts, B+Tree levels)
inno pages -f users.ibd -t INDEX

# Hex dump page 3
inno dump -f users.ibd -p 3

# Extract table/column definitions from MySQL 8.0+ SDI
inno sdi -f users.ibd --pretty

# Analyze redo log checkpoints
inno log -f /var/lib/mysql/ib_logfile0

# Compare two tablespace files (e.g., before/after a backup)
inno diff backup.ibd current.ibd -v

# Assess recoverability of a damaged tablespace
inno recover -f users.ibd --force -v

# Search an entire data directory for page number 42
inno find -d /var/lib/mysql -p 42

# Check if ibdata1 and redo log LSNs are in sync
inno info --lsn-check -d /var/lib/mysql
```

Every subcommand supports `--json` for machine-readable output and `--help` for full option details.

## Subcommands

| Command | Description |
|---------|-------------|
| [`parse`](#inno-parse) | Parse .ibd file and display page headers with type summary |
| [`pages`](#inno-pages) | Detailed page structure analysis (INDEX, UNDO, LOB, SDI) |
| [`dump`](#inno-dump) | Hex dump of raw page bytes |
| [`checksum`](#inno-checksum) | Validate page checksums (CRC-32C and legacy InnoDB) |
| [`diff`](#inno-diff) | Compare two tablespace files page-by-page |
| [`corrupt`](#inno-corrupt) | Intentionally corrupt pages for recovery testing |
| [`recover`](#inno-recover) | Assess page-level recoverability and count salvageable records |
| [`find`](#inno-find) | Search for pages across a MySQL data directory |
| [`tsid`](#inno-tsid) | List or find tablespace IDs |
| [`sdi`](#inno-sdi) | Extract SDI metadata from MySQL 8.0+ tablespaces |
| [`log`](#inno-log) | Analyze InnoDB redo log files |
| [`info`](#inno-info) | Inspect ibdata1 headers, compare LSNs, query MySQL |

---

## Subcommand Reference

### inno parse

Parse an `.ibd` file and display page-level information. Shows FIL headers, FSP metadata, checksums, and a page type summary across the entire tablespace.

```
inno parse -f <file> [-p <page>] [-v] [-e] [--json] [--page-size <size>]
```

| Flag | Description |
|------|-------------|
| `-f, --file` | Path to .ibd file (required) |
| `-p, --page` | Show a single page instead of all pages |
| `-v, --verbose` | Include checksum validation, record counts, LSN details |
| `-e, --no-empty` | Skip pages with zero checksum and Allocated type |
| `--json` | Output as JSON array |
| `--page-size` | Override page size in bytes (default: auto-detect from FSP flags) |

**All-pages mode** (default) iterates every page and prints a summary table showing how many pages of each type exist in the tablespace. **Single-page mode** (`-p N`) displays the full FIL header for that page including checksum, page type, LSN, prev/next page chain, and space ID.

```bash
# Overview of a tablespace
inno parse -f users.ibd

# Detailed look at the FSP header page
inno parse -f users.ibd -p 0 -v

# Machine-readable output for scripting
inno parse -f users.ibd --json | jq '.[] | select(.page_type == "INDEX")'
```

---

### inno pages

Detailed structural analysis of page internals. Goes deeper than `parse` by decoding type-specific headers: INDEX page B+Tree metadata, UNDO segment state, LOB/BLOB chain headers, and SDI content.

```
inno pages -f <file> [-p <page>] [-v] [-e] [-l] [-t <type>] [--json] [--page-size <size>]
```

| Flag | Description |
|------|-------------|
| `-f, --file` | Path to .ibd file (required) |
| `-p, --page` | Analyze a single page |
| `-v, --verbose` | Show additional structural details |
| `-e, --show-empty` | Include empty/Allocated pages (hidden by default) |
| `-l, --list` | Compact one-line-per-page listing |
| `-t, --type` | Filter by page type name |
| `--json` | Output as JSON |
| `--page-size` | Override page size |

**Type filter** accepts exact type names and aliases:

| Filter value | Matches |
|-------------|---------|
| `INDEX` | B+Tree index pages |
| `UNDO` | Undo log pages |
| `BLOB` | Old-style BLOB, ZBlob, ZBlob2 |
| `LOB` | MySQL 8.0+ LobIndex, LobData, LobFirst |
| `SDI` | SDI and SDI BLOB pages |
| `COMPRESSED` or `COMP` | Compressed and CompressedEncrypted |
| `ENCRYPTED` or `ENC` | Encrypted, CompressedEncrypted, EncryptedRtree |

**What it shows per page type:**

- **INDEX pages** — index header (n_recs, level, index_id, heap_top, direction), FSEG headers (leaf/non-leaf segment pointers), system records (infimum/supremum next pointers), compact vs redundant format flag
- **UNDO pages** — undo page header (type: INSERT or UPDATE, log start/free offsets), undo segment header (state: Active/Cached/ToFree/ToPurge/Prepared)
- **BLOB pages** — part_len (bytes stored on this page), next_page_no in chain
- **LOB_FIRST pages** — version, flags, total data_len, trx_id
- **FSP_HDR pages** — space_id, tablespace size, free limit, compression algorithm, encryption flag, first unused segment ID

```bash
# List all INDEX pages with their B+Tree levels
inno pages -f users.ibd -t INDEX -l

# Deep inspection of a specific undo page
inno pages -f undo_001.ibu -p 5 -v

# Find all BLOB pages in a tablespace
inno pages -f users.ibd -t BLOB -l
```

---

### inno dump

Hex dump of raw page bytes. Output follows the standard `offset | hex bytes | ASCII` format with 16 bytes per line.

```
inno dump -f <file> [-p <page>] [--offset <byte>] [-l <length>] [--raw] [--page-size <size>]
```

| Flag | Description |
|------|-------------|
| `-f, --file` | Path to file (required) |
| `-p, --page` | Page number to dump (default: 0) |
| `--offset` | Absolute byte offset into the file (bypasses page mode) |
| `-l, --length` | Number of bytes to dump (default: full page or 256 in offset mode) |
| `--raw` | Output raw binary bytes to stdout (no hex formatting) |
| `--page-size` | Override page size |

```bash
# Dump the FIL header of page 0 (first 38 bytes)
inno dump -f users.ibd -p 0 -l 38

# Dump raw bytes at a specific file offset
inno dump -f users.ibd --offset 16384 -l 64

# Extract a raw page to a file
inno dump -f users.ibd -p 3 --raw > page3.bin
```

---

### inno checksum

Validate page checksums across an entire tablespace. Supports both CRC-32C (MySQL 5.7.7+ default) and legacy InnoDB checksum algorithms. Also validates LSN consistency between the FIL header and trailer of each page.

```
inno checksum -f <file> [-v] [--json] [--page-size <size>]
```

| Flag | Description |
|------|-------------|
| `-f, --file` | Path to .ibd file (required) |
| `-v, --verbose` | Show per-page checksum status |
| `--json` | Output as JSON with summary statistics |
| `--page-size` | Override page size |

**Validation logic:**
1. Pages with the magic checksum `0xDEADBEEF` are skipped (no checksum set)
2. All-zero pages are treated as valid (freshly allocated)
3. CRC-32C is tried first (covers bytes 4-25 and 38 to page_size-8)
4. Falls back to legacy InnoDB algorithm (`ut_fold_binary`) if CRC-32C doesn't match
5. LSN low 32 bits from the FIL header (offset 16) are compared against the trailer (last 4 bytes)

**Exit code:** Returns `1` if any invalid checksums are found.

```bash
# Quick validation
inno checksum -f users.ibd

# Detailed per-page report
inno checksum -f users.ibd -v

# JSON output for monitoring
inno checksum -f users.ibd --json
```

**JSON output structure:**
```json
{
  "file": "users.ibd",
  "page_size": 16384,
  "total_pages": 100,
  "empty_pages": 10,
  "valid_pages": 88,
  "invalid_pages": 2,
  "lsn_mismatches": 0,
  "pages": [...]
}
```

---

### inno diff

Compare two InnoDB tablespace files page-by-page. Reports which pages are identical, modified, or only present in one file. Useful for analyzing changes between backups, before/after schema operations, or detecting corruption drift over time.

```
inno diff <file1> <file2> [-v] [-b] [-p <page>] [--json] [--page-size <size>]
```

| Flag | Description |
|------|-------------|
| `<file1>` | First .ibd file (positional, required) |
| `<file2>` | Second .ibd file (positional, required) |
| `-v, --verbose` | Show per-page FIL header field diffs for modified pages |
| `-b, --byte-ranges` | Show byte-offset ranges where content differs (requires `-v`) |
| `-p, --page` | Compare a single page only |
| `--json` | Output as JSON |
| `--page-size` | Override page size |

**Comparison levels:**
1. **Quick byte equality** — fast path for identical pages (`page1 == page2`)
2. **Header field diff** (`-v`) — parse both FIL headers, show changed fields with old/new values
3. **Byte-range scan** (`-v -b`) — walk bytes to find contiguous diff ranges with totals and percentages

**Page size mismatch:** When files have different page sizes, only FIL headers (first 38 bytes) are compared and a warning is displayed.

**Different page counts:** Pages beyond the shorter file are reported as "only in file 1" or "only in file 2".

```bash
# Summary comparison
inno diff backup.ibd current.ibd

# Per-page header diffs
inno diff backup.ibd current.ibd -v

# Full byte-range detail
inno diff backup.ibd current.ibd -v -b

# Compare just page 5
inno diff backup.ibd current.ibd -p 5

# Machine-readable output
inno diff backup.ibd current.ibd --json
```

---

### inno corrupt

Intentionally corrupt page bytes for testing InnoDB crash recovery, backup validation, or checksum detection. Writes random bytes to specified locations in the file.

**Warning:** This command modifies files. Always work on copies, not production data.

```
inno corrupt -f <file> [-p <page>] [-b <bytes>] [-k] [-r] [--offset <byte>] [--json] [--page-size <size>]
```

| Flag | Description |
|------|-------------|
| `-f, --file` | Path to file to corrupt (required) |
| `-p, --page` | Page number to corrupt (random page if omitted) |
| `-b, --bytes` | Number of bytes to overwrite with random data (default: 1) |
| `-k, --header` | Target the FIL header area (first 38 bytes of the page) |
| `-r, --records` | Target the user data area (bytes 120 through page_size-8) |
| `--offset` | Absolute byte offset (bypasses page calculation entirely) |
| `--json` | Output corruption details as JSON |
| `--page-size` | Override page size |

Works on any InnoDB file type: `.ibd` tablespaces, `ibdata1`, undo tablespaces, redo logs.

```bash
# Corrupt 4 bytes in the record area of page 5
inno corrupt -f users_copy.ibd -p 5 -b 4 -r

# Corrupt the FIL header of a random page
inno corrupt -f users_copy.ibd -k

# Corrupt at a specific file offset
inno corrupt -f ibdata1_copy -b 8 --offset 65536

# Get corruption details as JSON for test automation
inno corrupt -f users_copy.ibd -p 3 -b 2 --json
```

---

### inno recover

Scan a tablespace file and assess page-level data recoverability. Classifies each page as intact, corrupt, empty, or unreadable, and counts recoverable user records on INDEX pages by walking the compact record chain.

```
inno recover -f <file> [-p <page>] [-v] [--json] [--force] [--page-size <size>]
```

| Flag | Description |
|------|-------------|
| `-f, --file` | Path to .ibd file (required) |
| `-p, --page` | Analyze a single page instead of full scan |
| `-v, --verbose` | Show per-page details (type, status, LSN, record count) |
| `--json` | Output structured JSON report |
| `--force` | Extract records from corrupt pages with valid headers |
| `--page-size` | Override page size (critical when page 0 is damaged) |

**Smart page size fallback:** When auto-detection fails (e.g., page 0 is corrupt), the tool tries common page sizes (16K, 8K, 4K, 32K, 64K) based on file size divisibility. Use `--page-size` to override manually.

**`--force` mode:** By default, records are only counted on pages with valid checksums. With `--force`, the tool also walks the record chain on corrupt pages — useful when a page has a bad checksum but the record data is still intact.

```bash
# Basic recovery assessment
inno recover -f damaged.ibd

# Verbose per-page report
inno recover -f damaged.ibd -v

# JSON output for scripting
inno recover -f damaged.ibd --json | jq .summary

# Include records from corrupt pages
inno recover -f damaged.ibd --force -v

# Override page size when page 0 is damaged
inno recover -f damaged.ibd --page-size 16384
```

---

### inno find

Recursively search a MySQL data directory for `.ibd` files containing a specific page number. Useful for locating which tablespace owns a particular page when debugging InnoDB errors that reference page numbers.

```
inno find -d <datadir> -p <page> [-c <checksum>] [-s <space_id>] [--first] [--json]
```

| Flag | Description |
|------|-------------|
| `-d, --datadir` | MySQL data directory path (required) |
| `-p, --page` | Page number to search for (required) |
| `-c, --checksum` | Also match this checksum value |
| `-s, --space-id` | Also match this space ID |
| `--first` | Stop after the first match |
| `--json` | Output as JSON |

```bash
# Find which tablespace contains page 42
inno find -d /var/lib/mysql -p 42

# Find page 0 with a specific space ID
inno find -d /var/lib/mysql -p 0 -s 15

# Stop at first match for speed
inno find -d /var/lib/mysql -p 100 --first
```

---

### inno tsid

List tablespace IDs from all `.ibd` and `.ibu` files in a data directory. Reads the space ID from the FSP header on page 0 of each file.

```
inno tsid -d <datadir> [-l] [-t <tsid>] [--json]
```

| Flag | Description |
|------|-------------|
| `-d, --datadir` | MySQL data directory path (required) |
| `-l, --list` | List all tablespace IDs found |
| `-t, --tsid` | Find the file with this specific tablespace ID |
| `--json` | Output as JSON |

```bash
# List all tablespace IDs
inno tsid -d /var/lib/mysql -l

# Find which file has tablespace ID 42
inno tsid -d /var/lib/mysql -t 42
```

---

### inno sdi

Extract SDI (Serialized Dictionary Information) metadata from MySQL 8.0+ tablespaces. SDI records contain the full table and tablespace definitions (column types, indexes, partitions, etc.) stored as compressed JSON directly in the `.ibd` file.

```
inno sdi -f <file> [--pretty] [--page-size <size>]
```

| Flag | Description |
|------|-------------|
| `-f, --file` | Path to .ibd file (required) |
| `--pretty` | Pretty-print the JSON output |
| `--page-size` | Override page size |

SDI records are zlib-compressed and may span multiple linked pages. The tool handles multi-page reassembly automatically by following the page chain.

Each record has a type (1 = Table definition, 2 = Tablespace definition) and contains the full JSON representation of the object as MySQL's data dictionary sees it.

```bash
# Extract and pretty-print SDI
inno sdi -f users.ibd --pretty

# Pipe to jq for specific fields
inno sdi -f users.ibd | jq '.dd_object.columns[].name'
```

---

### inno log

Analyze InnoDB redo log files. Supports both legacy format (`ib_logfile0`, `ib_logfile1`) and the MySQL 8.0.30+ directory format (`#innodb_redo/#ib_redo*`).

Redo logs are structured as a sequence of 512-byte blocks. The first 4 blocks are reserved: block 0 is the file header, blocks 1 and 3 are checkpoints, and data blocks start at block 4.

```
inno log -f <file> [-b <blocks>] [--no-empty] [-v] [--json]
```

| Flag | Description |
|------|-------------|
| `-f, --file` | Path to redo log file (required) |
| `-b, --blocks` | Limit output to the first N data blocks |
| `--no-empty` | Skip blocks with no data |
| `-v, --verbose` | Show MLOG record type breakdown per block |
| `--json` | Output as JSON |

**Verbose mode** scans each data block for MLOG record type bytes and reports counts. This gives a rough breakdown of the types of operations recorded in each block (inserts, updates, page creates, file operations, etc.).

```bash
# Show log file header and checkpoints
inno log -f /var/lib/mysql/ib_logfile0

# Analyze first 100 data blocks with record type breakdown
inno log -f /var/lib/mysql/ib_logfile0 -b 100 -v

# Skip empty blocks
inno log -f /var/lib/mysql/ib_logfile0 --no-empty
```

---

### inno info

Inspect InnoDB system files and optionally query a live MySQL instance. Has three modes of operation.

```
inno info [--ibdata] [--lsn-check] [-d <datadir>]
         [-D <database> -t <table>] [--host <host>] [--port <port>]
         [--user <user>] [--password <pass>] [--defaults-file <path>]
         [--json]
```

**Mode 1: ibdata1 inspection (`--ibdata`)**

Reads page 0 of `ibdata1` and displays the FIL header fields (checksum, page type, LSN, flush LSN, space ID). Also reads the redo log checkpoint LSN for reference.

```bash
inno info --ibdata -d /var/lib/mysql
```

**Mode 2: LSN consistency check (`--lsn-check`)**

Compares the LSN from `ibdata1` page 0 with the redo log checkpoint LSN. Reports whether they are `IN SYNC` or `OUT OF SYNC`. Returns exit code `1` if out of sync.

Tries MySQL 8.0.30+ format (`#innodb_redo/`) first, falls back to legacy `ib_logfile0`.

```bash
inno info --lsn-check -d /var/lib/mysql
```

**Mode 3: MySQL query mode (`-D`, `-t`)**

Requires the `mysql` feature. Connects to a running MySQL instance and queries `information_schema` for table metadata: space_id, table_id, indexes (name, index_id, root_page), and current LSN from `SHOW ENGINE INNODB STATUS`.

```bash
# Using credentials from ~/.my.cnf
inno info -D mydb -t users

# Explicit connection parameters
inno info -D mydb -t users --host 127.0.0.1 --user root --password secret

# Using a specific defaults file
inno info -D mydb -t users --defaults-file /etc/mysql/my.cnf
```

| Flag | Description |
|------|-------------|
| `--ibdata` | Inspect ibdata1 page 0 header |
| `--lsn-check` | Compare ibdata1 and redo log checkpoint LSNs |
| `-d, --datadir` | MySQL data directory (default: `/var/lib/mysql`) |
| `-D, --database` | Database name (MySQL query mode) |
| `-t, --table` | Table name (MySQL query mode) |
| `--host` | MySQL host |
| `--port` | MySQL port |
| `--user` | MySQL user |
| `--password` | MySQL password |
| `--defaults-file` | Path to `.my.cnf` defaults file |
| `--json` | Output as JSON |

MySQL credentials are resolved in order: CLI flags, then `--defaults-file`, then `~/.my.cnf`, then `/etc/my.cnf`.

---

## Supported MySQL Versions

| Version | Tablespaces | Redo Logs | SDI | System Tables |
|---------|-------------|-----------|-----|---------------|
| MySQL 5.7 | `.ibd` | `ib_logfile*` | N/A | `innodb_sys_tables` |
| MySQL 8.0 | `.ibd` | `ib_logfile*` | Yes | `innodb_tables` / `innodb_indexes` |
| MySQL 8.0.30+ | `.ibd` | `#innodb_redo/` | Yes | `innodb_tables` / `innodb_indexes` |
| MySQL 8.4 / 9.x | `.ibd` | `#innodb_redo/` | Yes | `innodb_tables` / `innodb_indexes` |

**Page sizes:** 4K, 8K, 16K (default), 32K, and 64K are all supported and auto-detected from FSP flags.

---

## InnoDB File Format Primer

Understanding the on-disk format helps when interpreting `inno` output.

### Tablespace Files (.ibd)

Each `.ibd` file is a tablespace containing one or more tables. The file is divided into fixed-size **pages** (default 16,384 bytes). Every page has the same basic structure:

```
+------------------+  byte 0
| FIL Header       |  38 bytes — checksum, page number, prev/next, LSN, type, space_id
+------------------+  byte 38
| Page Body        |  varies by page type
|                  |
+------------------+  byte (page_size - 8)
| FIL Trailer      |  8 bytes — old checksum, LSN low 32 bits
+------------------+
```

**Page 0** is always an `FSP_HDR` (File Space Header) page containing tablespace-level metadata: total size, space ID, flags encoding page size and compression/encryption settings, and extent descriptors.

### Page Types

InnoDB uses ~26 page types. The most common ones you'll encounter:

| Type | Value | Description |
|------|-------|-------------|
| `INDEX` | 17855 | B+Tree leaf and non-leaf nodes (the bulk of any tablespace) |
| `FSP_HDR` | 8 | File space header (always page 0) |
| `INODE` | 3 | File segment inode (segment metadata) |
| `XDES` | 9 | Extent descriptor (tracks extent allocation) |
| `ALLOCATED` | 0 | Freshly allocated, not yet used |
| `UNDO_LOG` | 2 | Undo log records for MVCC |
| `BLOB` | 10 | Externally stored column data |
| `SDI` | 17853 | Serialized Dictionary Information (MySQL 8.0+) |
| `TRX_SYS` | 7 | Transaction system header (ibdata1 page 5) |

### Checksums

Each page stores a checksum in the first 4 bytes. MySQL supports two algorithms:

- **CRC-32C** (default since MySQL 5.7.7) — hardware-accelerated on modern CPUs
- **Legacy InnoDB** — a custom folding hash from the original InnoDB codebase

Both are computed over the same byte ranges (excluding the checksum field itself and the FIL trailer checksum). The `inno checksum` command tries CRC-32C first and falls back to legacy.

### LSN (Log Sequence Number)

The LSN is a monotonically increasing counter tracking the position in the redo log. Each page records its last-modified LSN in both the FIL header (8 bytes at offset 16) and the FIL trailer (low 32 bits). A mismatch between these two values indicates a torn page write.

### Redo Log Structure

Redo logs are sequences of 512-byte blocks:

| Block | Purpose |
|-------|---------|
| 0 | File header (group_id, start_lsn, file_no, creator string) |
| 1 | Checkpoint 1 (number, lsn, offset, buf_size) |
| 2 | Reserved |
| 3 | Checkpoint 2 |
| 4+ | Data blocks (14-byte header, up to 494 bytes of log records, 4-byte checksum) |

Each data block is checksummed with CRC-32C over the first 508 bytes.

---

## JSON Output

Every subcommand supports `--json` for structured output suitable for scripting, monitoring, and integration with other tools.

```bash
# Pipe to jq for filtering
inno parse -f users.ibd --json | jq '[.[] | select(.page_type == "INDEX")]'

# Checksum validation in CI
if ! inno checksum -f users.ibd --json | jq -e '.invalid_pages == 0' > /dev/null; then
  echo "Checksum validation failed"
  exit 1
fi

# Extract column names from SDI
inno sdi -f users.ibd | jq -r '.dd_object.columns[].name'

# Count pages by type
inno parse -f users.ibd --json | jq 'group_by(.page_type) | map({type: .[0].page_type, count: length})'
```

---

## Building and Testing

### Build

```bash
# Standard build
cargo build --release

# With MySQL query support
cargo build --release --features mysql
```

### Test

The test suite includes 74 unit tests covering InnoDB parsing logic (checksums, page types, headers, compression, encryption, SDI, redo logs, records, undo, recovery) and 87 integration tests that build synthetic `.ibd` files and run CLI commands against them.

```bash
# Run all tests
cargo test

# Run a specific test
cargo test test_crc32c_checksum_valid_pages
```

### Lint

The project enforces zero clippy warnings:

```bash
cargo clippy -- -D warnings
```

### CI

GitHub Actions runs on every push and pull request:
- `cargo test` on Ubuntu
- `cargo clippy -- -D warnings`
- `cargo build --release` on Ubuntu and macOS
- `cargo build --release --features mysql`

---

## Project Structure

```
src/
  main.rs              CLI entry point and subcommand dispatch (clap derive)
  lib.rs               IdbError type, module re-exports
  cli/
    parse.rs           inno parse
    pages.rs           inno pages
    dump.rs            inno dump
    checksum.rs        inno checksum
    diff.rs            inno diff
    corrupt.rs         inno corrupt
    recover.rs         inno recover
    find.rs            inno find
    tsid.rs            inno tsid
    sdi.rs             inno sdi
    log.rs             inno log
    info.rs            inno info
  innodb/
    constants.rs       InnoDB on-disk format constants (offsets, sizes, flags)
    page.rs            FIL header, FIL trailer, FSP header parsing
    page_types.rs      PageType enum (26 variants) with names and descriptions
    tablespace.rs      Tablespace file I/O abstraction (open, read_page, iterate)
    checksum.rs        CRC-32C and legacy InnoDB checksum validation
    index.rs           INDEX page header, FSEG headers, system records
    record.rs          Compact record header parsing, record chain walking
    undo.rs            Undo page/segment headers, rollback segment arrays
    lob.rs             BLOB page headers, LOB first page, chain walking
    sdi.rs             SDI page detection, record extraction, zlib decompression
    log.rs             Redo log headers, checkpoints, block parsing, MLOG types
    compression.rs     Compression detection (zlib/lz4), decompression
    encryption.rs      Encryption detection (AES flag in FSP flags)
  util/
    hex.rs             Hex dump formatting, offset/value formatters
    mysql.rs           MySQL connection, .my.cnf parsing (feature-gated)
tests/
  integration_test.rs  Integration tests with synthetic .ibd files
  diff_test.rs         Diff subcommand integration tests
```

Each CLI subcommand follows the same pattern: an `Options` struct with clap derive attributes and an `execute()` function returning `Result<(), IdbError>`.

All InnoDB constants use `UPPERCASE_WITH_UNDERSCORES` and match the names from MySQL/InnoDB source code (`fil0fil.h`, `page0page.h`, `fsp0fsp.h`).

## License

MIT
