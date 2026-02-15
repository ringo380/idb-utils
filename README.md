# IDB Utils

A command-line toolkit for inspecting, validating, and manipulating InnoDB database files. Written in Rust for performance and reliability, the `inno` binary operates directly on `.ibd` tablespace files, redo logs, and system tablespaces — no running MySQL instance required.

Also available as a [browser-based analyzer](https://ringo380.github.io/idb-utils/) powered by WebAssembly.

**[Full Documentation](https://ringo380.github.io/idb-utils/book/)**

## What It Does

- **Inspect tablespace files** — parse page headers, examine B+Tree index structures, walk undo logs, inspect BLOB/LOB chains
- **Validate data integrity** — verify CRC-32C and legacy InnoDB checksums, detect LSN mismatches
- **Compare tablespace files** — diff two `.ibd` files page-by-page
- **Monitor tablespace changes** — watch a live `.ibd` file for page-level modifications in real time
- **Extract metadata** — read MySQL 8.0+ SDI directly from `.ibd` files
- **Analyze redo logs** — parse log file headers, checkpoints, and data blocks
- **Assess data recoverability** — scan damaged tablespaces, count salvageable records
- **Test recovery scenarios** — intentionally corrupt pages for testing
- **Decrypt encrypted tablespaces** — read MySQL encrypted `.ibd` files using the keyring file
- **Audit a data directory** — search for pages, list tablespace IDs, compare LSNs

## Installation

```bash
# From crates.io
cargo install innodb-utils

# Homebrew (macOS/Linux)
brew install ringo380/tap/inno

# From source
git clone https://github.com/ringo380/idb-utils.git
cd idb-utils
cargo build --release
```

Pre-built binaries for Linux and macOS (x86_64 + aarch64) are available on the [releases page](https://github.com/ringo380/idb-utils/releases).

## Quick Start

```bash
# Parse a tablespace and show a page type summary
inno parse -f /var/lib/mysql/mydb/users.ibd

# Validate every page checksum
inno checksum -f users.ibd

# Get detailed INDEX page structure
inno pages -f users.ibd -t INDEX

# Hex dump page 3
inno dump -f users.ibd -p 3

# Extract table/column definitions from MySQL 8.0+ SDI
inno sdi -f users.ibd --pretty

# Analyze redo log checkpoints
inno log -f /var/lib/mysql/ib_logfile0

# Compare two tablespace files
inno diff backup.ibd current.ibd -v

# Monitor a tablespace for live changes
inno watch -f users.ibd -i 500 -v

# Assess recoverability of a damaged tablespace
inno recover -f users.ibd --force -v

# Read an encrypted tablespace
inno parse -f encrypted.ibd --keyring /var/lib/mysql-keyring/keyring
```

Every subcommand supports `--json` for machine-readable output and `--help` for full option details.

## Subcommands

| Command | Description |
|---------|-------------|
| `parse` | Parse .ibd file and display page headers with type summary |
| `pages` | Detailed page structure analysis (INDEX, UNDO, LOB, SDI) |
| `dump` | Hex dump of raw page bytes |
| `checksum` | Validate page checksums (CRC-32C and legacy InnoDB) |
| `diff` | Compare two tablespace files page-by-page |
| `watch` | Monitor a tablespace file for page-level changes |
| `corrupt` | Intentionally corrupt pages for recovery testing |
| `recover` | Assess page-level recoverability and count salvageable records |
| `find` | Search for pages across a MySQL data directory |
| `tsid` | List or find tablespace IDs |
| `sdi` | Extract SDI metadata from MySQL 8.0+ tablespaces |
| `log` | Analyze InnoDB redo log files |
| `info` | Inspect ibdata1 headers, compare LSNs, query MySQL |

See the [CLI Reference](https://ringo380.github.io/idb-utils/book/cli/overview.html) for detailed documentation on each subcommand.

## Supported InnoDB Variants

| Vendor | Checksums | SDI | Redo Logs |
|--------|-----------|-----|-----------|
| **MySQL** 5.7+ | CRC-32C, Legacy | Yes (8.0+) | Full parsing |
| **Percona XtraDB** | Same as MySQL | Yes (8.0+) | Full parsing |
| **MariaDB** 10.1+ | `full_crc32` (10.5+), CRC-32C | N/A | Header + checkpoints |

Page sizes 4K, 8K, 16K (default), 32K, and 64K are all supported and auto-detected.

## Building and Testing

```bash
# Build
cargo build --release

# With MySQL query support
cargo build --release --features mysql

# Run tests
cargo test

# Lint (zero warnings enforced)
cargo clippy -- -D warnings
```

## Documentation

- **[User Guide & CLI Reference](https://ringo380.github.io/idb-utils/book/)** — full documentation site
- **[API Docs (docs.rs)](https://docs.rs/innodb-utils)** — Rust library API reference
- **[Web Analyzer](https://ringo380.github.io/idb-utils/)** — browser-based file analysis
- **[Contributing](CONTRIBUTING.md)** — development setup and contribution guidelines

## License

MIT
