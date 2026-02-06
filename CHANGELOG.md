# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-02-05

### Added

- **`--color auto|always|never`** global flag to control colored output across all subcommands
- **`--output <file>`** global flag to redirect output to a file instead of stdout
- **`--verify` flag on `idb corrupt`** to show before/after checksum comparison when corrupting pages
- **`--page-size` on `find`, `tsid`, and `info`** subcommands (previously only on 6 of 10 subcommands)
- **Progress bars** for long-running operations in `checksum`, `parse`, and `find` subcommands (via `indicatif`)
- **Man page generation** at build time via `clap_mangen` (`idb.1` + one per subcommand)
- **Pre-built binary releases** via GitHub Actions workflow for Linux (x86_64, aarch64) and macOS (x86_64, aarch64)
- **crates.io metadata** (repository, homepage, keywords, categories) for publishing

## [1.0.0] - 2026-02-05

Complete rewrite from Perl scripts to a unified Rust CLI tool.

### Added

- **Unified `idb` binary** with 10 subcommands dispatched via clap derive macros
- **`idb parse`** — Parse .ibd files with page headers, type summaries, verbose/extended modes
- **`idb pages`** — Detailed page structure analysis for INDEX, UNDO, LOB, and SDI pages
- **`idb dump`** — Hex dump of raw page bytes with offset and length controls
- **`idb checksum`** — Validate page checksums using CRC-32C and legacy InnoDB algorithms
- **`idb corrupt`** — Intentionally corrupt pages for testing recovery and tooling
- **`idb find`** — Search a data directory for pages by number and space ID
- **`idb tsid`** — List and look up tablespace IDs across a data directory
- **`idb sdi`** — Extract SDI metadata from MySQL 8.0+ tablespaces with zlib decompression
- **`idb log`** — Analyze InnoDB redo log files with block-level parsing
- **`idb info`** — Inspect ibdata1 system tablespace, compare LSNs, query MySQL (feature-gated)
- **`--json` output** on all subcommands via `serde_json` serialization
- **Automatic page size detection** from FSP header flags (4K, 8K, 16K, 32K, 64K)
- **Tablespace abstraction** for file I/O, page iteration, and page size management
- **SDI multi-page chain reassembly** with zlib decompression for large metadata records
- **Redo log parser** with checkpoint, block header, and log record extraction
- **InnoDB record parsing** for INDEX pages (compact and redundant row formats)
- **Encryption detection** for tablespace-level and page-level encryption flags
- **55 unit tests** covering page parsing, checksums, SDI extraction, and redo log handling
- **14 integration tests** building synthetic .ibd files with valid CRC-32C checksums
- **GitHub Actions CI** with build, test, and clippy lint checks
- **Feature-gated MySQL support** (`--features mysql`) for live database queries via mysql_async

### Removed

- Legacy Perl scripts (`idb-parse.pl`, `idb-pages.pl`, `idb-checksum.pl`, `idb-corrupter.pl`,
  `idb-findpage.pl`, `idb-findtsid.pl`, `idb-liveinfo.pl`)
- Perl module library (`IdbUtils/`)
- All Perl dependencies and infrastructure
