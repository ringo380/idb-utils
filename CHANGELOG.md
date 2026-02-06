# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.0] - 2026-02-05

### Changed

- **Rename binary from `idb` to `inno`** to avoid conflict with Facebook's iOS Development Bridge (`fb-idb`)
- Install via `cargo install innodb-utils`, run as `inno <subcommand>`

## [1.1.0] - 2026-02-05

### Added

- **`--color auto|always|never`** global flag to control colored output across all subcommands
- **`--output <file>`** global flag to redirect output to a file instead of stdout
- **`--verify` flag on `inno corrupt`** to show before/after checksum comparison when corrupting pages
- **`--page-size` on `find`, `tsid`, and `info`** subcommands (previously only on 6 of 10 subcommands)
- **Progress bars** for long-running operations in `checksum`, `parse`, and `find` subcommands (via `indicatif`)
- **Man page generation** at build time via `clap_mangen` (`inno.1` + one per subcommand)
- **Pre-built binary releases** via GitHub Actions workflow for Linux (x86_64, aarch64) and macOS (x86_64, aarch64)
- **crates.io metadata** (repository, homepage, keywords, categories) for publishing

## [1.0.0] - 2026-02-05

Complete rewrite from Perl scripts to a unified Rust CLI tool.

### Added

- **Unified `inno` binary** with 10 subcommands dispatched via clap derive macros
- **`inno parse`** — Parse .ibd files with page headers, type summaries, verbose/extended modes
- **`inno pages`** — Detailed page structure analysis for INDEX, UNDO, LOB, and SDI pages
- **`inno dump`** — Hex dump of raw page bytes with offset and length controls
- **`inno checksum`** — Validate page checksums using CRC-32C and legacy InnoDB algorithms
- **`inno corrupt`** — Intentionally corrupt pages for testing recovery and tooling
- **`inno find`** — Search a data directory for pages by number and space ID
- **`inno tsid`** — List and look up tablespace IDs across a data directory
- **`inno sdi`** — Extract SDI metadata from MySQL 8.0+ tablespaces with zlib decompression
- **`inno log`** — Analyze InnoDB redo log files with block-level parsing
- **`inno info`** — Inspect ibdata1 system tablespace, compare LSNs, query MySQL (feature-gated)
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
