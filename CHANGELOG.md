# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.2.3] - 2026-02-07

### Added

- **`inno recover`** — New subcommand for page-level data recovery assessment. Scans a tablespace and classifies each page as intact, corrupt, empty, or unreadable. Counts recoverable user records on INDEX pages by walking compact record chains. Supports `--force` to extract records from corrupt pages with valid headers, `--page-size` override when page 0 is damaged, and smart page size fallback. Text and JSON output modes with optional per-page/per-record detail via `--verbose`. (Closes #9)
- `inno recover` added to crate-level docs.rs documentation
- 6 unit tests for recover page analysis (empty, corrupt, force, unreadable, valid INDEX)
- 9 integration tests covering recover text/JSON output, corrupt detection, empty pages, single-page mode, `--force`, and `--page-size` override

## [1.2.2] - 2026-02-06

### Changed

- Extract shared `create_progress_bar()` helper, replacing duplicate progress bar setup in `parse`, `checksum`, and `find` subcommands
- Consolidate duplicate directory traversal into `util::fs::find_tablespace_files()`, replacing separate implementations in `find` and `tsid`
- Standardize JSON serialization error handling in `parse` and `pages` to return proper errors instead of silent `"[]"` fallback
- Refactor `PageType` to use single `metadata()` method instead of three parallel 25-arm match statements

### Added

- 8 unit tests for `Tablespace` (open, page size override, reject small file, read page, out of range, parse header/trailer, for_each_page)
- 17 integration tests covering `pages`, `corrupt`, `find`, `tsid`, `sdi`, `dump --raw`, error paths, and JSON output validation

## [1.2.1] - 2026-02-06

### Fixed

- **CRC-32C checksum validation** now correctly XORs two independently-computed CRC values over disjoint byte ranges, matching MySQL's `buf_calc_page_crc32()` implementation. Previously used chained CRC which produced incorrect results against real .ibd files.
- **Legacy InnoDB checksum validation** now uses 32-bit wrapping arithmetic with byte-by-byte folding, matching the `ut_fold_binary` / `buf_calc_page_new_checksum` implementation. Previously used 64-bit word-based processing which produced incorrect results for MySQL < 5.7.7 files.
- Verified correct checksum validation against real .ibd files from MySQL 5.0, 5.6, 5.7, 8.0, 8.4, and 9.0 (test fixtures from ibdNinja, innodb_ruby, and innodb-java-reader projects).

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
