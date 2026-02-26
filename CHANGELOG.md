# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [4.0.0] - 2026-02-26

### Added

- **`inno compat` subcommand** — MySQL version compatibility checking for InnoDB tablespaces. Analyzes page size, row format, SDI presence, encryption, vendor compatibility, compression, and instant columns against a target MySQL version. Reports errors, warnings, and info-level findings. `--scan <datadir>` mode checks all tablespaces in a directory in parallel. (Closes #97, #98)
- **`inno verify` subcommand** — Structural integrity validation for tablespace files. Six checks: page number sequence, space ID consistency, LSN monotonicity, B+Tree level consistency, page chain bounds, and trailer LSN match. `--redo <path>` verifies redo log continuity against tablespace LSNs. `--chain <files>` verifies backup chain ordering by LSN with gap detection. (Closes #100, #101, #102)
- **`inno validate` subcommand** — Live MySQL cross-validation (requires `--features mysql`). Compares on-disk tablespace files against `INFORMATION_SCHEMA.INNODB_TABLESPACES` to detect orphan files, missing tablespaces, and space ID mismatches. `--table <db.table>` deep mode verifies index root pages and page types against MySQL metadata. (Closes #103, #104)
- **`inno diff --version-aware`** — Annotates diff output with MySQL version information extracted from SDI metadata on both files. (Closes #99)
- **`inno info --tablespace-map`** — Scans data directory and builds a mapping of file paths to space IDs from page 0 of each `.ibd` file. (Closes #105)
- **Comprehensive data type decoding** — Added decoders for DECIMAL (packed BCD), TIME2 (offset-encoded packed bit-field), BLOB/TEXT (off-page pointer detection), ENUM (1-2 byte index into element list), SET (bitmask), JSON (length-prefixed binary), and GEOMETRY (WKB) in `field_decode.rs`. (Closes #107)
- **WASM bindings** — `verify_tablespace()` and `check_compatibility()` functions for the web UI. (Closes #108)
- **MySQL version matrix CI** — GitHub Actions workflow testing against MySQL 5.7, 8.0, 8.4, and 9.0 Docker containers. Runs checksum, parse, pages, SDI, verify, and compat against real tablespace files from each version. (Closes #110)
- **Documentation** — 6 new guides (upgrade compatibility, backup verification, live validation, data type decoding, audit logging, version matrix) and 10 CLI reference pages for mdBook site. (Closes #109)
- 100+ new unit and integration tests across compat, verify, validate, field_decode, and diff

### Changed

- `inno info` doc comment updated to document all four modes including `--tablespace-map`
- `extract_mysql_version()` in diff now continues to next SDI record on parse failure instead of aborting early

### Fixed

- Incorrect `has_instant_columns` heuristic in compat that falsely flagged tables with standard DEFAULT values
- TIME2 decoder documentation and test comments incorrectly described XOR encoding instead of offset encoding

## [3.2.0] - 2026-02-25

### Added

- **`inno audit` subcommand** — New subcommand for directory-wide integrity scanning, health metrics, and checksum mismatch detection. Three modes: default integrity (`inno audit -d <datadir>`), health (`--health`), and mismatch listing (`--checksum-mismatch`). Scans `.ibd` files in parallel with rayon. Supports `--json`, `--csv`, and `--prometheus` output formats, threshold filters (`--min-fill-factor`, `--max-fragmentation`), and `--keyring` for encrypted tablespaces. (Closes #83, #84, #85)
- **`--audit-log <path>` global flag** — Structured NDJSON audit logging for write operations (repair, corrupt, defrag, transplant). `AuditLogger` with mutex + fs2 file locking, `AuditEvent` tagged enum (session_start, page_write, file_write, backup_created, session_end). (Closes #86)
- **`--prometheus` flag on `inno audit` and `inno health`** — Prometheus exposition format output for integration with textfile collectors. Per-file and directory-wide gauge metrics for pages, corruption, fill factor, fragmentation, garbage ratio, and scan duration. (Closes #87)
- **`inno watch --events`** — Structured NDJSON change event stream. Emits `watch_start`, `page_change`, `watch_error`, and `watch_stop` events with per-page detail (page type, LSN delta, checksum validity). (Closes #88)
- **`inno repair --batch <dir>`** — Batch repair mode scanning a data directory for corrupt tablespaces and repairing them in parallel. Auto-detects checksum algorithm per file. `BatchRepairReport` JSON output with per-file results and summary. (Closes #89)
- **`inno find --corrupt`** — Scan data directory for pages with checksum mismatches. (Closes #90)
- **`--depth` flag on `inno find`, `inno tsid`, and `inno audit`** — Control recursive directory scanning depth. `None` = default depth 2, `--depth 0` = unlimited, `--depth N` = N levels. Symlink loop detection via canonical path tracking. (Closes #93)
- **Shell completion generation** — `inno completions <shell>` generates completions for bash, zsh, fish, and PowerShell via `clap_complete`. (Closes #92)
- **Web UI audit dashboard** — New "Audit" tab in the web analyzer for multi-file integrity and health analysis. Drop 3+ `.ibd` files to see summary cards (total files, pages, corrupt pages, integrity %, avg fill factor), progress bars for integrity and fragmentation, and a sortable/filterable per-file detail table. (Closes #91)
- **WASM `analyze_health()` binding** — New wasm-bindgen export for B+Tree health analysis from in-memory tablespace bytes.
- New `src/util/prometheus.rs` module with Prometheus exposition format helpers (`format_gauge`, `format_gauge_int`, `escape_label_value`, `help_line`, `type_line`)
- New `src/util/audit.rs` module with `AuditLogger` and `AuditEvent` types
- New `web/src/components/audit.js` audit dashboard component
- 30+ new integration tests across audit, find, health, and watch test files

### Changed

- `find_tablespace_files()` rewritten with configurable depth and symlink loop safety via `HashSet<PathBuf>` canonical path tracking
- Web UI dropzone extended for 3+ file multi-drop (audit mode)
- Web UI tab bar conditionally shows Audit tab (keyboard shortcut: 0)

## [3.1.0] - 2026-02-23

### Added

- **`inno schema`** — New subcommand to extract SDI metadata and reconstruct `CREATE TABLE` DDL from MySQL 8.0+ tablespaces. Parses columns (types, defaults, nullability, character sets, collations), indexes (BTREE, FULLTEXT, SPATIAL with prefix lengths and sort order), foreign keys (with referential actions), partitioning, `AUTO_INCREMENT`, `ROW_FORMAT`, and table comments. Pre-8.0 tablespace inference when SDI is unavailable. Supports `--verbose` for column detail and `--json` for machine-readable output. (Closes #71)
- **`inno export`** — New subcommand to export record data from INDEX pages as CSV, JSON, or hex dump. Decodes InnoDB field types including INT, BIGINT, FLOAT, DOUBLE, DECIMAL, DATE, DATETIME, TIMESTAMP, VARCHAR, CHAR, TEXT, and BLOB. Handles signed integer encoding (high-bit XOR for memcmp ordering). Supports `--where-delete-mark` to include delete-marked records, `--system-columns` to show DB_TRX_ID/DB_ROLL_PTR, and `--page` to target specific pages. (Closes #72)
- **`inno health`** — New subcommand for per-index B+Tree health metrics. Computes fill factor (used vs available space), fragmentation score (out-of-order page ratio), and garbage ratio (delete-marked records vs total). Resolves index names from SDI metadata when available. Supports `--verbose` for per-page detail, `--json` for machine-readable output, and `--keyring` for encrypted tablespaces. (Closes #75)
- **`--format text|json|csv`** global flag on the top-level `inno` command, providing a unified output format selector across subcommands. Per-subcommand `--json` flags continue to work as overrides. CSV output supported for `parse`, `pages`, `checksum`, and `health`. (Closes #80)
- **`--deleted` flag on `inno pages`** — Walks compact records on INDEX pages and reports delete-marked record counts in text, JSON, and CSV output modes. (Closes #74)
- **Fill-factor display in `inno pages`** — Per-page fill factor shown for INDEX pages via `compute_fill_factor()` from the health module. Color-coded in text output (green/yellow/red), included in JSON and CSV. (Closes #76)
- **Corruption pattern analysis in `inno recover`** — New `src/innodb/corruption.rs` module with `CorruptionPattern` enum (ZeroFill, RandomNoise, TornWrite, HeaderOnly, Bitrot, Unknown). Classification uses Shannon entropy and Hamming distance heuristics. Integrated into `inno recover` verbose and JSON output. (Closes #73)
- **WASM `extract_schema()` binding** — New wasm-bindgen export in `src/wasm.rs` that extracts schema metadata and DDL from in-memory tablespace bytes. Returns JSON with columns, indexes, foreign keys, and DDL string. (Closes #77)
- **Web UI Schema tab** — New tab in the web analyzer displaying columns, indexes, and foreign keys as formatted tables, plus reconstructed DDL with a copy-to-clipboard button. Inserted after SDI (keyboard shortcut: 5). (Closes #78)
- 15 new integration tests for write operation edge cases (repair, defrag, transplant, rebuild) covering multi-index tablespaces, concurrent page types, and boundary conditions. (Closes #79)
- New `src/innodb/corruption.rs` module for corruption pattern classification
- New `src/innodb/field_decode.rs` module for InnoDB field type decoding and column layout building
- New `src/innodb/health.rs` module with `IndexPageSnapshot`, `IndexHealth`, and `HealthReport` types
- New `src/innodb/schema.rs` module for SDI deserialization, DDL generation, and type mapping

### Changed

- Web UI keyboard shortcuts renumbered: Schema=5, Hex=6, Recovery=7, Heatmap=8, Diff=9

## [3.0.0] - 2026-02-20

### Added

- **`inno repair`** — New subcommand to recalculate and fix corrupt page checksums in-place. Auto-detects the checksum algorithm from page 0 (or accepts `--algorithm crc32c|innodb|full_crc32`). Creates a `.bak` backup by default. Supports `--dry-run` to preview repairs, `--page N` to target a single page, and `--json` for machine-readable output. (Closes #33)
- **`inno defrag`** — New subcommand to defragment a tablespace by removing empty/corrupt pages, sorting INDEX pages by (index_id, level, page_number), fixing prev/next chain pointers, and writing a clean output file. Always writes to a new file (source is never modified). (Closes #34)
- **`inno transplant`** — New subcommand to copy specific pages from a donor tablespace into a target. Validates page size and space ID compatibility. Rejects page 0 and corrupt donor pages unless `--force` is used. Creates a target backup by default. Supports `--dry-run` and `--json`. (Closes #37)
- **`inno recover --rebuild`** — New `--rebuild <path>` option on the recover subcommand. Writes a new tablespace from recoverable pages, building a fresh page 0, renumbering pages sequentially, and recalculating all checksums. Use `--force` to include corrupt pages. (Closes #35)
- New `src/innodb/write.rs` module with shared write utilities: `create_backup`, `read_page_raw`, `write_page`, `write_tablespace`, `build_fsp_page`, `detect_algorithm`, `fix_page_checksum` (gated with `cfg(not(wasm32))`)
- `recalculate_checksum()` public function in `innodb::checksum` for writing correct checksums into page buffers
- `Serialize` derive on `ChecksumAlgorithm` for JSON report output
- Made `calculate_crc32c`, `calculate_innodb_checksum`, and `calculate_mariadb_full_crc32` public for use by write operations
- 20 new integration tests across `repair_test.rs` (7), `rebuild_test.rs` (3), `defrag_test.rs` (4), `transplant_test.rs` (6)
- 11 new unit tests for write utilities and checksum recalculation

## [2.1.0] - 2026-02-19

### Added

- **Redo log analysis in web UI** — New "Redo Log" tab auto-detects redo log files (`ib_logfile*`, `#ib_redo*`) and displays file header, checkpoint slots, block-level analysis with filtering, and MLOG record type distribution. Uses the existing `parse_redo_log` WASM function. (Closes #21)
- **Export functionality** — Download JSON and Copy to Clipboard buttons on every analysis tab. "Export All" button in the analyzer header runs all analysis functions and downloads a combined JSON file. Shared utilities in `web/src/utils/export.js`. (Closes #31)
- **Page type heatmap** — New "Heatmap" tab renders a canvas-based grid visualization of all pages, color-coded by page type. Three color modes: Page Type (categorical), LSN Age (blue-to-red gradient), and Checksum Status (green/red/gray). Supports mouse wheel zoom, click-and-drag pan, hover tooltips, and click-to-inspect navigation to the Pages tab. (Closes #25)
- **Record-level INDEX page inspection** — "View Records" button on INDEX pages in the Pages tab. Displays individual B+Tree record headers (type, heap number, n_owned, delete mark, min_rec, next offset) and raw hex bytes. Supports both compact (MySQL 5.0+) and redundant (pre-5.0) row formats. New `inspect_index_records` WASM function. (Closes #23)
- **Encrypted tablespace support in web UI** — Automatic encryption detection with keyring file upload banner. New `decrypt_tablespace` and `get_encryption_info` WASM functions. Decrypted data is transparently passed to all analysis tabs. (Closes #29)
- **Redundant row format parsing** — New `RedundantRecordHeader` struct and `walk_redundant_records()` function in `innodb::record` for pre-MySQL 5.0 row format support. Parses 6-byte headers with n_fields, one_byte_offs flag, and absolute next-record offsets.
- **Accessibility improvements** — ARIA `role="tablist"`/`role="tab"` attributes with arrow key navigation, `scope="col"` on all table headers, skip-to-content link, keyboard shortcuts panel (`?` key), `aria-live` announcements for tab changes, focus-visible outlines, reduced-motion and high-contrast media queries, visible theme toggle button. (Closes #32)
- MySQL 9.0/9.1 test fixtures and integration tests (compressed, multipage, standard, redo logs)
- Percona Server 8.0/8.4 test fixtures and integration tests
- MySQL 9.x redo log format support (version-conditional parsing for pre/post-8.0.30 layouts)
- Memory-mapped I/O (`--mmap` flag) for large tablespace analysis
- Streaming analysis mode (`--streaming` flag) for memory-constrained environments
- Criterion benchmarking infrastructure for core operations
- Docker image with multi-arch support and GitHub Actions workflow
- npm package for WASM module with TypeScript types
- deb and rpm package generation in release workflow
- AUR PKGBUILD for Arch Linux packaging
- Reference Homebrew formula for homebrew-core submission
- Git LFS tracking for binary test fixtures
- LICENSE file (MIT)
- 3 unit tests for redundant record header parsing and RecordHeader enum accessors

### Changed

- **BREAKING**: `RecordInfo.header` changed from `CompactRecordHeader` to `RecordHeader` enum (wrapping `Compact` or `Redundant` variants). Direct field access (e.g. `rec.header.heap_no`) must be replaced with accessor methods (e.g. `rec.header.heap_no()`). This enables unified handling of both compact and redundant row formats.
- `PageType::Unknown` now carries original type code as `Unknown(u16)`, preserving unrecognized page type values
- Benchmarks use `iter_batched` instead of cloning data inside `b.iter()` for accurate timing
- Web UI tab bar now includes Heatmap (key 7) and conditionally shows Diff (key 8) and Redo Log (key 9)
- Diff view grid is now responsive (`grid-cols-1 md:grid-cols-2`)
- Keyboard shortcuts no longer fire when a `<select>` element has focus

### Fixed

- XSS in heatmap tooltip — numeric values (`page_number`, `lsn`) are now escaped with `esc()`
- Memory leak in heatmap — `mouseup` listener scoped to canvas instead of window, with `mouseleave` cleanup
- `recover` now threads `vendor_info` to `validate_checksum()`, fixing MariaDB full_crc32 recovery assessment
- Bounds checks in `checksum` and `parse` parallel paths prevent out-of-bounds access on truncated files
- `find --first` files_searched counter correctly counts all opened files from parallel results
- Progress bars in `checksum`, `parse`, and `find` display during parallel work instead of after
- Corrected `PageType` enum values and added missing page types
- `LogFileHeader::parse()` version-conditional layout for pre-8.0.30 redo logs
- CI test job fetches Git LFS objects for binary fixture files

### Dependencies

- `rand` 0.9.2 → 0.10.0
- `colored` 2.2.0 → 3.1.1
- `indicatif` 0.18.3 → 0.18.4
- `lz4_flex` 0.11.5 → 0.12.0
- `ctrlc` 3.5.1 → 3.5.2
- `actions/checkout` v4 → v6, `actions/setup-node` v4 → v6
- `actions/upload-artifact` v4 → v6, `actions/download-artifact` v4 → v7
- `actions/upload-pages-artifact` v3 → v4, `peter-evans/repository-dispatch` v3 → v4

## [2.0.0] - 2026-02-14

### Added

- **WebAssembly build target** — The entire InnoDB analysis library now compiles to WASM via `wasm-bindgen`. Nine analysis functions are exported: `get_tablespace_info`, `parse_tablespace`, `analyze_pages`, `validate_checksums`, `extract_sdi`, `diff_tablespaces`, `hex_dump_page`, `assess_recovery`, and `parse_redo_log`. All accept raw file bytes and return JSON strings. (Closes #14)
- **Web-based InnoDB Analyzer** — Single-page app in `web/` built with Vite and Tailwind CSS v4. Drag-and-drop .ibd file input (500 MB limit) with seven tabbed analysis views: Overview, Pages, Checksums, SDI, Hex Dump, Recovery, and Diff (two-file comparison). Dark/light theme toggle, keyboard navigation, and responsive layout.
- GitHub Pages deployment workflow (`.github/workflows/pages.yml`) for automatic web UI publishing
- WASM compilation check added to CI pipeline
- `[profile.release]` optimized for WASM size (opt-level z, LTO, single codegen unit, strip)
- SVG favicon and SEO/Open Graph meta tags for the web UI
- Shared `esc()` HTML-escaping utility in `web/src/utils/html.js`
- 6 new edge-case unit tests for `from_bytes()` constructors (empty and too-small input for `Tablespace`, `LogFile`, and `Keyring`)

### Changed

- **Library refactored for dual-target support** — `Tablespace`, `LogFile`, and `Keyring` now use a `Box<dyn ReadSeek>` abstraction instead of `std::fs::File` directly. New `from_bytes()` constructors accept in-memory buffers for WASM use. File-based `open()` and `load()` methods are conditionally compiled with `#[cfg(not(target_arch = "wasm32"))]`.
- CLI dependencies (`clap`, `colored`, `indicatif`, `ctrlc`, `chrono`, `rand`) moved behind a `cli` feature gate. The `default` feature enables CLI; `--no-default-features` builds the pure library for WASM.
- Library crate type changed to `["cdylib", "rlib"]` to support both WASM and native linking
- Applied `cargo fmt` formatting across codebase

### Fixed

- `assess_recovery()` WASM binding now checks both checksum validity and LSN consistency when classifying pages as intact, matching the CLI `recover` behavior. Previously only checked checksums, which could misclassify torn-write pages.
- Page size mismatch in `diff_tablespaces()` WASM binding now returns a descriptive error instead of silently comparing pages of different sizes.

## [1.4.0] - 2026-02-13

### Added

- **`inno watch`** — New subcommand for real-time tablespace monitoring. Polls an InnoDB tablespace file at a configurable interval and reports page-level changes (modified, added, removed) based on LSN comparison. Validates checksums for each changed page to detect corruption during writes. Re-opens the tablespace each cycle to detect file growth and avoid stale file handles. Supports `--verbose` for per-field diffs, `--json` for NDJSON streaming output, `--page-size` override, and `--keyring` for encrypted tablespaces. Graceful Ctrl+C shutdown with a summary of total changes. (Closes #13)
- New dependencies: `ctrlc 3`, `chrono 0.4`
- 5 unit tests and 5 integration tests for `watch` subcommand
- **Encrypted tablespace decryption** — Read encrypted InnoDB tablespaces when provided with a MySQL `keyring_file` keyring. Parses encryption info from page 0 (magic version, master key ID, server UUID, encrypted tablespace key+IV). Loads the MySQL `keyring_file` binary format with XOR de-obfuscation and SHA-256 integrity verification. Decrypts tablespace key+IV using AES-256-ECB with the master key, then decrypts page bodies using AES-256-CBC. Transparent decryption: when `--keyring` is provided, `read_page()` automatically decrypts encrypted pages before returning data. Supports `keyring_file` plugin format (MySQL 5.7.11+); magic versions V1 (`lCA`), V2 (`lCB`), V3 (`lCC`/MySQL 8.0.5+). (Closes #12)
- `--keyring <path>` option on `parse`, `pages`, `dump`, `checksum`, `recover`, `sdi`, `diff`, and `watch` subcommands
- `--decrypt` flag on `dump` subcommand for hex-dumping decrypted page content
- `inno pages` displays encryption info (master key ID, server UUID, magic version) in FSP header detail when encryption is detected
- New `innodb::decryption` module with `DecryptionContext` for AES-256 page decryption
- New `innodb::keyring` module for parsing MySQL `keyring_file` binary format
- `Tablespace::encryption_info()` and `Tablespace::is_encrypted()` accessors
- 14 encryption integration tests covering end-to-end decrypt, keyring loading, error cases, and CLI subcommand behavior
- New dependencies: `aes 0.8`, `cbc 0.1`, `ecb 0.1`, `sha2 0.10`

## [1.3.0] - 2026-02-12

### Added

- **Percona and MariaDB tablespace format support** — Automatic vendor detection (MySQL, Percona XtraDB, MariaDB) from FSP flags and redo log creator strings. MariaDB `full_crc32` checksum algorithm (single CRC-32C over `[0..page_size-4)`). MariaDB-specific page types: `PageCompressed` (34354), `PageCompressedEncrypted` (37401), `Instant` (18). Vendor-aware page size detection for MariaDB `full_crc32` format (page size in FSP flags bits 0-3). MariaDB compression algorithm detection (zlib, LZ4, LZO, LZMA, bzip2, Snappy) from both FSP flags and per-page headers. Vendor-aware encryption detection. All CLI subcommands pass vendor context for correct checksum validation, compression/encryption reporting, and page type resolution. `inno sdi` returns a clear error for MariaDB tablespaces (MariaDB does not use SDI). `inno log` detects vendor from redo log creator string and skips incompatible MLOG record type decoding for MariaDB logs. (Closes #11)
- New `innodb::vendor` module with `InnoDbVendor`, `MariaDbFormat`, and `VendorInfo` types
- `Tablespace::vendor_info()` accessor for detected vendor and format details
- 24 MariaDB integration tests covering vendor detection, checksum validation, page types, compression, encryption, and backward compatibility
- **`inno diff`** — New subcommand to compare two tablespace files page-by-page. Reports identical, modified, and only-in-one-file page counts with a list of modified page numbers. With `--verbose`, shows per-page FIL header field diffs (checksum, LSN, page type, space ID, prev/next, flush LSN). With `--byte-ranges` (and `-v`), shows exact byte-offset ranges where page content differs with totals and percentages. Supports `--json` for machine-readable output, `-p` for single-page comparison, and `--page-size` override. Handles page size mismatches by comparing only FIL headers with a warning. (Closes #10)
- 8 integration tests for `diff` subcommand covering identical files, different LSNs, different page types, different page counts, single-page mode, byte ranges, JSON output, and page size mismatch

## [1.2.4] - 2026-02-07

### Added

- 30 integration tests for comprehensive subcommand flag/mode coverage across all 11 subcommands (79 integration tests total)
- 5 unit tests for `find_tablespace_files()` utility (74 unit tests total)
- Format check (`cargo fmt --check`) and security audit CI jobs

### Changed

- Refactor `recover` subcommand to use `RecoverStats` struct, eliminating `too_many_arguments` warnings
- Replace `std::process::exit(1)` with proper `Err(IdbError)` returns in `checksum`
- Replace `.unwrap()` with descriptive `.expect()` in `corrupt`
- Apply `rustfmt` formatting across entire codebase

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
- **`--page-size` on `find`, `tsid`, and `info`** subcommands (previously only on 6 of 11 subcommands)
- **Progress bars** for long-running operations in `checksum`, `parse`, and `find` subcommands (via `indicatif`)
- **Man page generation** at build time via `clap_mangen` (`inno.1` + one per subcommand)
- **Pre-built binary releases** via GitHub Actions workflow for Linux (x86_64, aarch64) and macOS (x86_64, aarch64)
- **crates.io metadata** (repository, homepage, keywords, categories) for publishing

## [1.0.0] - 2026-02-05

Complete rewrite from Perl scripts to a unified Rust CLI tool.

### Added

- **Unified `inno` binary** with 11 subcommands dispatched via clap derive macros
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
