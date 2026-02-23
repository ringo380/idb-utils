use clap::{Parser, Subcommand, ValueEnum};

/// Top-level CLI definition for the `inno` binary.
#[derive(Parser)]
#[command(name = "inno")]
#[command(about = "InnoDB file analysis toolkit")]
#[command(version)]
pub struct Cli {
    /// Control colored output
    #[arg(long, default_value = "auto", global = true)]
    pub color: ColorMode,

    /// Write output to a file instead of stdout
    #[arg(short, long, global = true)]
    pub output: Option<String>,

    /// Number of threads for parallel page processing (0 = auto-detect CPU count)
    #[arg(long, default_value = "0", global = true)]
    pub threads: usize,

    /// Use memory-mapped I/O for file access (can be faster for large files)
    #[arg(long, global = true)]
    pub mmap: bool,

    /// Output format (text, json, or csv); overrides per-subcommand --json
    #[arg(long, global = true, default_value = "text")]
    pub format: OutputFormat,

    #[command(subcommand)]
    pub command: Commands,
}

/// Controls when colored output is emitted.
#[derive(Clone, Copy, ValueEnum)]
pub enum ColorMode {
    Auto,
    Always,
    Never,
}

/// Global output format selection.
#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
    Csv,
}

/// Available subcommands for the `inno` CLI.
#[derive(Subcommand)]
pub enum Commands {
    /// Parse .ibd file and display page summary
    ///
    /// Reads the 38-byte FIL header of every page in a tablespace, decodes the
    /// page type, checksum, LSN, prev/next pointers, and space ID, then prints
    /// a per-page breakdown followed by a page-type frequency summary table.
    /// Page 0 additionally shows the FSP header (space ID, size, flags).
    /// Use `--no-empty` to skip zero-checksum allocated pages, or `-p` to
    /// inspect a single page in detail. With `--verbose`, checksum validation
    /// and LSN consistency results are included for each page.
    Parse {
        /// Path to InnoDB data file (.ibd)
        #[arg(short, long)]
        file: String,

        /// Display a specific page number
        #[arg(short, long)]
        page: Option<u64>,

        /// Display additional information
        #[arg(short, long)]
        verbose: bool,

        /// Skip empty/allocated pages
        #[arg(short = 'e', long = "no-empty")]
        no_empty: bool,

        /// Output in JSON format
        #[arg(long)]
        json: bool,

        /// Override page size (default: auto-detect)
        #[arg(long = "page-size")]
        page_size: Option<u32>,

        /// Path to MySQL keyring file for decrypting encrypted tablespaces
        #[arg(long)]
        keyring: Option<String>,

        /// Stream results incrementally for lower memory usage (disables parallel processing)
        #[arg(long)]
        streaming: bool,
    },

    /// Detailed page structure analysis
    ///
    /// Goes beyond FIL headers to decode the internal structure of each page
    /// type: INDEX pages show the B+Tree index header, FSEG inode pointers, and
    /// infimum/supremum system records; UNDO pages show the undo page header
    /// and segment state; BLOB/LOB pages show chain pointers and data lengths;
    /// and page 0 shows extended FSP header fields including compression and
    /// encryption flags. Use `-l` for a compact one-line-per-page listing,
    /// `-t INDEX` to filter by page type, or `-p` for a single page deep dive.
    Pages {
        /// Path to InnoDB data file (.ibd)
        #[arg(short, long)]
        file: String,

        /// Display a specific page number
        #[arg(short, long)]
        page: Option<u64>,

        /// Display additional information
        #[arg(short, long)]
        verbose: bool,

        /// Show empty/allocated pages
        #[arg(short = 'e', long = "show-empty")]
        show_empty: bool,

        /// Compact list mode (one line per page)
        #[arg(short, long)]
        list: bool,

        /// Filter by page type (e.g., INDEX)
        #[arg(short = 't', long = "type")]
        filter_type: Option<String>,

        /// Show delete-marked record statistics for INDEX pages
        #[arg(long)]
        deleted: bool,

        /// Output in JSON format
        #[arg(long)]
        json: bool,

        /// Override page size (default: auto-detect)
        #[arg(long = "page-size")]
        page_size: Option<u32>,

        /// Path to MySQL keyring file for decrypting encrypted tablespaces
        #[arg(long)]
        keyring: Option<String>,
    },

    /// Hex dump of raw page bytes
    ///
    /// Operates in two modes: **page mode** (default) reads a full page by
    /// number and produces a formatted hex dump with file-relative offsets;
    /// **offset mode** (`--offset`) reads bytes at an arbitrary file position,
    /// useful for inspecting structures that cross page boundaries. Use
    /// `--length` to limit the number of bytes shown, or `--raw` to emit
    /// unformatted binary bytes suitable for piping to other tools.
    Dump {
        /// Path to InnoDB data file
        #[arg(short, long)]
        file: String,

        /// Page number to dump (default: 0)
        #[arg(short, long)]
        page: Option<u64>,

        /// Absolute byte offset to start dumping (bypasses page mode)
        #[arg(long)]
        offset: Option<u64>,

        /// Number of bytes to dump (default: page size or 256 for offset mode)
        #[arg(short, long)]
        length: Option<usize>,

        /// Output raw binary bytes (no formatting)
        #[arg(long)]
        raw: bool,

        /// Override page size (default: auto-detect)
        #[arg(long = "page-size")]
        page_size: Option<u32>,

        /// Path to MySQL keyring file for decrypting encrypted tablespaces
        #[arg(long)]
        keyring: Option<String>,

        /// Decrypt page before dumping (requires --keyring)
        #[arg(long)]
        decrypt: bool,
    },

    /// Intentionally corrupt pages for testing
    ///
    /// Writes random bytes into a tablespace file to simulate data corruption.
    /// Targets can be the FIL header (`-k`), the record data area (`-r`), or
    /// an absolute byte offset (`--offset`). If no page is specified, one is
    /// chosen at random. Use `--verify` to print before/after checksum
    /// comparisons confirming the page is now invalid — useful for verifying
    /// that `inno checksum` correctly detects the damage.
    Corrupt {
        /// Path to data file
        #[arg(short, long)]
        file: String,

        /// Page number to corrupt (random if not specified)
        #[arg(short, long)]
        page: Option<u64>,

        /// Number of bytes to corrupt
        #[arg(short, long, default_value = "1")]
        bytes: usize,

        /// Corrupt the FIL header area
        #[arg(short = 'k', long = "header")]
        header: bool,

        /// Corrupt the record data area
        #[arg(short, long)]
        records: bool,

        /// Absolute byte offset to corrupt (bypasses page calculation)
        #[arg(long)]
        offset: Option<u64>,

        /// Show before/after checksum comparison
        #[arg(long)]
        verify: bool,

        /// Output in JSON format
        #[arg(long)]
        json: bool,

        /// Override page size (default: auto-detect)
        #[arg(long = "page-size")]
        page_size: Option<u32>,
    },

    /// Export record-level data from a tablespace
    ///
    /// Extracts user records from clustered index leaf pages and outputs
    /// them as CSV, JSON, or raw hex. Uses SDI metadata (MySQL 8.0+) to
    /// decode field types and column names. Without SDI, falls back to
    /// hex-only output.
    ///
    /// Supported types: integers (TINYINT–BIGINT), FLOAT, DOUBLE,
    /// DATE, DATETIME, TIMESTAMP, YEAR, VARCHAR, CHAR. Unsupported
    /// types (DECIMAL, BLOB, JSON, etc.) are exported as hex strings.
    ///
    /// Use `--where-delete-mark` to include only delete-marked records
    /// (useful for forensic recovery). Use `--system-columns` to include
    /// DB_TRX_ID and DB_ROLL_PTR in the output.
    Export {
        /// Path to InnoDB data file (.ibd)
        #[arg(short, long)]
        file: String,

        /// Export records from a specific page only
        #[arg(short, long)]
        page: Option<u64>,

        /// Output format: csv, json, or hex
        #[arg(long, default_value = "csv")]
        format: String,

        /// Include only delete-marked records
        #[arg(long = "where-delete-mark")]
        where_delete_mark: bool,

        /// Include system columns (DB_TRX_ID, DB_ROLL_PTR) in output
        #[arg(long = "system-columns")]
        system_columns: bool,

        /// Show additional details
        #[arg(short, long)]
        verbose: bool,

        /// Override page size (default: auto-detect)
        #[arg(long = "page-size")]
        page_size: Option<u32>,

        /// Path to MySQL keyring file for decrypting encrypted tablespaces
        #[arg(long)]
        keyring: Option<String>,
    },

    /// Search for pages across data directory
    ///
    /// Recursively discovers all `.ibd` files under a MySQL data directory,
    /// opens each as a tablespace, and reads the FIL header of every page
    /// looking for a matching `page_number` field. Optional `--checksum` and
    /// `--space-id` filters narrow results when the same page number appears
    /// in multiple tablespaces. Use `--first` to stop after the first match
    /// for faster lookups.
    Find {
        /// MySQL data directory path
        #[arg(short, long)]
        datadir: String,

        /// Page number to search for
        #[arg(short, long)]
        page: u64,

        /// Checksum to match
        #[arg(short, long)]
        checksum: Option<u32>,

        /// Space ID to match
        #[arg(short, long)]
        space_id: Option<u32>,

        /// Stop at first match
        #[arg(long)]
        first: bool,

        /// Output in JSON format
        #[arg(long)]
        json: bool,

        /// Override page size (default: auto-detect)
        #[arg(long = "page-size")]
        page_size: Option<u32>,
    },

    /// List/find tablespace IDs
    ///
    /// Scans `.ibd` and `.ibu` files under a MySQL data directory and reads
    /// the space ID from the FSP header (page 0, offset 38) of each file.
    /// In **list mode** (`-l`) it prints every file and its space ID; in
    /// **lookup mode** (`-t <id>`) it finds the file that owns a specific
    /// tablespace ID. Useful for mapping a space ID seen in error logs or
    /// `INFORMATION_SCHEMA` back to a physical file on disk.
    Tsid {
        /// MySQL data directory path
        #[arg(short, long)]
        datadir: String,

        /// List all tablespace IDs
        #[arg(short, long)]
        list: bool,

        /// Find table file by tablespace ID
        #[arg(short = 't', long = "tsid")]
        tablespace_id: Option<u32>,

        /// Output in JSON format
        #[arg(long)]
        json: bool,

        /// Override page size (default: auto-detect)
        #[arg(long = "page-size")]
        page_size: Option<u32>,
    },

    /// Extract SDI metadata (MySQL 8.0+)
    ///
    /// Locates SDI (Serialized Dictionary Information) pages in a tablespace
    /// by scanning for page type 17853, then reassembles multi-page SDI
    /// records by following the page chain. The zlib-compressed payload is
    /// decompressed and printed as JSON. Each tablespace in MySQL 8.0+
    /// embeds its own table/column/index definitions as SDI records,
    /// eliminating the need for the `.frm` files used in older versions.
    /// Use `--pretty` for indented JSON output.
    Sdi {
        /// Path to InnoDB data file (.ibd)
        #[arg(short, long)]
        file: String,

        /// Pretty-print JSON output
        #[arg(short, long)]
        pretty: bool,

        /// Override page size (default: auto-detect)
        #[arg(long = "page-size")]
        page_size: Option<u32>,

        /// Path to MySQL keyring file for decrypting encrypted tablespaces
        #[arg(long)]
        keyring: Option<String>,
    },

    /// Extract schema and reconstruct DDL from tablespace metadata
    ///
    /// Reads SDI (Serialized Dictionary Information) from MySQL 8.0+
    /// tablespaces, parses the embedded data dictionary JSON into typed
    /// column, index, and foreign key definitions, and reconstructs a
    /// complete `CREATE TABLE` DDL statement. For pre-8.0 tablespaces
    /// that lack SDI, scans INDEX pages to infer basic index structure
    /// and record format (compact vs. redundant).
    ///
    /// Use `--verbose` for a structured breakdown of columns, indexes,
    /// and foreign keys above the DDL. Use `--json` for machine-readable
    /// output including the full schema definition and DDL as a JSON object.
    Schema {
        /// Path to InnoDB data file (.ibd)
        #[arg(short, long)]
        file: String,

        /// Show structured schema breakdown above the DDL
        #[arg(short, long)]
        verbose: bool,

        /// Output in JSON format
        #[arg(long)]
        json: bool,

        /// Override page size (default: auto-detect)
        #[arg(long = "page-size")]
        page_size: Option<u32>,

        /// Path to MySQL keyring file for decrypting encrypted tablespaces
        #[arg(long)]
        keyring: Option<String>,
    },

    /// Analyze InnoDB redo log files
    ///
    /// Opens an InnoDB redo log file (`ib_logfile0`/`ib_logfile1` for
    /// MySQL < 8.0.30, or `#ib_redo*` files for 8.0.30+) and displays
    /// the log file header, both checkpoint records, and per-block details
    /// including block number, data length, checkpoint number, and CRC-32C
    /// checksum status. With `--verbose`, MLOG record types within each
    /// data block are decoded and summarized. Use `--blocks N` to limit
    /// output to the first N data blocks, or `--no-empty` to skip blocks
    /// that contain no redo data.
    Log {
        /// Path to redo log file (ib_logfile0, ib_logfile1, or #ib_redo*)
        #[arg(short, long)]
        file: String,

        /// Limit to first N data blocks
        #[arg(short, long)]
        blocks: Option<u64>,

        /// Skip empty blocks
        #[arg(long)]
        no_empty: bool,

        /// Display additional information
        #[arg(short, long)]
        verbose: bool,

        /// Output in JSON format
        #[arg(long)]
        json: bool,
    },

    /// Show InnoDB file and system information
    ///
    /// Operates in three modes. **`--ibdata`** reads the `ibdata1` page 0
    /// FIL header and redo log checkpoint LSNs. **`--lsn-check`** compares
    /// the `ibdata1` header LSN with the latest redo log checkpoint LSN to
    /// detect whether the system tablespace and redo log are in sync (useful
    /// for diagnosing crash-recovery state). **`-D`/`-t`** queries a live
    /// MySQL instance via `INFORMATION_SCHEMA.INNODB_TABLES` and
    /// `INNODB_INDEXES` for tablespace IDs, table IDs, index root pages,
    /// and key InnoDB status metrics (requires the `mysql` feature).
    Info {
        /// Inspect ibdata1 page 0 header
        #[arg(long)]
        ibdata: bool,

        /// Compare ibdata1 and redo log LSNs
        #[arg(long = "lsn-check")]
        lsn_check: bool,

        /// MySQL data directory path
        #[arg(short, long)]
        datadir: Option<String>,

        /// Database name (for table/index info)
        #[arg(short = 'D', long)]
        database: Option<String>,

        /// Table name (for table/index info)
        #[arg(short, long)]
        table: Option<String>,

        /// MySQL host
        #[arg(long)]
        host: Option<String>,

        /// MySQL port
        #[arg(long)]
        port: Option<u16>,

        /// MySQL user
        #[arg(long)]
        user: Option<String>,

        /// MySQL password
        #[arg(long)]
        password: Option<String>,

        /// Path to MySQL defaults file (.my.cnf)
        #[arg(long = "defaults-file")]
        defaults_file: Option<String>,

        /// Output in JSON format
        #[arg(long)]
        json: bool,

        /// Override page size (default: auto-detect)
        #[arg(long = "page-size")]
        page_size: Option<u32>,
    },

    /// Recover data from corrupt/damaged tablespace files
    ///
    /// Scans a tablespace file and classifies each page as intact, corrupt,
    /// empty, or unreadable. For INDEX pages, counts recoverable user records
    /// by walking the compact record chain. Produces a recovery assessment
    /// showing how many pages and records can be salvaged.
    ///
    /// Use `--force` to also extract records from pages with bad checksums
    /// but valid-looking headers — useful when data is partially damaged
    /// but the record chain is still intact. Use `--page-size` to override
    /// page size detection when page 0 is corrupt.
    ///
    /// With `--verbose`, per-page details are shown including page type,
    /// status, LSN, and record count. With `--json`, a structured report
    /// is emitted including optional per-record detail when combined with
    /// `--verbose`.
    Recover {
        /// Path to InnoDB data file (.ibd)
        #[arg(short, long)]
        file: String,

        /// Analyze a single page instead of full scan
        #[arg(short, long)]
        page: Option<u64>,

        /// Show per-page details
        #[arg(short, long)]
        verbose: bool,

        /// Output in JSON format
        #[arg(long)]
        json: bool,

        /// Extract records from corrupt pages with valid headers
        #[arg(long)]
        force: bool,

        /// Override page size (critical when page 0 is corrupt)
        #[arg(long = "page-size")]
        page_size: Option<u32>,

        /// Path to MySQL keyring file for decrypting encrypted tablespaces
        #[arg(long)]
        keyring: Option<String>,

        /// Stream results incrementally for lower memory usage (disables parallel processing)
        #[arg(long)]
        streaming: bool,

        /// Write a new tablespace from recoverable pages to the given path
        #[arg(long)]
        rebuild: Option<String>,
    },

    /// Validate page checksums
    ///
    /// Reads every page in a tablespace and validates its stored checksum
    /// against both CRC-32C (MySQL 5.7.7+) and legacy InnoDB algorithms.
    /// Also checks that the header LSN low-32 bits match the FIL trailer.
    /// All-zero pages are counted as empty and skipped. With `--verbose`,
    /// per-page results are printed including the detected algorithm and
    /// stored vs. calculated values. Exits with code 1 if any page has an
    /// invalid checksum, making it suitable for use in scripts and CI.
    Checksum {
        /// Path to InnoDB data file (.ibd)
        #[arg(short, long)]
        file: String,

        /// Show per-page checksum details
        #[arg(short, long)]
        verbose: bool,

        /// Output in JSON format
        #[arg(long)]
        json: bool,

        /// Override page size (default: auto-detect)
        #[arg(long = "page-size")]
        page_size: Option<u32>,

        /// Path to MySQL keyring file for decrypting encrypted tablespaces
        #[arg(long)]
        keyring: Option<String>,

        /// Stream results incrementally for lower memory usage (disables parallel processing)
        #[arg(long)]
        streaming: bool,
    },

    /// Monitor a tablespace file for page-level changes
    ///
    /// Polls an InnoDB tablespace file at a configurable interval and reports
    /// which pages have been modified, added, or removed since the last poll.
    /// Change detection is based on LSN comparison — if a page's LSN changes
    /// between polls, it was modified by a write. Checksums are validated for
    /// each changed page to detect corruption during writes.
    ///
    /// The tablespace is re-opened each cycle to detect file growth and avoid
    /// stale file handles. Use `--verbose` for per-field diffs on changed
    /// pages, or `--json` for NDJSON streaming output (one JSON object per
    /// line). Press Ctrl+C for a clean exit with a summary of total changes.
    Watch {
        /// Path to InnoDB data file (.ibd)
        #[arg(short, long)]
        file: String,

        /// Polling interval in milliseconds
        #[arg(short, long, default_value = "1000")]
        interval: u64,

        /// Show per-field diffs for changed pages
        #[arg(short, long)]
        verbose: bool,

        /// Output in NDJSON streaming format
        #[arg(long)]
        json: bool,

        /// Override page size (default: auto-detect)
        #[arg(long = "page-size")]
        page_size: Option<u32>,

        /// Path to MySQL keyring file for decrypting encrypted tablespaces
        #[arg(long)]
        keyring: Option<String>,
    },

    /// Repair corrupt page checksums
    ///
    /// Recalculates and writes correct checksums for pages with invalid checksums
    /// or LSN mismatches. By default, auto-detects the checksum algorithm from
    /// page 0 and creates a `.bak` backup before modifying the file. Use
    /// `--algorithm` to force a specific algorithm, `--dry-run` to preview
    /// repairs without modifying the file, or `--no-backup` to skip the backup.
    Repair {
        /// Path to InnoDB data file (.ibd)
        #[arg(short, long)]
        file: String,

        /// Repair only a specific page number
        #[arg(short, long)]
        page: Option<u64>,

        /// Checksum algorithm: auto, crc32c, innodb, full_crc32
        #[arg(short, long, default_value = "auto")]
        algorithm: String,

        /// Skip creating a backup before repair
        #[arg(long)]
        no_backup: bool,

        /// Preview repairs without modifying the file
        #[arg(long)]
        dry_run: bool,

        /// Show per-page repair details
        #[arg(short, long)]
        verbose: bool,

        /// Output in JSON format
        #[arg(long)]
        json: bool,

        /// Override page size (default: auto-detect)
        #[arg(long = "page-size")]
        page_size: Option<u32>,

        /// Path to MySQL keyring file for decrypting encrypted tablespaces
        #[arg(long)]
        keyring: Option<String>,
    },

    /// Compare two tablespace files page-by-page
    ///
    /// Reads two InnoDB tablespace files and compares them page-by-page,
    /// reporting which pages are identical, modified, or only present in
    /// one file. With `--verbose`, per-page FIL header field diffs are
    /// shown for modified pages, highlighting changes to checksums, LSNs,
    /// page types, and space IDs. Add `--byte-ranges` (with `-v`) to see
    /// the exact byte offsets where page content differs. Use `-p` to
    /// compare a single page, or `--json` for machine-readable output.
    ///
    /// When files have different page sizes, only FIL headers (first 38
    /// bytes) are compared and a warning is displayed.
    Diff {
        /// First InnoDB data file (.ibd)
        file1: String,

        /// Second InnoDB data file (.ibd)
        file2: String,

        /// Show per-page header field diffs
        #[arg(short, long)]
        verbose: bool,

        /// Show byte-range diffs for changed pages (requires -v)
        #[arg(short = 'b', long = "byte-ranges")]
        byte_ranges: bool,

        /// Compare a single page only
        #[arg(short, long)]
        page: Option<u64>,

        /// Output in JSON format
        #[arg(long)]
        json: bool,

        /// Override page size (default: auto-detect)
        #[arg(long = "page-size")]
        page_size: Option<u32>,

        /// Path to MySQL keyring file for decrypting encrypted tablespaces
        #[arg(long)]
        keyring: Option<String>,
    },

    /// Copy specific pages from a donor tablespace into a target
    ///
    /// Reads pages from the donor file and writes them into the target file at
    /// the same page number. Safety checks ensure page sizes and space IDs
    /// match. Page 0 (FSP_HDR) is rejected unless `--force` is used. Donor
    /// pages with invalid checksums are skipped unless `--force` is used.
    ///
    /// A backup of the target is created by default. Use `--dry-run` to preview
    /// which pages would be transplanted without modifying the target.
    Transplant {
        /// Path to donor tablespace file (source of pages)
        donor: String,

        /// Path to target tablespace file (destination)
        target: String,

        /// Page numbers to transplant (comma-separated)
        #[arg(short, long, value_delimiter = ',')]
        pages: Vec<u64>,

        /// Skip creating a backup of the target
        #[arg(long)]
        no_backup: bool,

        /// Allow space ID mismatch, corrupt donor pages, and page 0 transplant
        #[arg(long)]
        force: bool,

        /// Preview without modifying the target file
        #[arg(long)]
        dry_run: bool,

        /// Show per-page details
        #[arg(short, long)]
        verbose: bool,

        /// Output in JSON format
        #[arg(long)]
        json: bool,

        /// Override page size (default: auto-detect)
        #[arg(long = "page-size")]
        page_size: Option<u32>,

        /// Path to MySQL keyring file for decrypting encrypted tablespaces
        #[arg(long)]
        keyring: Option<String>,
    },

    /// Per-index B+Tree health metrics
    ///
    /// Scans all INDEX pages in a tablespace and computes per-index health
    /// metrics including fill factor (average, min, max), garbage ratio,
    /// fragmentation, tree depth, and page counts. Optionally resolves
    /// index names from SDI metadata (MySQL 8.0+).
    ///
    /// Use `--verbose` for additional detail including total records and
    /// empty leaf page counts. Use `--json` for machine-readable output.
    Health {
        /// Path to InnoDB data file (.ibd)
        #[arg(short, long)]
        file: String,

        /// Show additional detail (records, empty leaves)
        #[arg(short, long)]
        verbose: bool,

        /// Output in JSON format
        #[arg(long)]
        json: bool,

        /// Override page size (default: auto-detect)
        #[arg(long = "page-size")]
        page_size: Option<u32>,

        /// Path to MySQL keyring file for decrypting encrypted tablespaces
        #[arg(long)]
        keyring: Option<String>,
    },

    /// Audit a MySQL data directory for integrity, health, or corruption
    ///
    /// Scans all `.ibd` files under a data directory and validates checksums,
    /// computes health metrics, or lists corrupt pages — replacing the need
    /// to run `inno checksum` or `inno health` file-by-file. Three modes:
    ///
    /// **Default (integrity)**: validates checksums across all tablespace files
    /// and reports per-file pass/fail with a directory-wide integrity percentage.
    ///
    /// **`--health`**: computes per-tablespace fill factor, fragmentation, and
    /// garbage ratio, ranked worst-first. Use `--min-fill-factor` and
    /// `--max-fragmentation` to filter for unhealthy tablespaces only.
    ///
    /// **`--checksum-mismatch`**: compact listing of only corrupt pages with
    /// stored vs. calculated checksums, suitable for piping to `inno repair`.
    ///
    /// `--health` and `--checksum-mismatch` are mutually exclusive.
    Audit {
        /// MySQL data directory path
        #[arg(short, long)]
        datadir: String,

        /// Show per-tablespace health metrics instead of checksum validation
        #[arg(long)]
        health: bool,

        /// List only pages with checksum mismatches (compact output)
        #[arg(long = "checksum-mismatch")]
        checksum_mismatch: bool,

        /// Show additional details (per-page results in default mode)
        #[arg(short, long)]
        verbose: bool,

        /// Output in JSON format
        #[arg(long)]
        json: bool,

        /// Override page size (default: auto-detect per file)
        #[arg(long = "page-size")]
        page_size: Option<u32>,

        /// Path to MySQL keyring file for decrypting encrypted tablespaces
        #[arg(long)]
        keyring: Option<String>,

        /// Show tables with fill factor below this threshold (0-100, --health only)
        #[arg(long = "min-fill-factor")]
        min_fill_factor: Option<f64>,

        /// Show tables with fragmentation above this threshold (0-100, --health only)
        #[arg(long = "max-fragmentation")]
        max_fragmentation: Option<f64>,
    },

    /// Defragment a tablespace by reclaiming free space and reordering pages
    ///
    /// Reads all pages from a source tablespace, removes empty and corrupt
    /// pages, sorts INDEX pages by (index_id, level, page_number), fixes
    /// prev/next chain pointers within each index group, renumbers pages
    /// sequentially, rebuilds page 0, recalculates all checksums, and writes
    /// the result to a new output file. The source file is never modified.
    Defrag {
        /// Path to source InnoDB data file (.ibd)
        #[arg(short, long)]
        file: String,

        /// Path to output file (required — always writes a new file)
        #[arg(short, long)]
        output: String,

        /// Show per-page details
        #[arg(short, long)]
        verbose: bool,

        /// Output in JSON format
        #[arg(long)]
        json: bool,

        /// Override page size (default: auto-detect)
        #[arg(long = "page-size")]
        page_size: Option<u32>,

        /// Path to MySQL keyring file for decrypting encrypted tablespaces
        #[arg(long)]
        keyring: Option<String>,
    },
}
