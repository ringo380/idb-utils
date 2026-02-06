use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(name = "idb")]
#[command(about = "InnoDB file analysis toolkit")]
#[command(version)]
pub struct Cli {
    /// Control colored output
    #[arg(long, default_value = "auto", global = true)]
    pub color: ColorMode,

    /// Write output to a file instead of stdout
    #[arg(short, long, global = true)]
    pub output: Option<String>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Clone, Copy, ValueEnum)]
pub enum ColorMode {
    Auto,
    Always,
    Never,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Parse .ibd file and display page summary
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
    },

    /// Detailed page structure analysis
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

        /// Output in JSON format
        #[arg(long)]
        json: bool,

        /// Override page size (default: auto-detect)
        #[arg(long = "page-size")]
        page_size: Option<u32>,
    },

    /// Hex dump of raw page bytes
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
    },

    /// Intentionally corrupt pages for testing
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

    /// Search for pages across data directory
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
    },

    /// Analyze InnoDB redo log files
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

    /// Validate page checksums
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
    },
}
