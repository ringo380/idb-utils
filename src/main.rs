use clap::{Parser, Subcommand};
use std::process;

use idb::cli;

#[derive(Parser)]
#[command(name = "idb")]
#[command(about = "InnoDB file analysis toolkit")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
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

    /// Intentionally corrupt pages for testing
    Corrupt {
        /// Path to InnoDB data file (.ibd)
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

    /// Validate page checksums
    Checksum {
        /// Path to InnoDB data file (.ibd)
        #[arg(short, long)]
        file: String,

        /// Override page size (default: auto-detect)
        #[arg(long = "page-size")]
        page_size: Option<u32>,
    },
}

fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Parse {
            file,
            page,
            verbose,
            no_empty,
            json,
            page_size,
        } => cli::parse::execute(&cli::parse::ParseOptions {
            file,
            page,
            verbose,
            no_empty,
            page_size,
            json,
        }),

        Commands::Pages {
            file,
            page,
            verbose,
            show_empty,
            list,
            filter_type,
            json,
            page_size,
        } => cli::pages::execute(&cli::pages::PagesOptions {
            file,
            page,
            verbose,
            show_empty,
            list_mode: list,
            filter_type,
            page_size,
            json,
        }),

        Commands::Corrupt {
            file,
            page,
            bytes,
            header,
            records,
            page_size,
        } => cli::corrupt::execute(&cli::corrupt::CorruptOptions {
            file,
            page,
            bytes,
            header,
            records,
            page_size,
        }),

        Commands::Find {
            datadir,
            page,
            checksum,
            space_id,
        } => cli::find::execute(&cli::find::FindOptions {
            datadir,
            page,
            checksum,
            space_id,
        }),

        Commands::Tsid {
            datadir,
            list,
            tablespace_id,
        } => cli::tsid::execute(&cli::tsid::TsidOptions {
            datadir,
            list,
            tablespace_id,
        }),

        Commands::Sdi {
            file,
            pretty,
            page_size,
        } => cli::sdi::execute(&cli::sdi::SdiOptions {
            file,
            pretty,
            page_size,
        }),

        Commands::Checksum { file, page_size } => {
            cli::checksum::execute(&cli::checksum::ChecksumOptions { file, page_size })
        }
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}
