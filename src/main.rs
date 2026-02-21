#[cfg(not(feature = "cli"))]
compile_error!("The `inno` binary requires the `cli` feature. Build with `--features cli`.");

use clap::Parser;
use std::fs::File;
use std::io::Write;
use std::process;

use idb::cli;
use idb::cli::app::{Cli, ColorMode, Commands};
use idb::IdbError;

fn main() {
    let cli = Cli::parse();

    // Configure rayon thread pool if --threads was specified
    if cli.threads > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(cli.threads)
            .build_global()
            .ok(); // Ignore if already initialized
    }

    match cli.color {
        ColorMode::Always => colored::control::set_override(true),
        ColorMode::Never => colored::control::set_override(false),
        ColorMode::Auto => {} // colored auto-detects tty
    }

    let writer_result: Result<Box<dyn Write>, IdbError> = match &cli.output {
        Some(path) => File::create(path)
            .map(|f| Box::new(f) as Box<dyn Write>)
            .map_err(|e| IdbError::Io(format!("Cannot create {}: {}", path, e))),
        None => Ok(Box::new(std::io::stdout()) as Box<dyn Write>),
    };

    let mut writer = match writer_result {
        Ok(w) => w,
        Err(e) => {
            eprintln!("Error: {}", e);
            process::exit(1);
        }
    };

    let result = match cli.command {
        Commands::Parse {
            file,
            page,
            verbose,
            no_empty,
            json,
            page_size,
            keyring,
            streaming,
        } => cli::parse::execute(
            &cli::parse::ParseOptions {
                file,
                page,
                verbose,
                no_empty,
                page_size,
                json,
                keyring,
                threads: cli.threads,
                mmap: cli.mmap,
                streaming,
            },
            &mut writer,
        ),

        Commands::Pages {
            file,
            page,
            verbose,
            show_empty,
            list,
            filter_type,
            json,
            page_size,
            keyring,
        } => cli::pages::execute(
            &cli::pages::PagesOptions {
                file,
                page,
                verbose,
                show_empty,
                list_mode: list,
                filter_type,
                page_size,
                json,
                keyring,
                mmap: cli.mmap,
            },
            &mut writer,
        ),

        Commands::Dump {
            file,
            page,
            offset,
            length,
            raw,
            page_size,
            keyring,
            decrypt,
        } => cli::dump::execute(
            &cli::dump::DumpOptions {
                file,
                page,
                offset,
                length,
                raw,
                page_size,
                keyring,
                decrypt,
                mmap: cli.mmap,
            },
            &mut writer,
        ),

        Commands::Corrupt {
            file,
            page,
            bytes,
            header,
            records,
            offset,
            verify,
            json,
            page_size,
        } => cli::corrupt::execute(
            &cli::corrupt::CorruptOptions {
                file,
                page,
                bytes,
                header,
                records,
                offset,
                verify,
                json,
                page_size,
                mmap: cli.mmap,
            },
            &mut writer,
        ),

        Commands::Find {
            datadir,
            page,
            checksum,
            space_id,
            first,
            json,
            page_size,
        } => cli::find::execute(
            &cli::find::FindOptions {
                datadir,
                page,
                checksum,
                space_id,
                first,
                json,
                page_size,
                threads: cli.threads,
                mmap: cli.mmap,
            },
            &mut writer,
        ),

        Commands::Tsid {
            datadir,
            list,
            tablespace_id,
            json,
            page_size,
        } => cli::tsid::execute(
            &cli::tsid::TsidOptions {
                datadir,
                list,
                tablespace_id,
                json,
                page_size,
                mmap: cli.mmap,
            },
            &mut writer,
        ),

        Commands::Sdi {
            file,
            pretty,
            page_size,
            keyring,
        } => cli::sdi::execute(
            &cli::sdi::SdiOptions {
                file,
                pretty,
                page_size,
                keyring,
                mmap: cli.mmap,
            },
            &mut writer,
        ),

        Commands::Log {
            file,
            blocks,
            no_empty,
            verbose,
            json,
        } => cli::log::execute(
            &cli::log::LogOptions {
                file,
                blocks,
                no_empty,
                verbose,
                json,
            },
            &mut writer,
        ),

        Commands::Info {
            ibdata,
            lsn_check,
            datadir,
            database,
            table,
            host,
            port,
            user,
            password,
            defaults_file,
            json,
            page_size,
        } => cli::info::execute(
            &cli::info::InfoOptions {
                ibdata,
                lsn_check,
                datadir,
                database,
                table,
                host,
                port,
                user,
                password,
                defaults_file,
                json,
                page_size,
            },
            &mut writer,
        ),

        Commands::Recover {
            file,
            page,
            verbose,
            json,
            force,
            page_size,
            keyring,
            streaming,
            rebuild,
        } => cli::recover::execute(
            &cli::recover::RecoverOptions {
                file,
                page,
                verbose,
                json,
                force,
                page_size,
                keyring,
                threads: cli.threads,
                mmap: cli.mmap,
                streaming,
                rebuild,
            },
            &mut writer,
        ),

        Commands::Checksum {
            file,
            verbose,
            json,
            page_size,
            keyring,
            streaming,
        } => cli::checksum::execute(
            &cli::checksum::ChecksumOptions {
                file,
                verbose,
                json,
                page_size,
                keyring,
                threads: cli.threads,
                mmap: cli.mmap,
                streaming,
            },
            &mut writer,
        ),

        Commands::Watch {
            file,
            interval,
            verbose,
            json,
            page_size,
            keyring,
        } => cli::watch::execute(
            &cli::watch::WatchOptions {
                file,
                interval,
                verbose,
                json,
                page_size,
                keyring,
                mmap: cli.mmap,
            },
            &mut writer,
        ),

        Commands::Repair {
            file,
            page,
            algorithm,
            no_backup,
            dry_run,
            verbose,
            json,
            page_size,
            keyring,
        } => cli::repair::execute(
            &cli::repair::RepairOptions {
                file,
                page,
                algorithm,
                no_backup,
                dry_run,
                verbose,
                json,
                page_size,
                keyring,
                mmap: cli.mmap,
            },
            &mut writer,
        ),

        Commands::Diff {
            file1,
            file2,
            verbose,
            byte_ranges,
            page,
            json,
            page_size,
            keyring,
        } => cli::diff::execute(
            &cli::diff::DiffOptions {
                file1,
                file2,
                verbose,
                byte_ranges,
                page,
                json,
                page_size,
                keyring,
                mmap: cli.mmap,
            },
            &mut writer,
        ),

        Commands::Transplant {
            donor,
            target,
            pages,
            no_backup,
            force,
            dry_run,
            verbose,
            json,
            page_size,
            keyring,
        } => cli::transplant::execute(
            &cli::transplant::TransplantOptions {
                donor,
                target,
                pages,
                no_backup,
                force,
                dry_run,
                verbose,
                json,
                page_size,
                keyring,
                mmap: cli.mmap,
            },
            &mut writer,
        ),

        Commands::Defrag {
            file,
            output,
            verbose,
            json,
            page_size,
            keyring,
        } => cli::defrag::execute(
            &cli::defrag::DefragOptions {
                file,
                output,
                verbose,
                json,
                page_size,
                keyring,
                mmap: cli.mmap,
            },
            &mut writer,
        ),
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}
