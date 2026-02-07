use clap::Parser;
use std::fs::File;
use std::io::Write;
use std::process;

use idb::cli;
use idb::cli::app::{Cli, ColorMode, Commands};
use idb::IdbError;

fn main() {
    let cli = Cli::parse();

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
        } => cli::parse::execute(
            &cli::parse::ParseOptions {
                file,
                page,
                verbose,
                no_empty,
                page_size,
                json,
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
        } => cli::dump::execute(
            &cli::dump::DumpOptions {
                file,
                page,
                offset,
                length,
                raw,
                page_size,
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
            },
            &mut writer,
        ),

        Commands::Sdi {
            file,
            pretty,
            page_size,
        } => cli::sdi::execute(
            &cli::sdi::SdiOptions {
                file,
                pretty,
                page_size,
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
        } => cli::recover::execute(
            &cli::recover::RecoverOptions {
                file,
                page,
                verbose,
                json,
                force,
                page_size,
            },
            &mut writer,
        ),

        Commands::Checksum {
            file,
            verbose,
            json,
            page_size,
        } => cli::checksum::execute(
            &cli::checksum::ChecksumOptions {
                file,
                verbose,
                json,
                page_size,
            },
            &mut writer,
        ),
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}
