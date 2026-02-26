#[cfg(not(feature = "cli"))]
compile_error!("The `inno` binary requires the `cli` feature. Build with `--features cli`.");

use clap::Parser;
use std::fs::File;
use std::io::Write;
use std::process;
use std::sync::Arc;

use idb::cli;
use idb::cli::app::{Cli, ColorMode, Commands, OutputFormat};
use idb::util::audit::AuditLogger;
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

    // Resolve effective output format: --format overrides per-subcommand --json
    let global_format = cli.format;

    // Create audit logger if --audit-log was specified
    let audit_logger: Option<Arc<AuditLogger>> = match &cli.audit_log {
        Some(path) => {
            let logger = match AuditLogger::open(path) {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("Error: {}", e);
                    process::exit(1);
                }
            };
            let args: Vec<String> = std::env::args().collect();
            let _ = logger.start_session(args);
            Some(Arc::new(logger))
        }
        None => None,
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
                json: json || global_format == OutputFormat::Json,
                csv: global_format == OutputFormat::Csv,
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
            deleted,
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
                json: json || global_format == OutputFormat::Json,
                csv: global_format == OutputFormat::Csv,
                keyring,
                mmap: cli.mmap,
                deleted,
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
                audit_logger: audit_logger.clone(),
            },
            &mut writer,
        ),

        Commands::Find {
            datadir,
            page,
            checksum,
            space_id,
            corrupt,
            first,
            json,
            page_size,
            depth,
        } => cli::find::execute(
            &cli::find::FindOptions {
                datadir,
                page,
                checksum,
                space_id,
                corrupt,
                first,
                json,
                page_size,
                threads: cli.threads,
                mmap: cli.mmap,
                depth,
            },
            &mut writer,
        ),

        Commands::Tsid {
            datadir,
            list,
            tablespace_id,
            json,
            page_size,
            depth,
        } => cli::tsid::execute(
            &cli::tsid::TsidOptions {
                datadir,
                list,
                tablespace_id,
                json,
                page_size,
                mmap: cli.mmap,
                depth,
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

        Commands::Schema {
            file,
            verbose,
            json,
            page_size,
            keyring,
        } => cli::schema::execute(
            &cli::schema::SchemaOptions {
                file,
                verbose,
                json,
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
            tablespace_map,
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
                tablespace_map,
                json,
                page_size,
                mmap: cli.mmap,
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
                json: json || global_format == OutputFormat::Json,
                csv: global_format == OutputFormat::Csv,
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
            events,
            page_size,
            keyring,
        } => cli::watch::execute(
            &cli::watch::WatchOptions {
                file,
                interval,
                verbose,
                json,
                events,
                page_size,
                keyring,
                mmap: cli.mmap,
            },
            &mut writer,
        ),

        Commands::Repair {
            file,
            batch,
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
                batch,
                page,
                algorithm,
                no_backup,
                dry_run,
                verbose,
                json,
                page_size,
                keyring,
                mmap: cli.mmap,
                audit_logger: audit_logger.clone(),
            },
            &mut writer,
        ),

        Commands::Diff {
            file1,
            file2,
            verbose,
            byte_ranges,
            page,
            version_aware,
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
                version_aware,
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
                audit_logger: audit_logger.clone(),
            },
            &mut writer,
        ),

        Commands::Health {
            file,
            verbose,
            json,
            prometheus,
            page_size,
            keyring,
        } => cli::health::execute(
            &cli::health::HealthOptions {
                file,
                verbose,
                json: json || global_format == OutputFormat::Json,
                csv: global_format == OutputFormat::Csv,
                prometheus,
                page_size,
                keyring,
                mmap: cli.mmap,
            },
            &mut writer,
        ),

        Commands::Export {
            file,
            page,
            format,
            where_delete_mark,
            system_columns,
            verbose,
            page_size,
            keyring,
        } => cli::export::execute(
            &cli::export::ExportOptions {
                file,
                page,
                format,
                where_delete_mark,
                system_columns,
                verbose,
                page_size,
                keyring,
                mmap: cli.mmap,
            },
            &mut writer,
        ),

        Commands::Audit {
            datadir,
            health,
            checksum_mismatch,
            verbose,
            json,
            prometheus,
            page_size,
            keyring,
            min_fill_factor,
            max_fragmentation,
            depth,
        } => cli::audit::execute(
            &cli::audit::AuditOptions {
                datadir,
                health,
                checksum_mismatch,
                verbose,
                json: json || global_format == OutputFormat::Json,
                csv: global_format == OutputFormat::Csv,
                prometheus,
                page_size,
                keyring,
                mmap: cli.mmap,
                min_fill_factor,
                max_fragmentation,
                depth,
            },
            &mut writer,
        ),

        Commands::Compat {
            file,
            scan,
            target,
            verbose,
            json,
            page_size,
            keyring,
            depth,
        } => cli::compat::execute(
            &cli::compat::CompatOptions {
                file,
                scan,
                target,
                verbose,
                json: json || global_format == OutputFormat::Json,
                page_size,
                keyring,
                mmap: cli.mmap,
                depth,
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
                audit_logger: audit_logger.clone(),
            },
            &mut writer,
        ),

        Commands::Validate {
            datadir,
            database,
            table,
            host,
            port,
            user,
            password,
            defaults_file,
            verbose,
            json,
            page_size,
            depth,
        } => cli::validate::execute(
            &cli::validate::ValidateOptions {
                datadir,
                database,
                table,
                host,
                port,
                user,
                password,
                defaults_file,
                verbose,
                json: json || global_format == OutputFormat::Json,
                page_size,
                depth,
                mmap: cli.mmap,
            },
            &mut writer,
        ),

        Commands::Verify {
            file,
            verbose,
            json,
            page_size,
            keyring,
            redo,
            chain,
        } => cli::verify::execute(
            &cli::verify::VerifyOptions {
                file,
                verbose,
                json: json || global_format == OutputFormat::Json,
                page_size,
                keyring,
                mmap: cli.mmap,
                redo,
                chain,
            },
            &mut writer,
        ),

        Commands::Completions { shell } => {
            let mut cmd = <Cli as clap::CommandFactory>::command();
            clap_complete::generate(shell, &mut cmd, "inno", &mut std::io::stdout());
            Ok(())
        }
    };

    // End audit session if logger was created
    if let Some(ref logger) = audit_logger {
        let _ = logger.end_session();
    }

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}
