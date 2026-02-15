# Quick Start

This page walks through the most common `inno` operations using practical examples. Each example can be run against any `.ibd` tablespace file from MySQL 5.7+, Percona, or MariaDB 10.1+.

## Parse a Tablespace

Display page headers and a summary of page types in a tablespace file:

```bash
inno parse -f /var/lib/mysql/mydb/users.ibd
```

Use `-p` to focus on a single page, `-v` for verbose output, or `-e` to include extent descriptor details:

```bash
inno parse -f users.ibd -p 0 -v
```

## Validate Checksums

Verify the integrity of every page in a tablespace by checking CRC-32C, legacy InnoDB, or MariaDB full\_crc32 checksums:

```bash
inno checksum -f users.ibd
```

Add `-v` to see per-page checksum details.

## Inspect Page Structures

Get detailed structural analysis of specific page types. Use `-t` to filter by type:

```bash
inno pages -f users.ibd -t INDEX
```

This shows B+Tree node details for INDEX pages, including record counts, page level, and directory slot information.

## Hex Dump

View raw bytes of any page:

```bash
inno dump -f users.ibd -p 3
```

Use `--offset` and `-l` to narrow the dump to a specific byte range within the page:

```bash
inno dump -f users.ibd -p 3 --offset 0 -l 38
```

## Extract SDI Metadata

Read Serialized Dictionary Information from MySQL 8.0+ tablespace files. This contains table definitions, column types, and index metadata:

```bash
inno sdi -f users.ibd --pretty
```

## Analyze Redo Logs

Parse InnoDB redo log file headers, checkpoint blocks, and log record blocks:

```bash
inno log -f /var/lib/mysql/ib_logfile0
```

Use `-b` to limit the number of blocks analyzed, or `--no-empty` to skip empty blocks:

```bash
inno log -f ib_logfile0 -b 10 --no-empty -v
```

## Compare Two Tablespaces

Diff two tablespace files page-by-page to see what changed between a backup and the current state:

```bash
inno diff backup.ibd current.ibd -v
```

Use `-b` for byte-level detail on changed pages, or `-p` to compare a single page:

```bash
inno diff backup.ibd current.ibd -p 3 -b
```

## Monitor Live Changes

Watch a tablespace file for modifications in real time. The watch loop uses LSN-based change detection:

```bash
inno watch -f users.ibd -i 500 -v
```

The `-i` flag sets the polling interval in milliseconds.

## Assess Recoverability

Evaluate page-level damage and estimate how many records are salvageable:

```bash
inno recover -f users.ibd --force -v
```

The `--force` flag attempts recovery assessment even on pages that appear severely damaged.

## Read Encrypted Tablespaces

Parse tablespace files that use InnoDB tablespace encryption by providing a keyring file:

```bash
inno parse -f encrypted.ibd --keyring /var/lib/mysql-keyring/keyring
```

## Search a Data Directory

Find all tablespace files containing a specific page number:

```bash
inno find -d /var/lib/mysql -p 42
```

Add `-s` to filter by space ID, or `--first` to stop after the first match.

## List and Look Up Tablespace IDs

List all tablespace IDs in a data directory, or look up a specific one:

```bash
inno tsid -d /var/lib/mysql -l
inno tsid -d /var/lib/mysql -t 15
```

## Check LSN Consistency

Compare the LSN in ibdata1 against redo log checkpoints to detect sync issues:

```bash
inno info --lsn-check -d /var/lib/mysql
```

## JSON Output

Every subcommand supports `--json` for machine-readable output, making it straightforward to integrate with scripts and pipelines:

```bash
inno parse -f users.ibd --json | jq '.pages | length'
inno checksum -f users.ibd --json
inno recover -f users.ibd --json
```

## Getting Help

Every subcommand has its own `--help` flag with a full description of available options:

```bash
inno --help
inno parse --help
inno checksum --help
```

For detailed documentation on each subcommand, see the [CLI Reference](cli/overview.md).
