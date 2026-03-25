# inno timeline

Build a unified modification timeline by correlating entries from redo logs, undo tablespaces, and binary logs. Orders all events by LSN to show the sequence of changes across multiple log sources.

## Usage

```bash
# Timeline from redo log only
inno timeline --redo-log /var/lib/mysql/ib_logfile0

# Combine redo + undo + binlog sources
inno timeline --redo-log ib_logfile0 --undo-file undo_001 --binlog binlog.000001

# Filter to a specific tablespace/page
inno timeline --redo-log ib_logfile0 --undo-file undo_001 -s 5 -p 3

# Filter by table name (binlog entries)
inno timeline --binlog binlog.000001 -d /var/lib/mysql --table users

# Limit output and use JSON
inno timeline --redo-log ib_logfile0 --binlog binlog.000001 --limit 50 --json
```

## Options

| Option | Description |
|--------|-------------|
| `--redo-log` | Path to InnoDB redo log file |
| `--undo-file` | Path to undo tablespace file |
| `--binlog` | Path to MySQL binary log file |
| `-d, --datadir` | MySQL data directory (resolves table names to space IDs for binlog entries) |
| `-s, --space-id` | Filter entries by tablespace ID |
| `-p, --page` | Filter entries by page number |
| `--table` | Filter binlog entries by table name (case-insensitive substring match) |
| `--limit` | Maximum number of entries to display |
| `-v, --verbose` | Show additional detail per entry |
| `--json` | Output in JSON format |
| `--page-size` | Override page size |
| `--keyring` | Path to MySQL keyring file |

At least one of `--redo-log`, `--undo-file`, or `--binlog` is required.

## Output

The timeline displays entries sorted by LSN with columns:

- **SEQ** -- Sequence number
- **LSN** -- Log sequence number (used for ordering)
- **SOURCE** -- Origin: `redo`, `undo`, or `binlog`
- **SPACE:PAGE** -- Tablespace ID and page number (when available)
- **ACTION** -- Description of the modification

A **Page Summary** section groups entries by space:page, showing how many redo, undo, and binlog entries touch each page along with first/last LSN.

## Correlation

When `--datadir` is provided, the timeline resolves binlog table names to InnoDB space IDs by scanning `.ibd` files in the data directory. This enables cross-referencing binlog row events with redo/undo page-level changes affecting the same tablespace.

## See Also

- [`inno log`](log.md) -- Redo log analysis
- [`inno undo`](undo.md) -- Undo tablespace analysis
- [`inno binlog`](binlog.md) -- Binary log analysis
