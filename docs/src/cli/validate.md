# inno validate

Cross-validate tablespace files against live MySQL metadata.

## Usage

```bash
# Disk-only scan
inno validate -d /var/lib/mysql

# MySQL cross-validation
inno validate -d /var/lib/mysql --host 127.0.0.1 --user root

# Filter by database
inno validate -d /var/lib/mysql --host 127.0.0.1 --user root -D mydb

# Deep table validation
inno validate -d /var/lib/mysql --host 127.0.0.1 --user root --table mydb.users

# JSON output
inno validate -d /var/lib/mysql --host 127.0.0.1 --user root --json
```

## Options

| Option | Description |
|--------|-------------|
| `-d, --datadir` | Path to MySQL data directory |
| `-D, --database` | Database name to filter |
| `-t, --table` | Deep-validate a specific table (format: db.table) |
| `--host` | MySQL server hostname |
| `--port` | MySQL server port |
| `-u, --user` | MySQL username |
| `-p, --password` | MySQL password |
| `--defaults-file` | Path to MySQL defaults file |
| `-v, --verbose` | Show detailed output |
| `--json` | Output in JSON format |
| `--page-size` | Override page size |
| `--depth` | Maximum directory recursion depth |

> **Note**: MySQL cross-validation requires the `mysql` feature: `cargo build --features mysql`

See the [Live MySQL Validation](../guides/live-validation.md) guide for detailed usage.
