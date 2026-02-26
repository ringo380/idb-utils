# inno export

Export record-level data from a tablespace.

## Usage

```bash
# CSV export (default)
inno export -f users.ibd

# JSON export
inno export -f users.ibd --format json

# Hex dump
inno export -f users.ibd --format hex

# Export specific page
inno export -f users.ibd -p 3

# Include delete-marked records only
inno export -f users.ibd --where-delete-mark

# Include system columns
inno export -f users.ibd --system-columns
```

## Options

| Option | Description |
|--------|-------------|
| `-f, --file` | Path to InnoDB data file |
| `-p, --page` | Export records from a specific page only |
| `--format` | Output format: csv, json, or hex (default: csv) |
| `--where-delete-mark` | Include only delete-marked records |
| `--system-columns` | Include DB_TRX_ID and DB_ROLL_PTR columns |
| `-v, --verbose` | Show additional details |
| `--page-size` | Override page size |
| `--keyring` | Path to MySQL keyring file |

## Supported Types

See the [Data Type Decoding](../guides/data-type-decoding.md) guide for the full list of supported column types and encoding details.
