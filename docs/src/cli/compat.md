# inno compat

Check tablespace compatibility with a target MySQL version.

## Usage

```bash
# Single file check
inno compat -f users.ibd -t 8.4.0

# Directory scan
inno compat --scan /var/lib/mysql -t 9.0.0

# Verbose output
inno compat -f users.ibd -t 8.4.0 -v

# JSON output
inno compat -f users.ibd -t 8.4.0 --json
```

## Options

| Option | Description |
|--------|-------------|
| `-f, --file` | Path to InnoDB data file (mutually exclusive with --scan) |
| `-s, --scan` | Scan a data directory (mutually exclusive with --file) |
| `-t, --target` | Target MySQL version (e.g., "8.4.0", "9.0.0") |
| `-v, --verbose` | Show detailed check information |
| `--json` | Output in JSON format |
| `--page-size` | Override page size |
| `--keyring` | Path to MySQL keyring file |
| `--depth` | Maximum directory recursion depth (default: 2, 0 = unlimited) |

## Checks

See the [Upgrade Compatibility](../guides/upgrade-compatibility.md) guide for the full list of checks and common upgrade scenarios.

See the [MySQL Version Matrix](../guides/version-matrix.md) for a reference of feature support across MySQL versions.
