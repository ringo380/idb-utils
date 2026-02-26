# inno schema

Extract schema and reconstruct DDL from tablespace metadata.

## Usage

```bash
# Show DDL
inno schema -f users.ibd

# Verbose output with column/index breakdown
inno schema -f users.ibd -v

# JSON output
inno schema -f users.ibd --json
```

## Options

| Option | Description |
|--------|-------------|
| `-f, --file` | Path to InnoDB data file |
| `-v, --verbose` | Show structured schema breakdown above the DDL |
| `--json` | Output in JSON format |
| `--page-size` | Override page size |
| `--keyring` | Path to MySQL keyring file |

## Behavior

For MySQL 8.0+ tablespaces with SDI metadata:
- Parses the embedded data dictionary JSON
- Extracts column definitions, indexes, and foreign keys
- Reconstructs a complete `CREATE TABLE` DDL statement
- Resolves column types, defaults, character sets, and collations

For pre-8.0 tablespaces without SDI:
- Scans INDEX pages to infer basic index structure
- Determines record format (compact vs. redundant)
- Provides limited structural information
