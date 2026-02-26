# inno health

Per-index B+Tree health metrics including fill factor, fragmentation, and garbage ratio.

## Usage

```bash
# Text output
inno health -f users.ibd

# JSON output
inno health -f users.ibd --json

# Prometheus metrics
inno health -f users.ibd --prometheus

# Verbose output
inno health -f users.ibd -v
```

## Options

| Option | Description |
|--------|-------------|
| `-f, --file` | Path to InnoDB data file |
| `-v, --verbose` | Show additional detail (records, empty leaves) |
| `--json` | Output in JSON format |
| `--prometheus` | Output in Prometheus exposition format |
| `--page-size` | Override page size |
| `--keyring` | Path to MySQL keyring file |

## Metrics

| Metric | Description |
|--------|-------------|
| Fill factor | Average, min, and max page utilization (0-100%) |
| Garbage ratio | Percentage of space occupied by deleted records |
| Fragmentation | How out-of-order pages are relative to sequential layout |
| Tree depth | B+Tree depth per index |
| Page counts | Total, leaf, and internal page counts per index |
