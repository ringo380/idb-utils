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
| `--bloat` | Compute bloat scores (grade A-F) per index |
| `--cardinality` | Estimate cardinality of leading primary key columns |
| `--sample-size` | Number of leaf pages to sample per index for cardinality (default: 100) |

## Metrics

| Metric | Description |
|--------|-------------|
| Fill factor | Average, min, and max page utilization (0-100%) |
| Garbage ratio | Percentage of space occupied by deleted records |
| Fragmentation | How out-of-order pages are relative to sequential layout |
| Tree depth | B+Tree depth per index |
| Page counts | Total, leaf, and internal page counts per index |

## Bloat Scoring

Use `--bloat` to compute a weighted bloat score and letter grade (A-F) for each index:

```bash
inno health -f users.ibd --bloat
```

The score combines fill factor deficit (30%), garbage ratio (25%), fragmentation (25%), and delete-mark ratio (20%). See the [Bloat Scoring](../guides/bloat-scoring.md) guide for the full formula and grade definitions.

## Cardinality Estimation

Use `--cardinality` to estimate distinct values for the leading primary key column using deterministic page sampling:

```bash
inno health -f users.ibd --cardinality --sample-size 200
```
