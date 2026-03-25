# inno audit

Audit a MySQL data directory for integrity, health, or corruption.

## Usage

```bash
# Default: integrity check (checksum validation)
inno audit -d /var/lib/mysql

# Health mode: per-tablespace metrics
inno audit -d /var/lib/mysql --health

# Checksum mismatch mode: list only corrupt pages
inno audit -d /var/lib/mysql --checksum-mismatch

# Filter unhealthy tablespaces
inno audit -d /var/lib/mysql --health --min-fill-factor 50

# JSON output
inno audit -d /var/lib/mysql --json

# Prometheus metrics
inno audit -d /var/lib/mysql --health --prometheus
```

## Options

| Option | Description |
|--------|-------------|
| `-d, --datadir` | MySQL data directory path |
| `--health` | Show per-tablespace health metrics |
| `--checksum-mismatch` | List only pages with checksum mismatches |
| `-v, --verbose` | Show additional details |
| `--json` | Output in JSON format |
| `--prometheus` | Output in Prometheus exposition format |
| `--page-size` | Override page size |
| `--keyring` | Path to MySQL keyring file |
| `--min-fill-factor` | Filter: show tables with fill factor below threshold (0-100) |
| `--max-fragmentation` | Filter: show tables with fragmentation above threshold (0-100) |
| `--bloat` | Enable bloat scoring in health mode |
| `--max-bloat-grade` | Filter: show tables with worst bloat grade at or worse than threshold (A-F) |
| `--depth` | Maximum directory recursion depth (default: 2, 0 = unlimited) |

## Modes

### Integrity Mode (default)

Validates checksums across all tablespace files. Reports per-file pass/fail with a directory-wide integrity percentage.

### Health Mode

Computes per-tablespace fill factor, fragmentation, and garbage ratio, ranked worst-first. Use threshold filters to focus on unhealthy tablespaces.

### Bloat Alerts

Use `--bloat` with health mode to compute per-tablespace bloat scores and grades:

```bash
inno audit -d /var/lib/mysql --health --bloat
inno audit -d /var/lib/mysql --health --max-bloat-grade C --json
```

The `--max-bloat-grade` flag filters to tables at or worse than the given grade (A through F). It implies `--bloat`. See the [Bloat Scoring](../guides/bloat-scoring.md) guide for grade definitions.

### Checksum Mismatch Mode

Compact listing of only corrupt pages with stored vs. calculated checksums. Suitable for piping to `inno repair`.
