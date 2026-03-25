# Bloat Scoring

The `inno health --bloat` flag computes a bloat score and letter grade (A-F) for each index in a tablespace. Use it to identify tables that need `OPTIMIZE TABLE` or `ALTER TABLE ... FORCE`.

## When to Use

- Scheduling table maintenance: identify which tables benefit most from optimization
- Investigating slow queries: high bloat correlates with suboptimal scan performance
- Monitoring index health over time with `inno audit --health --bloat --prometheus`
- Setting alerts on bloat grade thresholds with `inno audit --health --max-bloat-grade`

## Quick Start

```bash
# Single tablespace
inno health -f users.ibd --bloat

# Directory-wide audit
inno audit -d /var/lib/mysql --health --bloat

# Filter to worst offenders (grade C or worse)
inno audit -d /var/lib/mysql --health --max-bloat-grade C
```

## Formula

The bloat score is a weighted average of four components:

| Component | Weight | Description |
|-----------|--------|-------------|
| Fill factor deficit | 30% | `1 - avg_fill_factor` — lower fill means more wasted space |
| Garbage ratio | 25% | Average garbage bytes / usable page space |
| Fragmentation | 25% | Ratio of non-sequential leaf page transitions |
| Delete-mark ratio | 20% | Delete-marked records / total walked records |

The final score ranges from 0.0 (no bloat) to 1.0 (maximum bloat).

## Grade Thresholds

| Grade | Score Range | Interpretation |
|-------|-------------|----------------|
| **A** | < 0.10 | Healthy — no action needed |
| **B** | 0.10 - 0.19 | Minor bloat — monitor but no immediate action |
| **C** | 0.20 - 0.34 | Moderate — consider `OPTIMIZE TABLE` during maintenance window |
| **D** | 0.35 - 0.49 | Significant — schedule optimization soon |
| **F** | >= 0.50 | Critical — `OPTIMIZE TABLE` or `ALTER TABLE ... FORCE` recommended |

## JSON Output

```bash
inno health -f users.ibd --bloat --json
```

Each index in the JSON output includes a `bloat` object:

```json
{
  "bloat": {
    "score": 0.23,
    "grade": "C",
    "components": {
      "fill_factor_deficit": 0.15,
      "garbage_ratio": 0.30,
      "fragmentation": 0.20,
      "delete_mark_ratio": 0.10
    },
    "recommendation": "Consider OPTIMIZE TABLE during next maintenance window"
  }
}
```

## Directory-Wide Alerts

Use `inno audit` with bloat to scan all tablespaces:

```bash
# Show only tables with grade C or worse
inno audit -d /var/lib/mysql --health --max-bloat-grade C --json
```

The `--max-bloat-grade` flag implies `--bloat` — you don't need both flags.

## Cardinality Estimation

The `--cardinality` flag estimates distinct values for the leading primary key column:

```bash
inno health -f users.ibd --cardinality --sample-size 200
```

This uses deterministic sampling (every k-th leaf page) without any random number generation.

## Related Commands

- `inno health` — per-index B+Tree health metrics
- `inno audit --health` — directory-wide health scanning
- `inno pages --deleted` — view delete-marked records per page
