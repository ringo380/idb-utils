# inno simulate

Simulate InnoDB crash recovery at different `innodb_force_recovery` levels.

## Usage

```bash
# Simulate level 1 recovery
inno simulate -f ibdata1 -d /var/lib/mysql --level 1

# Simulate all levels (1-6) and show results
inno simulate -f ibdata1 -d /var/lib/mysql -v

# JSON output
inno simulate -f ibdata1 -d /var/lib/mysql --level 3 --json
```

## Options

| Option | Description |
|--------|-------------|
| `-f, --file` | Path to InnoDB system tablespace (ibdata1) |
| `-d, --datadir` | MySQL data directory path |
| `--level` | Force recovery level to simulate (1-6, default: simulate all) |
| `-v, --verbose` | Show per-page details |
| `--json` | Output in JSON format |
| `--page-size` | Override page size |
| `--keyring` | Path to MySQL keyring file |

## Recovery Levels

| Level | Name | Effect |
|-------|------|--------|
| 1 | `SRV_FORCE_IGNORE_CORRUPT` | Skip corrupt pages during recovery |
| 2 | `SRV_FORCE_NO_BACKGROUND` | Prevent background operations (purge, insert buffer merge) |
| 3 | `SRV_FORCE_NO_TRX_UNDO` | Skip transaction rollbacks after recovery |
| 4 | `SRV_FORCE_NO_IBUF_MERGE` | Skip insert buffer merge |
| 5 | `SRV_FORCE_NO_UNDO_LOG_SCAN` | Skip undo log scanning |
| 6 | `SRV_FORCE_NO_LOG_REDO` | Skip redo log apply entirely |

## Output

For each simulated level, the report includes:

- Whether the tablespace would survive recovery at that level
- Number of pages that would be affected
- Specific risks or data loss implications
- Recommended recovery actions

See the [Crash Recovery](../guides/crash-recovery.md) guide for decision trees and real-world scenarios.
