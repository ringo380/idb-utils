# inno backup

Analyze incremental backups and validate backup chains. Two subcommands: `diff` compares page LSNs between backup snapshots, `chain` validates XtraBackup checkpoint continuity.

## backup diff

Compare page LSNs between a base (backup) and current tablespace to identify changed pages.

```bash
# Compare backup vs current
inno backup diff --base backup/users.ibd --current /var/lib/mysql/mydb/users.ibd

# Verbose: show per-page details
inno backup diff --base backup/users.ibd --current current/users.ibd -v

# JSON output
inno backup diff --base backup/users.ibd --current current/users.ibd --json
```

### Options

| Option | Description |
|--------|-------------|
| `--base` | Path to base (backup) tablespace file |
| `--current` | Path to current tablespace file |
| `-v, --verbose` | Show per-page change details |
| `--json` | Output in JSON format |
| `--page-size` | Override page size |
| `--keyring` | Path to MySQL keyring file |

### Page Change States

| Status | Description |
|--------|-------------|
| Unchanged | Same LSN in both files |
| Modified | Current LSN > base LSN |
| Added | Page only exists in current file |
| Removed | Page only exists in base file |
| Regressed | Current LSN < base LSN (unusual) |

## backup chain

Validate XtraBackup backup chain continuity by reading `xtrabackup_checkpoints` files.

```bash
# Validate a backup directory
inno backup chain -d /backups/mysql

# Verbose output
inno backup chain -d /backups/mysql -v

# JSON output
inno backup chain -d /backups/mysql --json
```

### Options

| Option | Description |
|--------|-------------|
| `-d, --dir` | Directory containing backup subdirectories |
| `-v, --verbose` | Show detailed checkpoint info |
| `--json` | Output in JSON format |

### Chain Validation

The chain validator reads `xtrabackup_checkpoints` files from each backup subdirectory and checks:

- A full backup exists in the chain
- LSN ranges are contiguous (no gaps between `to_lsn` of one backup and `from_lsn` of the next)
- Overlapping LSN ranges are flagged as warnings but do not break the chain

See the [Backup Analysis](../guides/backup-analysis.md) guide for workflows and XtraBackup integration.
