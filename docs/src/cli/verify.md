# inno verify

Verify structural integrity of a tablespace.

## Usage

```bash
# Basic verification
inno verify -f users.ibd

# Verbose output
inno verify -f users.ibd -v

# JSON output
inno verify -f users.ibd --json

# Verify with redo log
inno verify -f users.ibd --redo ib_logfile0

# Verify backup chain
inno verify --chain full.ibd incr1.ibd incr2.ibd
```

## Options

| Option | Description |
|--------|-------------|
| `-f, --file` | Path to InnoDB data file |
| `-v, --verbose` | Show per-page findings |
| `--json` | Output in JSON format |
| `--page-size` | Override page size |
| `--keyring` | Path to MySQL keyring file |
| `--redo` | Path to redo log file for LSN continuity check |
| `--chain` | Verify backup chain (accepts multiple files) |
| `--backup-meta` | Path to XtraBackup checkpoint file for LSN cross-reference |

## Structural Checks

| Check | Description |
|-------|-------------|
| PageNumberSequence | Page numbers match expected file positions |
| SpaceIdConsistency | All pages have consistent space IDs |
| LsnMonotonicity | LSNs are non-decreasing |
| BTreeLevelConsistency | B+Tree levels are valid |
| PageChainBounds | prev/next pointers within bounds |
| TrailerLsnMatch | Trailer LSN matches header LSN |

## Backup Metadata Verification

Use `--backup-meta` to cross-reference tablespace LSNs against an XtraBackup checkpoint file:

```bash
inno verify -f users.ibd --backup-meta /backups/full/xtrabackup_checkpoints
```

Pages with LSNs outside the checkpoint's `from_lsn..to_lsn` window are reported as inconsistent with the backup point-in-time.

See the [Backup Verification](../guides/backup-verification.md) guide for detailed usage of `--chain`, `--redo`, and `--backup-meta`.
