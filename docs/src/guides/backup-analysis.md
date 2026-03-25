# Backup Analysis

The `inno backup` subcommands help you verify backup integrity and understand what changed between backups. Use `backup diff` to compare page-level changes and `backup chain` to validate XtraBackup checkpoint continuity.

## When to Use

- Validating that incremental backups are complete before a restore
- Identifying which pages changed between a full backup and the current state
- Verifying XtraBackup chain integrity after a backup rotation
- Cross-referencing tablespace LSNs against backup checkpoint metadata

## Quick Start

Compare a backup against the current tablespace:

```bash
inno backup diff --base /backups/full/mydb/users.ibd \
                 --current /var/lib/mysql/mydb/users.ibd
```

Validate an XtraBackup chain:

```bash
inno backup chain -d /backups/mysql
```

Cross-reference with `inno verify`:

```bash
inno verify -f users.ibd --backup-meta /backups/full/xtrabackup_checkpoints
```

## Incremental Backup Comparison

The `backup diff` command compares page LSNs between two snapshots of the same tablespace:

```bash
inno backup diff --base backup/users.ibd --current live/users.ibd --json
```

```json
{
  "base_file": "backup/users.ibd",
  "current_file": "live/users.ibd",
  "summary": {
    "unchanged": 120,
    "modified": 8,
    "added": 2,
    "removed": 0,
    "regressed": 0
  },
  "modified_page_types": {
    "INDEX": 7,
    "UNDO_LOG": 1
  }
}
```

**Regressed pages** (current LSN < base LSN) are unusual and may indicate a partial restore or tablespace corruption.

## XtraBackup Chain Validation

The `backup chain` command reads `xtrabackup_checkpoints` files from backup subdirectories:

```bash
inno backup chain -d /backups/mysql -v
```

Expected directory layout:

```text
/backups/mysql/
  full_2026-03-01/
    xtrabackup_checkpoints
  incr_2026-03-02/
    xtrabackup_checkpoints
  incr_2026-03-03/
    xtrabackup_checkpoints
```

The validator checks:
- At least one full backup exists
- LSN ranges are contiguous (no gaps)
- Overlapping ranges are flagged but don't break the chain

## Backup Metadata Verification

The `inno verify --backup-meta` flag cross-references a tablespace's page LSNs against an XtraBackup checkpoint file:

```bash
inno verify -f /var/lib/mysql/mydb/users.ibd \
            --backup-meta /backups/full/xtrabackup_checkpoints
```

This checks that all page LSNs fall within the checkpoint's `from_lsn..to_lsn` window. Pages outside the window indicate either:
- The tablespace has been modified since the backup (pages after window)
- The tablespace contains pages from before the backup's coverage (pages before window)

## Related Commands

- `inno verify` — structural integrity verification
- `inno checksum` — page-level checksum validation
- `inno diff` — byte-level comparison between two tablespace files
