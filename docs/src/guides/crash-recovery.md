# Crash Recovery Simulation

The `inno simulate` subcommand helps you understand what would happen at each InnoDB `innodb_force_recovery` level before you commit to setting it in production.

## When to Use

- MySQL refuses to start after a crash and you need to choose a recovery level
- You want to assess the risk of data loss before setting `innodb_force_recovery`
- You need to document the state of a corrupted tablespace for incident response
- You want to verify that a tablespace can survive recovery without data loss

## Quick Start

```bash
# Simulate all recovery levels against your system tablespace
inno simulate -f /var/lib/mysql/ibdata1 -d /var/lib/mysql
```

```bash
# Focus on a specific level
inno simulate -f /var/lib/mysql/ibdata1 -d /var/lib/mysql --level 3 -v
```

## Recovery Level Decision Tree

Start at level 1 and only increase if the previous level is insufficient:

1. **Level 1** — Try first. Skips corrupt pages but preserves all recoverable data. Safe for most single-page corruption.

2. **Level 2** — If level 1 hangs on background operations. Prevents purge and insert buffer merge.

3. **Level 3** — If crash recovery itself fails. Skips transaction rollback, which means uncommitted transactions remain visible.

4. **Level 4** — If insert buffer corruption is suspected. Skips merge operations entirely.

5. **Level 5** — If undo log is corrupted. Skips undo scanning, meaning the undo tablespace is treated as empty.

6. **Level 6** — Last resort. Skips redo log entirely. The database starts in whatever state the data files are in, ignoring any pending redo operations.

## JSON Output

```bash
inno simulate -f ibdata1 -d /var/lib/mysql --json
```

```json
{
  "file": "ibdata1",
  "levels": [
    {
      "level": 1,
      "name": "SRV_FORCE_IGNORE_CORRUPT",
      "survivable": true,
      "affected_pages": 2,
      "risks": ["2 corrupt pages will be skipped"]
    }
  ]
}
```

## Workflow: Post-Crash Recovery

1. **Assess**: Run `inno simulate` to understand the damage
2. **Back up**: Copy all data files before attempting recovery
3. **Recover**: Set `innodb_force_recovery` to the lowest effective level
4. **Export**: Use `mysqldump` to export all data
5. **Rebuild**: Restore from the dump into a fresh MySQL instance
6. **Verify**: Run `inno verify` and `inno audit` on the new data directory

## Related Commands

- `inno verify` — structural integrity verification
- `inno recover` — page-level recoverability assessment
- `inno checksum` — checksum validation
- `inno repair` — fix corrupt checksums
