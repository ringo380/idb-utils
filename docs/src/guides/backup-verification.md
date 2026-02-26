# Backup Verification

The `inno verify` subcommand performs structural integrity checks on InnoDB tablespace files without requiring valid checksums. It can also verify backup chain continuity and redo log consistency.

## Quick Start

Verify a single tablespace:

```bash
inno verify -f /var/lib/mysql/mydb/users.ibd
```

Verify a backup chain:

```bash
inno verify --chain full_backup.ibd incremental_1.ibd incremental_2.ibd
```

Verify redo log continuity:

```bash
inno verify -f users.ibd --redo /var/lib/mysql/ib_logfile0
```

## Structural Checks

The `verify` subcommand runs six structural checks on every page in the tablespace:

| Check | Description |
|-------|-------------|
| **PageNumberSequence** | The page number stored at offset 4 matches its expected position in the file |
| **SpaceIdConsistency** | All pages have the same space ID as page 0 |
| **LsnMonotonicity** | LSNs are non-decreasing across pages (within tolerance) |
| **BTreeLevelConsistency** | INDEX leaf pages have level 0, internal pages have level > 0 |
| **PageChainBounds** | prev/next pointers are within file bounds; first page has prev = FIL_NULL |
| **TrailerLsnMatch** | The trailer LSN low-32 bits match the header LSN low-32 bits |

## Usage

### Basic Verification

```bash
inno verify -f table.ibd
```

With verbose output showing per-page findings:

```bash
inno verify -f table.ibd -v
```

### JSON Output

```bash
inno verify -f table.ibd --json
```

Example output:

```json
{
  "file": "table.ibd",
  "total_pages": 128,
  "page_size": 16384,
  "passed": true,
  "findings": [],
  "summary": [
    {"kind": "PageNumberSequence", "pages_checked": 128, "issues_found": 0, "passed": true},
    {"kind": "SpaceIdConsistency", "pages_checked": 128, "issues_found": 0, "passed": true}
  ]
}
```

## Backup Chain Verification

The `--chain` flag accepts multiple tablespace files and verifies that they form a valid backup chain based on LSN ordering.

```bash
inno verify --chain full.ibd incr1.ibd incr2.ibd
```

The chain verifier checks:
- Files are ordered by ascending max LSN
- No LSN gaps between consecutive files in the chain (the max LSN of file N should be less than or equal to the max LSN of file N+1)
- All files have the same space ID
- At least two files are provided

### JSON Output

```bash
inno verify --chain full.ibd incr1.ibd --json
```

Returns a `ChainReport` with per-file info and any detected gaps.

## Redo Log Continuity

The `--redo` flag verifies that a tablespace's LSN state is consistent with a redo log file.

```bash
inno verify -f table.ibd --redo /var/lib/mysql/ib_logfile0
```

This compares the checkpoint LSN from the redo log with the maximum LSN found in the tablespace. If the tablespace contains changes beyond the redo log's checkpoint, the redo log may be behind.

## When to Use

- **After backups**: Verify that backup files are structurally sound before relying on them for disaster recovery.
- **Before restores**: Check that a backup chain is complete and contiguous.
- **Corruption diagnosis**: Identify structural issues that go beyond checksum validation (which `inno checksum` handles).
- **Monitoring**: Include in periodic health checks alongside `inno audit` and `inno health`.
