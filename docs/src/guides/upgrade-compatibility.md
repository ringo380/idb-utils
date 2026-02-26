# Upgrade Compatibility Checking

The `inno compat` subcommand analyzes InnoDB tablespace files to determine whether they are compatible with a target MySQL version. This is useful when planning MySQL version upgrades, especially across major versions (e.g., 5.7 to 8.0, or 8.0 to 9.0).

## Quick Start

Check a single tablespace against MySQL 8.4:

```bash
inno compat -f /var/lib/mysql/mydb/users.ibd -t 8.4.0
```

Scan an entire data directory:

```bash
inno compat --scan /var/lib/mysql -t 9.0.0
```

## What Gets Checked

| Check | Description | Severity |
|-------|-------------|----------|
| **page_size** | Non-default page sizes (4K/8K/32K/64K) require MySQL 5.7.6+ | Error |
| **sdi** | SDI metadata is required for MySQL 8.0+ but absent in pre-8.0 files | Error |
| **encryption** | Tablespace-level encryption requires MySQL 5.7.11+ | Error |
| **vendor** | MariaDB tablespaces are incompatible with MySQL (divergent formats) | Error |
| **row_format** | COMPRESSED is deprecated in MySQL 8.4+, REDUNDANT in 9.0+ | Warning |
| **compression** | Page compression detected (informational) | Info |

## Severity Levels

- **Error**: The tablespace cannot be used with the target version. Migration or conversion is required.
- **Warning**: The tablespace will work but uses deprecated features. Plan for future migration.
- **Info**: Informational finding, no action required.

## Single-File Mode

Analyze one tablespace in detail:

```bash
inno compat -f table.ibd -t 8.4.0 -v
```

The `-v` (verbose) flag shows current and expected values for each check. JSON output is available with `--json`:

```bash
inno compat -f table.ibd -t 8.4.0 --json
```

Example JSON output:

```json
{
  "file": "table.ibd",
  "target_version": "8.4.0",
  "source_version": "8.0.32",
  "compatible": true,
  "checks": [
    {
      "check": "vendor",
      "message": "MySQL tablespace detected",
      "severity": "info"
    }
  ],
  "summary": {
    "total_checks": 1,
    "errors": 0,
    "warnings": 0,
    "info": 1
  }
}
```

## Directory Scan Mode

Scan all `.ibd` files under a data directory:

```bash
inno compat --scan /var/lib/mysql -t 9.0.0
```

The scan uses parallel processing for fast analysis of large data directories. Add `--depth 0` for unlimited recursion, or `--depth 3` to limit traversal depth.

With `--json`, the output includes per-file results and aggregate counts:

```bash
inno compat --scan /var/lib/mysql -t 9.0.0 --json
```

## Common Upgrade Scenarios

### MySQL 5.7 to 8.0

Key checks:
- SDI metadata is required (added in 8.0). Pre-8.0 tablespaces will report an SDI error.
- Encryption support is available but the format changed.

### MySQL 8.0 to 8.4

Key checks:
- `ROW_FORMAT=COMPRESSED` is deprecated. Tables using it will get a warning.
- All other formats (DYNAMIC, COMPACT) remain fully supported.

### MySQL 8.x to 9.0

Key checks:
- `ROW_FORMAT=REDUNDANT` is deprecated in 9.0+.
- `ROW_FORMAT=COMPRESSED` deprecation continues from 8.4.

### MariaDB to MySQL

MariaDB tablespaces are flagged as incompatible with MySQL due to divergent on-disk formats (different checksum algorithms, FSP flags, and page types). Migration requires a logical dump and reload.
