# Live MySQL Validation

The `inno validate` subcommand cross-validates on-disk tablespace files against live MySQL metadata. It detects orphan files, missing tablespaces, and space ID mismatches between the filesystem and MySQL's internal registry.

> **Note**: MySQL cross-validation requires the `mysql` feature. Build with:
> ```bash
> cargo build --release --features mysql
> ```

## Quick Start

Scan a data directory without MySQL (disk-only mode):

```bash
inno validate -d /var/lib/mysql
```

Cross-validate against a live MySQL instance:

```bash
inno validate -d /var/lib/mysql --host 127.0.0.1 --user root
```

## Modes

### Disk-Only Mode

Without MySQL connection options, `validate` scans for `.ibd` files and reports what it finds on disk:

```bash
inno validate -d /var/lib/mysql
```

This lists each tablespace file with its space ID (read from page 0). Useful for quick inventory without a running MySQL instance.

### MySQL Cross-Validation Mode

With `--host` and `--user` (or `--defaults-file`), `validate` queries `INFORMATION_SCHEMA.INNODB_TABLESPACES` and compares the results against disk:

```bash
inno validate -d /var/lib/mysql --host 127.0.0.1 --user root --password secret
```

Filter to a specific database:

```bash
inno validate -d /var/lib/mysql --host 127.0.0.1 --user root -D mydb
```

### Deep Table Validation

The `--table` flag performs deep validation of a specific table, verifying index root pages and space ID consistency:

```bash
inno validate -d /var/lib/mysql --host 127.0.0.1 --user root --table mydb.users
```

This checks:
- The `.ibd` file exists at the expected path
- The space ID in the file matches MySQL's registry
- Each index root page exists and contains a valid INDEX page
- Row format consistency between MySQL and the on-disk file

## What Gets Detected

| Finding | Description |
|---------|-------------|
| **Orphan files** | `.ibd` files on disk that have no matching entry in MySQL's tablespace registry |
| **Missing files** | Tablespaces registered in MySQL but with no corresponding `.ibd` file on disk |
| **Space ID mismatches** | Files where the on-disk space ID differs from MySQL's recorded space ID |
| **Invalid index roots** | Root pages that don't exist or aren't INDEX pages (--table mode) |

## JSON Output

```bash
inno validate -d /var/lib/mysql --host 127.0.0.1 --user root --json
```

The cross-validation report includes:

```json
{
  "disk_files": 42,
  "mysql_tablespaces": 42,
  "orphans": [],
  "missing": [],
  "mismatches": [],
  "passed": true
}
```

## Connection Options

| Option | Description |
|--------|-------------|
| `--host` | MySQL server hostname or IP |
| `--port` | MySQL server port (default: 3306) |
| `--user` / `-u` | MySQL username |
| `--password` / `-p` | MySQL password |
| `--defaults-file` | Path to MySQL defaults file (`.my.cnf`) |

You can also use a defaults file:

```bash
inno validate -d /var/lib/mysql --defaults-file ~/.my.cnf
```

## Common Use Cases

### Post-Migration Check

After migrating tablespace files or restoring from backup, verify that all files match MySQL's internal state:

```bash
inno validate -d /var/lib/mysql --host 127.0.0.1 --user root --json
```

### Orphan File Cleanup

Find `.ibd` files left behind after `DROP TABLE` operations that didn't fully clean up:

```bash
inno validate -d /var/lib/mysql --host 127.0.0.1 --user root -v
```

Orphan files are listed with their paths and space IDs for manual review.

### Disaster Recovery Triage

After a crash, quickly assess whether all tablespaces match MySQL's expectations:

```bash
inno validate -d /var/lib/mysql --host 127.0.0.1 --user root
```

A `FAIL` result with missing files indicates tablespaces that MySQL expects but cannot find, which may need restoration from backup.
