# inno repair

Recalculate and fix corrupt page checksums.

## Usage

```bash
# Repair a single file
inno repair -f table.ibd

# Repair a specific page
inno repair -f table.ibd -p 5

# Dry run (preview without modifying)
inno repair -f table.ibd --dry-run

# Batch repair all files in a directory
inno repair --batch /var/lib/mysql

# Force a specific algorithm
inno repair -f table.ibd -a crc32c

# Skip backup creation
inno repair -f table.ibd --no-backup
```

## Options

| Option | Description |
|--------|-------------|
| `-f, --file` | Path to InnoDB data file |
| `--batch` | Repair all .ibd files under a directory |
| `-p, --page` | Repair only a specific page number |
| `-a, --algorithm` | Checksum algorithm: auto, crc32c, innodb, full_crc32 (default: auto) |
| `--no-backup` | Skip creating a .bak backup |
| `--dry-run` | Preview repairs without modifying files |
| `-v, --verbose` | Show per-page repair details |
| `--json` | Output in JSON format |
| `--page-size` | Override page size |
| `--keyring` | Path to MySQL keyring file |

## Behavior

- Auto-detects the checksum algorithm from page 0 unless `--algorithm` is specified
- Creates a `.bak` backup before modifying the file (unless `--no-backup`)
- Only rewrites pages with invalid checksums
- Batch mode processes files in parallel using rayon
- Compatible with `--audit-log` for write operation tracking
