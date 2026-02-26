# inno transplant

Copy specific pages from a donor tablespace into a target.

## Usage

```bash
# Transplant pages 3 and 5 from donor to target
inno transplant donor.ibd target.ibd -p 3,5

# Preview without modifying
inno transplant donor.ibd target.ibd -p 3,5 --dry-run

# Force transplant (skip safety checks)
inno transplant donor.ibd target.ibd -p 3,5 --force
```

## Options

| Option | Description |
|--------|-------------|
| `donor` | Path to donor tablespace file (source of pages) |
| `target` | Path to target tablespace file (destination) |
| `-p, --pages` | Page numbers to transplant (comma-separated) |
| `--no-backup` | Skip creating a backup of the target |
| `--force` | Allow space ID mismatch, corrupt donor pages, and page 0 transplant |
| `--dry-run` | Preview without modifying the target |
| `-v, --verbose` | Show per-page details |
| `--json` | Output in JSON format |
| `--page-size` | Override page size |
| `--keyring` | Path to MySQL keyring file |

## Safety Checks

- Page sizes must match between donor and target
- Space IDs must match (unless `--force`)
- Page 0 (FSP_HDR) is rejected (unless `--force`)
- Donor pages with invalid checksums are skipped (unless `--force`)
- A backup of the target is created by default
