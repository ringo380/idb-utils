# inno checksum

Validate page checksums for every page in an InnoDB tablespace.

## Synopsis

```text
inno checksum -f <file> [-v] [--json] [--page-size <size>] [--keyring <path>]
```

## Description

Iterates over all pages in a tablespace and validates the stored checksum (bytes 0-3 of the FIL header) against multiple algorithms:

- **CRC-32C** (MySQL 5.7.7+): XORs two independent CRC-32C values computed over bytes [4..26) and [38..page_size-8). This is the default algorithm for modern MySQL.
- **Legacy InnoDB** (MySQL < 5.7.7): Uses `ut_fold_ulint_pair` with u32 wrapping arithmetic over the same two byte ranges.
- **MariaDB full_crc32** (MariaDB 10.5+): Single CRC-32C over bytes [0..page_size-4). Checksum is stored in the last 4 bytes of the page, not the FIL header. Detected via FSP flags bit 4.

A page is considered valid if any algorithm matches the stored checksum value.

Additionally checks **LSN consistency**: the low 32 bits of the header LSN (bytes 16-23) must match the LSN value in the 8-byte FIL trailer at the end of the page.

Special pages are handled as follows:
- Pages with checksum `0xDEADBEEF` (magic value) are skipped.
- All-zero pages are counted as empty and skipped.

The process exits with code 1 if any page has an invalid checksum, making this suitable for scripted integrity checks.

## Flags

| Flag | Short | Required | Default | Description |
|------|-------|----------|---------|-------------|
| `--file <path>` | `-f` | Yes | -- | Path to the InnoDB data file (.ibd). |
| `--verbose` | `-v` | No | Off | Show per-page checksum details (algorithm, stored/calculated values). |
| `--json` | -- | No | Off | Output in JSON format. |
| `--page-size <size>` | -- | No | Auto-detect | Override page size. |
| `--keyring <path>` | -- | No | -- | Path to MySQL keyring file for decrypting encrypted tablespaces. |

## Examples

### Validate all page checksums

```bash
inno checksum -f /var/lib/mysql/sakila/actor.ibd
```

### Verbose per-page output

```bash
inno checksum -f actor.ibd -v
```

### JSON output for scripting

```bash
inno checksum -f actor.ibd --json
```

### Use in a script with exit code

```bash
if inno checksum -f actor.ibd; then
    echo "All checksums valid"
else
    echo "Corrupt pages detected!"
fi
```

## Output

### Text Mode

Displays a progress bar during validation, then prints a summary:

```text
Validating checksums for actor.ibd (7 pages, page size 16384)...

Summary:
  Total pages: 7
  Empty pages: 3
  Valid checksums: 4
  Invalid checksums: 0
```

With `--verbose`, each non-empty page is printed individually:

```text
Page 0: OK (Crc32c, stored=2741936599, calculated=2741936599)
Page 1: OK (Crc32c, stored=1849326041, calculated=1849326041)
Page 3: INVALID checksum (stored=12345, calculated=2741936599, algorithm=Crc32c)
```

### JSON Mode

```json
{
  "file": "actor.ibd",
  "page_size": 16384,
  "total_pages": 7,
  "empty_pages": 3,
  "valid_pages": 4,
  "invalid_pages": 0,
  "lsn_mismatches": 0,
  "pages": []
}
```

With `--verbose`, the `pages` array includes every non-empty page. Without `--verbose`, only invalid pages and pages with LSN mismatches are included:

```json
{
  "pages": [
    {
      "page_number": 3,
      "status": "invalid",
      "algorithm": "crc32c",
      "stored_checksum": 12345,
      "calculated_checksum": 2741936599,
      "lsn_valid": true
    }
  ]
}
```

The `algorithm` field will be one of: `crc32c`, `innodb`, `mariadb_full_crc32`, or `none`.
