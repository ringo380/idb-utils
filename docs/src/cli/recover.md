# inno recover

Assess page-level recoverability and count salvageable records in an InnoDB tablespace.

## Synopsis

```text
inno recover -f <file> [-p <page>] [-v] [--json] [--force] [--page-size <size>] [--keyring <path>]
```

## Description

Scans a tablespace file and classifies each page into one of four states:

- **Intact**: Checksum is valid and LSN is consistent between header and trailer.
- **Corrupt**: Checksum mismatch or LSN inconsistency, but the FIL header is parseable.
- **Empty**: All-zero page (allocated but unused).
- **Unreadable**: Page data is too short or the FIL header cannot be parsed.

For INDEX pages, the compact record chain is walked to count recoverable user records. This gives a concrete estimate of how many rows can be salvaged from a damaged tablespace.

### Smart page size fallback

When page 0 is damaged and auto-detection fails, `inno recover` tries common page sizes (16K, 8K, 4K, 32K, 64K) in order, selecting the first one that divides evenly into the file size. Use `--page-size` to force a specific size when the heuristic is not sufficient.

### Force mode

By default, records are only counted on intact INDEX pages. With `--force`, records are also extracted from corrupt pages that have valid-looking FIL headers. This is useful when the checksum is damaged but the record chain is still intact, allowing more data to be recovered.

## Flags

| Flag | Short | Required | Default | Description |
|------|-------|----------|---------|-------------|
| `--file <path>` | `-f` | Yes | -- | Path to the InnoDB data file (.ibd). |
| `--page <number>` | `-p` | No | All pages | Analyze a single page instead of a full scan. |
| `--verbose` | `-v` | No | Off | Show per-page details (type, status, LSN, record count). |
| `--json` | -- | No | Off | Output in JSON format. |
| `--force` | -- | No | Off | Extract records from corrupt pages with valid headers. |
| `--page-size <size>` | -- | No | Auto-detect (with fallback) | Override page size. Critical when page 0 is corrupt. |
| `--keyring <path>` | -- | No | -- | Path to MySQL keyring file for decrypting encrypted tablespaces. |

## Examples

### Quick recovery assessment

```bash
inno recover -f damaged_table.ibd
```

### Verbose per-page analysis

```bash
inno recover -f damaged_table.ibd -v
```

### Force-extract records from corrupt pages

```bash
inno recover -f damaged_table.ibd --force -v
```

### Analyze a single page

```bash
inno recover -f damaged_table.ibd -p 3 -v
```

### JSON output with per-page detail

```bash
inno recover -f damaged_table.ibd --json -v
```

### Override page size for a file with corrupt page 0

```bash
inno recover -f damaged_table.ibd --page-size 16384
```

## Output

### Text Mode

```text
Recovery Analysis: damaged_table.ibd
File size: 114688 bytes (7 pages x 16384 bytes)
Page size: 16384 (auto-detected)

Page Status Summary:
  Intact:         5 pages
  Corrupt:        1 pages (pages 3)
  Empty:          1 pages
  Unreadable:     0 pages
  Total:          7 pages

Recoverable INDEX Pages: 3 of 4
  Total user records: 200
  Records on corrupt pages: 52 (use --force to include)

Overall: 83.3% of pages intact
```

With `--verbose`, per-page lines are displayed before the summary:

```text
Page    0: FSP_HDR        intact       LSN=1000
Page    1: IBUF_BITMAP    intact       LSN=1001
Page    2: INODE          intact       LSN=1002
Page    3: INDEX          CORRUPT      LSN=5000  records=52  checksum mismatch
Page    4: INDEX          intact       LSN=5001  records=100
Page    5: INDEX          intact       LSN=5002  records=100
Page    6: Allocated      empty        LSN=0
```

### JSON Mode

```json
{
  "file": "damaged_table.ibd",
  "file_size": 114688,
  "page_size": 16384,
  "total_pages": 7,
  "summary": {
    "intact": 5,
    "corrupt": 1,
    "empty": 1,
    "unreadable": 0
  },
  "recoverable_records": 200,
  "force_recoverable_records": 52,
  "pages": []
}
```

With `--verbose`, the `pages` array includes per-page detail. When combined with `--verbose --json`, each INDEX page also includes per-record data with byte offsets, heap numbers, delete marks, and hex-encoded record bytes.
