# inno diff

Compare two InnoDB tablespace files page-by-page.

## Synopsis

```text
inno diff <file1> <file2> [-v] [-b] [-p <page>] [--json] [--page-size <size>] [--keyring <path>]
```

## Description

Reads two InnoDB tablespace files and compares them page-by-page, reporting which pages are identical, modified, or only present in one file.

Three levels of comparison detail are available:

1. **Quick comparison** (default): Performs full-page byte equality checks and reports a summary of identical, modified, and file-only page counts.

2. **Header field diff** (`-v`): For modified pages, decodes and compares the FIL header fields (checksum, page number, prev/next pointers, LSN, page type, flush LSN, space ID) and reports which fields changed.

3. **Byte-range scan** (`-v -b`): In addition to header diffs, scans the full page content to identify the exact byte offset ranges where data differs, along with a total bytes-changed count and percentage.

When files have different page sizes, only the FIL headers (first 38 bytes) are compared and a warning is displayed.

## Flags

| Flag | Short | Required | Default | Description |
|------|-------|----------|---------|-------------|
| `<file1>` | -- | Yes | -- | First InnoDB data file (.ibd). Positional argument. |
| `<file2>` | -- | Yes | -- | Second InnoDB data file (.ibd). Positional argument. |
| `--verbose` | `-v` | No | Off | Show per-page header field diffs for modified pages. |
| `--byte-ranges` | `-b` | No | Off | Show exact byte-range diffs for changed pages. Requires `-v`. |
| `--page <number>` | `-p` | No | All pages | Compare a single page only. |
| `--json` | -- | No | Off | Output in JSON format. |
| `--page-size <size>` | -- | No | Auto-detect | Override page size. |
| `--keyring <path>` | -- | No | -- | Path to MySQL keyring file for decrypting encrypted tablespaces. |

## Examples

### Quick comparison of two tablespace files

```bash
inno diff original.ibd modified.ibd
```

### Verbose field-level diff

```bash
inno diff original.ibd modified.ibd -v
```

### Verbose with byte-range analysis

```bash
inno diff original.ibd modified.ibd -v -b
```

### Compare a single page

```bash
inno diff original.ibd modified.ibd -p 3 -v
```

### JSON output

```bash
inno diff original.ibd modified.ibd --json | jq '.summary'
```

## Output

### Text Mode

```text
Comparing:
  File 1: original.ibd (7 pages, 16384 bytes/page)
  File 2: modified.ibd (7 pages, 16384 bytes/page)

Summary:
  Identical pages:  5
  Modified pages:   2
  Only in file 1:   0
  Only in file 2:   0

Modified pages: 3, 4
```

With `-v`, modified pages show field-level changes:

```text
Page 3: MODIFIED
  Checksum: 0xA3B1C5D7 -> 0x12345678
  LSN: 5539 -> 6012
  Page Type: INDEX (unchanged)
```

With `-v -b`, byte-range diffs are appended:

```text
  Byte diff ranges:
    0-4 (4 bytes)
    38-16376 (16338 bytes)
  Total: 16342 bytes changed (99.7% of page)
```

### JSON Mode

```json
{
  "file1": { "path": "original.ibd", "page_count": 7, "page_size": 16384 },
  "file2": { "path": "modified.ibd", "page_count": 7, "page_size": 16384 },
  "page_size_mismatch": false,
  "summary": {
    "identical": 5,
    "modified": 2,
    "only_in_file1": 0,
    "only_in_file2": 0
  },
  "modified_pages": [
    {
      "page_number": 3,
      "file1_header": { "checksum": "0xA3B1C5D7", "lsn": 5539, "page_type": "INDEX" },
      "file2_header": { "checksum": "0x12345678", "lsn": 6012, "page_type": "INDEX" },
      "changed_fields": [
        { "field": "Checksum", "old_value": "0xA3B1C5D7", "new_value": "0x12345678" },
        { "field": "LSN", "old_value": "5539", "new_value": "6012" }
      ]
    }
  ]
}
```
