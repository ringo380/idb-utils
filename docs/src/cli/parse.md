# inno parse

Parse an InnoDB tablespace file and display page headers with a type summary.

## Synopsis

```
inno parse -f <file> [-p <page>] [-v] [-e] [--json] [--page-size <size>] [--keyring <path>]
```

## Description

Reads the 38-byte FIL header of every page in a tablespace, decodes the page type, checksum, LSN, prev/next pointers, and space ID, then prints a per-page breakdown followed by a page-type frequency summary table. Page 0 additionally shows the FSP header (space ID, tablespace size, free-page limit, and flags).

In **single-page mode** (`-p N`), only the specified page is printed with its full FIL header and trailer. In **full-file mode** (the default), all pages are listed and a frequency summary table is appended showing how many pages of each type exist.

Pages with zero checksum and type `Allocated` are skipped by default unless `--verbose` is set. The `--no-empty` flag additionally filters these from `--json` output.

With `--verbose`, each page also shows checksum validation status (algorithm, stored vs. calculated values) and LSN consistency between the FIL header and trailer.

## Flags

| Flag | Short | Required | Default | Description |
|------|-------|----------|---------|-------------|
| `--file <path>` | `-f` | Yes | -- | Path to the InnoDB data file (.ibd). |
| `--page <number>` | `-p` | No | All pages | Display a specific page number only. |
| `--verbose` | `-v` | No | Off | Show checksum validation and LSN consistency per page. |
| `--no-empty` | `-e` | No | Off | Skip empty/allocated pages (zero checksum, type Allocated). |
| `--json` | -- | No | Off | Output in JSON format. |
| `--page-size <size>` | -- | No | Auto-detect | Override page size (e.g., 4096, 8192, 16384, 32768, 65536). |
| `--keyring <path>` | -- | No | -- | Path to MySQL keyring file for decrypting encrypted tablespaces. |

## Examples

### Parse all pages in a tablespace

```bash
inno parse -f /var/lib/mysql/sakila/actor.ibd
```

### Parse a single page with verbose output

```bash
inno parse -f actor.ibd -p 3 -v
```

### Parse with empty pages filtered out

```bash
inno parse -f actor.ibd -e
```

### JSON output for scripting

```bash
inno parse -f actor.ibd --json | jq '.[].page_type_name'
```

### Parse an encrypted tablespace

```bash
inno parse -f encrypted_table.ibd --keyring /var/lib/mysql-keyring/keyring
```

## Output

In text mode, each page displays:

- **Page number** and byte offset range
- **Page type** with numeric code, name, description, and usage
- **Prev/Next page** pointers (or "Not used" for null pointers)
- **LSN** (Log Sequence Number)
- **Space ID**
- **Checksum** (stored value)
- **Trailer** with old-style checksum and LSN low 32 bits

Page 0 additionally shows the FSP header:

- Space ID, size (in pages), free-page limit, and flags

The full-file scan ends with a **Page Type Summary** table showing the count of each page type found in the file.

In JSON mode, the output is an array of page objects with `page_number`, `header`, `page_type_name`, `page_type_description`, `byte_start`, `byte_end`, and an optional `fsp_header` for page 0.
