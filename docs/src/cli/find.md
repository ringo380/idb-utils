# inno find

Search a MySQL data directory for pages matching a given page number.

## Synopsis

```
inno find -d <datadir> -p <page> [-c <checksum>] [-s <space_id>] [--first] [--json] [--page-size <size>]
```

## Description

Recursively discovers all `.ibd` files under a MySQL data directory, opens each as a tablespace, and reads the FIL header of every page looking for a matching `page_number` field. This is useful for locating which tablespace file contains a specific page when you have a page number from an error log or diagnostic tool.

Optional filters narrow results when the same page number appears in multiple tablespaces:

- `--checksum`: Only match pages whose stored checksum (bytes 0-3 of the FIL header) equals the given value.
- `--space-id`: Only match pages whose space ID (bytes 34-37 of the FIL header) equals the given value.

With `--first`, searching stops after the first match across all files, providing a fast lookup when only one hit is expected.

A progress bar is displayed for the file-level scan (suppressed in `--json` mode).

## Flags

| Flag | Short | Required | Default | Description |
|------|-------|----------|---------|-------------|
| `--datadir <path>` | `-d` | Yes | -- | MySQL data directory path to search. |
| `--page <number>` | `-p` | Yes | -- | Page number to search for across all tablespace files. |
| `--checksum <value>` | `-c` | No | -- | Only match pages with this stored checksum value. |
| `--space-id <id>` | `-s` | No | -- | Only match pages in this tablespace (by space ID). |
| `--first` | -- | No | Off | Stop after the first match for faster lookups. |
| `--json` | -- | No | Off | Output in JSON format. |
| `--page-size <size>` | -- | No | Auto-detect | Override page size. |

## Examples

### Search for page 3 across all tablespaces

```bash
inno find -d /var/lib/mysql -p 3
```

### Search with space ID filter

```bash
inno find -d /var/lib/mysql -p 3 -s 42
```

### Stop at first match

```bash
inno find -d /var/lib/mysql -p 3 --first
```

### JSON output

```bash
inno find -d /var/lib/mysql -p 3 --json | jq '.matches'
```

### Search with checksum filter

```bash
inno find -d /var/lib/mysql -p 3 -c 2741936599
```

## Output

### Text Mode

```
Checking sakila/actor.ibd..
Checking sakila/film.ibd..
Found page 3 in sakila/actor.ibd (checksum: 2741936599, space_id: 42)

Found 1 match(es) in 2 file(s) searched.
```

If no match is found:

```
Page 3 not found in any .ibd file.
```

### JSON Mode

```json
{
  "datadir": "/var/lib/mysql",
  "target_page": 3,
  "matches": [
    {
      "file": "sakila/actor.ibd",
      "page_number": 3,
      "checksum": 2741936599,
      "space_id": 42
    }
  ],
  "files_searched": 2
}
```
