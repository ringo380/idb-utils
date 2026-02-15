# inno tsid

List or look up tablespace IDs from files in a MySQL data directory.

## Synopsis

```
inno tsid -d <datadir> [-l] [-t <tsid>] [--json] [--page-size <size>]
```

## Description

Scans `.ibd` (tablespace) and `.ibu` (undo tablespace) files under a MySQL data directory, opens page 0 of each, and reads the space ID from the FSP header at offset `FIL_PAGE_DATA` (byte 38). The space ID uniquely identifies each tablespace within a MySQL instance and appears in error logs, `INFORMATION_SCHEMA.INNODB_TABLESPACES`, and the FIL header of every page.

Two modes are available:

- **List mode** (`-l`): Prints every discovered file alongside its space ID, sorted by file path. Useful for building a map of the data directory.

- **Lookup mode** (`-t <id>`): Filters results to only the file(s) with the given space ID. Useful for resolving a space ID from an error message back to a physical `.ibd` file on disk.

If neither `-l` nor `-t` is specified, all discovered tablespaces are listed (same behavior as `-l`).

## Flags

| Flag | Short | Required | Default | Description |
|------|-------|----------|---------|-------------|
| `--datadir <path>` | `-d` | Yes | -- | MySQL data directory path to scan. |
| `--list` | `-l` | No | Off | List all tablespace IDs found in the data directory. |
| `--tsid <id>` | `-t` | No | -- | Find the tablespace file with this specific space ID. |
| `--json` | -- | No | Off | Output in JSON format. |
| `--page-size <size>` | -- | No | Auto-detect | Override page size. |

## Examples

### List all tablespace IDs

```bash
inno tsid -d /var/lib/mysql -l
```

### Find which file owns space ID 42

```bash
inno tsid -d /var/lib/mysql -t 42
```

### JSON output for all tablespaces

```bash
inno tsid -d /var/lib/mysql -l --json
```

### Look up a space ID from an error log

```bash
# Error log says: "InnoDB: Error in space 42"
inno tsid -d /var/lib/mysql -t 42
```

## Output

### Text Mode

List mode:

```
sakila/actor.ibd - Space ID: 42
sakila/film.ibd - Space ID: 43
sakila/film_actor.ibd - Space ID: 44
```

Lookup mode when no match is found:

```
Tablespace ID 999 not found.
```

### JSON Mode

```json
{
  "datadir": "/var/lib/mysql",
  "tablespaces": [
    { "file": "sakila/actor.ibd", "space_id": 42 },
    { "file": "sakila/film.ibd", "space_id": 43 },
    { "file": "sakila/film_actor.ibd", "space_id": 44 }
  ]
}
```
