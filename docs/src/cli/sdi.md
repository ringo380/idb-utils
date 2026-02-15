# inno sdi

Extract SDI (Serialized Dictionary Information) metadata from MySQL 8.0+ tablespaces.

## Synopsis

```
inno sdi -f <file> [--pretty] [--page-size <size>] [--keyring <path>]
```

## Description

Locates SDI pages in a tablespace by scanning for page type 17853 (`FIL_PAGE_SDI`), then reassembles multi-page SDI records by following the page chain. The zlib-compressed payload is decompressed and printed as JSON.

Each tablespace in MySQL 8.0+ embeds its own table, column, and index definitions as SDI records, eliminating the need for the `.frm` files used in MySQL 5.x. This command extracts that embedded metadata for inspection, backup verification, or schema reconstruction.

SDI records contain:
- **Table SDI** (type 1): Full table definition including column names, types, charset, default values, and index definitions.
- **Tablespace SDI** (type 2): Tablespace properties.

Use `--pretty` for indented JSON output. Without it, the raw JSON string from the SDI record is printed as-is.

MariaDB tablespaces do not use SDI. Running this command against a MariaDB tablespace will produce an error message explaining that MariaDB stores metadata differently.

## Flags

| Flag | Short | Required | Default | Description |
|------|-------|----------|---------|-------------|
| `--file <path>` | `-f` | Yes | -- | Path to the InnoDB data file (.ibd). |
| `--pretty` | `-p` | No | Off | Pretty-print the extracted JSON with indentation. |
| `--page-size <size>` | -- | No | Auto-detect | Override page size. |
| `--keyring <path>` | -- | No | -- | Path to MySQL keyring file for decrypting encrypted tablespaces. |

## Examples

### Extract SDI metadata

```bash
inno sdi -f /var/lib/mysql/sakila/actor.ibd
```

### Pretty-print the JSON output

```bash
inno sdi -f actor.ibd --pretty
```

### Pipe to jq for column extraction

```bash
inno sdi -f actor.ibd --pretty | jq '.dd_object.columns[].name'
```

### Extract SDI from an encrypted tablespace

```bash
inno sdi -f encrypted_table.ibd --keyring /path/to/keyring --pretty
```

## Output

```
Found 1 SDI page(s): [3]

=== SDI Record: type=1 (Table), id=373
Compressed: 482 bytes, Uncompressed: 2048 bytes
{
  "mysqld_version_id": 80400,
  "dd_version": 80300,
  "sdi_version": 80019,
  "dd_object_type": "Table",
  "dd_object": {
    "name": "actor",
    "columns": [
      { "name": "actor_id", "type": 4, ... },
      { "name": "first_name", "type": 16, ... },
      { "name": "last_name", "type": 16, ... },
      { "name": "last_update", "type": 18, ... }
    ],
    "indexes": [
      { "name": "PRIMARY", "type": 1, ... },
      { "name": "idx_actor_last_name", "type": 2, ... }
    ],
    ...
  }
}

Total SDI records: 1
```

If the tablespace has no SDI pages (e.g., a pre-MySQL 8.0 file):

```
No SDI pages found in actor.ibd.
SDI is only available in MySQL 8.0+ tablespaces.
```
