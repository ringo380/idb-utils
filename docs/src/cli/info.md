# inno info

Inspect InnoDB system files, compare LSNs, or query a live MySQL instance.

## Synopsis

```
inno info [--ibdata] [--lsn-check] [-d <datadir>] [-D <database> -t <table>] [--host <host>] [--port <port>] [--user <user>] [--password <pass>] [--defaults-file <path>] [--json] [--page-size <size>]
```

## Description

Operates in three mutually exclusive modes:

### ibdata1 inspection (`--ibdata`)

Reads page 0 of `ibdata1` (the system tablespace) and decodes its FIL header -- checksum, page type, LSN, flush LSN, and space ID. Also attempts to read checkpoint LSNs from the redo log, trying the MySQL 8.0.30+ `#innodb_redo/#ib_redo*` directory first, then falling back to the legacy `ib_logfile0`.

This gives a quick snapshot of the system tablespace state without starting MySQL.

### LSN consistency check (`--lsn-check`)

Compares the LSN from the `ibdata1` page 0 header with the latest redo log checkpoint LSN. If they match, the system is "in sync" and InnoDB shut down cleanly. If not, the difference in bytes is reported, indicating that crash recovery may be needed.

### MySQL query mode (`-D <database> -t <table>`)

Requires the `mysql` feature (`cargo build --features mysql`).

Connects to a live MySQL instance and queries `INFORMATION_SCHEMA.INNODB_TABLES` and `INNODB_INDEXES` for the space ID, table ID, index names, and root page numbers. Also parses `SHOW ENGINE INNODB STATUS` for the current log sequence number and transaction ID counter.

Connection parameters come from CLI flags or a `.my.cnf` defaults file. CLI flags override defaults file values.

## Flags

| Flag | Short | Required | Default | Description |
|------|-------|----------|---------|-------------|
| `--ibdata` | -- | No | -- | Inspect ibdata1 page 0 header and redo log checkpoints. |
| `--lsn-check` | -- | No | -- | Compare ibdata1 and redo log LSNs for sync status. |
| `--datadir <path>` | `-d` | No | `/var/lib/mysql` | MySQL data directory path. Used by `--ibdata` and `--lsn-check`. |
| `--database <name>` | `-D` | No | -- | Database name for MySQL query mode. |
| `--table <name>` | `-t` | No | -- | Table name for MySQL query mode. |
| `--host <host>` | -- | No | `localhost` | MySQL host for live queries. |
| `--port <port>` | -- | No | `3306` | MySQL port for live queries. |
| `--user <user>` | -- | No | `root` | MySQL user for live queries. |
| `--password <pass>` | -- | No | -- | MySQL password for live queries. |
| `--defaults-file <path>` | -- | No | Auto-detect `.my.cnf` | Path to a MySQL defaults file. |
| `--json` | -- | No | Off | Output in JSON format. |
| `--page-size <size>` | -- | No | Auto-detect | Override page size for ibdata1 reading. |

## Examples

### Inspect ibdata1 header

```bash
inno info --ibdata -d /var/lib/mysql
```

### Check LSN sync status

```bash
inno info --lsn-check -d /var/lib/mysql
```

### Query table info from a live MySQL instance

```bash
inno info -D sakila -t actor --host 127.0.0.1 --user root --password secret
```

### Use a defaults file for MySQL connection

```bash
inno info -D sakila -t actor --defaults-file ~/.my.cnf
```

### JSON output for ibdata1 inspection

```bash
inno info --ibdata -d /var/lib/mysql --json
```

### Scripted sync check

```bash
if inno info --lsn-check -d /var/lib/mysql --json | jq -e '.in_sync'; then
    echo "InnoDB is in sync"
else
    echo "InnoDB may need crash recovery"
fi
```

## Output

### ibdata1 Inspection (Text Mode)

```
ibdata1 Page 0 Header
  File:       /var/lib/mysql/ibdata1
  Checksum:   2741936599
  Page No:    0
  Page Type:  8 (FSP_HDR)
  LSN:        19218432
  Flush LSN:  19218432
  Space ID:   0

Redo Log Checkpoint 1 LSN: 19218432
Redo Log Checkpoint 2 LSN: 19218000
```

### ibdata1 Inspection (JSON Mode)

```json
{
  "ibdata_file": "/var/lib/mysql/ibdata1",
  "page_checksum": 2741936599,
  "page_number": 0,
  "page_type": 8,
  "lsn": 19218432,
  "flush_lsn": 19218432,
  "space_id": 0,
  "redo_checkpoint_1_lsn": 19218432,
  "redo_checkpoint_2_lsn": 19218000
}
```

### LSN Check (Text Mode)

```
LSN Sync Check
  ibdata1 LSN:          19218432
  Redo checkpoint LSN:  19218432
  Status: IN SYNC
```

If out of sync:

```
LSN Sync Check
  ibdata1 LSN:          19218432
  Redo checkpoint LSN:  19217920
  Status: OUT OF SYNC
  Difference: 512 bytes
```

### LSN Check (JSON Mode)

```json
{
  "ibdata_lsn": 19218432,
  "redo_checkpoint_lsn": 19218432,
  "in_sync": true
}
```

### MySQL Query Mode

```
Table: sakila.actor
  Space ID:  42
  Table ID:  1234

Indexes:
  PRIMARY (index_id=157, root_page=3)
  idx_actor_last_name (index_id=158, root_page=4)

InnoDB Status:
  Log sequence number 19218432
  Log flushed up to   19218432
  Trx id counter 12345
```
