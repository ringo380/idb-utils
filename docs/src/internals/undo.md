# Undo Log Structure

InnoDB undo logs store previous versions of modified records, enabling MVCC (multi-version concurrency control) and transaction rollback. Understanding the on-disk undo format is essential for forensic analysis and data recovery.

## Tablespace Layout

Since MySQL 8.0, undo logs live in dedicated undo tablespaces (`.ibu` files) rather than the system tablespace. Each undo tablespace begins with an **RSEG Array** page (page 0) that holds an array of rollback segment page numbers.

```text
Undo tablespace (.ibu)
+-----------------------------+
| Page 0: RSEG Array          |  Array of rollback segment page numbers
+-----------------------------+
| Page N: Rollback Segment    |  Up to 128 RSEG headers per tablespace
| Page M: Rollback Segment    |
+-----------------------------+
| Page X: Undo Log pages      |  Actual undo records (type FIL_PAGE_UNDO_LOG)
+-----------------------------+
```

## Rollback Segments

Each rollback segment header page contains:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 38+0 | 4 | max_size | Maximum number of undo pages this segment can use |
| 38+4 | 4 | history_size | Number of committed transactions in the history list |
| 38+24 | 4096 | slots[1024] | Array of 1024 undo segment page numbers (FIL_NULL = empty) |

Each non-empty slot points to the first page of an undo segment -- the page where undo records for a single transaction are written.

## Undo Page Structure

Every `FIL_PAGE_UNDO_LOG` page (type 2) has three headers stacked at the start of the page body:

```text
+-------------------------------+  byte 0
| FIL Header (38 bytes)         |
+-------------------------------+  byte 38
| Undo Page Header (18 bytes)   |  page type (INSERT/UPDATE), start/free offsets
+-------------------------------+  byte 56
| Undo Segment Header (30 bytes)|  state, last log offset (first page of segment only)
+-------------------------------+  byte 86
| Undo Log Header (34 bytes)    |  trx_id, trx_no, del_marks, table_id
+-------------------------------+  byte 120+
| Undo Records                  |  variable-length records
+-------------------------------+
```

### Undo Page Header

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 38+0 | 2 | page_type | 1 = INSERT, 2 = UPDATE |
| 38+2 | 2 | start | Offset of the first undo record |
| 38+4 | 2 | free | Offset of the first free byte |

INSERT undo logs contain only insert undo records and can be discarded immediately after transaction commit. UPDATE undo logs contain update and delete undo records and must be retained until the purge system processes them.

### Undo Segment Header

Present only on the first page of each undo segment:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 56+0 | 2 | state | Segment state (see below) |
| 56+2 | 2 | last_log | Offset of the most recent undo log header |

Segment states:

| Value | State | Meaning |
|-------|-------|---------|
| 1 | ACTIVE | Transaction is still running |
| 2 | CACHED | Segment is cached for reuse |
| 3 | TO_FREE | Insert undo, safe to free after purge |
| 4 | TO_PURGE | Update undo, retained for MVCC reads |
| 5 | PREPARED | Two-phase commit, prepared but not yet committed |

### Undo Log Header

Each undo log within a segment starts with a 34-byte header:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | trx_id | Transaction ID that created this undo log |
| 8 | 8 | trx_no | Transaction serial number (commit order) |
| 16 | 2 | del_marks | Non-zero if delete-mark records exist |
| 18 | 2 | log_start | Offset of the first undo record |
| 20 | 1 | xid_exists | Non-zero if XID info follows (distributed transactions) |
| 21 | 1 | dict_trans | Non-zero if this is a DDL transaction |
| 22 | 8 | table_id | Table ID (for insert undo logs) |
| 30 | 2 | next_log | Offset of the next undo log header (0 if last) |
| 32 | 2 | prev_log | Offset of the previous undo log header (0 if first) |

## Undo Record Types

Each undo record begins with a type byte that encodes the operation:

| Type | Name | Description |
|------|------|-------------|
| 11 | INSERT_REC | Undo for an INSERT -- stores the inserted PK for rollback deletion |
| 12 | UPD_EXIST_REC | Undo for an in-place UPDATE -- stores old column values |
| 13 | UPD_DEL_REC | Undo for an UPDATE that was implemented as delete + insert |
| 14 | DEL_MARK_REC | Undo for a DELETE -- stores the delete-marked record's old values |

The type byte also encodes a "compiled" flag (bit 4) and an "extern" flag (bit 5) indicating whether external LOB references are present.

## Field Order Gotcha

The field layout within an undo record depends on the record type, and getting this wrong is a common source of parsing bugs.

**INSERT_REC** records have a straightforward layout:

```text
[type_cmpl | table_id (compressed) | PK fields...]
```

**Modify-type records** (UPD_EXIST_REC, UPD_DEL_REC, DEL_MARK_REC) place transaction metadata **before** the primary key:

```text
[type_cmpl | table_id (compressed) | trx_id (6 bytes, fixed BE) | roll_ptr (7 bytes) | update_vector... | PK fields...]
```

Key details:
- `trx_id` is a **fixed 6-byte big-endian** value (written by `mach_write_to_6` in MySQL source), not InnoDB compressed format
- `roll_ptr` is always 7 bytes
- The update vector (changed column count + old values) comes before the PK fields
- `table_id` uses InnoDB's variable-length compressed integer encoding

This ordering matches the MySQL source in `trx0rec.cc`.

## Undo Chain Traversal

Undo records within a page are linked via 2-byte offsets. Each record stores a pointer to the next record's offset. Walking the chain starts at the `log_start` offset from the undo log header and follows `next` pointers until reaching offset 0 or the `free` boundary.

For multi-page undo logs, the undo page header contains a `FLST_NODE` linking pages in a doubly-linked list owned by the undo segment.

## Source Reference

- Undo page parsing: `src/innodb/undo.rs` -- `UndoPageHeader`, `UndoSegmentHeader`, `UndoLogHeader`, `RollbackSegmentHeader`
- Undo CLI: `src/cli/undo.rs` -- `inno undo` subcommand
- Detailed undo record parsing for undelete: `src/innodb/undelete.rs` -- `DetailedUndoRecord`, `parse_undo_records()`
- MySQL source: `storage/innobase/trx/trx0undo.cc`, `trx0rec.cc`
