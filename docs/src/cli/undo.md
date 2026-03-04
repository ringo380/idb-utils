# inno undo

Analyze undo tablespace files (`.ibu` or `.ibd`) for rollback segment and transaction history.

## Usage

```bash
# Text summary
inno undo -f undo_001.ibu

# JSON output
inno undo -f undo_001.ibu --json

# Specific undo page
inno undo -f undo_001.ibu -p 3

# Verbose with undo records
inno undo -f undo_001.ibu -v

# Encrypted tablespace
inno undo -f undo_001.ibu --keyring /var/lib/mysql-keyring/keyring
```

## Options

| Option | Description |
|--------|-------------|
| `-f, --file` | Path to InnoDB undo tablespace file (`.ibu` or `.ibd`) |
| `-p, --page` | Show a specific undo page only |
| `-v, --verbose` | Show additional detail including undo records |
| `--json` | Output in JSON format |
| `--page-size` | Override page size (default: auto-detect) |
| `--keyring` | Path to MySQL keyring file for encrypted tablespaces |
| `--mmap` | Use memory-mapped I/O (faster for large files) |

## Output

### Text Mode

Text output shows three sections:

1. **RSEG Array** — rollback segment slots from the RSEG array header page, including which slots are active vs empty.

2. **Segment Summary** — per-segment state (Active, Cached, ToPurge, ToFree, Prepared), transaction count, and undo log type.

3. **Transaction Listing** — undo log headers with transaction IDs, transaction numbers, type (INSERT/UPDATE), and XID presence.

### JSON Mode

Returns a structured `UndoAnalysis` object:

```json
{
  "rseg_slots": [3, 4, 5],
  "rseg_headers": [...],
  "segments": [
    {
      "page_no": 3,
      "page_header": { "type_code": 2, "start": 150, "free": 256 },
      "segment_header": { "state": "Active", "last_log": 150 },
      "log_headers": [
        {
          "trx_id": 12345,
          "trx_no": 12344,
          "del_marks": false,
          "log_start": 176,
          "xid_exists": true,
          "dict_trans": false
        }
      ],
      "record_count": 8
    }
  ],
  "total_transactions": 42,
  "active_transactions": 3
}
```

## Undo Segment States

| State | Description |
|-------|-------------|
| Active | Currently in use by an active transaction |
| Cached | Available for reuse, contains committed history |
| ToPurge | Marked for purge by the background purge thread |
| ToFree | Marked for segment deallocation |
| Prepared | Part of an XA transaction in the PREPARED state |

## Background

MySQL 8.0+ supports dedicated undo tablespaces (`.ibu` files) that can be created, dropped, and truncated independently. Each undo tablespace contains a rollback segment (RSEG) array header page that points to up to 128 rollback segments, each of which manages up to 1024 undo log slots.

The `inno undo` subcommand reads the RSEG array, follows the slot pointers to rollback segment header pages, then walks undo segment pages to extract log headers and (with `-v`) individual undo records.
