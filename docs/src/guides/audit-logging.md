# Audit Logging

IDB Utils provides structured audit logging for all write operations. Every page write, backup creation, and repair operation can be recorded as NDJSON events for compliance, forensics, and operational traceability.

## Quick Start

Enable audit logging with the global `--audit-log` flag:

```bash
inno repair -f table.ibd --audit-log /var/log/inno-audit.jsonl
```

All write operations in that session are recorded to the specified file.

## Supported Operations

Audit logging is available on all subcommands that modify tablespace files:

| Subcommand | Events Logged |
|------------|---------------|
| `inno repair` | `page_write` per repaired page, `backup_created` |
| `inno repair --batch` | Same as above, per file in the batch |
| `inno corrupt` | `page_write` per corrupted page |
| `inno defrag` | `file_write` for the output file |
| `inno transplant` | `page_write` per transplanted page, `backup_created` |

## Event Format

Each event is a single JSON line (NDJSON format):

```json
{"timestamp":"2025-06-15T13:30:45.123Z","event":"session_start","session_id":"abc123","args":["inno","repair","-f","table.ibd"]}
{"timestamp":"2025-06-15T13:30:45.200Z","event":"backup_created","session_id":"abc123","path":"table.ibd.bak"}
{"timestamp":"2025-06-15T13:30:45.300Z","event":"page_write","session_id":"abc123","file":"table.ibd","page":5,"algorithm":"crc32c"}
{"timestamp":"2025-06-15T13:30:45.400Z","event":"session_end","session_id":"abc123"}
```

## Event Types

| Event | Description | Fields |
|-------|-------------|--------|
| `session_start` | Beginning of a write session | `session_id`, `args` |
| `page_write` | A page was modified | `session_id`, `file`, `page`, `algorithm` |
| `file_write` | A new file was created | `session_id`, `path` |
| `backup_created` | A backup file was created | `session_id`, `path` |
| `session_end` | End of a write session | `session_id` |

## Batch Repair with Audit

The `--batch` mode repairs all `.ibd` files under a directory, with full audit logging:

```bash
inno repair --batch /var/lib/mysql --audit-log /var/log/repair-audit.jsonl
```

Each file is processed in parallel, and all page writes are logged with the correct file path.

## File Locking

The audit log file uses `fs2` file-level locking to prevent corruption when multiple `inno` processes write to the same audit log simultaneously. This is safe for concurrent batch operations.

## Integration

### Parsing Audit Logs

Since the format is NDJSON, standard tools work:

```bash
# Count page writes
grep '"page_write"' /var/log/inno-audit.jsonl | wc -l

# Extract all modified files
grep '"page_write"' /var/log/inno-audit.jsonl | jq -r .file | sort -u

# Filter by session
grep '"abc123"' /var/log/inno-audit.jsonl | jq .
```

### Watch Events

The `inno watch --events` flag produces similar NDJSON output for real-time monitoring:

```bash
inno watch -f table.ibd --events
```

Events include `watch_start`, `page_change`, `watch_error`, and `watch_stop`.
