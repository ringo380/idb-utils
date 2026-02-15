# inno watch

Monitor an InnoDB tablespace file for page-level changes in real time.

## Synopsis

```text
inno watch -f <file> [-i <ms>] [-v] [--json] [--page-size <size>] [--keyring <path>]
```

## Description

Polls an InnoDB tablespace file at a configurable interval and reports which pages have been modified, added, or removed since the last poll.

Change detection is based on **LSN comparison** -- if a page's LSN changes between polls, it was modified by a write. Checksums are validated for each changed page to detect corruption during writes.

The tablespace is **re-opened each cycle** to detect file growth and avoid stale file handles. This means `inno watch` will correctly observe pages being added as the tablespace grows.

Three types of changes are reported:
- **Modified**: A page's LSN changed between polls.
- **Added**: A new page appeared (tablespace grew).
- **Removed**: A page disappeared (tablespace shrunk, which is rare).

Press **Ctrl+C** for a clean exit with a summary of total changes observed.

## Flags

| Flag | Short | Required | Default | Description |
|------|-------|----------|---------|-------------|
| `--file <path>` | `-f` | Yes | -- | Path to the InnoDB data file (.ibd). |
| `--interval <ms>` | `-i` | No | `1000` | Polling interval in milliseconds. |
| `--verbose` | `-v` | No | Off | Show per-field diffs for changed pages (old LSN, new LSN, delta). |
| `--json` | -- | No | Off | Output in NDJSON streaming format (one JSON object per line). |
| `--page-size <size>` | -- | No | Auto-detect | Override page size. |
| `--keyring <path>` | -- | No | -- | Path to MySQL keyring file for decrypting encrypted tablespaces. |

## Examples

### Watch a tablespace with default 1-second interval

```bash
inno watch -f /var/lib/mysql/sakila/actor.ibd
```

### Poll every 500ms with verbose output

```bash
inno watch -f actor.ibd -i 500 -v
```

### NDJSON streaming output for log processing

```bash
inno watch -f actor.ibd --json | tee changes.ndjson
```

### Pipe to jq for live filtering

```bash
inno watch -f actor.ibd --json | jq -c 'select(.event == "poll")'
```

## Output

### Text Mode

On startup:

```text
Watching actor.ibd (7 pages, 16384 bytes/page, MySQL)
Polling every 1000ms. Press Ctrl+C to stop.
```

When changes are detected:

```text
14:32:15  2 pages modified
  Page 3     INDEX        LSN +473  checksum valid
  Page 4     INDEX        LSN +473  checksum valid
```

With `--verbose`, full LSN values are shown:

```text
14:32:15  2 pages modified
  Page 3     INDEX        LSN 5539 -> 6012 (+473)  checksum valid
  Page 4     INDEX        LSN 5540 -> 6013 (+473)  checksum valid
```

On Ctrl+C:

```text
Stopped after 12 polls. Total page changes: 6
```

### NDJSON Mode

Each event is a single JSON line. Three event types are emitted:

**Started** (first line):

```json
{"timestamp":"2026-02-15T14:32:14.123-06:00","event":"started","pages":7,"page_size":16384,"vendor":"MySQL"}
```

**Poll** (when changes detected):

```json
{"timestamp":"2026-02-15T14:32:15.125-06:00","event":"poll","pages":7,"modified":2,"added":0,"removed":0,"changes":[{"page":3,"kind":"modified","page_type":"INDEX","old_lsn":5539,"new_lsn":6012,"lsn_delta":473,"checksum_valid":true}]}
```

**Stopped** (on Ctrl+C):

```json
{"timestamp":"2026-02-15T14:32:26.130-06:00","event":"stopped","total_changes":6,"total_polls":12}
```

If an error occurs (e.g., file deleted), an `error` event is emitted:

```json
{"timestamp":"2026-02-15T14:32:20.127-06:00","event":"error","error":"File no longer exists"}
```
