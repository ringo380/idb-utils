# JSON Output

Every `inno` subcommand supports `--json` for structured, machine-readable output. This makes it straightforward to integrate with scripts, CI pipelines, and tools like `jq`.

## Examples with jq

### Filter Pages by Type

```bash
inno parse -f users.ibd --json | jq '[.[] | select(.page_type == "INDEX")]'
```

### Checksum Validation in CI

```bash
if ! inno checksum -f users.ibd --json | jq -e '.invalid_pages == 0' > /dev/null; then
  echo "Checksum validation failed"
  exit 1
fi
```

### Extract Column Names from SDI

```bash
inno sdi -f users.ibd | jq -r '.dd_object.columns[].name'
```

### Count Pages by Type

```bash
inno parse -f users.ibd --json | jq 'group_by(.page_type) | map({type: .[0].page_type, count: length})'
```

### Monitor Changes as NDJSON Stream

```bash
inno watch -f users.ibd --json | jq -c 'select(.event == "poll")'
```

### Diff Summary

```bash
inno diff old.ibd new.ibd --json | jq '{modified: .modified, identical: .identical}'
```

### Recovery Assessment

```bash
inno recover -f damaged.ibd --json | jq '.summary'
```

### Find Tablespace by Space ID

```bash
inno find -d /var/lib/mysql -p 0 -s 42 --json | jq -r '.[].file'
```

### Redo Log Creator String

```bash
inno log -f ib_logfile0 --json | jq -r '.header.creator'
```

## Output Structure

Each subcommand produces a consistent JSON structure. Below are representative examples.

### Checksum Output

```json
{
  "file": "users.ibd",
  "page_size": 16384,
  "total_pages": 100,
  "empty_pages": 10,
  "valid_pages": 88,
  "invalid_pages": 2,
  "lsn_mismatches": 0,
  "pages": [
    {
      "page_number": 0,
      "page_type": "FSP_HDR",
      "stored_checksum": 3456789012,
      "computed_crc32c": 3456789012,
      "computed_legacy": 1234567890,
      "valid": true
    }
  ]
}
```

### Parse Output

```json
[
  {
    "page_number": 0,
    "page_type": "FSP_HDR",
    "space_id": 42,
    "lsn": 12345678,
    "checksum": 3456789012
  },
  {
    "page_number": 1,
    "page_type": "IBUF_BITMAP",
    "space_id": 42,
    "lsn": 12345678,
    "checksum": 2345678901
  }
]
```

### Diff Output

```json
{
  "file1": "old.ibd",
  "file2": "new.ibd",
  "page_size": 16384,
  "total_pages": 100,
  "identical": 95,
  "modified": 5,
  "differences": [
    {
      "page_number": 3,
      "page_type": "INDEX",
      "lsn1": 12345678,
      "lsn2": 12345999
    }
  ]
}
```

## Tips

- All JSON output goes to stdout. Progress bars, warnings, and verbose messages go to stderr, so piping to `jq` works without interference.
- The `watch` subcommand with `--json` emits newline-delimited JSON (NDJSON), one object per poll cycle. Use `jq -c` for compact processing.
- Optional fields are omitted from JSON output rather than set to null. Use `jq`'s `//` operator to provide defaults: `.field // "default"`.
- Combine `--json` with `-p` (page number) to get output for a single page.
