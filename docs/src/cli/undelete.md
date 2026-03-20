# inno undelete

Recover deleted records from InnoDB tablespace files using multiple recovery strategies.

## Usage

```bash
# Scan for deleted records (CSV output)
inno undelete -f employees.ibd

# JSON record output
inno undelete -f employees.ibd --format json

# SQL INSERT statements
inno undelete -f employees.ibd --format sql

# Filter by minimum confidence
inno undelete -f employees.ibd --confidence 0.5

# Include undo log scanning for deeper recovery
inno undelete -f employees.ibd --undo-file undo_001.ibu

# Full metadata JSON envelope
inno undelete -f employees.ibd --json

# Filter by table name
inno undelete -f employees.ibd --table employees

# Scan a specific page only
inno undelete -f employees.ibd -p 4

# Verbose output with recovery details
inno undelete -f employees.ibd -v

# Encrypted tablespace
inno undelete -f employees.ibd --keyring /var/lib/mysql-keyring/keyring
```

## Options

| Option | Description |
|--------|-------------|
| `-f, --file` | Path to the InnoDB tablespace file (`.ibd`) |
| `--undo-file` | Path to an undo tablespace (`ibdata1` or `.ibu`) for undo log scanning |
| `--table` | Filter by table name |
| `--min-trx-id` | Minimum transaction ID to include |
| `--confidence` | Minimum confidence threshold (0.0–1.0, default: 0.0) |
| `--format` | Record output format: `csv`, `json`, `sql`, `hex` (default: `csv`) |
| `--json` | Output full metadata JSON envelope (overrides `--format`) |
| `-v, --verbose` | Show additional recovery detail |
| `-p, --page` | Recover from a specific page only |
| `--page-size` | Override page size (default: auto-detect) |
| `--keyring` | Path to MySQL keyring file for encrypted tablespaces |
| `--mmap` | Use memory-mapped I/O (faster for large files) |

## Recovery Strategies

`inno undelete` uses three complementary strategies, each with a different confidence level:

### 1. Delete-Marked Records (confidence: 1.0)

Scans INDEX pages for records with the delete mark flag set. These are records that have been `DELETE`d but not yet purged by the InnoDB background purge thread. Highest confidence because the record data is fully intact.

### 2. Free-List Records (confidence: 0.2–0.7)

Walks the page-level free list to find records that have been removed from the active record chain but still exist in page memory. Confidence varies based on how much of the record structure is intact.

### 3. Undo Log Records (confidence: 0.1–0.3)

When `--undo-file` is provided, parses undo log entries to reconstruct before-images of modified or deleted rows. Lower confidence because undo records may be incomplete or partially overwritten.

## Output Formats

### CSV (default)

Comma-separated values with column headers. Suitable for import into spreadsheets or databases.

### JSON

One JSON object per record with column names as keys.

### SQL

`INSERT INTO` statements that can be executed directly against a MySQL table to restore records.

### Hex

Raw hex dump of recovered record bytes for manual inspection.

### JSON Envelope (`--json`)

Full metadata output including scan summary, recovery statistics per strategy, and all recovered records with confidence scores and source information.

```json
{
  "table_name": "employees",
  "strategies_used": ["delete_marked", "free_list"],
  "summary": {
    "total_records": 15,
    "by_source": {
      "delete_marked": 10,
      "free_list": 5
    }
  },
  "records": [
    {
      "source": "delete_marked",
      "confidence": 1.0,
      "page_number": 4,
      "columns": { "id": 42, "name": "John Doe", ... }
    }
  ]
}
```

## Background

When a row is deleted in InnoDB, it is not immediately removed from the tablespace. Instead, the record is delete-marked and later purged by a background thread. Until purge completes, the record data remains on the page and can be recovered with high confidence.

Even after purge, record data may persist in page free space or in undo log entries. The `inno undelete` subcommand combines all three recovery approaches to maximize the number of recoverable records.

Use `--confidence` to filter results by reliability — setting `--confidence 0.5` excludes low-confidence free-list and undo log recoveries, returning only records that are likely intact.
