# Full-Text Search Auxiliary Tables

InnoDB implements full-text search (FTS) using a set of auxiliary tables that are stored as separate `.ibd` tablespace files. These tables maintain an inverted index mapping tokens to document IDs.

## Architecture Overview

When you create a `FULLTEXT` index on an InnoDB table, MySQL creates several auxiliary tablespace files alongside the main table's `.ibd` file:

```text
schema/
  articles.ibd                                    # Main table
  FTS_0000000000000437_CONFIG.ibd                 # FTS configuration
  FTS_0000000000000437_DELETED.ibd                # Deleted document IDs
  FTS_0000000000000437_DELETED_CACHE.ibd          # Deleted IDs (in-memory cache)
  FTS_0000000000000437_BEING_DELETED.ibd          # Documents being purged
  FTS_0000000000000437_BEING_DELETED_CACHE.ibd    # Being-purged cache
  FTS_0000000000000437_00000000000004a2_INDEX_0.ibd  # Index shard 0
  FTS_0000000000000437_00000000000004a2_INDEX_1.ibd  # Index shard 1
  ...
  FTS_0000000000000437_00000000000004a2_INDEX_5.ibd  # Index shard 5
```

The filename encodes the parent table ID and (for index shards) the index ID, both as 16-digit zero-padded hexadecimal strings.

## Auxiliary Table Types

### CONFIG

The CONFIG table stores FTS configuration parameters as key-value pairs:

| Key | Description |
|-----|-------------|
| `FTS_SYNCED_DOC_ID` | Highest document ID that has been synced to disk |
| `FTS_TOTAL_DELETED_COUNT` | Number of documents marked for deletion |
| `FTS_TOTAL_WORD_COUNT` | Total number of indexed tokens |
| `FTS_LAST_OPTIMIZED_WORD` | Last word processed by `OPTIMIZE TABLE` |

### Index Shards (INDEX_0 through INDEX_5)

The inverted index is partitioned into 6 shards based on the first character of each token. Each shard is a B+Tree table with columns:

| Column | Type | Description |
|--------|------|-------------|
| word | VARCHAR | The indexed token |
| first_doc_id | BIGINT | First document ID containing this token |
| last_doc_id | BIGINT | Last document ID containing this token |
| doc_count | INT | Number of documents containing this token |
| ilist | BLOB | Encoded list of (document_id, position) pairs |

The `ilist` column is a compressed binary format encoding document IDs and word positions within each document. This is what enables `MATCH() AGAINST()` queries with relevance ranking.

### Deletion Tracking

FTS uses a lazy deletion strategy:

1. When a row is deleted from the main table, its document ID is added to DELETED / DELETED_CACHE
2. When a row is updated, the old document ID goes to DELETED and the new version is indexed
3. The BEING_DELETED tables track documents currently being purged by the background FTS optimize thread
4. `OPTIMIZE TABLE` triggers a full merge and purge of deleted entries

## Filename Parsing

IDB Utils can identify and parse FTS auxiliary filenames to extract the table ID, index ID, and file type:

```text
FTS_<table_id_hex>_CONFIG.ibd
FTS_<table_id_hex>_DELETED.ibd
FTS_<table_id_hex>_DELETED_CACHE.ibd
FTS_<table_id_hex>_BEING_DELETED.ibd
FTS_<table_id_hex>_BEING_DELETED_CACHE.ibd
FTS_<table_id_hex>_<index_id_hex>_INDEX_<N>.ibd    (N = 0-5)
```

The `is_fts_auxiliary()` function tests whether a filename matches this pattern, and `parse_fts_filename()` extracts the structured metadata.

## FTS Behavior Notes

- FTS indexes are only available on InnoDB tables (MyISAM has a separate FTS implementation)
- The auxiliary tables are not visible through `SHOW TABLES` but exist as physical `.ibd` files
- `DROP INDEX` on a fulltext index removes all associated auxiliary files
- FTS uses its own document ID counter (`FTS_DOC_ID`), which is either an explicit column or an implicit hidden column
- `innodb_ft_cache_size` controls how much memory is used before flushing tokens to the index shards

## Inspecting FTS Tables with inno

FTS auxiliary tables are standard InnoDB tablespaces and can be inspected with any `inno` subcommand:

```bash
# Parse the FTS config table
inno pages -f FTS_0000000000000437_CONFIG.ibd

# Check integrity of an index shard
inno checksum -f FTS_0000000000000437_00000000000004a2_INDEX_0.ibd

# The audit command identifies FTS auxiliary files in directory scans
inno audit -d /var/lib/mysql/mydb/
```

## Source Reference

- FTS filename parsing: `src/innodb/fts.rs` -- `FtsFileType`, `FtsFileInfo`, `parse_fts_filename()`, `is_fts_auxiliary()`
- MySQL source: `storage/innobase/fts/fts0fts.cc`, `fts0opt.cc`, `fts0que.cc`
