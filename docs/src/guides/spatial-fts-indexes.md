# Spatial and Full-Text Indexes

This guide covers inspection and visualization of InnoDB R-tree spatial indexes and full-text search (FTS) auxiliary tables.

## Spatial Indexes (R-tree)

### Background

InnoDB spatial indexes use R-tree data structures to index geometry columns (POINT, LINESTRING, POLYGON, etc.). R-tree pages have the same basic structure as B+Tree INDEX pages but store Minimum Bounding Rectangles (MBRs) instead of key values.

Each MBR consists of four 64-bit floats (big-endian):

```text
┌─────────┬─────────┬─────────┬─────────┐
│  min_x  │  min_y  │  max_x  │  max_y  │
│ 8 bytes │ 8 bytes │ 8 bytes │ 8 bytes │
└─────────┴─────────┴─────────┴─────────┘
```

### CLI Inspection

R-tree pages appear in `inno pages` output with additional MBR detail:

```bash
# Show all pages including R-tree
inno pages -f spatial_table.ibd

# Focus on a specific R-tree page
inno pages -f spatial_table.ibd -p 4 -v
```

R-tree page output includes the tree level, record count, and enclosing MBR for the page.

### Health Metrics

The `inno health` subcommand reports spatial index metrics alongside B+Tree indexes:

```bash
inno health -f spatial_table.ibd --json | jq '.indexes[] | select(.index_type == "RTREE")'
```

### Web UI Visualization

The web analyzer includes a Spatial tab (keyboard shortcut: **S**) that provides:

- **Summary cards** — R-tree page count, tree levels, leaf pages, total MBRs
- **Spatial extent** — bounding box coordinates covering all MBRs
- **Canvas visualization** — interactive MBR rectangle rendering with:
  - Color-coded rectangles (hue varies by MBR index)
  - Coordinate grid with axis labels
  - Tree level selector for multi-level indexes
- **Page summary table** — per-page level, record count, MBR count, and enclosing MBR

## Full-Text Indexes (FTS)

### Background

InnoDB full-text indexes are implemented using auxiliary tables stored as separate `.ibd` files in the same schema directory. Each FTS index creates multiple auxiliary files:

| File Pattern | Purpose |
|-------------|---------|
| `FTS_<table_id>_CONFIG.ibd` | FTS configuration and state |
| `FTS_<table_id>_<index_id>_INDEX_<N>.ibd` | Inverted index partitions (0-5) |
| `FTS_<table_id>_DELETE.ibd` | Deleted document IDs |
| `FTS_<table_id>_BEING_DELETED.ibd` | Documents being removed from index |
| `FTS_<table_id>_DELETE_CACHE.ibd` | Cached deletes pending merge |
| `FTS_<table_id>_BEING_DELETED_CACHE.ibd` | Cached being-deleted entries |

### Detection

IDB Utils can detect FTS auxiliary files by filename pattern:

```bash
# Audit a data directory to identify FTS tables
inno audit -d /var/lib/mysql/mydb --health --json | jq '.files[] | select(.name | startswith("FTS_"))'
```

### FTS File Analysis

Each FTS auxiliary file is a regular InnoDB tablespace and can be inspected with any subcommand:

```bash
# Check FTS config table
inno pages -f /var/lib/mysql/mydb/FTS_0000000000000437_CONFIG.ibd

# Verify FTS index partition integrity
inno checksum -f /var/lib/mysql/mydb/FTS_0000000000000437_00000000000004a2_INDEX_1.ibd

# Health of FTS inverted index
inno health -f /var/lib/mysql/mydb/FTS_0000000000000437_00000000000004a2_INDEX_0.ibd
```

### FTS in Health Reports

When running health analysis, FTS auxiliary files are identified and summarized:

```bash
inno health -f table.ibd --json
```

The health report includes an `fts_info` section when FTS auxiliary tables are detected, showing:
- Table ID and associated index count
- Whether CONFIG and DELETE tables exist
- Index partition coverage (expected: 6 partitions numbered 0-5)

## Combining Spatial and FTS Analysis

For tables with both spatial and full-text indexes:

```bash
# Full health picture
inno health -f geosearch_table.ibd -v

# Audit entire schema
inno audit -d /var/lib/mysql/mydb --health
```

The audit subcommand automatically identifies R-tree pages within tablespaces and FTS auxiliary files in the directory, providing a comprehensive view of all index types.
