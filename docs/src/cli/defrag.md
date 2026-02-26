# inno defrag

Defragment a tablespace by reclaiming free space and reordering pages.

## Usage

```bash
inno defrag -f table.ibd -o table_defrag.ibd
inno defrag -f table.ibd -o table_defrag.ibd -v --json
```

## Options

| Option | Description |
|--------|-------------|
| `-f, --file` | Path to source InnoDB data file |
| `-o, --output` | Path to output file (required) |
| `-v, --verbose` | Show per-page details |
| `--json` | Output in JSON format |
| `--page-size` | Override page size |
| `--keyring` | Path to MySQL keyring file |

## Behavior

- Reads all pages from the source file
- Removes empty and corrupt pages
- Sorts INDEX pages by (index_id, level, page_number)
- Fixes prev/next chain pointers within each index group
- Renumbers pages sequentially
- Rebuilds page 0 (FSP_HDR)
- Recalculates all checksums
- Writes to a new output file (source is never modified)
