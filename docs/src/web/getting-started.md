# Getting Started

## Opening the Analyzer

Visit [ringo380.github.io/idb-utils](https://ringo380.github.io/idb-utils/) or run it locally:

```bash
cd web
npm ci
npm run dev
```

## Loading Files

**Drag and drop** an `.ibd` tablespace file or redo log file onto the drop zone, or click to open a file picker.

Supported file types:
- `.ibd` — InnoDB tablespace files
- `ib_logfile*` / `#ib_redo*` — InnoDB redo log files
- `ibdata1` — InnoDB system tablespace

## Analysis Tabs

Once a file is loaded, the analyzer provides several tabs:

### Parse
Displays the FIL header for every page in the tablespace: page number, checksum, page type, LSN, prev/next page, and space ID. A summary table shows the count of each page type.

### Pages
Deep structural analysis of page internals. For INDEX pages, shows the B+Tree level, record count, and index ID. For UNDO pages, shows the segment state and log type. For BLOB pages, shows the data length and chain pointers.

### Checksum
Validates every page's checksum and LSN consistency. Shows per-page status (valid/invalid), the algorithm used (CRC-32C, legacy InnoDB, or MariaDB full_crc32), and stored vs calculated values.

### Hex Dump
Raw hex dump of any page in the tablespace. Select a page number to view offset/hex/ASCII output with 16 bytes per line.

### SDI
Extracts Serialized Dictionary Information from MySQL 8.0+ tablespaces. Shows the table and tablespace definitions as formatted JSON, including column types, indexes, and partitions.

### Recovery
Assesses page-level data recoverability. Classifies each page as intact, corrupt, or empty, and counts recoverable user records on INDEX pages.

### Diff
Compare two tablespace files. Load a second file to see which pages are identical, modified, or only present in one file.

### Redo Log
For redo log files, displays the file header (creator string, start LSN), checkpoint records, and data block details with checksum validation.

## Tips

- The page type summary in the Parse tab gives a quick overview of tablespace composition
- Use the Checksum tab to verify data integrity after backups or file transfers
- SDI extraction works on any MySQL 8.0+ `.ibd` file without needing a running MySQL instance
- For large files, analysis may take a few seconds depending on your browser
