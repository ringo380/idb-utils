# inno pages

Detailed page structure analysis for InnoDB tablespace files.

## Synopsis

```text
inno pages -f <file> [-p <page>] [-v] [-e] [-l] [-t <type>] [--json] [--page-size <size>] [--keyring <path>]
```

## Description

Goes beyond FIL headers to decode the internal structure of each page type. Unlike `inno parse`, which only reads FIL headers, this command dives into page-type-specific fields:

- **INDEX pages** (type 17855): Decodes the index header (index ID, B+Tree level, record counts, heap top, garbage bytes, insert direction), FSEG inode pointers for leaf and non-leaf segments, and infimum/supremum system records.
- **UNDO pages** (type 2): Shows the undo page header (type, start/free offsets, used bytes) and segment header (state, last log offset).
- **BLOB/ZBLOB pages** (types 10, 11, 12): Shows data length and next-page chain pointer for old-style externally stored columns.
- **LOB_FIRST pages** (MySQL 8.0+): Shows version, flags, total data length, and transaction ID for new-style LOB first pages.
- **Page 0** (FSP_HDR): Shows extended FSP header fields including compression algorithm, encryption flags, vendor detection, and first unused segment ID.

In **list mode** (`-l`), output is a compact one-line-per-page summary showing page number, type, description, index ID (for INDEX pages), and byte offset. In **detail mode** (the default), each page gets a full multi-section breakdown.

## Flags

| Flag | Short | Required | Default | Description |
|------|-------|----------|---------|-------------|
| `--file <path>` | `-f` | Yes | -- | Path to the InnoDB data file (.ibd). |
| `--page <number>` | `-p` | No | All pages | Display a specific page number only. |
| `--verbose` | `-v` | No | Off | Show checksum status, FSEG internals, and additional detail. |
| `--show-empty` | `-e` | No | Off | Include empty/allocated pages in the output. |
| `--list` | `-l` | No | Off | Compact one-line-per-page listing mode. |
| `--type <filter>` | `-t` | No | All types | Filter output to pages matching this type name. |
| `--json` | -- | No | Off | Output in JSON format. |
| `--page-size <size>` | -- | No | Auto-detect | Override page size. |
| `--keyring <path>` | -- | No | -- | Path to MySQL keyring file for decrypting encrypted tablespaces. |

### Type Filter Values

The `-t` flag accepts these values (case-insensitive):

| Filter | Matches |
|--------|---------|
| `INDEX` | INDEX pages (B+Tree nodes) |
| `UNDO` | Undo log pages |
| `BLOB` | BLOB, ZBLOB, and ZBLOB2 pages |
| `LOB` | LOB_INDEX, LOB_DATA, and LOB_FIRST pages |
| `SDI` | SDI and SDI_BLOB pages |
| `COMPRESSED` or `COMP` | All compressed page types |
| `ENCRYPTED` or `ENC` | All encrypted page types |
| `INSTANT` | INSTANT pages |

Any other string is matched as a substring against the page type name.

## Examples

### List all pages in compact mode

```bash
inno pages -f actor.ibd -l
```

### Deep-dive into a single INDEX page

```bash
inno pages -f actor.ibd -p 3 -v
```

### Show only INDEX pages

```bash
inno pages -f actor.ibd -t INDEX
```

### Show only UNDO pages in list mode

```bash
inno pages -f actor.ibd -t UNDO -l
```

### JSON output for INDEX pages

```bash
inno pages -f actor.ibd -t INDEX --json | jq '.[].index_header'
```

## Output

### Detail Mode (default)

For each page, displays sections based on page type:

**FIL Header** (all pages): Page number, byte offset, page type, prev/next pointers, LSN, space ID, checksum.

**INDEX Header** (INDEX pages): Index ID, node level, max transaction ID, directory slots, heap top, record counts, free list start, garbage bytes, last insert position and direction.

**FSEG Header** (INDEX pages): Inode space ID, page number, and offset for leaf and non-leaf segments.

**System Records** (INDEX pages): Record status, owned records, deleted flag, heap number, infimum/supremum next-record offsets.

**BLOB Header** (BLOB/ZBLOB pages): Data length and next page number in the chain.

**LOB First Page Header** (LOB_FIRST pages): Version, flags, total data length, transaction ID.

**UNDO Header** (UNDO pages): Undo type, start offset, free offset, used bytes, segment state, last log offset.

**FSP Header** (page 0): Vendor, space ID, size, flags, free-page limit, compression algorithm, encryption info, first unused segment ID.

**FIL Trailer** (all pages): Old-style checksum, LSN low 32 bits, byte end offset. With `--verbose`, includes checksum validation status and LSN consistency.

### List Mode (`-l`)

One line per page:

```text
-- Page 3 - INDEX: B-tree Node, Index ID: 157, Byte Start: 0x0000C000
```
