# LOB/BLOB Chain Formats

When a column value exceeds the space available on an INDEX page, InnoDB stores the overflow data on dedicated BLOB or LOB pages. Understanding these formats is necessary for inspecting externally stored columns and tracing data chains across pages.

## When Does InnoDB Use External Storage?

InnoDB stores column data externally when:

- A row's total size exceeds roughly half the page size (keeping at least two rows per page for B+Tree efficiency)
- The column is a large `BLOB`, `TEXT`, `JSON`, or `LONGBLOB` value
- The row format is DYNAMIC or COMPRESSED (MySQL 5.7+), which stores only a 20-byte external pointer on the INDEX page

The INDEX page record contains a 20-byte **BLOB reference** (also called an "extern field ref") pointing to the first overflow page:

```text
BLOB Reference (20 bytes on INDEX page):
+----------+---------+----------+--------+
| space_id | page_no | offset   | length |
| 4 bytes  | 4 bytes | 4 bytes  | 8 bytes|
+----------+---------+----------+--------+
```

## Old-Style BLOB Pages (Pre-8.0)

Three page types handle old-style externally stored data:

| Type | Value | Description |
|------|-------|-------------|
| BLOB | 10 | Uncompressed overflow data |
| ZBLOB | 11 | First page of a compressed BLOB chain |
| ZBLOB2 | 12 | Subsequent pages of a compressed BLOB chain |

### BLOB Page Header (8 bytes)

Each old-style overflow page has a simple header at `FIL_PAGE_DATA` (byte 38):

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | part_len | Number of data bytes stored on this page |
| 4 | 4 | next_page_no | Next overflow page number (FIL_NULL = last page) |

The data immediately follows the 8-byte header. With the default 16K page size, each BLOB page carries up to 16,338 bytes of payload (16384 - 38 header - 8 BLOB header).

### Chain Traversal

Old-style BLOB chains are singly linked lists:

```text
INDEX page          BLOB page 5        BLOB page 12       BLOB page 20
+------------+      +------------+      +------------+      +------------+
| extern ref |----->| part_len   |      | part_len   |      | part_len   |
| page_no=5  |      | next=12    |----->| next=20    |----->| next=NULL  |
+------------+      | data...    |      | data...    |      | data...    |
                    +------------+      +------------+      +------------+
```

Walk the chain by reading `next_page_no` until it equals `FIL_NULL` (0xFFFFFFFF).

## New-Style LOB Pages (MySQL 8.0+)

MySQL 8.0 introduced a richer LOB format with MVCC support at the LOB level. This format uses four new page types:

| Type | Value | Description |
|------|-------|-------------|
| LOB_FIRST | 22 | First page of an uncompressed LOB |
| LOB_DATA | 23 | Data page in an uncompressed LOB |
| LOB_INDEX | 24 | Index page linking LOB data pages |
| ZLOB_FIRST | 25 | First page of a compressed LOB |
| ZLOB_DATA | 26 | Data page in a compressed LOB |
| ZLOB_FRAG | 27 | Fragment page for small compressed LOBs |
| ZLOB_FRAG_ENTRY | 28 | Fragment entry index page |
| ZLOB_INDEX | 29 | Index page for compressed LOB |

### LOB First Page Header (12 bytes)

The first page of a new-style LOB contains metadata at `FIL_PAGE_DATA`:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 1 | version | LOB format version |
| 1 | 1 | flags | LOB flags |
| 2 | 4 | data_len | Total uncompressed data length |
| 6 | 6 | trx_id | Transaction ID that created the LOB |

After the header, the first page contains LOB index entries that describe where the data chunks reside.

### LOB Index Entries (60 bytes each)

LOB index entries form a doubly-linked list and describe individual data chunks:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 6 | prev_node | Previous entry (page_no + offset) |
| 6 | 6 | next_node | Next entry (page_no + offset) |
| 12 | 1 | versions | Number of versions of this entry |
| 14 | 6 | trx_id | Transaction that created this chunk |
| 20 | 4 | trx_undo_no | Undo record number |
| 24 | 4 | page_no | Page containing the data for this chunk |
| 28 | 4 | data_len | Length of data on the referenced page |
| 32 | 4 | lob_version | LOB version number |

This structure enables MVCC for LOBs -- different transactions can see different versions of the same LOB by following different index entry chains.

### LOB Data Page Header (11 bytes)

Each LOB_DATA page has a minimal header:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 1 | version | LOB format version |
| 1 | 4 | data_len | Bytes of data stored on this page |
| 5 | 6 | trx_id | Transaction that wrote this page |

## Compressed LOBs

Compressed LOB pages (ZLOB_FIRST, ZLOB_DATA) use the same header layout as their uncompressed counterparts. The `data_len` field in the ZLOB first page header represents the total **uncompressed** length, while the data stored on individual ZLOB_DATA pages is zlib-compressed.

ZLOB_FRAG pages handle small compressed LOBs that fit within a single page fragment, avoiding the overhead of a full LOB chain.

## Inspecting LOB Chains with inno

```bash
# Show all LOB pages in a tablespace
inno pages -f table.ibd -t blob

# Inspect LOB chain starting from a specific page
inno pages -f table.ibd --lob-chain -p 5

# JSON output for programmatic analysis
inno pages -f table.ibd --lob-chain -p 5 --json
```

The web UI Pages tab includes a LOB Chain panel that visualizes the chain when you select a LOB start page (BLOB, ZBLOB, ZBLOB2, LOB_FIRST, or ZLOB_FIRST).

## Source Reference

- LOB page parsing: `src/innodb/lob.rs` -- `BlobPageHeader`, `LobFirstPageHeader`, `LobDataPageHeader`, `LobIndexEntry`
- LOB chain traversal: `src/innodb/lob.rs` -- `walk_blob_chain()`, `walk_lob_chain()`
- WASM LOB chain: `src/wasm.rs` -- `analyze_lob_chain()`
- MySQL source: `storage/innobase/lob/lob0lob.cc`, `lob0first.cc`
