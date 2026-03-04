# LOB Chain Traversal

This guide explains how to inspect Large Object (LOB/BLOB) storage chains in InnoDB tablespaces using the `--lob-chain` flag on `inno pages`.

## When to Use

- **Diagnosing LOB corruption** — verify chain integrity when BLOB data reads fail
- **Storage analysis** — understand how much space LOB data consumes across pages
- **Version investigation** — examine LOB version chains for MVCC-related issues
- **Recovery assessment** — determine which LOB pages are intact before attempting recovery

## LOB Storage in InnoDB

InnoDB stores large values (TEXT, BLOB, JSON, long VARCHAR) externally when they exceed the inline threshold. There are two storage formats:

### Old-Style BLOB Chains (Pre-8.0.12)

Simple linked list of BLOB pages (type `FIL_PAGE_TYPE_BLOB`):

```text
INDEX page record ──▶ BLOB page 1 ──▶ BLOB page 2 ──▶ BLOB page 3
                      (first page)     (next page)     (next page)
```

### New-Style LOB (MySQL 8.0.12+)

Three-level structure with index entries for partial updates:

```text
INDEX page record ──▶ LOB_FIRST page
                      ├── LOB index entries (60 bytes each)
                      │   ├── entry 1 ──▶ LOB_DATA page
                      │   ├── entry 2 ──▶ LOB_DATA page
                      │   └── entry 3 ──▶ LOB_DATA page
                      └── LOB_INDEX pages (overflow index entries)
```

Compressed LOBs use ZLOB variants (`ZLOB_FIRST`, `ZLOB_DATA`, `ZLOB_FRAG`, `ZLOB_FRAG_ENTRY`, `ZLOB_INDEX`).

## Using --lob-chain

```bash
# Show LOB chain info for all LOB pages
inno pages -f table.ibd --lob-chain

# Focus on a specific LOB first page
inno pages -f table.ibd -p 42 --lob-chain

# JSON output with chain details
inno pages -f table.ibd --lob-chain --json
```

When `--lob-chain` is enabled, LOB-type pages (Blob, ZBlob, LobFirst, LobData, LobIndex, ZlobFirst, ZlobData, ZlobIndex, ZlobFrag, ZlobFragEntry) display additional chain traversal information.

## Chain Output

For each LOB chain origin (BLOB first page or LOB_FIRST page), the output includes:

| Field | Description |
|-------|-------------|
| Chain type | `old_blob`, `lob`, or `zlob` |
| First page | Page number of the chain start |
| Total data length | Sum of data across all pages in the chain |
| Page count | Number of pages in the chain |
| Pages | List of page numbers with per-page data lengths |

## Identifying LOB Pages

Use page type filtering to find LOB pages:

```bash
# List all LOB-related pages
inno pages -f table.ibd -t Blob
inno pages -f table.ibd -t LobFirst
inno pages -f table.ibd -t LobData
```

## Diagnosing Broken Chains

If a LOB chain is broken (missing page, corrupt pointer), the chain traversal will stop at the break point and report the last valid page. Compare the `total_data_length` against the expected column size to assess data loss.

```bash
inno pages -f table.ibd --lob-chain --json | jq '.[] | select(.lob_chain.page_count < 2)'
```
