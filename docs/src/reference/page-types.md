# Page Types

Complete table of all InnoDB page types recognized by `inno`:

| Name | Value | Description | Usage |
|------|-------|-------------|-------|
| `ALLOCATED` | 0 | Freshly allocated | Page type field not yet initialized; appears in tablespaces with preallocated but unused extents |
| `UNDO_LOG` | 2 | Undo log | Stores previous values of modified records for MVCC and rollback |
| `INODE` | 3 | File segment inode | Bookkeeping for file segments (collections of extents belonging to an index) |
| `IBUF_FREE_LIST` | 4 | Insert buffer free list | Insert buffer free space management (system tablespace only) |
| `IBUF_BITMAP` | 5 | Insert buffer bitmap | Tracks which pages have buffered writes pending (page 1 of every tablespace) |
| `SYS` | 6 | System internal | Various system tablespace purposes (data dictionary, doublewrite buffer) |
| `TRX_SYS` | 7 | Transaction system header | Transaction system bookkeeping including rollback segment pointers (system tablespace only) |
| `FSP_HDR` | 8 | File space header | Page 0 of each tablespace; contains space ID, size, flags, and extent descriptors |
| `XDES` | 9 | Extent descriptor | Extent descriptor page for each group of 16,384 pages beyond the first |
| `BLOB` | 10 | Uncompressed BLOB | Externally stored column data for columns exceeding the inline limit |
| `ZBLOB` | 11 | First compressed BLOB | First page of a compressed externally stored column |
| `ZBLOB2` | 12 | Subsequent compressed BLOB | Continuation pages of a compressed externally stored column |
| `COMPRESSED` | 14 | Compressed page | Page stored in compressed format (requires `KEY_BLOCK_SIZE`) |
| `ENCRYPTED` | 15 | Encrypted page | Page encrypted with tablespace-level encryption (MySQL) |
| `COMPRESSED_ENCRYPTED` | 16 | Compressed + encrypted | Page that is both compressed and encrypted |
| `ENCRYPTED_RTREE` | 17 | Encrypted R-tree | Encrypted spatial index page |
| `INSTANT` / `SDI_BLOB` | 18 | Vendor-dependent | MariaDB: instant ALTER TABLE metadata; MySQL: SDI overflow data |
| `LOB_INDEX` | 20 | LOB index | Large object index page for new LOB format (MySQL 8.0+) |
| `LOB_DATA` | 21 | LOB data | Large object data page for new LOB format (MySQL 8.0+) |
| `LOB_FIRST` | 22 | LOB first page | First page of a large object in new LOB format (MySQL 8.0+) |
| `RSEG_ARRAY` | 23 | Rollback segment array | Array of rollback segment header page numbers (MySQL 8.0+) |
| `SDI` | 17853 | SDI | Serialized Dictionary Information containing table/index metadata (MySQL 8.0+) |
| `SDI_BLOB` | 17854 | SDI BLOB | SDI overflow data for large metadata records (MySQL 8.0+) |
| `INDEX` | 17855 | B+Tree index | Primary key and secondary index data; the most common page type in data tablespaces |
| `RTREE` | 17856 | R-tree index | Spatial index data for geometry columns |
| `PAGE_COMPRESSED` | 34354 | MariaDB page compression | Page-level compression using zlib, LZ4, LZO, LZMA, bzip2, or Snappy (MariaDB only) |
| `PAGE_COMPRESSED_ENCRYPTED` | 37401 | MariaDB compressed + encrypted | Page-level compression combined with per-page encryption (MariaDB only) |

## Notes

### Value 18 Ambiguity

Page type value 18 has different meanings depending on the vendor:

- **MySQL 8.0+**: `SDI_BLOB` -- overflow pages for SDI records that exceed a single page
- **MariaDB**: `INSTANT` -- metadata for columns added with instant ALTER TABLE

`inno` resolves this automatically based on detected vendor. If vendor detection is ambiguous, the output will note the dual interpretation.

### Page Type Distribution

A typical InnoDB tablespace has the following page type distribution:

- **INDEX** pages dominate (often 90%+ of all pages) -- these hold the actual table data and index entries
- **FSP_HDR** appears exactly once (page 0)
- **IBUF_BITMAP** appears once per tablespace (page 1)
- **INODE** appears once or twice (page 2, sometimes more for large tablespaces)
- **ALLOCATED** pages indicate preallocated but unused space
- **SDI** pages appear in MySQL 8.0+ tablespaces (typically pages 3-4)

Use `inno parse` to see the page type summary for any tablespace file.
