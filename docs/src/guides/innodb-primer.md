# InnoDB File Format Primer

Understanding the on-disk format helps when interpreting `inno` output.

## Tablespace Files (.ibd)

Each `.ibd` file is a tablespace divided into fixed-size pages (default 16,384 bytes). Every page has:

```text
+------------------+  byte 0
| FIL Header       |  38 bytes -- checksum, page number, prev/next, LSN, type, space_id
+------------------+  byte 38
| Page Body        |  varies by page type
|                  |
+------------------+  byte (page_size - 8)
| FIL Trailer      |  8 bytes -- old checksum, LSN low 32 bits
+------------------+
```

Page 0 is always `FSP_HDR` with tablespace metadata including the space ID, tablespace size, FSP flags, and extent descriptors.

## Page Types

| Type | Value | Description |
|------|-------|-------------|
| `ALLOCATED` | 0 | Freshly allocated, type field not yet initialized |
| `UNDO_LOG` | 2 | Stores previous values of modified records |
| `INODE` | 3 | File segment inode bookkeeping |
| `TRX_SYS` | 7 | Transaction system header (system tablespace only) |
| `FSP_HDR` | 8 | File space header, always page 0 |
| `XDES` | 9 | Extent descriptor for 16,384-page blocks |
| `BLOB` | 10 | Externally stored column data |
| `SDI` | 17853 | Serialized Dictionary Information (MySQL 8.0+) |
| `INDEX` | 17855 | B+Tree index node -- table and index data |

See the [Page Types](../reference/page-types.md) reference for the full list.

## Checksums

Two primary algorithms protect page integrity:

- **CRC-32C** (default since MySQL 5.7.7) -- hardware-accelerated, XOR of two independent CRC-32C values computed over bytes `[4..26)` and `[38..page_size-8)`.
- **Legacy InnoDB** (MySQL < 5.7.7) -- custom `ut_fold_ulint_pair` hash with wrapping u32 arithmetic, computed over the same two byte ranges.

MariaDB 10.5+ introduces a third algorithm, `full_crc32`, which computes a single CRC-32C over `[0..page_size-4)` and stores the result in the last 4 bytes of the page. See [Checksum Algorithms](../reference/checksum-algorithms.md) for details.

## LSN (Log Sequence Number)

The LSN is a monotonically increasing counter that tracks position in the redo log. Each page records the LSN in two places:

- **FIL header** (8 bytes at offset 16) -- full 64-bit LSN of the last modification
- **FIL trailer** (4 bytes at offset `page_size - 4`) -- low 32 bits of the same LSN

A mismatch between the header and trailer LSN values indicates a torn page write -- the page was only partially flushed to disk. The `inno checksum` command reports these as LSN mismatches.

## Redo Log Structure

InnoDB redo logs are organized as a sequence of 512-byte blocks:

| Block | Purpose |
|-------|---------|
| 0 | File header (group_id, start_lsn, file_no, creator string) |
| 1 | Checkpoint 1 |
| 2 | Reserved |
| 3 | Checkpoint 2 |
| 4+ | Data blocks (14-byte header, up to 494 bytes of log records, 4-byte checksum) |

The creator string in block 0 identifies the server version and vendor (e.g., "MySQL 8.0.32", "Percona XtraDB 8.0.35"). See [Redo Log Format](../reference/redo-log-format.md) for the complete specification.

## Page Sizes

InnoDB supports five page sizes: 4K, 8K, 16K (default), 32K, and 64K. The page size is encoded in the FSP flags on page 0 and auto-detected by `inno`. All byte offsets and ranges in this documentation assume the default 16K page size unless stated otherwise.

## Further Reading

- [Checksum Algorithms](../reference/checksum-algorithms.md) -- detailed algorithm specifications
- [Page Types](../reference/page-types.md) -- complete page type table
- [Redo Log Format](../reference/redo-log-format.md) -- block and record layout
- [Vendor Support](vendor-support.md) -- MySQL, Percona, and MariaDB differences
