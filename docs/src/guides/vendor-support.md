# Vendor Support

IDB Utils supports three InnoDB implementations: MySQL, Percona XtraDB, and MariaDB. Each has differences in on-disk format, checksum algorithms, and feature availability.

## Compatibility Matrix

| Feature | MySQL | Percona XtraDB | MariaDB |
|---------|-------|----------------|---------|
| Detection | Default | Redo log `created_by` | FSP flags bit 4/16 |
| Checksums | CRC-32C, Legacy | Same as MySQL | `full_crc32` (10.5+), CRC-32C |
| Page Types | All standard | Same as MySQL | +PageCompressed, +Instant |
| Compression | zlib, LZ4 | Same as MySQL | zlib, LZ4, LZO\*, LZMA\*, bzip2\*, Snappy\* |
| Encryption | Tablespace-level | Same as MySQL | Per-page only |
| SDI | Yes (8.0+) | Yes (8.0+) | N/A |
| Redo Logs | Full parsing | Full parsing | Header + checkpoints only |

\*detection only, not decompressed

## Vendor Detection Logic

`inno` auto-detects the vendor from on-disk metadata. The detection priority is:

1. **FSP flags bit 4 set** -- MariaDB `full_crc32` format (10.5+). This is unambiguous.
2. **FSP flags bit 16 set, bits 11-14 zero** -- MariaDB original format.
3. **Redo log `created_by` string** -- contains "Percona" for Percona XtraDB, otherwise MySQL.
4. **Default** -- MySQL. Percona XtraDB is binary-compatible with MySQL at the tablespace level, so tablespace-only analysis cannot distinguish them.

## MariaDB Notes

### Checksum Differences

MariaDB 10.5+ uses the `full_crc32` checksum format:

- Single CRC-32C computed over bytes `[0..page_size-4)`
- Checksum stored in the **last 4 bytes** of the page, not in the FIL header
- Detected via FSP flags bit 4

Earlier MariaDB versions use the same CRC-32C and Legacy algorithms as MySQL.

### Page Type Ambiguity

Page type value 18 has different meanings depending on the vendor:

- **MySQL**: `SDI_BLOB` (SDI overflow data)
- **MariaDB**: `INSTANT` (instant ALTER TABLE metadata)

`inno` uses the detected vendor to resolve this ambiguity automatically.

### SDI Not Available

MariaDB does not use Serialized Dictionary Information. Running `inno sdi` on a MariaDB tablespace will return an error. MariaDB stores table metadata in `.frm` files (10.x) or its own data dictionary format.

### Compression Algorithms

MariaDB supports additional page compression algorithms beyond MySQL's zlib and LZ4. `inno` detects the compression type from page headers but only decompresses zlib and LZ4. Other algorithms (LZO, LZMA, bzip2, Snappy) are identified in output but their data is not decompressed.

## MySQL Version Support

| Version | Tablespace Files | Redo Log Files | SDI |
|---------|-----------------|----------------|-----|
| MySQL 5.7 | `.ibd` | `ib_logfile0`, `ib_logfile1` | N/A |
| MySQL 8.0 | `.ibd` | `ib_logfile0`, `ib_logfile1` | Yes |
| MySQL 8.0.30+ | `.ibd` | `#innodb_redo/#ib_redo*` | Yes |
| MySQL 8.4 | `.ibd` | `#innodb_redo/#ib_redo*` | Yes |
| MySQL 9.x | `.ibd` | `#innodb_redo/#ib_redo*` | Yes |

## Page Sizes

All vendors support the same set of page sizes: 4K, 8K, 16K (default), 32K, and 64K. The page size is encoded in the FSP flags on page 0 and is auto-detected by `inno` regardless of vendor.

## Percona XtraDB Notes

Percona XtraDB is a fork of MySQL's InnoDB engine. At the tablespace level, the on-disk format is binary-compatible with MySQL. The only reliable way to distinguish Percona from MySQL is through the redo log `created_by` string, which Percona sets to include "Percona" or "XtraDB".

All `inno` subcommands work identically for MySQL and Percona tablespace files.
