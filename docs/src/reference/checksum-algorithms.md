# Checksum Algorithms

IDB Utils supports three checksum algorithms used by different InnoDB implementations and versions.

## CRC-32C (MySQL 5.7.7+ default)

The default algorithm since MySQL 5.7.7. It computes the XOR of two **independent** CRC-32C values over non-overlapping byte ranges:

- **Range 1**: bytes `[4..26)` -- covers page number, prev/next pointers, LSN, and page type
- **Range 2**: bytes `[38..page_size-8)` -- covers the entire page body

```text
[checksum][--- Range 1 ---][flush_lsn+space_id][------- Range 2 -------][trailer]
 0    3  4             25  26              37  38                    PS-8   PS
```

The two CRC-32C values are computed independently (not chained) and XORed together. The result is stored in bytes 0-3 of the page (the FIL header checksum field).

CRC-32C is hardware-accelerated on modern x86 (SSE 4.2) and ARM (CRC extension) processors, making validation fast even for large tablespaces.

## Legacy InnoDB (MySQL < 5.7.7)

The original InnoDB checksum algorithm, used as the default before MySQL 5.7.7. It uses the internal `ut_fold_ulint_pair` function with wrapping u32 arithmetic.

The algorithm processes bytes **one at a time** (not as u32 words) over the same two byte ranges as CRC-32C:

- **Range 1**: bytes `[4..26)`
- **Range 2**: bytes `[38..page_size-8)`

The fold function accumulates a running hash by combining each byte with the previous hash value using wrapping multiplication, addition, and XOR operations. The final result is the sum of the fold values from both ranges.

The legacy checksum is stored in the same location as CRC-32C: bytes 0-3 of the page.

### Mixed Mode

MySQL 5.7.x supports `innodb_checksum_algorithm` values of `crc32`, `innodb`, `none`, and `strict_*` variants. During migration from legacy to CRC-32C, a tablespace may contain pages with either algorithm. `inno checksum` validates against both algorithms and reports a page as valid if either checksum matches.

## MariaDB full_crc32 (MariaDB 10.5+)

MariaDB 10.5 introduced a simplified checksum format called `full_crc32`:

- **Single CRC-32C** computed over bytes `[0..page_size-4)` -- covers nearly the entire page
- **Checksum location**: stored in the **last 4 bytes** of the page, not in the FIL header

```text
[------------- CRC-32C input ---------------][checksum]
 0                                        PS-4    PS
```

This differs from the MySQL format in two important ways:

1. The checksum covers bytes 0-3 (which are skipped in the MySQL CRC-32C algorithm), providing stronger integrity protection
2. The checksum is stored at the end of the page rather than the beginning

### Detection

MariaDB `full_crc32` is detected via FSP flags bit 4 on page 0. When this bit is set, `inno` automatically uses the `full_crc32` algorithm for all pages in the tablespace.

## Validation Behavior

`inno checksum` validates each page using the appropriate algorithm:

1. Check FSP flags on page 0 for MariaDB `full_crc32` -- if detected, use that for all pages
2. Otherwise, compute both CRC-32C and Legacy checksums
3. A page is valid if its stored checksum matches **either** computed value
4. Pages with all-zero content (empty/allocated pages) are reported separately and not counted as invalid
5. LSN consistency is checked independently: the low 32 bits of the header LSN must match the trailer value

Use `inno checksum -v` for per-page details or `inno checksum --json` for structured output.
