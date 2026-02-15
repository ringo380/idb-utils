# Checksums

The `idb::innodb::checksum` module implements the checksum algorithms used by MySQL and MariaDB InnoDB to validate page integrity. It provides a single entry point, `validate_checksum`, that tries the applicable algorithms and returns a detailed result.

## Validating a Page Checksum

```rust,ignore
use idb::innodb::tablespace::Tablespace;
use idb::innodb::checksum::{validate_checksum, ChecksumAlgorithm};

let mut ts = Tablespace::open("table.ibd").unwrap();
let page = ts.read_page(0).unwrap();

let result = validate_checksum(&page, ts.page_size(), Some(ts.vendor_info()));
println!("Valid: {}", result.valid);
println!("Algorithm: {:?}", result.algorithm);
println!("Stored checksum: 0x{:08X}", result.stored_checksum);
println!("Calculated checksum: 0x{:08X}", result.calculated_checksum);
```

### Function Signature

```rust,ignore
pub fn validate_checksum(
    page_data: &[u8],
    page_size: u32,
    vendor_info: Option<&VendorInfo>,
) -> ChecksumResult
```

### Validation Logic

The function applies different algorithms depending on the vendor and stored checksum value:

1. **All-zeros page**: If the entire page is zeroed, it is considered valid with algorithm `None`.
2. **No-checksum magic**: If the stored checksum is `0xDEADBEEF` (`BUF_NO_CHECKSUM_MAGIC`), the page was written with `innodb_checksum_algorithm=none` and is considered valid.
3. **MariaDB full\_crc32**: When `vendor_info` indicates MariaDB full\_crc32 format, the full\_crc32 algorithm is tried. If it does not match, failure is reported immediately (no fallback to other algorithms).
4. **CRC-32C** (tried first for MySQL/Percona): XOR of two independent CRC-32C values over the page data.
5. **Legacy InnoDB** (fallback): The `ut_fold_ulint_pair`-based algorithm used before MySQL 5.7.7.

If neither CRC-32C nor legacy InnoDB matches, the result reports failure with CRC-32C as the expected algorithm.

## ChecksumResult

| Field | Type | Description |
|-------|------|-------------|
| `algorithm` | `ChecksumAlgorithm` | The algorithm that was detected or attempted |
| `valid` | `bool` | Whether the stored checksum matches the calculated value |
| `stored_checksum` | `u32` | The checksum value stored in the page |
| `calculated_checksum` | `u32` | The checksum value calculated from the page data |

## ChecksumAlgorithm

| Variant | Description |
|---------|-------------|
| `Crc32c` | CRC-32C (hardware accelerated, MySQL 5.7.7+ default) |
| `InnoDB` | Legacy InnoDB checksum (`buf_calc_page_new_checksum` equivalent) |
| `MariaDbFullCrc32` | MariaDB full\_crc32 (single CRC-32C over entire page minus last 4 bytes) |
| `None` | No checksum (`innodb_checksum_algorithm=none` or all-zeros page) |

## LSN Validation

In addition to checksum validation, you can verify LSN consistency between the FIL header and FIL trailer:

```rust,ignore
use idb::innodb::tablespace::Tablespace;
use idb::innodb::checksum::validate_lsn;

let mut ts = Tablespace::open("table.ibd").unwrap();
let page = ts.read_page(0).unwrap();

let lsn_ok = validate_lsn(&page, ts.page_size());
println!("LSN consistent: {}", lsn_ok);
```

`validate_lsn` compares the low 32 bits of the 8-byte LSN at bytes 16-23 (FIL header) with the 4-byte value at the end of the FIL trailer (bytes `page_size - 4` to `page_size`). A mismatch indicates the page may be torn or corrupted.

## Algorithm Details

### CRC-32C (MySQL 5.7.7+)

The MySQL CRC-32C checksum is computed over two disjoint byte ranges and XORed together (not chained):

- **Range 1**: bytes `[4..26)` -- page number through page type (skipping the stored checksum at bytes 0-3)
- **Range 2**: bytes `[38..page_size-8)` -- page data area (skipping flush LSN, space ID, and the 8-byte trailer)

```text
stored_checksum = CRC32C(bytes[4..26]) XOR CRC32C(bytes[38..page_size-8])
```

The checksum is stored in bytes 0-3 of the page (the FIL header checksum field).

### Legacy InnoDB (MySQL < 5.7.7)

Uses `ut_fold_ulint_pair` with wrapping `u32` arithmetic, processing each byte individually over the same two ranges. The two fold values are summed (with wrapping) rather than XORed.

```text
stored_checksum = fold(bytes[4..26]) + fold(bytes[38..page_size-8])
```

### MariaDB full\_crc32 (MariaDB 10.5+)

A single CRC-32C over bytes `[0..page_size-4)`. The checksum is stored in the **last 4 bytes** of the page (not in the FIL header at bytes 0-3 like MySQL).

```text
stored_checksum (at page_size-4) = CRC32C(bytes[0..page_size-4])
```

## Combining Checksum and LSN Validation

For thorough page integrity checking, validate both:

```rust,ignore
use idb::innodb::tablespace::Tablespace;
use idb::innodb::checksum::{validate_checksum, validate_lsn};

let mut ts = Tablespace::open("table.ibd").unwrap();

ts.for_each_page(|page_num, page_data| {
    let cksum = validate_checksum(page_data, ts.page_size(), Some(ts.vendor_info()));
    let lsn_ok = validate_lsn(page_data, ts.page_size());

    if !cksum.valid || !lsn_ok {
        println!(
            "Page {}: checksum={} ({:?}), LSN consistent={}",
            page_num, cksum.valid, cksum.algorithm, lsn_ok
        );
    }
    Ok(())
}).unwrap();
```
