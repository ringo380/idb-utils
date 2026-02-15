# SDI Extraction

The `idb::innodb::sdi` module provides functions for extracting Serialized Dictionary Information (SDI) from MySQL 8.0+ tablespaces. SDI replaced `.frm` files as the mechanism for embedding table, column, and index definitions directly inside each `.ibd` file. SDI data is stored on dedicated SDI pages (page type 17853) as zlib-compressed JSON.

## Finding SDI Pages

```rust,no_run
use idb::innodb::tablespace::Tablespace;
use idb::innodb::sdi;

let mut ts = Tablespace::open("users.ibd").unwrap();
let sdi_pages = sdi::find_sdi_pages(&mut ts).unwrap();
println!("Found {} SDI pages: {:?}", sdi_pages.len(), sdi_pages);
```

`find_sdi_pages` uses a two-phase approach:

1. **Fast path**: Reads the SDI root page number from page 0 (located after the FIL header, FSP header, and XDES array). If the SDI version marker (value 1) is found and the root page is valid, it reads the root page and walks its prev/next linked list to collect all SDI leaf pages.
2. **Fallback**: If the fast path fails (e.g., corrupted page 0, pre-8.0 tablespace), it scans every page in the tablespace looking for SDI page types.

## Extracting SDI Records

```rust,no_run
use idb::innodb::tablespace::Tablespace;
use idb::innodb::sdi;

let mut ts = Tablespace::open("users.ibd").unwrap();
let sdi_pages = sdi::find_sdi_pages(&mut ts).unwrap();
let records = sdi::extract_sdi_from_pages(&mut ts, &sdi_pages).unwrap();

for rec in &records {
    println!("Type: {} ({})", rec.sdi_type, sdi::sdi_type_name(rec.sdi_type));
    println!("Object ID: {}", rec.sdi_id);
    println!("Compressed: {} bytes -> {} bytes uncompressed",
        rec.compressed_len, rec.uncompressed_len);
    // rec.data contains the decompressed JSON string
    println!("Data (first 200 chars): {}", &rec.data[..rec.data.len().min(200)]);
    println!("---");
}
```

### Function Signature

```rust,ignore
pub fn extract_sdi_from_pages(
    ts: &mut Tablespace,
    sdi_pages: &[u64],
) -> Result<Vec<SdiRecord>, IdbError>
```

The function:

1. Reads each SDI page and verifies it has page type SDI (17853)
2. Parses the INDEX page header to confirm it is a leaf page with records
3. Walks compact-format records on the page
4. For each ordinary record, extracts the SDI header fields (type, ID, lengths)
5. If compressed data fits within the page, decompresses it directly
6. If compressed data spans multiple pages, follows the next-page chain to collect all compressed bytes before decompression

## SdiRecord

| Field | Type | Description |
|-------|------|-------------|
| `sdi_type` | `u32` | SDI type: 1 = Table, 2 = Tablespace |
| `sdi_id` | `u64` | SDI object ID |
| `uncompressed_len` | `u32` | Expected uncompressed data length in bytes |
| `compressed_len` | `u32` | Compressed data length in bytes |
| `data` | `String` | Decompressed JSON string containing the dictionary definition |

## SDI Type Names

```rust,no_run
use idb::innodb::sdi;

assert_eq!(sdi::sdi_type_name(1), "Table");
assert_eq!(sdi::sdi_type_name(2), "Tablespace");
assert_eq!(sdi::sdi_type_name(99), "Unknown");
```

## Checking Individual Pages

```rust,no_run
use idb::innodb::sdi;

# let page_data = vec![0u8; 16384];
// Check if a page buffer is an SDI page
if sdi::is_sdi_page(&page_data) {
    // Extract records from a single page (no multi-page reassembly)
    if let Some(records) = sdi::extract_sdi_from_page(&page_data) {
        println!("Found {} SDI records on this page", records.len());
    }
}
```

`extract_sdi_from_page` works on a single page buffer without requiring a `Tablespace` handle. It does not follow page chains, so records spanning multiple pages may have truncated or incomplete data. Use `extract_sdi_from_pages` with a `Tablespace` for full multi-page reassembly.

## Reading the SDI Root Page

For advanced use, you can read the SDI root page number directly from page 0:

```rust,no_run
use idb::innodb::tablespace::Tablespace;
use idb::innodb::sdi;

let mut ts = Tablespace::open("users.ibd").unwrap();
let page0 = ts.read_page(0).unwrap();

if let Some(root_page) = sdi::read_sdi_root_page(&page0, ts.page_size(), ts.page_count()) {
    println!("SDI root page: {}", root_page);
}
```

This returns `None` if the SDI version marker is not found, the version is not 1, or the root page number is out of range.

## Limitations

- SDI is a MySQL 8.0+ feature. MariaDB tablespaces do not contain SDI pages.
- Pre-MySQL 8.0 tablespaces will return an empty list from `find_sdi_pages`.
- The SDI JSON schema is defined by MySQL's data dictionary and varies between MySQL versions.
