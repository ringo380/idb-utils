# Tablespace

The `Tablespace` struct (`idb::innodb::tablespace::Tablespace`) is the primary entry point for reading InnoDB `.ibd` tablespace files. It handles file I/O, automatic page size detection from FSP flags, vendor identification, and encryption awareness.

## Opening a Tablespace

### From a file path

```rust,no_run
use idb::innodb::tablespace::Tablespace;

// Auto-detect page size from FSP flags on page 0
let mut ts = Tablespace::open("table.ibd").unwrap();

// Override page size detection (useful for corrupted page 0)
let mut ts = Tablespace::open_with_page_size("table.ibd", 16384).unwrap();
```

`Tablespace::open` and `Tablespace::open_with_page_size` are not available when compiling for `wasm32` targets. Use `from_bytes` instead.

### From an in-memory buffer

```rust,no_run
use idb::innodb::tablespace::Tablespace;

let data: Vec<u8> = std::fs::read("table.ibd").unwrap();

// Auto-detect page size
let mut ts = Tablespace::from_bytes(data).unwrap();

// Or with explicit page size
let data2: Vec<u8> = std::fs::read("table.ibd").unwrap();
let mut ts2 = Tablespace::from_bytes_with_page_size(data2, 16384).unwrap();
```

The `from_bytes` constructors are available on all targets, including WASM.

## Reading Pages

### Single page by number

```rust,no_run
use idb::innodb::tablespace::Tablespace;
use idb::innodb::page::FilHeader;

let mut ts = Tablespace::open("table.ibd").unwrap();

let page = ts.read_page(0).unwrap();
let header = FilHeader::parse(&page).unwrap();
println!("Page 0 type: {}", header.page_type);
println!("Space ID: {}", header.space_id);
println!("LSN: {}", header.lsn);
```

`read_page` returns an `Err` if the page number is out of range (beyond `page_count()`). If a decryption context has been set, encrypted pages are transparently decrypted before being returned.

### Iterating all pages

```rust,no_run
use idb::innodb::tablespace::Tablespace;
use idb::innodb::page::FilHeader;

let mut ts = Tablespace::open("table.ibd").unwrap();

ts.for_each_page(|page_num, page_data| {
    let header = FilHeader::parse(page_data).unwrap();
    println!("Page {}: type={}, LSN={}", page_num, header.page_type, header.lsn);
    Ok(())
}).unwrap();
```

The callback receives `(page_number: u64, page_data: &[u8])` for each page. Returning an error from the callback stops iteration. If a decryption context is set, encrypted pages are decrypted before being passed to the callback.

## Metadata Accessors

| Method | Return Type | Description |
|--------|-------------|-------------|
| `page_size()` | `u32` | Detected or configured page size in bytes (4096, 8192, 16384, 32768, or 65536) |
| `page_count()` | `u64` | Total number of pages in the file (`file_size / page_size`) |
| `file_size()` | `u64` | File size in bytes |
| `fsp_header()` | `Option<&FspHeader>` | FSP header from page 0, if parseable |
| `vendor_info()` | `&VendorInfo` | Detected vendor (MySQL, Percona, or MariaDB with format variant) |
| `encryption_info()` | `Option<&EncryptionInfo>` | Encryption info from page 0, if present |
| `is_encrypted()` | `bool` | Whether the tablespace has encryption info on page 0 |

## Helper Methods

### Parsing headers from page buffers

```rust,no_run
use idb::innodb::tablespace::Tablespace;

let mut ts = Tablespace::open("table.ibd").unwrap();
let page = ts.read_page(3).unwrap();

// Static method -- parse FIL header from any page buffer
let header = Tablespace::parse_fil_header(&page).unwrap();
println!("Page number: {}", header.page_number);

// Instance method -- parse FIL trailer (needs page_size from tablespace)
let trailer = ts.parse_fil_trailer(&page).unwrap();
println!("Trailer LSN low32: {}", trailer.lsn_low32);
```

## Encryption Support

```rust,no_run
use idb::innodb::tablespace::Tablespace;
use idb::innodb::decryption::DecryptionContext;

let mut ts = Tablespace::open("encrypted_table.ibd").unwrap();

if ts.is_encrypted() {
    println!("Encryption info: {:?}", ts.encryption_info());

    // Set up decryption for transparent page decryption
    // (requires the tablespace key from the MySQL keyring)
    // let ctx = DecryptionContext::new(key, iv);
    // ts.set_decryption_context(ctx);
}
```

When a `DecryptionContext` is set via `set_decryption_context`, both `read_page` and `for_each_page` automatically decrypt pages with encrypted page types (15, 16, 17) before returning them.

## Page Size Detection

On initialization, `Tablespace` reads page 0 and parses the FSP header to determine the page size from the FSP flags field. The detection is vendor-aware:

- **MySQL / Percona**: Page size encoded in FSP flags bits 6-9 as `ssize`, where `page_size = 1 << (ssize + 9)`. A value of 0 means the default 16384 bytes.
- **MariaDB full\_crc32**: Page size encoded in FSP flags bits 0-3 (same `ssize` formula). The full\_crc32 marker at bit 4 triggers this alternate layout.

Supported page sizes: 4096, 8192, 16384, 32768, 65536.

## Error Handling

All fallible methods return `Result<T, IdbError>`. Common error conditions:

- File too small to be a valid tablespace (less than FIL header + FSP header size)
- Page number out of range
- I/O errors during seek or read operations
