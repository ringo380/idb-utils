# Page Parsing

Every InnoDB page has a fixed-layout header and trailer that frame the page-type-specific data. The `idb::innodb::page` module provides types for parsing these structures.

## FIL Header

The `FilHeader` struct represents the 38-byte header present at the start of every InnoDB page.

### Byte Layout

```text
Offset  Size  Field
------  ----  -----
0       4     Checksum (or space ID in older formats)
4       4     Page number within the tablespace
8       4     Previous page in doubly-linked list (FIL_NULL = 0xFFFFFFFF if unused)
12      4     Next page in doubly-linked list (FIL_NULL = 0xFFFFFFFF if unused)
16      8     LSN of newest modification to this page
24      2     Page type
26      8     Flush LSN (only meaningful for page 0 of system tablespace)
34      4     Space ID
```

### Parsing

```rust,ignore
use idb::innodb::page::FilHeader;

// page_data must be at least 38 bytes
let page_data: Vec<u8> = vec![0u8; 16384];
let header = FilHeader::parse(&page_data);

match header {
    Some(hdr) => {
        println!("Page number: {}", hdr.page_number);
        println!("Page type: {}", hdr.page_type);
        println!("LSN: {}", hdr.lsn);
        println!("Space ID: {}", hdr.space_id);
        println!("Checksum: 0x{:08X}", hdr.checksum);
    }
    None => println!("Buffer too small for FIL header"),
}
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `checksum` | `u32` | Stored checksum (bytes 0-3) |
| `page_number` | `u32` | Page number within the tablespace (bytes 4-7) |
| `prev_page` | `u32` | Previous page pointer, `0xFFFFFFFF` if unused (bytes 8-11) |
| `next_page` | `u32` | Next page pointer, `0xFFFFFFFF` if unused (bytes 12-15) |
| `lsn` | `u64` | LSN of newest modification (bytes 16-23) |
| `page_type` | `PageType` | Page type enum (bytes 24-25) |
| `flush_lsn` | `u64` | Flush LSN, only valid on page 0 of system tablespace (bytes 26-33) |
| `space_id` | `u32` | Space ID this page belongs to (bytes 34-37) |

### Page Chain Methods

```rust,ignore
use idb::innodb::page::FilHeader;

# let page_data = vec![0u8; 38];
# let header = FilHeader::parse(&page_data).unwrap();
// Check if the page has prev/next pointers set
if header.has_prev() {
    println!("Previous page: {}", header.prev_page);
}
if header.has_next() {
    println!("Next page: {}", header.next_page);
}
```

Both `has_prev()` and `has_next()` return `false` when the pointer is `FIL_NULL` (0xFFFFFFFF) or 0.

## FIL Trailer

The `FilTrailer` struct represents the 8-byte trailer at the end of every InnoDB page.

### Parsing

```rust,ignore
use idb::innodb::page::FilTrailer;

// The trailer is the last 8 bytes of the page
let page_data: Vec<u8> = vec![0u8; 16384];
let trailer_bytes = &page_data[16384 - 8..];
let trailer = FilTrailer::parse(trailer_bytes);

match trailer {
    Some(trl) => {
        println!("Trailer checksum: 0x{:08X}", trl.checksum);
        println!("Trailer LSN low32: 0x{:08X}", trl.lsn_low32);
    }
    None => println!("Buffer too small for FIL trailer"),
}
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `checksum` | `u32` | Old-style checksum (bytes 0-3 of trailer) |
| `lsn_low32` | `u32` | Low 32 bits of the LSN (bytes 4-7 of trailer). Should match the low 32 bits of `FilHeader.lsn`. |

## FSP Header

The `FspHeader` struct represents the FSP (File Space) header found on page 0 of every tablespace, starting at byte offset 38 (immediately after the FIL header).

### Parsing

```rust,ignore
use idb::innodb::tablespace::Tablespace;

let mut ts = Tablespace::open("table.ibd").unwrap();

if let Some(fsp) = ts.fsp_header() {
    println!("Space ID: {}", fsp.space_id);
    println!("Tablespace size: {} pages", fsp.size);
    println!("Free limit: {} pages", fsp.free_limit);
    println!("Flags: 0x{:08X}", fsp.flags);
    println!("Page size from flags: {}", fsp.page_size_from_flags());
}
```

You can also parse the FSP header directly from a page buffer:

```rust,ignore
use idb::innodb::page::FspHeader;

// page_data must be a full page 0 buffer
# let page_data = vec![0u8; 16384];
let fsp = FspHeader::parse(&page_data);
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `space_id` | `u32` | Tablespace space ID |
| `size` | `u32` | Tablespace size in pages |
| `free_limit` | `u32` | Minimum page number not yet initialized |
| `flags` | `u32` | Space flags (encodes page size, compression, encryption info) |
| `frag_n_used` | `u32` | Number of used pages in the FSP\_FREE\_FRAG list |

### Page Size Extraction

```rust,ignore
use idb::innodb::page::FspHeader;
use idb::innodb::vendor::VendorInfo;

# let page_data = vec![0u8; 16384];
let fsp = FspHeader::parse(&page_data).unwrap();

// Auto-detect vendor from flags, then extract page size
let page_size = fsp.page_size_from_flags();

// Or with explicit vendor info
let vendor = VendorInfo::mysql();
let page_size = fsp.page_size_from_flags_with_vendor(&vendor);
```

## PageType

The `PageType` enum (`idb::innodb::page_types::PageType`) maps the 2-byte page type field to named variants covering all InnoDB page types from MySQL 5.7 through 9.x, plus MariaDB-specific types.

### Parsing

```rust,ignore
use idb::innodb::page_types::PageType;
use idb::innodb::vendor::VendorInfo;

// Basic parsing (value 18 defaults to SdiBlob / MySQL interpretation)
let pt = PageType::from_u16(17855);
assert_eq!(pt, PageType::Index);

// Vendor-aware parsing (resolves type 18 ambiguity)
let mariadb = VendorInfo::mariadb(idb::innodb::vendor::MariaDbFormat::FullCrc32);
let pt = PageType::from_u16_with_vendor(18, &mariadb);
assert_eq!(pt, PageType::Instant);

let mysql = VendorInfo::mysql();
let pt = PageType::from_u16_with_vendor(18, &mysql);
assert_eq!(pt, PageType::SdiBlob);
```

### Metadata

Each `PageType` variant provides metadata through three methods:

```rust,ignore
use idb::innodb::page_types::PageType;

let pt = PageType::Index;
println!("Name: {}", pt.name());              // "INDEX"
println!("Description: {}", pt.description()); // "B+Tree index"
println!("Usage: {}", pt.usage());             // "Table and index data stored in B+Tree structure."
```

### Raw Value

```rust,ignore
use idb::innodb::page_types::PageType;

let pt = PageType::Index;
let raw: u16 = pt.as_u16();
assert_eq!(raw, 17855);
```

### Common Page Types

| Variant | Value | Description |
|---------|-------|-------------|
| `Allocated` | 0 | Freshly allocated, not yet initialized |
| `UndoLog` | 2 | Undo log page |
| `Inode` | 3 | File segment inode |
| `FspHdr` | 8 | File space header (page 0) |
| `Xdes` | 9 | Extent descriptor |
| `Blob` | 10 | Uncompressed BLOB data |
| `Sdi` | 17853 | SDI metadata (MySQL 8.0+) |
| `SdiBlob` | 17854 | SDI BLOB overflow (MySQL 8.0+) |
| `Index` | 17855 | B+Tree index page |
| `Rtree` | 17856 | R-tree spatial index |
| `LobIndex` | 20 | LOB index (MySQL 8.0+) |
| `LobData` | 21 | LOB data (MySQL 8.0+) |
| `LobFirst` | 22 | LOB first page (MySQL 8.0+) |
| `Encrypted` | 15 | Encrypted page |
| `PageCompressed` | 34354 | MariaDB page-level compression |
| `Instant` | 18 | MariaDB instant ALTER (conflicts with SdiBlob in MySQL) |
