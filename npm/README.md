# @ringo380/innodb-utils

InnoDB file analysis toolkit compiled to WebAssembly.

## Installation

```bash
npm install @ringo380/innodb-utils
```

## Usage

With a bundler (Webpack 5+, Vite, Rollup with WASM plugin):

```javascript
import { get_tablespace_info, validate_checksums } from '@ringo380/innodb-utils';

const fileBuffer = await fetch('table.ibd').then(r => r.arrayBuffer());
const data = new Uint8Array(fileBuffer);

const info = JSON.parse(get_tablespace_info(data));
console.log(`Page size: ${info.page_size}, Pages: ${info.page_count}`);

const checksums = JSON.parse(validate_checksums(data));
console.log(`Valid: ${checksums.valid_pages}/${checksums.total_pages}`);
```

The WASM module is automatically initialized by the bundler — no `init()` call needed.

## Available Functions

All functions accept a `Uint8Array` of the file contents and return a JSON string.

- `get_tablespace_info(data)` - Basic tablespace metadata
- `parse_tablespace(data)` - Page-by-page header details
- `analyze_pages(data, page_number)` - Detailed page analysis (pass `-1n` for all pages)
- `validate_checksums(data)` - Checksum validation for all pages
- `extract_sdi(data)` - Extract SDI metadata from MySQL 8.0+ tablespaces
- `hex_dump_page(data, page_number, offset, length)` - Hex dump of page bytes
- `assess_recovery(data)` - Recovery assessment
- `diff_tablespaces(data1, data2)` - Compare two tablespace files
- `parse_redo_log(data)` - Parse InnoDB redo log files
- `inspect_index_records(data, page_number)` - Inspect records on INDEX pages
- `decrypt_tablespace(data, keyring)` - Decrypt encrypted tablespace
- `get_encryption_info(data)` - Get encryption metadata

## BigInt Parameters

Some functions accept `i64` or `u64` parameters that map to JavaScript `BigInt` values.
For example, `analyze_pages` expects a `BigInt` page number:

```javascript
// Analyze all pages
const all = JSON.parse(analyze_pages(data, -1n));

// Analyze a specific page
const page3 = JSON.parse(analyze_pages(data, 3n));
```

## License

MIT
