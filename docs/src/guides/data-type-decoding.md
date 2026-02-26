# Data Type Decoding

The `inno export` subcommand extracts record-level data from InnoDB tablespace files. It uses SDI metadata (MySQL 8.0+) to decode field types and column names, producing CSV, JSON, or hex output.

## Supported Data Types

| Type | Decoding | Notes |
|------|----------|-------|
| TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT | Full | Signed and unsigned; high-bit XOR for InnoDB ordering |
| FLOAT | Full | IEEE 754 single-precision |
| DOUBLE | Full | IEEE 754 double-precision |
| DECIMAL | Full | InnoDB packed BCD encoding |
| DATE | Full | 3-byte packed format (day + month*32 + year*16*32) |
| DATETIME / DATETIME2 | Full | 5-byte packed bit-field + FSP bytes |
| TIMESTAMP / TIMESTAMP2 | Full | 4-byte UTC epoch + FSP bytes |
| TIME / TIME2 | Full | 3-byte offset encoding + FSP bytes |
| YEAR | Full | 1-byte, offset from 1900 |
| CHAR, VARCHAR | Full | Length-prefixed with character set |
| ENUM | Full | 1-2 byte index into element list from SDI |
| SET | Full | 1-8 byte bitmask into element list from SDI |
| TEXT, BLOB | Partial | Inline data as hex; off-page references noted |
| JSON | Partial | Inline data as hex; off-page references noted |
| GEOMETRY | Partial | Raw WKB bytes as hex |
| Other | Hex fallback | Unknown types shown as hex strings |

## Usage

### Basic CSV Export

```bash
inno export -f users.ibd
```

### JSON Export

```bash
inno export -f users.ibd --format json
```

### Hex Dump

```bash
inno export -f users.ibd --format hex
```

### Export Specific Page

```bash
inno export -f users.ibd -p 3
```

### Include Delete-Marked Records

Useful for forensic recovery of recently deleted rows:

```bash
inno export -f users.ibd --where-delete-mark
```

### Include System Columns

Show InnoDB internal columns (DB_TRX_ID, DB_ROLL_PTR):

```bash
inno export -f users.ibd --system-columns
```

## DECIMAL Encoding

InnoDB stores DECIMAL values in packed BCD (Binary-Coded Decimal) format. Key details:

- Groups of 9 digits are stored as 4-byte integers
- Leftover digits (1-8) use 1-3 bytes based on digit count
- The sign is encoded in the high bit of the first byte (1 = positive)
- Negative values have all bytes XOR'd with 0xFF

For `DECIMAL(10,2)`, the value `1234567.89` is stored as two groups: `1234567` (integer part) and `89` (fractional part), each packed as a 4-byte integer with appropriate padding.

## TIME2 Encoding

InnoDB TIME2 uses offset encoding (not sign-bit XOR like some other types):

- 3-byte base value stores `hours << 12 | minutes << 6 | seconds`
- The stored value is offset by `0x800000` to handle negative times
- Fractional seconds (FSP) follow in 0-3 additional bytes
- Negative times (e.g., `-12:30:45`) are supported

## ENUM and SET Types

When SDI metadata is available, ENUM and SET values are decoded using the element list from the data dictionary:

- **ENUM**: 1-2 byte index (1-based) mapped to the element name
- **SET**: 1-8 byte bitmask where each bit corresponds to a set element

Without SDI, these types fall back to numeric representation.

## Limitations

- **Off-page data**: BLOB, TEXT, JSON, and GEOMETRY values stored off-page (extern bit set) are reported as `[OFF-PAGE]` with the space ID, page number, and offset. The actual external data is not followed.
- **Pre-8.0 tablespaces**: Without SDI metadata, column types cannot be determined and all fields are exported as hex.
- **Compressed tablespaces**: Records in compressed pages are not decompressed for export.
