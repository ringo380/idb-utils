# Percona Server Test Fixtures

Test fixtures extracted from Percona Server 8.0 and 8.4 Docker containers for validating
InnoDB file parsing across Percona Server versions.

## Percona Versions

- **Percona 8.0**: `mysql  Ver 8.0.45-36 for Linux on aarch64 (Percona Server (GPL), Release 36, Revision 8fe4a72d)`
- **Percona 8.4**: `mysql  Ver 8.4.7-7 for Linux on aarch64 (Percona Server (GPL), Release 7, Revision 9a19f1fd)`

## Fixture Files

### Standard Tablespace (.ibd)

| File | Version | Description |
|------|---------|-------------|
| `percona80_standard.ibd` | 8.0.45-36 | Standard InnoDB table with 3 rows, 16K page size, 7 pages |
| `percona84_standard.ibd` | 8.4.7-7 | Standard InnoDB table with 3 rows, 16K page size, 7 pages |
| `percona80_compressed.ibd` | 8.0.45-36 | Compressed table (ROW_FORMAT=COMPRESSED, KEY_BLOCK_SIZE=8), 8K physical pages |
| `percona84_compressed.ibd` | 8.4.7-7 | Compressed table (ROW_FORMAT=COMPRESSED, KEY_BLOCK_SIZE=8), 8K physical pages |
| `percona80_multipage.ibd` | 8.0.45-36 | Multi-page table with 100 rows of 8KB BLOB data, 576 pages (52 INDEX pages) |
| `percona84_multipage.ibd` | 8.4.7-7 | Multi-page table with 100 rows of 8KB BLOB data, 576 pages (52 INDEX pages) |

## Table Schemas

### standard

```sql
CREATE TABLE standard (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    data TEXT
) ENGINE=InnoDB;

INSERT INTO standard VALUES (1, 'test_row_1', REPEAT('x', 1000));
INSERT INTO standard VALUES (2, 'test_row_2', REPEAT('y', 500));
INSERT INTO standard VALUES (3, 'test_row_3', 'short data');
```

### compressed

```sql
CREATE TABLE compressed (
    id INT PRIMARY KEY,
    val VARCHAR(255)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;

INSERT INTO compressed VALUES (1, 'compressed row 1');
INSERT INTO compressed VALUES (2, 'compressed row 2');
```

### multipage

```sql
CREATE TABLE multipage (
    id INT PRIMARY KEY AUTO_INCREMENT,
    payload BLOB
) ENGINE=InnoDB;

-- 100 rows with 8KB payloads to force multiple INDEX pages
INSERT INTO multipage (id, payload)
SELECT seq, REPEAT(CHAR(65 + (seq MOD 26)), 8000)
FROM (SELECT @row := @row + 1 AS seq FROM information_schema.columns a, (SELECT @row := 0) b LIMIT 100) t;
```

## Generation Process

1. Started Percona Server 8.0 and 8.4 Docker containers (`percona:8.0`, `percona:8.4`)
2. Created `fixtures` database and tables on both containers
3. Ran `FLUSH TABLES` to ensure data was written to disk
4. Extracted `.ibd` files via `docker cp` from `/var/lib/mysql/fixtures/`

## Notes

- **Vendor detection**: Percona Server uses the same FSP flags as MySQL, so vendor detection
  from `.ibd` files alone reports `MySQL`. Percona vendor can only be detected from redo log
  `created_by` strings, which contain "Percona Server".
- **Compressed tablespace**: The `compressed` fixtures use 8K physical pages due to
  `KEY_BLOCK_SIZE=8`. The FSP header page is still 16K, but data pages are stored at
  the compressed size. Standard CRC-32C checksum validation at 16K page size will
  report failures for these files; this is expected behavior for compressed tablespaces.
- **Page type distribution** (standard): FSP_HDR, IBUF_BITMAP, INODE, SDI, INDEX, plus ALLOCATED pages.
- **Page type distribution** (multipage): Same types with 52 INDEX pages and 520 ALLOCATED pages.
- **SDI metadata**: Contains `mysql_version_id` of `80045` (Percona 8.0) and `80407` (Percona 8.4),
  reflecting the upstream MySQL version Percona Server is based on.
