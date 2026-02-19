# MySQL 9.x Test Fixtures

Test fixtures extracted from MySQL 9.0 and 9.1 Docker containers for validating
InnoDB file parsing across MySQL 9.x versions.

## MySQL Versions

- **MySQL 9.0**: `mysql  Ver 9.0.1 for Linux on aarch64 (MySQL Community Server - GPL)`
- **MySQL 9.1**: `mysql  Ver 9.1.0 for Linux on aarch64 (MySQL Community Server - GPL)`

## Fixture Files

### Standard Tablespace (.ibd)

| File | Version | Description |
|------|---------|-------------|
| `mysql90_standard.ibd` | 9.0.1 | Standard InnoDB table with 3 rows, 16K page size, 7 pages |
| `mysql91_standard.ibd` | 9.1.0 | Standard InnoDB table with 3 rows, 16K page size, 7 pages |
| `mysql90_compressed.ibd` | 9.0.1 | Compressed table (ROW_FORMAT=COMPRESSED, KEY_BLOCK_SIZE=8), 8K physical pages |
| `mysql91_compressed.ibd` | 9.1.0 | Compressed table (ROW_FORMAT=COMPRESSED, KEY_BLOCK_SIZE=8), 8K physical pages |
| `mysql90_multipage.ibd` | 9.0.1 | Multi-page table with 200 rows of 8KB BLOB data, 640 pages (102 INDEX pages) |
| `mysql91_multipage.ibd` | 9.1.0 | Multi-page table with 200 rows of 8KB BLOB data, 640 pages (102 INDEX pages) |

### Redo Log Files

| File | Version | Description |
|------|---------|-------------|
| `mysql90_redo_9` | 9.0.1 | InnoDB redo log file (#ib_redo9), 3.1 MB |
| `mysql90_redo_10` | 9.0.1 | InnoDB redo log file (#ib_redo10), 3.1 MB |
| `mysql91_redo_9` | 9.1.0 | InnoDB redo log file (#ib_redo9), 3.1 MB |
| `mysql91_redo_10` | 9.1.0 | InnoDB redo log file (#ib_redo10), 3.1 MB |

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

-- 200 rows with 8KB payloads to force multiple INDEX pages
DELIMITER //
CREATE PROCEDURE fill_multipage()
BEGIN
  DECLARE i INT DEFAULT 1;
  WHILE i <= 200 DO
    INSERT INTO multipage VALUES (i, REPEAT(CHAR(65 + (i % 26)), 8000));
    SET i = i + 1;
  END WHILE;
END//
DELIMITER ;
CALL fill_multipage();
```

## Generation Process

1. Started MySQL 9.0 and 9.1 Docker containers (`mysql:9.0`, `mysql:9.1`)
2. Created `fixtures` database and tables on both containers
3. Ran `FLUSH TABLES` to ensure data was written to disk
4. Extracted `.ibd` files via `docker cp` from `/var/lib/mysql/fixtures/`
5. Extracted active redo log files from `/var/lib/mysql/#innodb_redo/`

## Notes

- **Compressed tablespace**: The `compressed` fixtures use 8K physical pages due to
  `KEY_BLOCK_SIZE=8`. The FSP header page is still 16K, but data pages are stored at
  the compressed size. Standard CRC-32C checksum validation at 16K page size will
  report failures for these files; this is expected behavior for compressed tablespaces.
- **Page type distribution** (standard): FSP_HDR, IBUF_BITMAP, INODE, SDI, INDEX, plus ALLOCATED pages.
- **Page type distribution** (multipage): Same types with 102 INDEX pages and 534 ALLOCATED pages.
- **Redo log "Created by" field**: Reports `MySQL 9.0.1` and `MySQL 9.1.0` respectively.
