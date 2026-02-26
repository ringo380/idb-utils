# MySQL Version Compatibility Reference

This page documents the InnoDB on-disk format differences across MySQL versions that affect `inno` tool behavior and compatibility checking.

## Version Feature Matrix

| Feature | 5.6 | 5.7 | 8.0 | 8.4 | 9.0 |
|---------|-----|-----|-----|-----|-----|
| Default page size | 16K | 16K | 16K | 16K | 16K |
| Non-default page sizes (4K/8K/32K/64K) | - | 5.7.6+ | Yes | Yes | Yes |
| CRC-32C checksums | - | 5.7.7+ | Yes | Yes | Yes |
| Legacy InnoDB checksums | Yes | Yes | Yes | Yes | Yes |
| SDI metadata | - | - | Yes | Yes | Yes |
| Tablespace-level encryption | - | 5.7.11+ | Yes | Yes | Yes |
| ROW_FORMAT=COMPRESSED | Yes | Yes | Yes | Deprecated | Deprecated |
| ROW_FORMAT=REDUNDANT | Yes | Yes | Yes | Yes | Deprecated |
| ROW_FORMAT=DYNAMIC | Yes | Default | Default | Default | Default |
| Instant ADD COLUMN | - | - | 8.0.12+ | Yes | Yes |
| Instant ADD COLUMN v2 | - | - | 8.0.29+ | Yes | Yes |
| Redo log format | Pre-8.0 | Pre-8.0 | Pre-8.0.30 / 8.0.30+ | 8.0.30+ | 8.0.30+ |

## Checksum Algorithms by Version

### MySQL

| Version Range | Algorithm | Detection |
|---------------|-----------|-----------|
| < 5.7.7 | Legacy InnoDB | `ut_fold_ulint_pair` byte-by-byte |
| 5.7.7+ | CRC-32C | XOR of two CRC32c ranges |
| All versions | Page 0 | May use either algorithm |

### MariaDB

| Version Range | Algorithm | Detection |
|---------------|-----------|-----------|
| < 10.5 | Legacy InnoDB or CRC-32C | Same as MySQL |
| 10.5+ | `full_crc32` | Single CRC-32C over entire page; checksum in last 4 bytes |

The `full_crc32` format is detected via FSP flags bit 4 and is mutually exclusive with MySQL's checksum format.

### Percona XtraDB

Percona XtraDB uses the same checksum algorithms as MySQL. Tablespace files are binary-compatible.

## Redo Log Format Changes

| Version | Format | Files |
|---------|--------|-------|
| < 8.0.30 | Legacy | `ib_logfile0`, `ib_logfile1` |
| 8.0.30+ | New | `#ib_redo*` numbered files |

The `inno log` subcommand supports both formats. The `inno verify --redo` flag accepts either format for LSN continuity verification.

## SDI (Serialized Dictionary Information)

MySQL 8.0 introduced SDI, embedding the data dictionary directly in each tablespace file. This replaces the `.frm` files used in MySQL 5.7 and earlier.

SDI enables:
- Schema extraction (`inno schema`)
- Column type decoding (`inno export`)
- Index name resolution (`inno health`)
- Version identification (via `mysqld_version_id`)
- Compatibility analysis (`inno compat`)

Pre-8.0 tablespaces without SDI have limited functionality:
- `inno schema` falls back to index structure inference
- `inno export` outputs hex-only data
- `inno compat` reports SDI absence as an error when targeting 8.0+

## Using `inno compat` for Version Checking

The `inno compat` subcommand codifies these version rules into automated checks:

```bash
# Check single file
inno compat -f table.ibd -t 9.0.0

# Scan entire data directory
inno compat --scan /var/lib/mysql -t 8.4.0

# JSON output for scripting
inno compat --scan /var/lib/mysql -t 9.0.0 --json
```

See the [Upgrade Compatibility](upgrade-compatibility.md) guide for detailed usage.
