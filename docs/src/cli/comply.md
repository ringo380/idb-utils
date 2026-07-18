# inno comply

Forensic deletion verification and data-residue scanning - the inverse of [`inno undelete`](undelete.md). Where `undelete` recovers data that still lingers, `comply` proves a value has been *purged* from every InnoDB-retained location in a file, and reports every place it still appears.

## Scope and honesty

`comply` verifies residue **within the file(s) you pass in only**. It cannot see the OS page cache, replicas, other backups, or binary-log archives, and it does not certify legal compliance. It reports byte-level and record-level residue; interpreting that against a regulation is your call, not the tool's.

## Modes

Exactly one mode per invocation:

| Mode | Flag | Answers |
|------|------|---------|
| Deletion verification | `--verify-deleted --where <col>=<value>` | Has this value been purged from all record structures? |
| Residue scan | `--scan-residue --pattern <needle>` | Where do these raw bytes still appear on disk? |
| Encryption audit | `--encryption-audit` | Which pages are encrypted, and is a key available? |

## Usage

```bash
# Verify a specific value has been deleted everywhere (logical structures)
inno comply -f users.ibd --verify-deleted --where email=alice@example.com

# Also sweep raw slack/unallocated space for the encoded value
inno comply -f users.ibd --verify-deleted --where email=alice@example.com --thorough

# Raw byte-pattern residue scan (UTF-8 text)
inno comply -f users.ibd --scan-residue --pattern "alice@example.com"

# Raw scan for a specific byte sequence
inno comply -f users.ibd --scan-residue --pattern hex:0a1b2c3d

# Encryption audit
inno comply -f users.ibd --encryption-audit

# JSON output for any mode
inno comply -f users.ibd --verify-deleted --where id=42 --json
```

## Options

| Option | Description |
|--------|-------------|
| `-f, --file` | Path to the InnoDB tablespace file (`.ibd`) |
| `--verify-deleted` | Mode: verify a value has been purged from all InnoDB-retained locations |
| `--scan-residue` | Mode: raw byte-pattern residue scan across all pages |
| `--encryption-audit` | Mode: report encrypted vs plaintext pages and key availability |
| `--where` | `column=value` to check (verify mode) |
| `--pattern` | Literal needle to search for (scan mode): UTF-8 text, or `hex:00ff..` for raw bytes |
| `--thorough` | Verify mode: also sweep raw slack space for the encoded value |
| `--max-hits` | Scan mode: cap on matches reported (default 1000) |
| `-t, --table` | Table-name filter (verify mode); errors if it mismatches the SDI table |
| `--json` | Emit JSON |
| `-v, --verbose` | Show additional detail |
| `--page-size` | Override page size (default: auto-detect) |
| `--keyring` | Path to MySQL keyring file for encrypted tablespaces |
| `--mmap` | Use memory-mapped I/O |

The global `--format csv` flag produces CSV output for any mode.

## How verification works

`--verify-deleted` runs a **logical** pass: it decodes real record structures and compares the target column's value. It covers, in order:

1. **Live clustered records** - the value exists in a current row (reported as `live_record`; from a deletion standpoint this is a failure - the value is still there).
2. **Delete-marked records** - `DELETE`d but not yet purged (`delete_marked`), with the approximate transaction id.
3. **Free-list records** - purged from the active chain but not overwritten (`free_list`).
4. **Undo `DEL_MARK` entries** - only scanned when the target column is part of the primary key, because undo stores PK fields for deletes (`undo_del_mark`). Undo lives in `ibdata1`/undo tablespaces, so a bare `.ibd` finds nothing here.

The logical pass is type-aware and authoritative for "is this value still reachable as a record field." It is **blind** to torn or overwritten bytes sitting in page slack space.

`--thorough` adds a **raw** pass: it encodes the target value the way InnoDB stores it (UTF-8 for strings; big-endian with the sign bit flipped for signed integers) and searches every page's bytes, including slack and unallocated regions (`raw_slack`). This is `O(file)` per value and noisier, so it is opt-in. Without `--thorough`, the summary states plainly that only logical structures were checked.

`fully_purged` is `true` only when no residue site was found in any scanned region.

## How residue scanning works

`--scan-residue --pattern` searches every page for a literal byte needle and reports each hit as `page / page_type / offset / region`, with 32 bytes of surrounding context in hex. Regions are `record_heap`, `free_space` (INDEX pages, at or above `heap_top`), `fil_header`, `fil_trailer`, and `body`.

There is no regex - the needle is literal. Pass UTF-8 text directly, or raw bytes with a `hex:` prefix (`hex:` followed by an even number of hex digits).

> **Note:** SDI metadata pages are zlib-compressed, so scanning for a schema string like a table or column name finds nothing - the bytes are not stored uncompressed. Residue scanning finds values as they appear in record data and slack space.

## JSON output

```json
{
  "table_name": "users",
  "column": "email",
  "target_value": "alice@example.com",
  "fully_purged": false,
  "residue_sites": [
    { "region": "delete_marked", "page_number": 4, "offset": 0, "delete_marked": true, "trx_id": 1875 }
  ],
  "regions_scanned": ["clustered_leaf_live", "clustered_leaf_delete_marked", "free_list"],
  "thorough": false,
  "records_examined": 3
}
```

## Directory-wide scanning

To sweep an entire data directory for residue rather than a single file, use [`inno audit --compliance`](audit.md):

```bash
inno audit -d /var/lib/mysql --compliance --pattern "alice@example.com"
```

See the [GDPR Verification](../guides/gdpr-verification.md) guide for an end-to-end walkthrough.
