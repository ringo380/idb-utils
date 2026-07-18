# GDPR Verification

This guide walks through verifying that a specific value has been deleted from an InnoDB tablespace - the practical core of a "right to erasure" (GDPR Article 17) check at the storage layer.

## What this can and cannot tell you

`inno comply` inspects the tablespace file(s) you give it. It can show you, byte for byte, whether a value still exists in a table's pages: in live rows, in delete-marked rows awaiting purge, in free-list records, in undo entries, and (with `--thorough`) in raw slack space.

It **cannot** see anything outside those files: the running server's buffer pool, replicas, filesystem snapshots, other backups, or binary-log archives. A clean result means "this value is not in this file," not "this value is gone from your estate." Treat it as one piece of evidence, not a compliance certificate.

## Step 1: Confirm the value is present before deletion

Start from a known state. Point `comply` at the table and the value you intend to erase:

```bash
inno comply -f /var/lib/mysql/app/users.ibd \
  --verify-deleted --where email=alice@example.com
```

Before deletion you expect a `live_record` hit:

```text
Deletion verification: users.email = 'alice@example.com'
Scanned: clustered_leaf_live, clustered_leaf_delete_marked, free_list - logical structures only; pass --thorough to also sweep slack space

RESULT: NOT purged - 1 residue site(s) found:
  [live_record] page 12 offset 0
```

This confirms you are checking the right column and value.

## Step 2: Delete, then re-check

After running your `DELETE` and letting InnoDB's purge thread run, re-run the same command. Immediately after a delete you will often see a `delete_marked` site - the row is gone logically but not yet purged:

```text
RESULT: NOT purged - 1 residue site(s) found:
  [delete_marked] page 12 offset 0, delete-marked, trx_id=1875
```

Once purge completes and the space is reused, the logical pass reports:

```text
RESULT: fully purged - no residue found in the scanned regions of this file.
```

## Step 3: Sweep slack space with `--thorough`

The logical pass only inspects record structures. Deleted bytes can linger in page slack space until that space is overwritten. To catch that, add `--thorough`:

```bash
inno comply -f /var/lib/mysql/app/users.ibd \
  --verify-deleted --where email=alice@example.com --thorough
```

This encodes the value (UTF-8 for strings, InnoDB's big-endian sign-flipped form for integers) and scans every page's raw bytes. Each hit is tagged `raw_<region>` for the page region it landed in: a `raw_free_space` hit means the bytes sit in unreferenced slack, while `raw_record_heap` means they overlap a live record heap (so the same value may also appear as a `live_record` from the logical pass). Full erasure at the storage layer means a clean `--thorough` result - which typically requires the page to be rewritten (e.g. `OPTIMIZE TABLE`, or natural reuse) after purge.

## Step 4: Scan across a whole data directory

To check every tablespace at once - useful when a value may have propagated across tables - use the directory-wide residue scan:

```bash
inno audit -d /var/lib/mysql --compliance --pattern "alice@example.com"
```

```text
Data-residue audit for pattern 'alice@example.com' across 42 file(s):
  app/users.ibd: 2 match(es) across 1 page(s)
  app/audit_log.ibd: 1 match(es) across 1 page(s)

3 match(es) in 2 of 42 file(s).
```

This is a raw literal byte scan (no decoding), so it finds the value wherever its bytes appear, in any table. Pass `--json` for a machine-readable report with per-file match counts and sample offsets.

## Interpreting residue regions

| Region | Meaning |
|--------|---------|
| `live_record` | Value is in a current row - not deleted at all |
| `delete_marked` | Deleted but not yet purged; still fully recoverable |
| `free_list` | Purged from the active chain but not overwritten |
| `undo_del_mark` | Present in undo (PK columns only); needs `ibdata1`/undo tablespace |
| `raw_<region>` | Raw byte-pass hit (`--thorough`), tagged with its page region, e.g. `raw_free_space` (slack/unallocated) or `raw_record_heap` (overlaps a live record) |

## A note on encryption

Encryption at rest changes the residue picture: on an encrypted tablespace, plaintext values are not present on disk without the key. Check a file's encryption posture with:

```bash
inno comply -f /var/lib/mysql/app/users.ibd --encryption-audit
```

If the tablespace is encrypted and you do not supply a `--keyring`, a residue scan sees only ciphertext and will not find plaintext values - which is the point of encryption at rest, but also means "no residue found" on an encrypted file without a key is not evidence of deletion.

## See also

- [`inno comply`](../cli/comply.md) - full option reference
- [`inno undelete`](../cli/undelete.md) - the inverse operation (recover deleted rows)
- [`inno audit`](../cli/audit.md) - directory-wide scanning
- [Encrypted Tablespaces](encrypted-tablespaces.md) - working with TDE
