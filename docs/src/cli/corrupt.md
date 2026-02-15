# inno corrupt

Intentionally corrupt pages in an InnoDB tablespace file for testing.

## Synopsis

```text
inno corrupt -f <file> [-p <page>] [-b <bytes>] [-k] [-r] [--offset <byte>] [--verify] [--json] [--page-size <size>]
```

## Description

Writes random bytes into a tablespace file to simulate data corruption. This is designed for testing checksum validation (`inno checksum`), InnoDB crash recovery, and backup-restore verification workflows.

> **Warning**: This command modifies the target file in place. Always work on a copy of the tablespace, never on a production file.

Three targeting modes are available:

- **Header mode** (`-k`): Writes into the 38-byte FIL header area (bytes 0-37 of the page), which will corrupt page metadata like the checksum, page number, LSN, or space ID.

- **Records mode** (`-r`): Writes into the user data area (after the page header and before the FIL trailer), corrupting actual row or index data without necessarily invalidating the stored checksum.

- **Offset mode** (`--offset`): Writes at an absolute file byte position, bypassing page calculations entirely. Note that `--verify` is unavailable in this mode since there is no page context.

If no page number is specified, one is chosen at random. If neither `-k` nor `-r` is specified, bytes are written at the beginning of the page.

## Flags

| Flag | Short | Required | Default | Description |
|------|-------|----------|---------|-------------|
| `--file <path>` | `-f` | Yes | -- | Path to the InnoDB data file (.ibd). |
| `--page <number>` | `-p` | No | Random | Page number to corrupt. If omitted, a random page is chosen. |
| `--bytes <count>` | `-b` | No | `1` | Number of random bytes to write. |
| `--header` | `-k` | No | Off | Target the FIL header area (first 38 bytes of the page). |
| `--records` | `-r` | No | Off | Target the record data area (after page header, before trailer). |
| `--offset <byte>` | -- | No | -- | Absolute byte offset to corrupt. Bypasses page calculation. |
| `--verify` | -- | No | Off | Show before/after checksum comparison to confirm corruption. |
| `--json` | -- | No | Off | Output in JSON format. |
| `--page-size <size>` | -- | No | Auto-detect | Override page size. |

## Examples

### Corrupt 1 byte on a random page

```bash
inno corrupt -f test_copy.ibd
```

### Corrupt 16 bytes in the header of page 3

```bash
inno corrupt -f test_copy.ibd -p 3 -k -b 16
```

### Corrupt the record area of page 5 and verify

```bash
inno corrupt -f test_copy.ibd -p 5 -r -b 32 --verify
```

### Corrupt at an absolute file offset

```bash
inno corrupt -f test_copy.ibd --offset 65536 -b 8
```

### JSON output for automation

```bash
inno corrupt -f test_copy.ibd -p 3 -b 4 --verify --json
```

### Test-and-validate workflow

```bash
cp actor.ibd test.ibd
inno corrupt -f test.ibd -p 3 -b 4 --verify
inno checksum -f test.ibd
```

## Output

### Text Mode

```text
Writing 16 bytes of random data to test.ibd at offset 49152 (page 3)...
Data written: A3 B1 C5 D7 E2 F0 11 22 33 44 55 66 77 88 99 AA
Completed.
```

With `--verify`:

```text
Verification:
  Before: OK (algorithm=Crc32c, stored=2741936599, calculated=2741936599)
  After:  INVALID (algorithm=Crc32c, stored=2741936599, calculated=1084227091)
```

### JSON Mode

```json
{
  "file": "test.ibd",
  "offset": 49152,
  "page": 3,
  "bytes_written": 16,
  "data": "A3 B1 C5 D7 E2 F0 11 22 33 44 55 66 77 88 99 AA",
  "verify": {
    "page": 3,
    "before": {
      "valid": true,
      "algorithm": "crc32c",
      "stored_checksum": 2741936599,
      "calculated_checksum": 2741936599
    },
    "after": {
      "valid": false,
      "algorithm": "crc32c",
      "stored_checksum": 2741936599,
      "calculated_checksum": 1084227091
    }
  }
}
```
