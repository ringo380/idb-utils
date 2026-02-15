# inno dump

Hex dump of raw bytes from an InnoDB tablespace file.

## Synopsis

```text
inno dump -f <file> [-p <page>] [--offset <byte>] [-l <length>] [--raw] [--page-size <size>] [--keyring <path>] [--decrypt]
```

## Description

Produces a hex dump of raw bytes from an InnoDB tablespace file. Operates in two modes:

- **Page mode** (default): Opens the file as a tablespace, reads the page specified by `-p` (or page 0 if omitted), and prints a formatted hex dump with file-relative byte offsets. The dump length defaults to the full page size but can be shortened with `--length`.

- **Offset mode** (`--offset`): Reads bytes starting at an arbitrary absolute file position without page-size awareness. The default read length is 256 bytes. This is useful for inspecting raw structures that do not align to page boundaries (e.g., redo log headers, doublewrite buffer regions).

In either mode, `--raw` suppresses the formatted hex layout and writes the raw binary bytes directly to stdout, suitable for piping into `xxd`, `hexdump`, or other tools.

## Flags

| Flag | Short | Required | Default | Description |
|------|-------|----------|---------|-------------|
| `--file <path>` | `-f` | Yes | -- | Path to the InnoDB data file. |
| `--page <number>` | `-p` | No | `0` | Page number to dump (page mode). |
| `--offset <byte>` | -- | No | -- | Absolute byte offset to start dumping (offset mode). Bypasses page mode. |
| `--length <bytes>` | `-l` | No | Page size (page mode) or 256 (offset mode) | Number of bytes to dump. |
| `--raw` | -- | No | Off | Output raw binary bytes instead of formatted hex dump. |
| `--page-size <size>` | -- | No | Auto-detect | Override page size. |
| `--keyring <path>` | -- | No | -- | Path to MySQL keyring file for decrypting encrypted tablespaces. |
| `--decrypt` | -- | No | Off | Decrypt page before dumping. Requires `--keyring`. |

## Examples

### Dump page 0 (FSP header page)

```bash
inno dump -f actor.ibd
```

### Dump the first 64 bytes of page 3

```bash
inno dump -f actor.ibd -p 3 -l 64
```

### Dump bytes at an absolute file offset

```bash
inno dump -f actor.ibd --offset 49152 -l 128
```

### Raw binary output piped to xxd

```bash
inno dump -f actor.ibd -p 0 --raw | xxd
```

### Dump a decrypted page

```bash
inno dump -f encrypted_table.ibd -p 3 --keyring /path/to/keyring --decrypt
```

### Extract a page to a file

```bash
inno dump -f actor.ibd -p 3 --raw -o page3.bin
```

## Output

The formatted hex dump displays 16 bytes per line with file-relative offsets on the left, hexadecimal byte values in the center, and ASCII character representation on the right:

```text
Hex dump of actor.ibd page 0 (16384 bytes):

00000000  A3 B1 C5 D7 00 00 00 00  FF FF FF FF FF FF FF FF  |................|
00000010  00 00 00 00 00 00 15 A3  00 08 00 00 00 00 00 00  |................|
...
```
