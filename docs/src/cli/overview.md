# inno -- CLI Overview

`inno` is the command-line interface for IDB Utils, an InnoDB file analysis toolkit. It provides 13 subcommands for inspecting, validating, comparing, and manipulating InnoDB tablespace files, redo logs, and system tablespaces.

## Installation

```bash
# From crates.io
cargo install innodb-utils

# From Homebrew
brew install ringo380/tap/inno
```

## Subcommand Reference

| Command | Description |
|---------|-------------|
| [`inno parse`](parse.md) | Parse .ibd file, display page headers and type summary |
| [`inno pages`](pages.md) | Detailed page structure analysis (INDEX, UNDO, LOB, SDI) |
| [`inno dump`](dump.md) | Hex dump of raw page bytes |
| [`inno checksum`](checksum.md) | Validate page checksums (CRC-32C, legacy, MariaDB full_crc32) |
| [`inno diff`](diff.md) | Compare two tablespace files page-by-page |
| [`inno watch`](watch.md) | Monitor a tablespace for page-level changes in real time |
| [`inno corrupt`](corrupt.md) | Intentionally corrupt pages for testing |
| [`inno recover`](recover.md) | Assess page-level recoverability and count salvageable records |
| [`inno find`](find.md) | Search a data directory for pages by number |
| [`inno tsid`](tsid.md) | List or find tablespace IDs |
| [`inno sdi`](sdi.md) | Extract SDI metadata from MySQL 8.0+ tablespaces |
| [`inno log`](log.md) | Analyze InnoDB redo log files |
| [`inno info`](info.md) | Inspect ibdata1, compare LSNs, query MySQL |

## Global Flags

These flags are available on every subcommand:

| Flag | Default | Description |
|------|---------|-------------|
| `--color <auto\|always\|never>` | `auto` | Control colored terminal output. `auto` enables color when stdout is a terminal. |
| `-o, --output <file>` | stdout | Write output to a file instead of printing to stdout. |

## Common Flags

Most subcommands also accept these flags (see individual pages for specifics):

| Flag | Description |
|------|-------------|
| `--json` | Emit structured JSON output instead of human-readable text. All subcommands support this. |
| `-v, --verbose` | Show additional detail (per-page checksums, FSEG internals, MLOG record types, etc.). |
| `--page-size <size>` | Override the auto-detected page size. Accepts `4096`, `8192`, `16384` (default), `32768`, or `65536`. |
| `--keyring <path>` | Path to a MySQL keyring file for decrypting encrypted tablespaces. |

## Common Patterns

### Page size auto-detection

By default, `inno` reads the FSP header flags from page 0 to determine the tablespace page size. If page 0 is corrupt or the file uses a non-standard size, use `--page-size` to override detection.

### JSON output

Every subcommand supports `--json` for machine-readable output. JSON structs are serialized with `serde_json` and optional fields are omitted when empty. This makes `inno` suitable for integration into scripts and pipelines:

```bash
inno checksum -f table.ibd --json | jq '.invalid_pages'
```

### Encrypted tablespaces

For tablespaces encrypted with MySQL's InnoDB tablespace encryption, provide the keyring file with `--keyring`. The tablespace key is extracted from the encryption info on page 0, decrypted using the master key from the keyring, and applied transparently to all page reads. Subcommands that support `--keyring` include `parse`, `pages`, `dump`, `checksum`, `diff`, `watch`, `recover`, and `sdi`.

### Exit codes

- **0** -- Success.
- **1** -- An error occurred, or (for `inno checksum`) invalid checksums were detected.
