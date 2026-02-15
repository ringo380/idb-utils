# Encrypted Tablespaces

`inno` can read encrypted InnoDB tablespaces when provided with the MySQL keyring file.

## Basic Usage

Pass the `--keyring` option to any subcommand that reads tablespace pages:

```bash
inno parse -f encrypted.ibd --keyring /var/lib/mysql-keyring/keyring
inno pages -f encrypted.ibd --keyring /var/lib/mysql-keyring/keyring
inno sdi -f encrypted.ibd --keyring /var/lib/mysql-keyring/keyring --pretty
```

The `--keyring` option works with: `parse`, `pages`, `dump`, `checksum`, `recover`, `sdi`, `diff`, and `watch`.

## Hex Dump with Decryption

For the `dump` subcommand, add `--decrypt` along with `--keyring` to see decrypted page content:

```bash
inno dump -f encrypted.ibd -p 3 --keyring /path/keyring --decrypt
```

Without `--decrypt`, the hex dump shows raw (encrypted) bytes.

## Supported Keyring Format

**Legacy `keyring_file` plugin** (binary format, MySQL 5.7.11+) is supported. This is the file created by the `keyring_file` plugin, typically located at `/var/lib/mysql-keyring/keyring`.

The newer `component_keyring_file` (JSON format, MySQL 8.0.34+) is **not yet supported**.

## How Encryption Works

MySQL uses a two-tier key architecture:

1. **Master key** -- stored in the keyring file, identified by a key ID embedded in the tablespace
2. **Per-tablespace key and IV** -- stored on page 0 of the tablespace, encrypted with the master key
3. **Page encryption** -- each page body is encrypted with AES-256-CBC using the per-tablespace key and IV

When `inno` opens an encrypted tablespace with `--keyring`:
- It reads the encryption info from page 0
- Looks up the master key by ID in the keyring file
- Decrypts the per-tablespace key and IV
- Decrypts individual pages on demand

## Troubleshooting

**"no encryption info on page 0"**

The tablespace either is not encrypted or uses a different encryption method. Verify that the table was created with `ENCRYPTION='Y'` or that the tablespace has encryption enabled.

**Wrong keyring file**

If the master key ID stored in the tablespace does not match any key in the provided keyring file, decryption will fail. Make sure you are using the keyring file from the same MySQL instance that encrypted the tablespace.

**MariaDB encrypted tablespaces**

MariaDB uses per-page encryption with a different on-disk format (page type 37401). This is distinct from MySQL's tablespace-level encryption. MariaDB keyring decryption is not supported by `inno`.

**Rotated master keys**

If the master key was rotated with `ALTER INSTANCE ROTATE INNODB MASTER KEY`, the keyring file must contain the current master key. Older keys may have been removed during rotation.

## Identifying Encrypted Pages

Use `inno parse` to identify encrypted pages by their page type:

```bash
inno parse -f encrypted.ibd --json | jq '[.[] | select(.page_type == "ENCRYPTED")]'
```

On encrypted tablespaces without the keyring, pages will show as type `ENCRYPTED` (15), `COMPRESSED_ENCRYPTED` (16), or `ENCRYPTED_RTREE` (17). With the correct keyring, `inno` decrypts them transparently and reports their actual underlying page type.
