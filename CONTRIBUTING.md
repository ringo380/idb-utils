# Contributing to IDB Utils

Thank you for your interest in contributing to `innodb-utils` (`inno`). This guide covers development setup, architecture, conventions, and the contribution workflow.

## Development Setup

**Requirements**: Rust 1.70+ (2021 edition), `cargo`, optionally Node.js for the web UI.

```bash
# Clone and build
git clone https://github.com/ringo380/idb-utils.git
cd idb-utils
cargo build

# Run tests (unit + integration + doc-tests)
cargo test

# Lint (zero warnings enforced)
cargo clippy -- -D warnings

# Format check
cargo fmt --check

# Build optimized binary
cargo build --release

# Build with MySQL query support (requires mysql_async + tokio)
cargo build --release --features mysql

# Security audit (requires cargo-audit)
cargo audit
```

### WASM Build

```bash
# Install wasm-pack if needed
cargo install wasm-pack

# Build WASM package
wasm-pack build --release --target web --no-default-features

# Quick check without full build
cargo check --target wasm32-unknown-unknown --no-default-features
```

### Web UI Development

```bash
cd web
npm ci
npm run dev    # Start dev server
npm run build  # Production build
```

## Code Architecture

The project is organized into three layers plus a web frontend:

### Binary (`inno`)

The CLI entry point lives in `src/main.rs`. It parses arguments with clap derive macros and dispatches to the appropriate subcommand module. CLI definitions (`Cli`, `Commands`, `ColorMode`) are in `src/cli/app.rs`, shared between `main.rs` and `build.rs` via `include!()`.

### Library (`idb`)

Core InnoDB parsing logic in `src/innodb/`:

- **tablespace.rs** -- File I/O, page size detection, page iteration
- **page.rs** -- FIL header/trailer parsing, page type identification
- **checksum.rs** -- CRC-32C, legacy InnoDB, and MariaDB full_crc32 algorithms
- **sdi.rs** -- SDI metadata extraction (MySQL 8.0+), multi-page zlib decompression
- **log.rs** -- Redo log file parsing and block analysis
- **record.rs** -- Record parsing within INDEX pages
- **compression.rs** -- Compressed page handling
- **encryption.rs** -- Encrypted tablespace detection
- **vendor.rs** -- MySQL/Percona/MariaDB identification from FSP flags and redo headers
- **constants.rs** -- InnoDB constants matching MySQL source names

### WASM

`src/wasm.rs` provides thin wrapper functions over the library layer, returning JSON strings via `wasm-bindgen`. Built with `--no-default-features` to exclude filesystem and MySQL dependencies.

### Web UI

`web/` contains a Vite + Tailwind SPA. Components live in `web/src/components/`, shared utilities in `web/src/utils/`.

### Module Organization

```
src/
  main.rs              # Binary entry point, subcommand dispatch
  lib.rs               # Library crate root
  cli/
    app.rs             # Cli, Commands, ColorMode (shared with build.rs)
    mod.rs             # wprintln!/wprint! macros, progress bar helper
    parse.rs           # inno parse
    pages.rs           # inno pages
    dump.rs            # inno dump
    checksum.rs        # inno checksum
    diff.rs            # inno diff
    corrupt.rs         # inno corrupt
    recover.rs         # inno recover
    find.rs            # inno find
    tsid.rs            # inno tsid
    sdi.rs             # inno sdi
    log.rs             # inno log
    info.rs            # inno info
    watch.rs           # inno watch
  innodb/              # Core parsing library
  util/
    hex.rs             # Hex dump formatting
    mysql.rs           # MySQL connection (feature-gated)
    fs.rs              # Directory traversal helpers
  wasm.rs              # WASM bindings
```

## Key Patterns

### Writer Pattern

Every subcommand accepts a writer for testability:

```rust
pub fn execute(opts: &Options, writer: &mut dyn Write) -> Result<(), IdbError> {
    // ...
}
```

In `main.rs`, this is called with `&mut std::io::stdout()`. In tests, use `Vec<u8>` as the writer to capture output.

### Output Macros

`wprintln!` and `wprint!` are wrappers around `writeln!`/`write!` that convert `std::io::Error` into `IdbError`:

```rust
wprintln!(writer, "Page {}: type={}", page_num, page_type);
```

### Clap Derive

Each subcommand defines an `Options` struct with clap derive attributes:

```rust
#[derive(Parser)]
pub struct Options {
    /// Path to the .ibd file
    #[arg(short, long)]
    pub file: PathBuf,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,
}
```

### Binary Parsing

All binary parsing uses `byteorder::BigEndian` for InnoDB's big-endian format:

```rust
use byteorder::{BigEndian, ReadBytesExt};
let page_number = cursor.read_u32::<BigEndian>()?;
```

### Error Handling

A single `IdbError` enum (defined with `thiserror`) covers all error cases:

- `Io` -- wraps `std::io::Error`
- `Parse` -- invalid data, unexpected values
- `Argument` -- invalid CLI arguments or combinations

### Constants

Constants use `UPPERCASE_WITH_UNDERSCORES` and match MySQL/InnoDB source names exactly (from `fil0fil.h`, `page0page.h`, `fsp0fsp.h`, etc.):

```rust
pub const FIL_PAGE_DATA: usize = 38;
pub const FIL_PAGE_INDEX: u16 = 17855;
```

## Adding a New Subcommand

1. **Create the module**: Add `src/cli/newcmd.rs` with an `Options` struct (clap derive) and a `pub fn execute(opts: &Options, writer: &mut dyn Write) -> Result<(), IdbError>` function.

2. **Add the command variant**: In `src/cli/app.rs`, add a variant to the `Commands` enum:
   ```rust
   /// Description of what the command does
   Newcmd(cli::newcmd::Options),
   ```

3. **Add dispatch**: In `src/main.rs`, add a match arm:
   ```rust
   Commands::Newcmd(opts) => cli::newcmd::execute(&opts, &mut writer),
   ```

4. **Register the module**: Add `pub mod newcmd;` to `src/cli/mod.rs`.

5. **Write unit tests**: Add a `#[cfg(test)]` module at the bottom of `src/cli/newcmd.rs`.

6. **Write integration tests**: Add test files in `tests/` that exercise the command end-to-end.

7. **Update CHANGELOG.md**: Document the new subcommand under the appropriate version heading.

## Testing

### Unit Tests

Inline `#[cfg(test)]` modules in each source file. Use the writer pattern to capture and assert on output:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_something() {
        let mut output = Vec::new();
        execute(&opts, &mut output).unwrap();
        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("expected text"));
    }
}
```

### Integration Tests

Located in `tests/`. Build synthetic `.ibd` files using `tempfile`, `byteorder`, and CRC-32C checksums to create valid test data without requiring real database files.

### Required Checks

```bash
# All of these must pass before submitting a PR
cargo test
cargo clippy -- -D warnings
cargo fmt --check

# Non-blocking but recommended
cargo audit

# WASM compatibility
cargo check --target wasm32-unknown-unknown --no-default-features

# Web UI build
cd web && npm run build
```

### Testing Gotchas

- `Tablespace` and `LogFile` do not derive `Debug` (they hold `Box<dyn ReadSeek>`). You cannot use `.unwrap_err()` in tests; use `match` instead.
- Always test against real-world `.ibd` files from multiple MySQL versions when possible, not just synthetic data.

## CI/CD

Three GitHub Actions workflows:

### `ci.yml` (on push/PR)

- Format check (`cargo fmt --check`)
- Tests (`cargo test`)
- Clippy lint (`cargo clippy -- -D warnings`)
- Security audit (`cargo audit`, non-blocking)
- Build on Ubuntu and macOS
- Build with MySQL feature (`cargo build --features mysql`)
- WASM compatibility check (`cargo check --target wasm32-unknown-unknown --no-default-features`)

### `pages.yml` (on push to master)

- Builds the WASM package
- Builds the web UI
- Deploys to GitHub Pages

### `release.yml` (on `v*` tag push)

- Builds optimized binaries for 4 targets:
  - `x86_64-unknown-linux-gnu`
  - `aarch64-unknown-linux-gnu`
  - `x86_64-apple-darwin`
  - `aarch64-apple-darwin`
- Creates a GitHub release with the built artifacts
- Dispatches a `repository_dispatch` event to the Homebrew tap repo (`ringo380/homebrew-tap`) to update the formula

## Pull Request Guidelines

- Keep PRs focused on a single change or feature.
- Ensure all CI checks pass (tests, clippy, format).
- Add tests for new functionality -- both unit and integration where appropriate.
- Update `CHANGELOG.md` with a description of the change.
- Keep commits clean and well-described.
- All subcommands must support `--json` output via `#[derive(Serialize)]` structs.

## Release Process

1. Bump version in `Cargo.toml`.
2. Update `CHANGELOG.md` with the new version and changes.
3. Commit the version bump.
4. Push to `master`.
5. Create and push a tag: `git tag vX.Y.Z && git push origin vX.Y.Z`.
6. GitHub Actions builds binaries, creates the release, and updates the Homebrew tap automatically.
7. Publish to crates.io: `cargo publish --allow-dirty` (Cargo.lock may change from the build).

## Code Style

- **Constants**: `UPPERCASE_WITH_UNDERSCORES`, matching InnoDB/MySQL source names exactly.
- **Subcommand pattern**: `Options` struct (clap derive) + `execute()` function per subcommand.
- **Zero warnings**: `cargo clippy -- -D warnings` must pass with no warnings.
- **Edition**: Rust 2021.
- **JSON output**: All output structs derive `Serialize`. Use `#[serde(skip_serializing_if = "...")]` for optional fields.
- **Error handling**: Use the `IdbError` enum. Do not introduce new error types.
- **Formatting**: Run `cargo fmt` before committing.
