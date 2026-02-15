# Installation

There are several ways to install `inno`, depending on your platform and preferences.

## From crates.io

If you have the Rust toolchain installed, the simplest method is:

```bash
cargo install innodb-utils
```

This compiles and installs the `inno` binary into `~/.cargo/bin/`.

## Homebrew (macOS and Linux)

```bash
brew install ringo380/tap/inno
```

The Homebrew formula is updated automatically with each release.

## Pre-built Binaries

Pre-built binaries are available from [GitHub Releases](https://github.com/ringo380/idb-utils/releases) for the following targets:

| Platform | Architecture |
|----------|-------------|
| Linux    | x86\_64     |
| Linux    | aarch64     |
| macOS    | x86\_64     |
| macOS    | aarch64     |

Download the appropriate archive, extract it, and place the `inno` binary somewhere on your `PATH`.

## From Source

```bash
git clone https://github.com/ringo380/idb-utils.git
cd idb-utils
cargo build --release
```

The compiled binary will be at `target/release/inno`. Copy it to a directory on your `PATH`:

```bash
cp target/release/inno /usr/local/bin/
```

### With MySQL Support

The `inno info` subcommand can optionally query a live MySQL instance to compare runtime state against tablespace files. This requires the `mysql` feature flag:

```bash
cargo build --release --features mysql
```

This pulls in `mysql_async` and `tokio` as additional dependencies. Without this feature, all other subcommands work normally -- only `inno info` MySQL query mode is gated.

## Requirements

- **Source builds**: Rust 1.70 or later.
- **Pre-built binaries**: No dependencies required.
- **Homebrew**: Homebrew handles all dependencies automatically.

## Verify Installation

After installing, confirm that `inno` is available:

```bash
inno --version
```

To see the full list of subcommands and options:

```bash
inno --help
```
