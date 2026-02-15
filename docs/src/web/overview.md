# Web Analyzer

IDB Utils includes a browser-based InnoDB file analyzer powered by WebAssembly. The web UI provides the same core analysis capabilities as the CLI, running entirely client-side with no server uploads.

**Live instance**: [ringo380.github.io/idb-utils](https://ringo380.github.io/idb-utils/)

## What It Does

The web analyzer lets you drag and drop `.ibd` tablespace files or redo log files directly into your browser for instant analysis:

- **Parse** tablespace files and view page headers, type summaries, and FIL header details
- **Validate checksums** across all pages (CRC-32C, legacy InnoDB, MariaDB full_crc32)
- **Inspect page structure** with deep analysis of INDEX, UNDO, BLOB/LOB, and SDI pages
- **Hex dump** individual pages with offset/hex/ASCII formatting
- **Extract SDI** metadata from MySQL 8.0+ tablespaces
- **Assess recovery** potential of damaged tablespaces
- **Compare** two tablespace files page-by-page
- **Analyze redo logs** with header, checkpoint, and block detail

## Privacy

All file processing happens locally in your browser via WebAssembly. Your `.ibd` files are never uploaded to any server. The WASM module runs the same Rust parsing code as the CLI `inno` binary, compiled to WebAssembly.

## Technology

- **WASM**: Rust library compiled with `wasm-pack` to WebAssembly
- **Frontend**: Vite + Tailwind CSS single-page application
- **Source**: `web/` directory in the repository
- **Bindings**: `src/wasm.rs` provides thin wrappers over the library API, returning JSON strings via `wasm-bindgen`

## Limitations

- No filesystem access — files must be loaded via drag-and-drop or file picker
- No encryption support — the `--keyring` option is CLI-only
- Large files may be slow to process depending on browser memory limits
- No `watch` mode — real-time monitoring requires the CLI
