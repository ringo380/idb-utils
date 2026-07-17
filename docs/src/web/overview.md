# Web Analyzer

IDB Utils includes a browser-based InnoDB file analyzer powered by WebAssembly. The web UI provides the same core analysis capabilities as the CLI, running client-side, with no uploads unless you explicitly opt in to share a file.

**Live instance**: [innodb.fyi](https://innodb.fyi/)

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

All file analysis happens locally in your browser via WebAssembly. The WASM module runs the same Rust parsing code as the CLI `inno` binary, compiled to WebAssembly.

Your `.ibd` files are not uploaded to any server as part of normal use. The one exception is deliberate and opt-in: after analyzing a file, you may choose to share it with the maintainers to help fix a bug you hit. That requires ticking an unchecked box **and** clicking Send - the checkbox alone does nothing, consent applies to that one file only, and you are asked again for any other file. Shared files are kept for 90 days, then deleted automatically. They are used only for reproducing bugs, regression test fixtures, and checksum/page-format verification. They are not sold and not shared with anyone else.

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
