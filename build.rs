use clap::CommandFactory;
use clap_mangen::Man;
use std::path::PathBuf;

// Include the CLI definition from the library crate
include!("src/cli/app.rs");

fn main() {
    let target_arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
    if target_arch == "wasm32" {
        return;
    }

    let out_dir =
        PathBuf::from(std::env::var("OUT_DIR").unwrap_or_else(|_| "target/man".to_string()));
    let man_dir = out_dir.join("man");
    std::fs::create_dir_all(&man_dir).unwrap();

    let cmd = Cli::command();

    // Generate main man page
    let mut buf = Vec::new();
    Man::new(cmd.clone()).render(&mut buf).unwrap();
    std::fs::write(man_dir.join("inno.1"), buf).unwrap();

    // Generate subcommand man pages
    for sub in cmd.get_subcommands() {
        let name = format!("inno-{}.1", sub.get_name());
        let mut buf = Vec::new();
        Man::new(sub.clone()).render(&mut buf).unwrap();
        std::fs::write(man_dir.join(&name), buf).unwrap();
    }

    println!("cargo:rerun-if-changed=src/cli/app.rs");
}
