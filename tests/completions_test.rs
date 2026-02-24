#![cfg(feature = "cli")]
//! Integration tests for `inno completions`.

use clap::CommandFactory;
use idb::cli::app::Cli;

fn generate_completions(shell: clap_complete::Shell) -> String {
    let mut cmd = Cli::command();
    let mut buf = Vec::new();
    clap_complete::generate(shell, &mut cmd, "inno", &mut buf);
    String::from_utf8(buf).expect("completions should be valid UTF-8")
}

#[test]
fn bash_completions_contain_subcommands() {
    let output = generate_completions(clap_complete::Shell::Bash);
    assert!(!output.is_empty());
    assert!(output.contains("inno"));
    assert!(output.contains("parse"));
    assert!(output.contains("checksum"));
    assert!(output.contains("completions"));
}

#[test]
fn zsh_completions_are_valid() {
    let output = generate_completions(clap_complete::Shell::Zsh);
    assert!(!output.is_empty());
    assert!(output.contains("inno"));
}

#[test]
fn fish_completions_are_valid() {
    let output = generate_completions(clap_complete::Shell::Fish);
    assert!(!output.is_empty());
    assert!(output.contains("inno"));
}

#[test]
fn powershell_completions_are_valid() {
    let output = generate_completions(clap_complete::Shell::PowerShell);
    assert!(!output.is_empty());
    assert!(output.contains("inno"));
}
