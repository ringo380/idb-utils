# inno completions

Generate shell completion scripts for `inno` subcommands, options, and arguments.

## Usage

```bash
# Generate completions for your shell
inno completions bash
inno completions zsh
inno completions fish
inno completions powershell
```

## Supported Shells

| Shell | Description |
|-------|-------------|
| `bash` | Bash completions (source in `.bashrc` or `/etc/bash_completion.d/`) |
| `zsh` | Zsh completions (place in `$fpath` directory) |
| `fish` | Fish completions (place in `~/.config/fish/completions/`) |
| `powershell` | PowerShell completions |

## Installation

### Bash

```bash
inno completions bash > /etc/bash_completion.d/inno
# or for current session:
source <(inno completions bash)
```

### Zsh

```bash
inno completions zsh > "${fpath[1]}/_inno"
# Rebuild completion cache:
rm -f ~/.zcompdump && compinit
```

### Fish

```bash
inno completions fish > ~/.config/fish/completions/inno.fish
```

### PowerShell

```powershell
inno completions powershell >> $PROFILE
```
