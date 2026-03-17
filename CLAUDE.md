# Seibi (整備) — Infrastructure Maintenance Toolkit

## Build & Test

```bash
cargo build
cargo test --lib
```

## Architecture

Multi-command infrastructure toolkit for NixOS and nix-darwin systems.
Each subcommand handles a specific operational task.

### Commands

| Command | Purpose |
|---------|---------|
| `ddns` | Update Cloudflare DNS with current public IP |
| `kubeconfig` | Export K3s kubeconfig with detected node IP |
| `helm-auth` | Generate Helm OCI registry auth config |
| `attic-push` | Push Nix store paths to Attic binary cache |
| `notify` | Send one-shot event notification via webhook |
| `monitor` | Run continuous monitoring daemon |
| `deploy-secret` | Deploy a secret file with correct permissions |
| `sops-key` | Manage SOPS age key (sync from 1Password / clean) |
| `sops-edit` | Edit SOPS-encrypted secrets (auto-provisions age key) |
| `auto-unlock` | Enroll TPM2 for automatic LUKS unlocking |
| `spotlight-sync` | Sync nix-managed apps to Spotlight via macOS aliases |

### Removed Commands

- `cluster-init` and `cluster-launch` — superseded by **kikai** (standalone cluster lifecycle orchestrator)

## Design Decisions

- **Edition 2024**, rust-version 1.89.0
- **No tempfile dependency** (removed with cluster commands)
- **tracing** with `--json` flag for systemd journal compatibility
- **spotlight-sync** uses osascript (AppleScript) for macOS Finder alias creation
