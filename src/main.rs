use anyhow::Result;
use clap::{Parser, Subcommand};
use std::process::ExitCode;

mod attic_push;
mod auto_unlock;
mod cluster_secrets;
mod ddns;
mod deploy_secret;
mod helm_auth;
mod kubeconfig;
mod metrics;
mod monitor;
mod notify;
mod probe;
mod rust_cleanup;
mod sops_edit;
mod sops_key;
mod spotlight_sync;
mod webhook;
mod wg_supervisor;

#[derive(Parser)]
#[command(name = "seibi", version, about = "Infrastructure maintenance toolkit")]
struct Cli {
    /// Enable JSON log output (for systemd journal)
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Update Cloudflare DNS with current public IP
    Ddns(ddns::Args),
    /// Export K3s kubeconfig with detected node IP
    Kubeconfig(kubeconfig::Args),
    /// Generate Helm OCI registry auth config
    HelmAuth(helm_auth::Args),
    /// Push Nix store paths to Attic binary cache
    AtticPush(attic_push::Args),
    /// Send one-shot event notification via webhook
    Notify(notify::Args),
    /// Run continuous monitoring daemon
    Monitor(monitor::Args),
    /// Extract cluster bootstrap secrets from SOPS (outputs eval-able exports)
    ClusterSecrets(cluster_secrets::Args),
    /// Deploy a secret file with correct permissions and ownership
    DeploySecret(deploy_secret::Args),
    /// Manage SOPS age key (sync from 1Password / clean)
    SopsKey(sops_key::Args),
    /// Edit SOPS-encrypted secrets (auto-provisions age key)
    SopsEdit(sops_edit::Args),
    /// Enroll TPM2 for automatic LUKS unlocking
    AutoUnlock(auto_unlock::Args),
    /// Sync nix-managed apps to Spotlight via macOS aliases
    SpotlightSync(spotlight_sync::Args),
    /// Clean Rust target/ directories and cargo cache to reclaim disk space
    RustCleanup(rust_cleanup::Args),
    /// Long-running `WireGuard` tunnel supervisor (key wait, health, auto-restart)
    WgSupervisor(wg_supervisor::Args),
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    init_tracing(cli.json);

    match run(cli.command).await {
        Ok(code) => code,
        Err(e) => {
            tracing::error!(error = %e, "fatal");
            ExitCode::FAILURE
        }
    }
}

async fn run(cmd: Command) -> Result<ExitCode> {
    match cmd {
        Command::Ddns(args) => ddns::run(args).await,
        Command::Kubeconfig(args) => kubeconfig::run(args).await,
        Command::HelmAuth(args) => helm_auth::run(args),
        Command::AtticPush(args) => attic_push::run(args),
        Command::Notify(args) => notify::run(args).await,
        Command::Monitor(args) => monitor::run(args).await,
        Command::ClusterSecrets(args) => cluster_secrets::run(args).await,
        Command::DeploySecret(args) => deploy_secret::run(args),
        Command::SopsKey(args) => sops_key::run(args).await,
        Command::SopsEdit(args) => sops_edit::run(args).await,
        Command::AutoUnlock(args) => auto_unlock::run(args),
        Command::SpotlightSync(args) => spotlight_sync::run(args).await,
        Command::RustCleanup(args) => rust_cleanup::run(args),
        Command::WgSupervisor(args) => wg_supervisor::run(args).await,
    }
}

fn init_tracing(json: bool) {
    use tracing_subscriber::{fmt, EnvFilter};

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    if json {
        fmt().json().with_env_filter(filter).init();
    } else {
        fmt().with_env_filter(filter).init();
    }
}
