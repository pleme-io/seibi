use anyhow::Result;
use clap::{Parser, Subcommand};
use std::process::ExitCode;

mod attic_push;
mod ddns;
mod helm_auth;
mod kubeconfig;
mod metrics;
mod monitor;
mod notify;
mod probe;
mod webhook;

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
        Command::AtticPush(args) => attic_push::run(args).await,
        Command::Notify(args) => notify::run(args).await,
        Command::Monitor(args) => monitor::run(args).await,
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
