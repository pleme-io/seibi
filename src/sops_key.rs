use anyhow::{Context, Result};
use clap::{Args as ClapArgs, Subcommand};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::ExitCode;
use tracing::info;

#[derive(ClapArgs)]
pub struct Args {
    #[command(subcommand)]
    command: SopsKeyCommand,
}

#[derive(Subcommand)]
enum SopsKeyCommand {
    /// Provision SOPS age key from 1Password
    Sync(SyncArgs),
    /// Remove local SOPS age key file
    Clean(CleanArgs),
}

#[derive(ClapArgs)]
struct SyncArgs {
    /// 1Password item reference
    #[arg(
        long,
        env = "SOPS_OP_ITEM",
        default_value = "op://Moura family/agekey/notesPlain"
    )]
    op_item: String,

    /// Destination key file path
    #[arg(long, env = "SOPS_AGE_KEY_FILE")]
    key_file: Option<PathBuf>,
}

#[derive(ClapArgs)]
struct CleanArgs {
    /// Key file to remove
    #[arg(long, env = "SOPS_AGE_KEY_FILE")]
    key_file: Option<PathBuf>,
}

fn default_key_file() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    PathBuf::from(home).join(".config/sops/age/keys.txt")
}

/// Dispatch to sync (provision from 1Password) or clean (remove) subcommand.
pub async fn run(args: Args) -> Result<ExitCode> {
    match args.command {
        SopsKeyCommand::Sync(a) => run_sync(a).await,
        SopsKeyCommand::Clean(a) => run_clean(a),
    }
}

async fn run_sync(args: SyncArgs) -> Result<ExitCode> {
    let key_file = args.key_file.unwrap_or_else(default_key_file);

    if let Some(parent) = key_file.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating directory {}", parent.display()))?;
    }

    let output = tokio::process::Command::new("op")
        .args(["read", &args.op_item])
        .output()
        .await
        .context("running 1Password CLI (op read)")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("op read failed: {stderr}");
    }

    fs::write(&key_file, &output.stdout)
        .with_context(|| format!("writing key to {}", key_file.display()))?;
    fs::set_permissions(&key_file, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("chmod 600 {}", key_file.display()))?;

    info!(key_file = %key_file.display(), "SOPS age key provisioned from 1Password");
    Ok(ExitCode::SUCCESS)
}

fn run_clean(args: CleanArgs) -> Result<ExitCode> {
    let key_file = args.key_file.unwrap_or_else(default_key_file);

    if key_file.exists() {
        fs::remove_file(&key_file)
            .with_context(|| format!("removing {}", key_file.display()))?;
        info!(key_file = %key_file.display(), "SOPS age key removed");
    } else {
        info!(key_file = %key_file.display(), "no key file found");
    }
    Ok(ExitCode::SUCCESS)
}
