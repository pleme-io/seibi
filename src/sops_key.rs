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
    crate::common::default_key_file()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_clean_removes_existing_file() {
        let dir = std::env::temp_dir().join("seibi-test-sops-clean-existing");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let key_file = dir.join("keys.txt");
        fs::write(&key_file, "AGE-SECRET-KEY-1...").unwrap();
        assert!(key_file.exists());

        let result = run_clean(CleanArgs {
            key_file: Some(key_file.clone()),
        });
        assert!(result.is_ok());
        assert!(!key_file.exists());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_clean_succeeds_when_file_absent() {
        let dir = std::env::temp_dir().join("seibi-test-sops-clean-absent");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let key_file = dir.join("nonexistent-key.txt");

        let result = run_clean(CleanArgs {
            key_file: Some(key_file),
        });
        assert!(result.is_ok());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn default_key_file_points_to_sops_dir() {
        let path = default_key_file();
        let s = path.to_string_lossy();
        assert!(s.ends_with(".config/sops/age/keys.txt"), "got: {s}");
    }
}
