use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::ExitCode;
use tracing::info;

#[derive(ClapArgs)]
pub struct Args {
    /// File to edit (default: <git-root>/nix/secrets.yaml)
    file: Option<PathBuf>,

    /// 1Password item reference for age key auto-provisioning
    #[arg(
        long,
        env = "SOPS_OP_ITEM",
        default_value = "op://Moura family/agekey/notesPlain"
    )]
    op_item: String,

    /// Age key file path
    #[arg(long, env = "SOPS_AGE_KEY_FILE")]
    key_file: Option<PathBuf>,
}

pub async fn run(args: Args) -> Result<ExitCode> {
    let key_file = args
        .key_file
        .unwrap_or_else(crate::common::default_key_file);

    // Auto-provision age key from 1Password if missing
    if !key_file.exists() {
        info!("age key not found — fetching from 1Password");

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
        fs::set_permissions(&key_file, fs::Permissions::from_mode(0o600))?;

        info!(key_file = %key_file.display(), "key provisioned from 1Password");
    }

    // Resolve target file
    let file = args.file.unwrap_or_else(|| {
        let root = crate::common::find_git_root().unwrap_or_else(|| PathBuf::from("."));
        root.join("nix/secrets.yaml")
    });

    if !file.exists() {
        anyhow::bail!("file not found: {}", file.display());
    }

    info!(file = %file.display(), "opening with sops");

    // exec sops — replaces the current process
    use std::os::unix::process::CommandExt;
    let err = std::process::Command::new("sops")
        .arg(&file)
        .env("SOPS_AGE_KEY_FILE", &key_file)
        .exec();

    // exec() only returns on error
    Err(err).context("exec sops")
}
