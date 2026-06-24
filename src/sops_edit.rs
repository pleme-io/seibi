use anyhow::{Context, Result, bail};
use clap::Args as ClapArgs;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use tracing::info;

#[derive(ClapArgs)]
pub struct Args {
    /// File to edit (default: `<git-root>/secrets.yaml`)
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

fn default_key_file() -> PathBuf {
    crate::common::default_key_file()
}

fn find_git_root() -> Option<PathBuf> {
    crate::common::find_git_root()
}

/// Edit the repo's SOPS secrets as a CLOSED operation: resolve the target
/// file, strictly preflight EVERY environmental requirement (the file is a
/// SOPS file, `sops` + `op` are present, `op` is authenticated when
/// provisioning, the age key exists-or-is-provisioned and is an age key, the
/// key actually DECRYPTS this file, `$EDITOR` is set) — each failing early
/// with an actionable message — and only then hand off to `sops`. Nothing is
/// left to ambient chance; the result is a function of (file, key, op_item).
pub async fn run(args: Args) -> Result<ExitCode> {
    let key_file = args.key_file.unwrap_or_else(default_key_file);

    // Resolve the target. Default = the git root's `secrets.yaml`. (NOT
    // `nix/secrets.yaml` — that stale join doubled to `nix/nix/secrets.yaml`
    // when run from inside the nix repo, which is itself the git root.)
    let file = match args.file {
        Some(f) => f,
        None => find_git_root()
            .context("not inside a git repository — pass the secrets file explicitly")?
            .join("secrets.yaml"),
    };

    preflight(&file, &key_file, &args.op_item).await?;

    info!(file = %file.display(), "preflight passed — opening with sops");
    let err = Command::new("sops")
        .arg(&file)
        .env("SOPS_AGE_KEY_FILE", &key_file)
        .exec();

    // exec() only returns on error.
    Err(err).context("exec sops")
}

/// Strictly verify every requirement of the edit, failing early. Each check
/// names the requirement and how to satisfy it.
async fn preflight(file: &Path, key_file: &Path, op_item: &str) -> Result<()> {
    // R1 — the target exists.
    if !file.exists() {
        bail!("secrets file not found: {}", file.display());
    }

    // R2 — the target is actually SOPS-encrypted (refuse to "edit" plaintext).
    let contents =
        fs::read_to_string(file).with_context(|| format!("reading {}", file.display()))?;
    if !contents.lines().any(|l| l.trim_start().starts_with("sops:")) {
        bail!(
            "{} has no `sops:` metadata block — it is not a SOPS-encrypted file; refusing to edit",
            file.display()
        );
    }

    // R3 — sops is on PATH.
    require_tool("sops", "install sops or add it to PATH")?;

    // R4 — the age key exists, else provision it from 1Password (which checks
    // that `op` is present + authenticated).
    if !key_file.exists() {
        info!(key_file = %key_file.display(), "age key absent — provisioning from 1Password");
        provision_age_key(key_file, op_item).await?;
    }

    // R5 — the key file is readable + actually an age secret key.
    let key = fs::read_to_string(key_file)
        .with_context(|| format!("reading age key {}", key_file.display()))?;
    if !key.contains("AGE-SECRET-KEY-") {
        bail!(
            "age key file {} contains no `AGE-SECRET-KEY-` entry — empty or wrong file",
            key_file.display()
        );
    }

    // R6 — $EDITOR is set (sops opens it interactively).
    if std::env::var_os("EDITOR").is_none() {
        bail!("$EDITOR is not set — sops needs it to open the secrets file");
    }

    // R7 — the strongest check: the key actually DECRYPTS this file. Catches a
    // rotated/foreign key now, instead of a confusing re-encrypt failure when
    // the editor saves.
    let out = Command::new("sops")
        .arg("--decrypt")
        .arg(file)
        .env("SOPS_AGE_KEY_FILE", key_file)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .context("running `sops --decrypt` for the key preflight")?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        bail!(
            "the age key at {} cannot decrypt {} — its public recipient is not among the \
             file's recipients (rotated or wrong key?).\nsops said:\n{}",
            key_file.display(),
            file.display(),
            stderr.trim()
        );
    }

    Ok(())
}

/// Provision the age key from a 1Password item. Checks `op` is present +
/// authenticated, failing early with `op signin` guidance.
async fn provision_age_key(key_file: &Path, op_item: &str) -> Result<()> {
    require_tool("op", "install the 1Password CLI or add it to PATH")?;

    if let Some(parent) = key_file.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating directory {}", parent.display()))?;
    }

    let output = tokio::process::Command::new("op")
        .args(["read", op_item])
        .output()
        .await
        .context("running 1Password CLI (op read)")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "`op read {op_item}` failed — is 1Password signed in on this host? try `op signin`.\n\
             op said: {}",
            stderr.trim()
        );
    }

    fs::write(key_file, &output.stdout)
        .with_context(|| format!("writing key to {}", key_file.display()))?;
    fs::set_permissions(key_file, fs::Permissions::from_mode(0o600))?;
    info!(key_file = %key_file.display(), "age key provisioned from 1Password");
    Ok(())
}

/// Fail early unless `tool` runs on PATH (`tool --version` exits 0).
fn require_tool(tool: &str, how: &str) -> Result<()> {
    let ok = Command::new(tool)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if !ok {
        bail!("`{tool}` is not on PATH (or not runnable) — {how}");
    }
    Ok(())
}
