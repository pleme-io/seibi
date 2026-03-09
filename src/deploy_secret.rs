use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::ExitCode;
use tracing::info;

#[derive(ClapArgs)]
pub struct Args {
    /// Source file path
    #[arg(long)]
    source: PathBuf,

    /// Destination file path
    #[arg(long)]
    dest: PathBuf,

    /// File mode in octal (e.g., 0600)
    #[arg(long, default_value = "0600")]
    mode: String,

    /// Owner in user:group format (runs chown)
    #[arg(long)]
    owner: Option<String>,
}

pub async fn run(args: Args) -> Result<ExitCode> {
    if let Some(parent) = args.dest.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating directory {}", parent.display()))?;
    }

    fs::copy(&args.source, &args.dest).with_context(|| {
        format!(
            "copying {} → {}",
            args.source.display(),
            args.dest.display()
        )
    })?;

    let mode = u32::from_str_radix(args.mode.trim_start_matches('0'), 8)
        .with_context(|| format!("parsing mode '{}'", args.mode))?;
    fs::set_permissions(&args.dest, fs::Permissions::from_mode(mode))
        .with_context(|| format!("chmod {} {}", args.mode, args.dest.display()))?;

    if let Some(ref owner) = args.owner {
        let status = std::process::Command::new("chown")
            .arg(owner)
            .arg(&args.dest)
            .status()
            .with_context(|| format!("running chown {owner} {}", args.dest.display()))?;
        if !status.success() {
            anyhow::bail!("chown {owner} {} failed", args.dest.display());
        }
    }

    info!(
        source = %args.source.display(),
        dest = %args.dest.display(),
        mode = %args.mode,
        owner = ?args.owner,
        "secret deployed"
    );
    Ok(ExitCode::SUCCESS)
}
