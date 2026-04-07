use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::path::PathBuf;
use std::process::{Command, ExitCode, Stdio};
use tracing::{info, warn};

#[derive(ClapArgs)]
pub struct Args {
    /// Path to Attic JWT token file
    #[arg(long, env = "SEIBI_ATTIC_TOKEN_FILE")]
    token_file: PathBuf,

    /// Attic cache name
    #[arg(long, env = "SEIBI_ATTIC_CACHE_NAME")]
    cache_name: String,

    /// Attic server URL
    #[arg(long, env = "SEIBI_ATTIC_CACHE_URL")]
    cache_url: String,

    /// Parallel push jobs
    #[arg(long, default_value = "8")]
    jobs: u32,

    /// Attic server alias for login
    #[arg(long, default_value = "nexus")]
    server_name: String,
}

pub async fn run(args: Args) -> Result<ExitCode> {
    let token = crate::common::read_trimmed_file(&args.token_file)?;

    // Login
    let login = Command::new("attic")
        .args(["login", &args.server_name, &args.cache_url, &token])
        .status()
        .context("running attic login")?;

    if !login.success() {
        warn!("attic login failed, skipping");
        return Ok(ExitCode::from(2));
    }

    // Check cache reachability
    let check = Command::new("attic")
        .args(["cache", "info", &args.cache_name])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("checking attic cache")?;

    if !check.success() {
        warn!(cache = %args.cache_name, "cache unreachable, skipping");
        return Ok(ExitCode::from(2));
    }

    // Push: nix path-info --all | attic push <cache> --stdin
    info!(cache = %args.cache_name, jobs = args.jobs, "pushing store paths...");

    let mut nix = Command::new("nix")
        .args(["path-info", "--all"])
        .stdout(Stdio::piped())
        .spawn()
        .context("spawning nix path-info")?;

    let nix_stdout = nix
        .stdout
        .take()
        .context("capturing nix path-info stdout")?;

    let push = Command::new("attic")
        .args([
            "push",
            &args.cache_name,
            "--stdin",
            "--jobs",
            &args.jobs.to_string(),
        ])
        .stdin(nix_stdout)
        .status();

    // Wait for nix to finish too
    let _ = nix.wait();

    match push {
        Ok(s) if s.success() => {
            info!(cache = %args.cache_name, "push complete");
        }
        Ok(s) => {
            warn!(cache = %args.cache_name, code = ?s.code(), "push finished with errors");
        }
        Err(e) => {
            warn!(error = %e, "attic push failed");
        }
    }

    Ok(ExitCode::SUCCESS)
}
