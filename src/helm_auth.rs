use anyhow::{Context, Result};
use base64::Engine;
use clap::Args as ClapArgs;
use serde_json::json;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::ExitCode;
use tracing::info;

#[derive(ClapArgs)]
pub struct Args {
    /// Path to registry token file
    #[arg(long, env = "SEIBI_HELM_TOKEN_FILE")]
    token_file: PathBuf,

    /// Registry username
    #[arg(long, env = "SEIBI_HELM_USERNAME")]
    username: String,

    /// Registry URL
    #[arg(long, default_value = "ghcr.io")]
    registry: String,

    /// Output path for config.json
    #[arg(long, env = "SEIBI_HELM_OUTPUT")]
    output: PathBuf,
}

pub fn run(args: Args) -> Result<ExitCode> {
    let token = fs::read_to_string(&args.token_file)
        .with_context(|| format!("reading token from {}", args.token_file.display()))?;
    let token = token.trim();

    let auth = base64::engine::general_purpose::STANDARD
        .encode(format!("{}:{token}", args.username));

    let config = json!({
        "auths": {
            &args.registry: {
                "auth": auth
            }
        }
    });

    if let Some(parent) = args.output.parent() {
        fs::create_dir_all(parent)?;
    }

    fs::write(&args.output, serde_json::to_string_pretty(&config)?)?;
    fs::set_permissions(&args.output, fs::Permissions::from_mode(0o600))?;

    info!(
        registry = %args.registry,
        output = %args.output.display(),
        "helm auth config written"
    );
    Ok(ExitCode::SUCCESS)
}
