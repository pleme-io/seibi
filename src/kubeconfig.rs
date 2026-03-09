use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;
use tracing::{info, warn};

#[derive(ClapArgs)]
pub struct Args {
    /// Path to K3s YAML config
    #[arg(long, default_value = "/etc/rancher/k3s/k3s.yaml")]
    k3s_yaml: PathBuf,

    /// Output path for local kubeconfig (unchanged)
    #[arg(long, default_value = "/etc/k3s-admin-kubeconfig")]
    output_local: PathBuf,

    /// Output path for remote kubeconfig (with detected IP)
    #[arg(long, default_value = "/etc/k3s-remote-kubeconfig")]
    output_remote: PathBuf,

    /// Try Hetzner metadata API for IP detection first
    #[arg(long)]
    hetzner: bool,

    /// Seconds to wait for k3s.yaml to appear
    #[arg(long, default_value = "300")]
    timeout: u64,
}

pub async fn run(args: Args) -> Result<ExitCode> {
    wait_for_file(&args.k3s_yaml, Duration::from_secs(args.timeout)).await?;

    let yaml = fs::read_to_string(&args.k3s_yaml).context("reading k3s.yaml")?;

    // Local copy — unchanged, for on-node use
    fs::write(&args.output_local, &yaml)?;
    set_perms(&args.output_local, 0o644)?;

    // Remote copy — rewrite 127.0.0.1 to the node's reachable IP
    let ip = detect_ip(args.hetzner).await;
    let remote_yaml = yaml.replace("127.0.0.1", &ip);
    fs::write(&args.output_remote, &remote_yaml)?;
    set_perms(&args.output_remote, 0o644)?;

    info!(
        ip = %ip,
        local = %args.output_local.display(),
        remote = %args.output_remote.display(),
        "kubeconfig exported"
    );
    Ok(ExitCode::SUCCESS)
}

async fn wait_for_file(path: &std::path::Path, timeout: Duration) -> Result<()> {
    let start = std::time::Instant::now();
    while !path.exists() {
        if start.elapsed() > timeout {
            anyhow::bail!("timed out waiting for {}", path.display());
        }
        info!(path = %path.display(), "waiting for file...");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    Ok(())
}

async fn detect_ip(try_hetzner: bool) -> String {
    if try_hetzner {
        if let Some(ip) = hetzner_ip().await {
            info!(ip = %ip, source = "hetzner", "detected IP");
            return ip;
        }
    }

    if let Some(ip) = first_global_ip() {
        info!(ip = %ip, source = "interface", "detected IP");
        return ip;
    }

    warn!("no global IP found, falling back to 127.0.0.1");
    "127.0.0.1".into()
}

async fn hetzner_ip() -> Option<String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .ok()?;

    client
        .get("http://169.254.169.254/hetzner/v1/metadata/public-ipv4")
        .send()
        .await
        .ok()?
        .text()
        .await
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
}

fn first_global_ip() -> Option<String> {
    let output = std::process::Command::new("ip")
        .args(["-4", "addr", "show", "scope", "global"])
        .output()
        .ok()?;

    let text = String::from_utf8_lossy(&output.stdout);
    text.lines().find_map(|line| {
        let trimmed = line.trim();
        trimmed
            .strip_prefix("inet ")
            .and_then(|rest| rest.split('/').next())
            .map(String::from)
    })
}

fn set_perms(path: &std::path::Path, mode: u32) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(mode))
        .with_context(|| format!("setting permissions on {}", path.display()))
}
