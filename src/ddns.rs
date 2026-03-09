use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use reqwest::Client;
use serde::Serialize;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;
use tracing::info;

#[derive(ClapArgs)]
pub struct Args {
    /// Cloudflare zone ID
    #[arg(long, env = "SEIBI_DDNS_ZONE_ID")]
    zone_id: String,

    /// Cloudflare DNS record ID
    #[arg(long, env = "SEIBI_DDNS_RECORD_ID")]
    record_id: String,

    /// Path to Cloudflare API token file
    #[arg(long, env = "SEIBI_DDNS_TOKEN_FILE")]
    token_file: PathBuf,

    /// DNS record hostname (e.g., "home.example.com")
    #[arg(long, env = "SEIBI_DDNS_HOSTNAME")]
    hostname: String,

    /// File to cache last known public IP
    #[arg(long, default_value = "/var/lib/ddns/last-ip")]
    state_file: PathBuf,
}

#[derive(Serialize)]
struct DnsRecord {
    #[serde(rename = "type")]
    record_type: &'static str,
    name: String,
    content: String,
    ttl: u32,
    proxied: bool,
}

pub async fn run(args: Args) -> Result<ExitCode> {
    let token = read_token(&args.token_file)?;
    let client = Client::new();

    let current_ip = client
        .get("https://api.ipify.org")
        .send()
        .await
        .context("fetching public IP")?
        .text()
        .await?
        .trim()
        .to_owned();

    let last_ip = fs::read_to_string(&args.state_file).ok();
    let last_ip = last_ip.as_deref().map(str::trim);

    if last_ip == Some(&current_ip) {
        info!(ip = %current_ip, "IP unchanged");
        return Ok(ExitCode::from(2));
    }

    let record = DnsRecord {
        record_type: "A",
        name: args.hostname.clone(),
        content: current_ip.clone(),
        ttl: 120,
        proxied: false,
    };

    let resp = client
        .put(format!(
            "https://api.cloudflare.com/client/v4/zones/{}/dns_records/{}",
            args.zone_id, args.record_id
        ))
        .bearer_auth(&token)
        .json(&record)
        .send()
        .await
        .context("updating Cloudflare DNS")?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Cloudflare API error: {body}");
    }

    if let Some(parent) = args.state_file.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&args.state_file, &current_ip)?;

    info!(
        old = ?last_ip,
        new = %current_ip,
        host = %args.hostname,
        "DNS record updated"
    );
    Ok(ExitCode::SUCCESS)
}

fn read_token(path: &PathBuf) -> Result<String> {
    fs::read_to_string(path)
        .map(|s| s.trim().to_owned())
        .with_context(|| format!("reading token from {}", path.display()))
}
