use anyhow::{bail, Result};
use clap::Args as ClapArgs;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;
use tokio::process::Command;
use tokio::time;
use tracing::{debug, error, info, warn};

#[derive(ClapArgs)]
pub struct Args {
    /// WireGuard interface name (e.g., wg-ryn-k3s)
    #[arg(long)]
    interface: String,

    /// Path to wg-quick config file
    #[arg(long)]
    config: PathBuf,

    /// Private key file to wait for before starting
    #[arg(long)]
    key_file: PathBuf,

    /// Path to wg-quick binary
    #[arg(long, default_value = "wg-quick")]
    wg_quick: String,

    /// Path to wg binary (for health checks)
    #[arg(long, default_value = "wg")]
    wg: String,

    /// How often to check interface health (seconds)
    #[arg(long, default_value = "30")]
    check_interval: u64,

    /// Log warning if latest handshake exceeds this age (seconds)
    #[arg(long, default_value = "300")]
    handshake_stale: u64,
}

pub async fn run(args: Args) -> Result<ExitCode> {
    let check_interval = Duration::from_secs(args.check_interval);
    let handshake_stale = Duration::from_secs(args.handshake_stale);

    info!(
        interface = %args.interface,
        config = %args.config.display(),
        key_file = %args.key_file.display(),
        check_interval_secs = args.check_interval,
        handshake_stale_secs = args.handshake_stale,
        "wg-supervisor starting"
    );

    // Phase 1: Wait for key file
    wait_for_key(&args.key_file).await;

    // Phase 2: Bring up the tunnel (tear down first if stale interface exists)
    tunnel_down(&args.wg_quick, &args.config).await;
    tunnel_up(&args.wg_quick, &args.config).await?;

    // Phase 3: Supervision loop — run until signalled
    let mut interval = time::interval(check_interval);
    interval.tick().await; // consume the immediate first tick

    loop {
        tokio::select! {
            biased;
            () = shutdown_signal() => {
                info!("received shutdown signal, tearing down tunnel");
                tunnel_down(&args.wg_quick, &args.config).await;
                return Ok(ExitCode::SUCCESS);
            }
            _ = interval.tick() => {
                match check_interface(&args.wg, &args.interface, handshake_stale).await {
                    InterfaceStatus::Healthy { latest_handshake, transfer } => {
                        debug!(
                            interface = %args.interface,
                            latest_handshake_secs = latest_handshake.map(|d| d.as_secs()),
                            transfer = transfer.as_deref().unwrap_or("n/a"),
                            "tunnel healthy"
                        );
                    }
                    InterfaceStatus::StaleHandshake { age } => {
                        warn!(
                            interface = %args.interface,
                            handshake_age_secs = age.as_secs(),
                            threshold_secs = handshake_stale.as_secs(),
                            "handshake stale — peer may be unreachable"
                        );
                    }
                    InterfaceStatus::NoHandshake => {
                        info!(
                            interface = %args.interface,
                            "interface up but no handshake yet — waiting for peer"
                        );
                    }
                    InterfaceStatus::Down => {
                        warn!(interface = %args.interface, "interface down — restarting tunnel");
                        tunnel_down(&args.wg_quick, &args.config).await;
                        if let Err(e) = tunnel_up(&args.wg_quick, &args.config).await {
                            error!(error = %e, "tunnel restart failed — will retry next interval");
                        }
                    }
                }
            }
        }
    }
}

// ── Key file wait ───────────────────────────────────────────────

async fn wait_for_key(path: &PathBuf) {
    if path.exists() {
        info!(path = %path.display(), "key file present");
        return;
    }

    info!(path = %path.display(), "waiting for key file");
    let mut last_log = std::time::Instant::now();

    loop {
        if path.exists() {
            info!(path = %path.display(), "key file appeared");
            return;
        }
        if last_log.elapsed() >= Duration::from_secs(10) {
            info!(path = %path.display(), "still waiting for key file");
            last_log = std::time::Instant::now();
        }
        time::sleep(Duration::from_secs(1)).await;
    }
}

// ── Tunnel lifecycle ────────────────────────────────────────────

async fn tunnel_up(wg_quick: &str, config: &PathBuf) -> Result<()> {
    info!(config = %config.display(), "bringing tunnel up");
    let output = Command::new(wg_quick)
        .args(["up", &config.display().to_string()])
        .output()
        .await?;

    if output.status.success() {
        info!("tunnel up");
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("wg-quick up failed (exit {}): {}", output.status, stderr.trim());
    }
}

async fn tunnel_down(wg_quick: &str, config: &PathBuf) {
    debug!(config = %config.display(), "tearing tunnel down");
    let result = Command::new(wg_quick)
        .args(["down", &config.display().to_string()])
        .output()
        .await;

    match result {
        Ok(o) if o.status.success() => debug!("tunnel down"),
        Ok(o) => debug!(exit = %o.status, "wg-quick down exited (may not have been up)"),
        Err(e) => warn!(error = %e, "wg-quick down failed"),
    }
}

// ── Interface health check ──────────────────────────────────────

enum InterfaceStatus {
    Healthy {
        latest_handshake: Option<Duration>,
        transfer: Option<String>,
    },
    StaleHandshake {
        age: Duration,
    },
    NoHandshake,
    Down,
}

async fn check_interface(wg: &str, interface: &str, stale_threshold: Duration) -> InterfaceStatus {
    let output = Command::new(wg)
        .args(["show", interface])
        .output()
        .await;

    let output = match output {
        Ok(o) if o.status.success() => o,
        _ => return InterfaceStatus::Down,
    };

    let stdout = String::from_utf8_lossy(&output.stdout);

    // No output or no interface line → down
    if !stdout.contains("interface:") {
        return InterfaceStatus::Down;
    }

    let handshake = parse_latest_handshake(&stdout);
    let transfer = parse_transfer(&stdout);

    match handshake {
        Some(age) if age > stale_threshold => InterfaceStatus::StaleHandshake { age },
        Some(age) => InterfaceStatus::Healthy {
            latest_handshake: Some(age),
            transfer,
        },
        None => InterfaceStatus::NoHandshake,
    }
}

// ── wg show output parsing ──────────────────────────────────────

/// Parse "latest handshake: 1 minute, 23 seconds ago" → Duration
fn parse_latest_handshake(wg_output: &str) -> Option<Duration> {
    for line in wg_output.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("latest handshake:") {
            return parse_duration_ago(rest.trim());
        }
    }
    None
}

fn parse_duration_ago(s: &str) -> Option<Duration> {
    let s = s.strip_suffix(" ago")?;
    let mut total_secs: u64 = 0;

    for part in s.split(", ") {
        let part = part.trim();
        let (num, unit) = part.rsplit_once(' ')?;
        let n: u64 = num.trim().parse().ok()?;
        let multiplier = match unit {
            "second" | "seconds" => 1,
            "minute" | "minutes" => 60,
            "hour" | "hours" => 3600,
            "day" | "days" => 86400,
            _ => return None,
        };
        total_secs += n * multiplier;
    }

    (total_secs > 0).then(|| Duration::from_secs(total_secs))
}

/// Parse "transfer: 1.23 KiB received, 4.56 KiB sent"
fn parse_transfer(wg_output: &str) -> Option<String> {
    for line in wg_output.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("transfer:") {
            return Some(rest.trim().to_owned());
        }
    }
    None
}

// ── Signal handling ─────────────────────────────────────────────

async fn shutdown_signal() {
    use tokio::signal;

    let ctrl_c = signal::ctrl_c();

    #[cfg(unix)]
    {
        let mut sigterm =
            signal::unix::signal(signal::unix::SignalKind::terminate()).expect("register SIGTERM");
        tokio::select! {
            _ = ctrl_c => {}
            _ = sigterm.recv() => {}
        }
    }

    #[cfg(not(unix))]
    ctrl_c.await.ok();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_handshake_seconds() {
        assert_eq!(
            parse_duration_ago("23 seconds ago"),
            Some(Duration::from_secs(23))
        );
    }

    #[test]
    fn parse_handshake_minutes_seconds() {
        assert_eq!(
            parse_duration_ago("1 minute, 42 seconds ago"),
            Some(Duration::from_secs(102))
        );
    }

    #[test]
    fn parse_handshake_hours() {
        assert_eq!(
            parse_duration_ago("2 hours, 5 minutes, 10 seconds ago"),
            Some(Duration::from_secs(7510))
        );
    }

    #[test]
    fn parse_handshake_no_ago_suffix() {
        assert_eq!(parse_duration_ago("23 seconds"), None);
    }

    #[test]
    fn parse_transfer_line() {
        let output = "  transfer: 1.23 KiB received, 4.56 KiB sent\n";
        assert_eq!(
            parse_transfer(output),
            Some("1.23 KiB received, 4.56 KiB sent".to_owned())
        );
    }
}
