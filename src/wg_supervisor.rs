use anyhow::{bail, Context, Result};
use clap::Args as ClapArgs;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;
use tokio::process::Command;
use tokio::time;
use tracing::{debug, error, info, warn};

#[derive(ClapArgs)]
pub struct Args {
    /// `WireGuard` interface name (e.g., wg-ryn-k3s)
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

/// Run the `WireGuard` tunnel supervisor: wait for key, bring up tunnel, then
/// monitor health and auto-restart on failure.
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

    // Phase 1: Wait for key file (sops-nix decrypts at rebuild time)
    wait_for_key(&args.key_file).await;

    // Phase 2: Wait for endpoint DNS to resolve (infrastructure may be materializing)
    // Extract endpoint from config, poll DNS until it resolves.
    // This handles the case where seph.1 was just deployed and the NLB
    // DNS hasn't propagated yet. Exponential backoff: 2s → 4s → 8s → ... → 60s max.
    if let Some(endpoint) = extract_endpoint(&args.config) {
        wait_for_dns(&endpoint).await;
    }

    // Phase 3: Bring up the tunnel (tear down first if stale interface exists)
    tunnel_down(&args.wg_quick, &args.config).await;
    tunnel_up(&args.wg_quick, &args.config, &args.key_file).await?;

    // Phase 4: Supervision loop — run until signalled
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
                        warn!(interface = %args.interface, "interface down — converging back to connected state");
                        tunnel_down(&args.wg_quick, &args.config).await;
                        // Re-check DNS before retry (infrastructure may have been destroyed/recreated)
                        if let Some(ref endpoint) = extract_endpoint(&args.config) {
                            wait_for_dns(endpoint).await;
                        }
                        if let Err(e) = tunnel_up(&args.wg_quick, &args.config, &args.key_file).await {
                            error!(error = %e, "tunnel restart failed — will retry next interval");
                        }
                    }
                }
            }
        }
    }
}

// ── Key file wait ───────────────────────────────────────────────

async fn wait_for_key(path: &Path) {
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

// ── DNS resolution wait ────────────────────────────────────────
// Infrastructure may be materializing (NLB just created, DNS propagating).
// This is convergence on the "connected" state — we keep trying regardless
// of whether the infrastructure exists yet.

/// Extract the Endpoint hostname from a wg-quick config file.
fn extract_endpoint(config: &Path) -> Option<String> {
    let text = fs::read_to_string(config).ok()?;
    for line in text.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("Endpoint") {
            let rest = rest.trim().strip_prefix('=')?.trim();
            // Endpoint = hostname:port — extract just the hostname
            let host = rest.split(':').next()?;
            return Some(host.to_string());
        }
    }
    None
}

/// Wait for a hostname to resolve via DNS. Exponential backoff: 2s → 60s max.
/// Logs state at each attempt so operators can see convergence progress.
async fn wait_for_dns(endpoint: &str) {
    use std::net::ToSocketAddrs;

    // Try immediate resolution
    let probe = format!("{endpoint}:0");
    if probe.to_socket_addrs().is_ok() {
        info!(endpoint = %endpoint, "DNS resolved immediately");
        return;
    }

    info!(
        endpoint = %endpoint,
        "DNS not yet resolvable — infrastructure may be materializing. \
         Will poll with exponential backoff until resolved."
    );

    let mut backoff = Duration::from_secs(2);
    let max_backoff = Duration::from_secs(60);
    let mut attempts = 0u32;

    loop {
        time::sleep(backoff).await;
        attempts += 1;

        let probe = format!("{endpoint}:0");
        match probe.to_socket_addrs() {
            Ok(addrs) => {
                let resolved: Vec<_> = addrs.collect();
                info!(
                    endpoint = %endpoint,
                    attempts = attempts,
                    resolved = ?resolved,
                    "DNS resolved — infrastructure is reachable"
                );
                return;
            }
            Err(_) => {
                if attempts % 5 == 0 {
                    info!(
                        endpoint = %endpoint,
                        attempts = attempts,
                        next_retry_secs = backoff.as_secs(),
                        "DNS still unresolvable — continuing to converge"
                    );
                } else {
                    debug!(
                        endpoint = %endpoint,
                        attempts = attempts,
                        "DNS not yet resolved"
                    );
                }
            }
        }

        backoff = (backoff * 2).min(max_backoff);
    }
}

// ── Tunnel lifecycle ────────────────────────────────────────────

const PLACEHOLDER: &str = "PLACEHOLDER_REPLACED_BY_POSTUP";
const PSK_MARKER: &str = "# PresharedKeyFile = ";

/// Build a complete wg-quick config with all secrets inlined.
///
/// macOS wg-quick validates `PrivateKey` before running `PostUp`, so placeholder
/// patterns and PostUp-based key injection do not work. This function always
/// produces a config with real keys inlined, supporting two config formats:
///
/// **New format** (no `PrivateKey` line):
///   - `# PrivateKeyFile:` comment in `[Interface]` — signals that the supervisor
///     must inject `PrivateKey` from `key_file`.
///   - `# PresharedKeyFile = /path` comment in `[Peer]` — resolved to
///     `PresharedKey = <contents>`.
///
/// **Legacy format** (backward compat):
///   - `PrivateKey = PLACEHOLDER_REPLACED_BY_POSTUP` — replaced with real key.
///   - `PostUp = wg set %i private-key /path` — stripped.
///   - `PostUp = wg set %i peer <pubkey> preshared-key /path` — converted to
///     `PresharedKey = <contents>`.
///
/// Returns `Some(resolved_text)` if the config needed resolution (always the
/// case for both formats), or `None` if the config already has real keys and
/// needs no modification.
fn resolve_config(config_text: &str, key_file: &Path) -> Result<Option<String>> {
    let has_placeholder = config_text.contains(PLACEHOLDER);
    let has_psk_marker = config_text.contains(PSK_MARKER);
    let has_privkey_comment = config_text.contains("# PrivateKeyFile:");
    let has_postup_privkey = config_text.lines().any(|l| {
        let t = l.trim();
        t.starts_with("PostUp") && t.contains("private-key")
    });
    let has_postup_psk = config_text.lines().any(|l| {
        let t = l.trim();
        t.starts_with("PostUp") && t.contains("preshared-key")
    });

    // Nothing to resolve — config already has real keys
    if !has_placeholder && !has_psk_marker && !has_privkey_comment
        && !has_postup_privkey && !has_postup_psk
    {
        return Ok(None);
    }

    let key = fs::read_to_string(key_file)
        .with_context(|| format!("reading key file {}", key_file.display()))?;

    let mut output = Vec::new();
    let mut privkey_injected = false;

    for line in config_text.lines() {
        let trimmed = line.trim();

        // ── New format: inject PrivateKey after the marker comment ──
        if trimmed.starts_with("# PrivateKeyFile:") {
            output.push(format!("PrivateKey = {}", key.trim()));
            privkey_injected = true;
            continue;
        }

        // ── New format: resolve PSK marker comment ──
        if let Some(psk_path) = trimmed.strip_prefix(PSK_MARKER) {
            let psk_path = psk_path.trim();
            match fs::read_to_string(psk_path) {
                Ok(psk) => {
                    output.push(format!("PresharedKey = {}", psk.trim()));
                }
                Err(e) => {
                    warn!(path = psk_path, error = %e, "could not read PSK file, skipping");
                }
            }
            continue;
        }

        // ── Legacy: replace placeholder PrivateKey ──
        if trimmed.starts_with("PrivateKey") && trimmed.contains(PLACEHOLDER) {
            output.push(format!("PrivateKey = {}", key.trim()));
            privkey_injected = true;
            continue;
        }

        // ── Legacy: strip PostUp that sets private-key ──
        if trimmed.starts_with("PostUp") && trimmed.contains("private-key") {
            continue;
        }

        // ── Legacy: convert PostUp preshared-key into inline PresharedKey ──
        if trimmed.starts_with("PostUp")
            && trimmed.contains("preshared-key")
            && let Some(psk_path) = trimmed.rsplit("preshared-key").next()
        {
            let psk_path = psk_path.trim();
            match fs::read_to_string(psk_path) {
                Ok(psk) => {
                    output.push(format!("PresharedKey = {}", psk.trim()));
                    continue;
                }
                Err(e) => {
                    warn!(path = psk_path, error = %e, "could not read PSK file, keeping PostUp line");
                }
            }
        }

        output.push(line.to_owned());
    }

    // Safety net: if we found markers but somehow didn't inject PrivateKey
    // (shouldn't happen, but guard against malformed configs), inject it
    // after [Interface].
    if !privkey_injected
        && (has_privkey_comment || has_placeholder)
        && let Some(pos) = output.iter().position(|l| l.trim() == "[Interface]")
    {
        output.insert(pos + 1, format!("PrivateKey = {}", key.trim()));
    }

    Ok(Some(output.join("\n")))
}

async fn tunnel_up(wg_quick: &str, config: &Path, key_file: &Path) -> Result<()> {
    info!(config = %config.display(), "bringing tunnel up");

    let config_text = fs::read_to_string(config)
        .with_context(|| format!("reading wg config {}", config.display()))?;

    let effective_config: PathBuf;
    let mut cleanup: Option<PathBuf> = None;

    if let Some(resolved) = resolve_config(&config_text, key_file)? {
        // wg-quick derives the interface name from the filename (stem before .conf).
        // Write to /tmp/<stem>.conf so the interface name matches the original.
        let filename = config.file_name().unwrap_or_default();
        let tmp = std::env::temp_dir().join(filename);
        fs::write(&tmp, &resolved)
            .with_context(|| format!("writing temp config {}", tmp.display()))?;
        fs::set_permissions(&tmp, fs::Permissions::from_mode(0o600))?;
        info!(tmp = %tmp.display(), "resolved config with inlined keys into temp config");
        effective_config = tmp.clone();
        cleanup = Some(tmp);
    } else {
        effective_config = config.to_path_buf();
    }

    let output = Command::new(wg_quick)
        .args(["up", &effective_config.display().to_string()])
        .output()
        .await?;

    if let Some(ref tmp) = cleanup
        && let Err(e) = fs::remove_file(tmp)
    {
        warn!(path = %tmp.display(), error = %e, "failed to remove temp config");
    }

    if output.status.success() {
        info!("tunnel up");
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("wg-quick up failed (exit {}): {}", output.status, stderr.trim());
    }
}

async fn tunnel_down(wg_quick: &str, config: &Path) {
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
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut sigterm) => {
                tokio::select! {
                    _ = ctrl_c => {}
                    _ = sigterm.recv() => {}
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to register SIGTERM handler, falling back to ctrl-c only");
                ctrl_c.await.ok();
            }
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

    #[test]
    fn resolve_legacy_placeholder_replaces_key_and_strips_postup() {
        let dir = std::env::temp_dir().join("seibi-test-resolve-legacy");
        let _ = fs::create_dir_all(&dir);
        let key_path = dir.join("private.key");
        fs::write(&key_path, "aBcDeFgHiJkLmNoPqRsTuVwXyZ=\n").unwrap();
        let psk_path = dir.join("psk");
        fs::write(&psk_path, "presharedkeyvalue123=\n").unwrap();

        let config = format!(
            "[Interface]\nPrivateKey = PLACEHOLDER_REPLACED_BY_POSTUP\nAddress = 10.0.0.2/32\n\
             PostUp = wg set %i private-key {key}\n\n\
             [Peer]\nPublicKey = ABCD=\nAllowedIPs = 10.0.0.1/32\n\
             PostUp = wg set %i peer ABCD= preshared-key {psk}\nPersistentKeepalive = 25\n",
            key = key_path.display(),
            psk = psk_path.display(),
        );
        let result = resolve_config(&config, &key_path).unwrap();
        assert!(result.is_some());
        let resolved = result.unwrap();
        assert!(resolved.contains("PrivateKey = aBcDeFgHiJkLmNoPqRsTuVwXyZ="));
        assert!(!resolved.contains("PLACEHOLDER_REPLACED_BY_POSTUP"));
        // PostUp for private-key should be stripped
        assert!(!resolved.contains("PostUp"));
        // PSK should be inlined
        assert!(resolved.contains("PresharedKey = presharedkeyvalue123="));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_new_format_injects_key_and_psk() {
        let dir = std::env::temp_dir().join("seibi-test-resolve-new");
        let _ = fs::create_dir_all(&dir);
        let key_path = dir.join("private.key");
        fs::write(&key_path, "NewFormatPrivKey123=\n").unwrap();
        let psk_path = dir.join("psk");
        fs::write(&psk_path, "NewFormatPSK456=\n").unwrap();

        let config = format!(
            "[Interface]\n\
             # PrivateKeyFile: injected at runtime by wg-supervisor from /some/path\n\
             Address = 10.0.0.2/32\n\
             MTU = 1420\n\n\
             [Peer]\n\
             PublicKey = ABCD=\n\
             AllowedIPs = 10.0.0.1/32\n\
             # PresharedKeyFile = {psk}\n\
             PersistentKeepalive = 25\n",
            psk = psk_path.display(),
        );
        let result = resolve_config(&config, &key_path).unwrap();
        assert!(result.is_some());
        let resolved = result.unwrap();
        // PrivateKey should be injected (replacing the comment)
        assert!(resolved.contains("PrivateKey = NewFormatPrivKey123="));
        assert!(!resolved.contains("# PrivateKeyFile:"));
        // PSK should be inlined from the marker comment
        assert!(resolved.contains("PresharedKey = NewFormatPSK456="));
        assert!(!resolved.contains("# PresharedKeyFile"));
        // No PostUp lines should exist
        assert!(!resolved.contains("PostUp"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_returns_none_without_markers() {
        let dir = std::env::temp_dir().join("seibi-test-no-markers");
        let _ = fs::create_dir_all(&dir);
        let key_path = dir.join("private.key");
        fs::write(&key_path, "somekey=\n").unwrap();

        let config = "[Interface]\nPrivateKey = aRealKey123=\nAddress = 10.0.0.2/32\n";
        let result = resolve_config(config, &key_path).unwrap();
        assert!(result.is_none());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn parse_handshake_single_second() {
        assert_eq!(
            parse_duration_ago("1 second ago"),
            Some(Duration::from_secs(1))
        );
    }

    #[test]
    fn parse_handshake_days() {
        assert_eq!(
            parse_duration_ago("1 day, 2 hours, 3 minutes ago"),
            Some(Duration::from_secs(86400 + 7200 + 180))
        );
    }

    #[test]
    fn parse_handshake_unknown_unit() {
        assert_eq!(parse_duration_ago("5 weeks ago"), None);
    }

    #[test]
    fn parse_handshake_empty_string() {
        assert_eq!(parse_duration_ago(""), None);
    }

    #[test]
    fn parse_handshake_zero_seconds() {
        assert_eq!(parse_duration_ago("0 seconds ago"), None);
    }

    #[test]
    fn parse_latest_handshake_from_wg_output() {
        let output = "\
interface: wg-test
  public key: AAAA=
  private key: (hidden)
  listening port: 51820

peer: BBBB=
  endpoint: 1.2.3.4:51820
  allowed ips: 10.0.0.0/24
  latest handshake: 45 seconds ago
  transfer: 1.23 MiB received, 4.56 MiB sent
";
        assert_eq!(
            parse_latest_handshake(output),
            Some(Duration::from_secs(45))
        );
    }

    #[test]
    fn parse_latest_handshake_missing() {
        let output = "\
interface: wg-test
  public key: AAAA=
  listening port: 51820

peer: BBBB=
  endpoint: 1.2.3.4:51820
";
        assert_eq!(parse_latest_handshake(output), None);
    }

    #[test]
    fn parse_transfer_missing() {
        let output = "interface: wg-test\n  public key: AAAA=\n";
        assert_eq!(parse_transfer(output), None);
    }

    #[test]
    fn parse_transfer_with_whitespace() {
        let output = "  transfer:   100 B received, 200 B sent  \n";
        assert_eq!(
            parse_transfer(output),
            Some("100 B received, 200 B sent".to_owned())
        );
    }

    #[test]
    fn resolve_config_key_file_missing_returns_error() {
        let config = "[Interface]\nPrivateKey = PLACEHOLDER_REPLACED_BY_POSTUP\n";
        let result = resolve_config(config, Path::new("/nonexistent/key"));
        assert!(result.is_err());
    }

    #[test]
    fn resolve_empty_config() {
        let dir = std::env::temp_dir().join("seibi-test-resolve-empty");
        let _ = fs::create_dir_all(&dir);
        let key_path = dir.join("private.key");
        fs::write(&key_path, "somekey=\n").unwrap();

        let result = resolve_config("", &key_path).unwrap();
        assert!(result.is_none());

        let _ = fs::remove_dir_all(&dir);
    }
}
