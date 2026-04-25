use anyhow::Result;
use clap::Args as ClapArgs;
use std::collections::BTreeMap;
use std::process::{Command, ExitCode, Stdio};
use std::time::Duration;
use tokio::time;
use tracing::{info, warn};

use crate::metrics::{SystemMetrics, read_disk_percent_root};
use crate::probe::Probe;
use crate::webhook::{self, Webhook};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Health {
    Unknown,
    Healthy,
    Unhealthy,
}

impl std::fmt::Display for Health {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown => f.write_str("Unknown"),
            Self::Healthy => f.write_str("Healthy"),
            Self::Unhealthy => f.write_str("Unhealthy"),
        }
    }
}

#[derive(ClapArgs)]
pub struct Args {
    /// Discord webhook URL
    #[arg(long, env = "SEIBI_WEBHOOK_URL")]
    webhook_url: String,

    /// Node hostname
    #[arg(long, env = "HOSTNAME")]
    hostname: String,

    /// Ping target for network probe
    #[arg(long, default_value = "8.8.8.8")]
    ping_target: String,

    /// `WiFi` interface name (omit to skip `WiFi` probe)
    #[arg(long)]
    wifi_interface: Option<String>,

    /// Systemd units to monitor (comma-separated)
    #[arg(long, value_delimiter = ',')]
    units: Vec<String>,

    /// Probe interval in seconds
    #[arg(long, default_value = "30")]
    interval: u64,

    /// Status report interval in seconds (0 to disable)
    #[arg(long, default_value = "1800")]
    report_interval: u64,

    /// Root-fs usage percentage that triggers `--on-disk-pressure` tasks.
    /// 0 disables the trigger entirely (default).
    #[arg(long, default_value_t = 0)]
    disk_threshold_percent: u32,

    /// Root-fs usage percentage that clears the pressured state. Defaults to
    /// `disk_threshold_percent - 5` so we get a 5-point hysteresis margin and
    /// don't refire while the disk hovers around the threshold.
    #[arg(long)]
    disk_clear_percent: Option<u32>,

    /// Comma-separated `teiki run <name>` task names to fire when the disk
    /// crosses `--disk-threshold-percent`. Each task is spawned detached so
    /// the probe loop keeps running.
    #[arg(long, value_delimiter = ',')]
    on_disk_pressure: Vec<String>,

    /// Path to the teiki binary used to fire `--on-disk-pressure` tasks.
    /// Defaults to `teiki` resolved via PATH; override for testing or when
    /// the daemon's PATH is hermetic.
    #[arg(long, default_value = "teiki")]
    teiki_bin: String,
}

/// Run the continuous monitoring daemon with periodic probes and reports.
pub async fn run(args: Args) -> Result<ExitCode> {
    let webhook = Webhook::new(&args.webhook_url, &args.hostname);

    let mut probes = vec![Probe::Ping {
        target: args.ping_target.clone(),
    }];

    if let Some(ref iface) = args.wifi_interface {
        probes.push(Probe::Wifi {
            interface: iface.clone(),
        });
    }

    for unit in &args.units {
        probes.push(Probe::Systemd { unit: unit.clone() });
    }

    info!(
        probes = probes.len(),
        interval = args.interval,
        report_interval = args.report_interval,
        disk_threshold_percent = args.disk_threshold_percent,
        on_disk_pressure = ?args.on_disk_pressure,
        "monitor starting"
    );

    let mut states: BTreeMap<String, Health> = BTreeMap::new();
    let mut last_report = std::time::Instant::now();
    let probe_interval = Duration::from_secs(args.interval);
    let report_interval = Duration::from_secs(args.report_interval);

    let disk_clear = args
        .disk_clear_percent
        .unwrap_or_else(|| args.disk_threshold_percent.saturating_sub(5));
    let mut disk_state = DiskPressureState::new();

    loop {
        for probe in &probes {
            let result = probe.check().await;
            let name = probe.name();
            let prev = states.get(name).copied().unwrap_or(Health::Unknown);
            let curr = if result.healthy {
                Health::Healthy
            } else {
                Health::Unhealthy
            };

            if prev != curr {
                let event = if result.healthy {
                    format!("{name}-up")
                } else {
                    format!("{name}-down")
                };

                if let Err(e) = webhook.event(&event, &result.detail).await {
                    warn!(error = %e, %event, "failed to send event");
                }

                states.insert(name.to_owned(), curr);
            }
        }

        // Disk-pressure trigger: cheap to poll every iteration, fires
        // configured cleanup tasks once per below→above transition.
        if args.disk_threshold_percent > 0
            && let Some(pct) = read_disk_percent_root()
        {
            match disk_state.observe(
                pct,
                f64::from(args.disk_threshold_percent),
                f64::from(disk_clear),
            ) {
                Some(DiskTransition::Pressured) => {
                    warn!(
                        disk_percent = pct,
                        threshold = args.disk_threshold_percent,
                        tasks = ?args.on_disk_pressure,
                        "disk pressure — firing cleanup tasks"
                    );
                    for task in &args.on_disk_pressure {
                        spawn_teiki_task(&args.teiki_bin, task);
                    }
                    if let Err(e) = webhook
                        .event(
                            "disk-pressure",
                            &format!("{pct:.1}% (threshold {}%)", args.disk_threshold_percent),
                        )
                        .await
                    {
                        warn!(error = %e, "failed to send disk-pressure event");
                    }
                }
                Some(DiskTransition::Cleared) => {
                    info!(
                        disk_percent = pct,
                        clear_at = disk_clear,
                        "disk pressure cleared"
                    );
                    if let Err(e) = webhook
                        .event(
                            "disk-pressure-cleared",
                            &format!("{pct:.1}% (clear {disk_clear}%)"),
                        )
                        .await
                    {
                        warn!(error = %e, "failed to send disk-pressure-cleared event");
                    }
                }
                None => {}
            }
        }

        // Periodic status report with full system metrics
        if args.report_interval > 0 && last_report.elapsed() >= report_interval {
            let metrics = SystemMetrics::collect();
            let health = metrics.health_assessment();
            let color = webhook::health_color(health);
            let embed = webhook::status_embed(
                &args.hostname,
                health,
                color,
                "Periodic monitoring report",
                &metrics,
            );

            if let Err(e) = webhook.send(embed).await {
                warn!(error = %e, "failed to send status report");
            }
            last_report = std::time::Instant::now();
        }

        time::sleep(probe_interval).await;
    }
}

/// State machine for the disk-pressure trigger. Fires `Pressured` once when
/// usage rises above `threshold`, then waits for usage to drop below `clear`
/// (the hysteresis margin) before it'll fire again.
#[derive(Debug, Default)]
struct DiskPressureState {
    above_threshold: bool,
}

#[derive(Debug, PartialEq, Eq)]
enum DiskTransition {
    Pressured,
    Cleared,
}

impl DiskPressureState {
    fn new() -> Self {
        Self::default()
    }

    fn observe(&mut self, current: f64, threshold: f64, clear: f64) -> Option<DiskTransition> {
        if current >= threshold && !self.above_threshold {
            self.above_threshold = true;
            return Some(DiskTransition::Pressured);
        }
        if current < clear && self.above_threshold {
            self.above_threshold = false;
            return Some(DiskTransition::Cleared);
        }
        None
    }
}

fn spawn_teiki_task(teiki_bin: &str, task: &str) {
    match Command::new(teiki_bin)
        .args(["run", task, "--json"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => info!(task, pid = child.id(), "spawned teiki task"),
        Err(e) => warn!(task, error = %e, "failed to spawn teiki task"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pressured_fires_once_on_crossing() {
        let mut s = DiskPressureState::new();
        assert_eq!(s.observe(70.0, 85.0, 80.0), None);
        assert_eq!(s.observe(86.0, 85.0, 80.0), Some(DiskTransition::Pressured));
        // Stays above — must not refire
        assert_eq!(s.observe(90.0, 85.0, 80.0), None);
        assert_eq!(s.observe(86.0, 85.0, 80.0), None);
    }

    #[test]
    fn cleared_only_below_hysteresis_floor() {
        let mut s = DiskPressureState::new();
        s.observe(86.0, 85.0, 80.0);
        // Drop just below threshold but above clear — still pressured
        assert_eq!(s.observe(82.0, 85.0, 80.0), None);
        // Drop below clear — fires Cleared
        assert_eq!(s.observe(79.0, 85.0, 80.0), Some(DiskTransition::Cleared));
        // Re-cross — fires again
        assert_eq!(s.observe(86.0, 85.0, 80.0), Some(DiskTransition::Pressured));
    }

    #[test]
    fn at_exactly_threshold_is_pressured() {
        let mut s = DiskPressureState::new();
        assert_eq!(s.observe(85.0, 85.0, 80.0), Some(DiskTransition::Pressured));
    }

    #[test]
    fn never_above_means_no_clear_event() {
        let mut s = DiskPressureState::new();
        // Going below "clear" without ever being pressured emits nothing
        assert_eq!(s.observe(50.0, 85.0, 80.0), None);
        assert_eq!(s.observe(10.0, 85.0, 80.0), None);
    }
}
