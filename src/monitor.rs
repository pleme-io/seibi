use anyhow::Result;
use clap::Args as ClapArgs;
use std::collections::BTreeMap;
use std::process::ExitCode;
use std::time::Duration;
use tokio::time;
use tracing::{info, warn};

use crate::disk_pressure::{DiskPressureState, DiskTransition, spawn_pressure_command};
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

    /// Root-fs usage percentage that fires `--on-disk-pressure` commands.
    /// 0 disables the trigger entirely (default).
    #[arg(long, default_value_t = 0)]
    disk_threshold_percent: u32,

    /// Root-fs usage percentage that clears the pressured state. Defaults to
    /// `disk_threshold_percent - 5` so we get a 5-point hysteresis margin and
    /// don't refire while the disk hovers around the threshold.
    #[arg(long)]
    disk_clear_percent: Option<u32>,

    /// Shell commands to fire when disk usage crosses `--disk-threshold-percent`.
    /// Each value is split on whitespace (no quoting / shell expansion) and
    /// spawned detached, so the probe loop keeps running. Pass the flag once
    /// per command, or comma-separate multiple commands inside one flag.
    /// Examples:
    ///   --on-disk-pressure 'seibi nix-gc'
    ///   --on-disk-pressure 'seibi nix-gc --keep-days 7'
    ///   --on-disk-pressure 'teiki run sweep --json'
    #[arg(long, value_delimiter = ',')]
    on_disk_pressure: Vec<String>,
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
                        commands = ?args.on_disk_pressure,
                        "disk pressure — firing cleanup commands"
                    );
                    for cmdline in &args.on_disk_pressure {
                        spawn_pressure_command(cmdline);
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
