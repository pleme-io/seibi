use anyhow::Result;
use clap::Args as ClapArgs;
use std::collections::HashMap;
use std::process::ExitCode;
use std::time::Duration;
use tokio::time;
use tracing::{info, warn};

use crate::metrics::SystemMetrics;
use crate::probe::Probe;
use crate::webhook::{self, EmbedBuilder, Webhook};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Health {
    Unknown,
    Healthy,
    Unhealthy,
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

    /// WiFi interface name (omit to skip WiFi probe)
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
}

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
        probes.push(Probe::Systemd {
            unit: unit.clone(),
        });
    }

    info!(
        probes = probes.len(),
        interval = args.interval,
        report_interval = args.report_interval,
        "monitor starting"
    );

    let mut states: HashMap<String, Health> = HashMap::new();
    let mut last_report = std::time::Instant::now();
    let probe_interval = Duration::from_secs(args.interval);
    let report_interval = Duration::from_secs(args.report_interval);

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

        // Periodic status report with full system metrics
        if args.report_interval > 0 && last_report.elapsed() >= report_interval {
            send_status_report(&webhook, &args.hostname).await;
            last_report = std::time::Instant::now();
        }

        time::sleep(probe_interval).await;
    }
}

async fn send_status_report(webhook: &Webhook, hostname: &str) {
    let metrics = SystemMetrics::collect();
    let health = metrics.health_assessment();
    let color = webhook::health_color(health);

    let embed = EmbedBuilder::new(format!(
        "Status \u{2014} {hostname} \u{2014} {health}"
    ))
    .description("Periodic monitoring report")
    .color(color)
    .field(
        "WiFi",
        format!("{} ({})", metrics.wifi_ssid, metrics.wifi_status),
        true,
    )
    .field("IP", &metrics.ip_address, true)
    .field("Load", &metrics.load_avg, true)
    .field(
        "Memory",
        format!("{} / {}", metrics.memory_used, metrics.memory_total),
        true,
    )
    .field(
        "Disk",
        format!(
            "{} / {} ({})",
            metrics.disk_used, metrics.disk_total, metrics.disk_percent
        ),
        true,
    )
    .field(
        "Battery",
        format!("{} ({})", metrics.battery_level, metrics.battery_status),
        true,
    )
    .footer(format!("{hostname} seibi"));

    if let Err(e) = webhook.send(embed).await {
        warn!(error = %e, "failed to send status report");
    }
}
