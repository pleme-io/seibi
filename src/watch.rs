//! Lightweight disk-pressure-only daemon.
//!
//! `seibi monitor` is the full health daemon: ping, WiFi, systemd units,
//! periodic Discord status reports. It also got a disk-pressure trigger
//! bolted on, but the webhook surface and probe set are mandatory.
//!
//! `seibi watch` is the strip-down for hosts that just need disk-pressure
//! cleanup without webhooks — typically a developer workstation. Same
//! `DiskPressureState` and `spawn_pressure_command` as monitor (shared via
//! `crate::disk_pressure`), no probes, no Discord.

use anyhow::Result;
use clap::Args as ClapArgs;
use std::process::ExitCode;
use std::time::Duration;
use tokio::time;
use tracing::{info, warn};

use crate::disk_pressure::{DiskPressureState, DiskTransition, spawn_pressure_command};
use crate::metrics::read_disk_percent_root;

#[derive(ClapArgs)]
pub struct Args {
    /// Seconds between disk-usage polls.
    #[arg(long, default_value_t = 60)]
    interval: u64,

    /// Root-fs usage percentage that fires `--on-disk-pressure` commands.
    #[arg(long)]
    disk_threshold_percent: u32,

    /// Root-fs usage percentage that clears the pressured state. Defaults
    /// to `disk_threshold_percent - 5` (5-point hysteresis).
    #[arg(long)]
    disk_clear_percent: Option<u32>,

    /// Shell commands to fire on each below→above crossing. Each value is
    /// split on whitespace (no quoting) and spawned detached. Pass the flag
    /// once per command, or comma-separate inside one flag.
    #[arg(long, value_delimiter = ',', required = true)]
    on_disk_pressure: Vec<String>,
}

/// Poll the root filesystem on `interval`; on each below→above crossing of
/// `disk_threshold_percent`, spawn every `on_disk_pressure` command detached.
/// Honours the same hysteresis contract as `seibi monitor`.
pub async fn run(args: Args) -> Result<ExitCode> {
    if args.disk_threshold_percent == 0 {
        anyhow::bail!("--disk-threshold-percent must be > 0");
    }
    if args.on_disk_pressure.is_empty() {
        anyhow::bail!("--on-disk-pressure requires at least one command");
    }

    let clear = args
        .disk_clear_percent
        .unwrap_or_else(|| args.disk_threshold_percent.saturating_sub(5));

    info!(
        interval = args.interval,
        threshold = args.disk_threshold_percent,
        clear,
        commands = ?args.on_disk_pressure,
        "watch starting"
    );

    let mut state = DiskPressureState::new();
    let interval = Duration::from_secs(args.interval);

    loop {
        if let Some(pct) = read_disk_percent_root() {
            match state.observe(
                pct,
                f64::from(args.disk_threshold_percent),
                f64::from(clear),
            ) {
                Some(DiskTransition::Pressured) => {
                    warn!(
                        disk_percent = pct,
                        threshold = args.disk_threshold_percent,
                        commands = ?args.on_disk_pressure,
                        "disk pressure — firing cleanup commands"
                    );
                    for cmd in &args.on_disk_pressure {
                        spawn_pressure_command(cmd);
                    }
                }
                Some(DiskTransition::Cleared) => {
                    info!(disk_percent = pct, clear, "disk pressure cleared");
                }
                None => {}
            }
        } else {
            warn!("could not read disk usage — skipping this iteration");
        }

        time::sleep(interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rejects_zero_threshold_args() {
        let result = run(Args {
            interval: 60,
            disk_threshold_percent: 0,
            disk_clear_percent: None,
            on_disk_pressure: vec!["echo".to_owned()],
        })
        .await;
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("threshold"), "unexpected error: {msg}");
    }

    #[tokio::test]
    async fn rejects_empty_command_list() {
        let result = run(Args {
            interval: 60,
            disk_threshold_percent: 85,
            disk_clear_percent: None,
            on_disk_pressure: vec![],
        })
        .await;
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("on-disk-pressure"), "unexpected error: {msg}");
    }
}
