//! Tune a network interface for K8s/container workloads.
//!
//! Wraps `ethtool` with K8s-aware defaults:
//! - Bumps RX/TX ring buffers to driver max (4096 on i40e — best burst absorption).
//! - Forces LRO **off** (mandatory for any Linux router/bridge/K8s host —
//!   kube-proxy and CNI break with LRO).
//! - Disables flow control (interferes with K8s SLOs in shared switch fabrics).
//! - Keeps adaptive interrupt coalescing (i40e default + recommended for
//!   mixed K8s traffic).
//!
//! Designed to be invoked from a `Type=oneshot` systemd template service
//! triggered by `sys-subsystem-net-devices-<iface>.device` so the tuning
//! re-applies whenever the NIC appears (boot OR cable plug).
//!
//! Driver-specific knobs are gated by `--driver` so this same binary can
//! be wired for igc, mt7921e, etc. in the future without a rebuild.

use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::process::{Command, ExitCode};
use tracing::{info, warn};

#[derive(ClapArgs)]
pub struct Args {
    /// Network interface to tune (e.g. enp5s0f0np0).
    interface: String,

    /// Driver hint — selects the per-driver tuning profile.
    /// Today only `i40e` (Intel X710 family) is supported.
    #[arg(long, default_value = "i40e")]
    driver: String,
}

pub async fn run(args: Args) -> Result<ExitCode> {
    let iface = &args.interface;

    match args.driver.as_str() {
        "i40e" => tune_i40e(iface),
        other => {
            warn!(driver = %other, "no tuning profile for this driver — no-op");
            Ok(ExitCode::SUCCESS)
        }
    }
}

fn tune_i40e(iface: &str) -> Result<ExitCode> {
    // -G: RX/TX ring sizes. Driver max on i40e in 6.12 is 4096.
    run_ethtool(iface, &["-G", iface, "rx", "4096", "tx", "4096"])
        .context("setting ring sizes")?;

    // -K: feature toggles. LRO MUST be off; everything else as per the
    // i40e + K8s recommended baseline (Intel kernel docs).
    run_ethtool(
        iface,
        &[
            "-K", iface,
            "gro", "on",
            "lro", "off",
            "tso", "on",
            "gso", "on",
            "rxhash", "on",
            "ntuple", "on",
        ],
    )
    .context("setting offload features")?;

    // -A: pause/flow control off (K8s SLOs hate it in shared fabrics).
    run_ethtool(iface, &["-A", iface, "rx", "off", "tx", "off"])
        .context("disabling flow control")?;

    info!(interface = %iface, "i40e tuned for K8s workloads");
    Ok(ExitCode::SUCCESS)
}

/// Run ethtool, treating non-zero exit as a soft warning (the NIC may
/// not yet have carrier or some features may be unsupported on this
/// firmware revision — neither is fatal). The systemd unit retries on
/// the next device-appears event anyway.
fn run_ethtool(iface: &str, args: &[&str]) -> Result<()> {
    let status = Command::new("ethtool")
        .args(args)
        .status()
        .context("invoking ethtool")?;
    if !status.success() {
        warn!(
            interface = %iface,
            args = ?args,
            status = %status,
            "ethtool returned non-zero — continuing"
        );
    }
    Ok(())
}
