//! Rename a kubeconfig context + cluster + user from one name to another,
//! IDEMPOTENTLY and CONSISTENTLY.
//!
//! K3s writes `/etc/rancher/k3s/k3s.yaml` with cluster/user/context all
//! named "default". To get a per-cluster name like "rio" in
//! `kubectl config get-contexts`, all three places must be renamed AND
//! the context's `cluster:` / `user:` reference fields must be re-pointed
//! at the renamed cluster + user. Missing the reference repoint leaves
//! a dangling context — kubectl silently falls back to localhost:8080
//! and downstream services fail with bewildering "connection refused"
//! errors despite the cluster being healthy on :6443.
//!
//! See feedback_kubeconfig_rename_consistency.md in the pleme-io/nix
//! memory for the full incident write-up.

use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::path::PathBuf;
use std::process::{Command, ExitCode};
use tracing::{info, warn};

#[derive(ClapArgs)]
pub struct Args {
    /// Kubeconfig file to mutate in place.
    #[arg(long, default_value = "/etc/rancher/k3s/k3s.yaml")]
    kubeconfig: PathBuf,

    /// Source name (the cluster/user/context all share this in k3s.yaml).
    #[arg(long, default_value = "default")]
    from: String,

    /// Target name — the cluster, user, and context all get renamed to this.
    #[arg(long)]
    to: String,
}

pub async fn run(args: Args) -> Result<ExitCode> {
    if !args.kubeconfig.exists() {
        warn!(path = %args.kubeconfig.display(), "kubeconfig not found, nothing to rename");
        return Ok(ExitCode::SUCCESS);
    }

    let kc = args.kubeconfig.to_string_lossy().to_string();
    let from = &args.from;
    let to = &args.to;

    // 1. Rename the cluster + user `name: <from>` lines via sed.
    //    kubectl has no `config rename-cluster` / `rename-user`, so we
    //    edit the YAML directly. The pattern matches `^\s*name: <from>$`
    //    so it catches both top-level (`clusters[].name`, `users[].name`)
    //    AND will NOT match `cluster: default` / `user: default` lines
    //    inside contexts (those are the references we re-point in step 3).
    let sed_pattern = format!(r"s/^\(\s*\)name: {}$/\1name: {}/", from, to);
    let status = Command::new("sed")
        .args(["-i", &sed_pattern, &kc])
        .status()
        .context("running sed for cluster + user rename")?;
    if !status.success() {
        anyhow::bail!("sed exited with status {}", status);
    }
    info!(from = %from, to = %to, "renamed cluster + user `name:` lines");

    // 2. Rename the context itself (kubectl knows how — it's a top-level
    //    operation that doesn't touch references). Idempotent if `from`
    //    no longer exists as a context.
    let probe = Command::new("kubectl")
        .args(["--kubeconfig", &kc, "config", "get-contexts", from])
        .status()
        .context("probing for source context")?;
    if probe.success() {
        let status = Command::new("kubectl")
            .args(["--kubeconfig", &kc, "config", "rename-context", from, to])
            .status()
            .context("running kubectl config rename-context")?;
        if !status.success() {
            anyhow::bail!("kubectl rename-context exited with status {}", status);
        }
        info!(from = %from, to = %to, "renamed context");
    } else {
        info!(from = %from, "source context absent — already renamed");
    }

    // 3. Re-point the context's cluster + user references at the renamed
    //    cluster + user. This is the step the original bash script missed.
    //    `set-context` modifies in place; safe to re-run.
    let status = Command::new("kubectl")
        .args([
            "--kubeconfig", &kc,
            "config", "set-context", to,
            &format!("--cluster={}", to),
            &format!("--user={}", to),
        ])
        .status()
        .context("running kubectl config set-context to repoint references")?;
    if !status.success() {
        anyhow::bail!("kubectl set-context exited with status {}", status);
    }
    info!(context = %to, "re-pointed cluster + user references");

    Ok(ExitCode::SUCCESS)
}
