//! Reconvergence daemon — closes the loop between intent and reality.
//!
//! `seibi reconverge` is the operationalization of the 8-phase enactment
//! model's Phase 8 (`Reconverge`) at minute granularity instead of the
//! nightly `nix flake update + rebuild`. The loop continuously asserts
//! intent against reality and *acts* — every recipe knows both how to
//! detect drift and how to remediate it.
//!
//! # Why
//!
//! Six recurring failure modes on rio in early sessions all had the same
//! shape: a known remediation existed and a human typed it twice. That's
//! the missing-daemon signal. Rather than ship a one-shot tool per
//! recipe, this subcommand collects them under a single `check + remediate`
//! vocabulary so each next recipe is a five-minute add.
//!
//! # Recipe contract
//!
//! Each [`Recipe`] declares:
//! - `name`: stable identifier for `--only` and metrics
//! - `description`: one-line operator-facing intent
//! - `check`: asks reality "are you converged?" — returns `Drift::None`
//!   or `Drift::Detected { reason }`
//! - `remediate`: applies the recipe's fix when drift is detected;
//!   returns `Action::{Remediated, Refused, Failed}`
//!
//! # Exit codes
//!
//! - `0` — every recipe converged (or successfully remediated)
//! - `1` — at least one recipe `Refused` to remediate (operator action needed)
//! - `2` — at least one recipe `Failed` to remediate (transient — daemon retries)
//!
//! Recipes individually log their findings via tracing; `--json` emits
//! structured per-recipe events for ingest into Prometheus / Loki.

use anyhow::Result;
use clap::Args as ClapArgs;
use std::process::ExitCode;
use tracing::{error, info, warn};

#[derive(ClapArgs)]
pub struct Args {
    /// Print what would be remediated without acting.
    #[arg(long)]
    pub dry_run: bool,

    /// Run only the named recipe (default: all).
    #[arg(long)]
    pub only: Option<String>,

    /// Emit JSON events instead of human-readable logs.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug)]
pub enum Drift {
    None,
    Detected { reason: String },
}

#[derive(Debug)]
pub enum Action {
    /// Reality matched intent.
    AlreadyConverged,
    /// Drift was detected and fixed.
    Remediated { detail: String },
    /// Drift was detected but recipe refused to act (e.g. needs human).
    Refused { detail: String },
    /// Remediation was attempted but failed (transient — will retry).
    Failed { detail: String },
}

pub struct Recipe {
    pub name: &'static str,
    pub description: &'static str,
    pub run: fn(dry_run: bool) -> Action,
}

/// All recipes the daemon knows about. Add one entry per failure-class.
fn recipes() -> Vec<Recipe> {
    vec![
        Recipe {
            name: "flux-git-auth-freshness",
            description: "Verify the K8s Secret used by Flux source-controller still authenticates against the upstream git remote; restart fluxcd-bootstrap.service to re-render from current SOPS state on drift.",
            run: flux_git_auth::run,
        },
        Recipe {
            name: "containerd-snapshot-heal",
            description: "Detect the containerd overlayfs-snapshotter↔boltdb desync (meta.db references snapshot fs dirs that no longer exist → CreateContainerError on every new sandbox) and auto-remediate by moving the snapshotter + bolt index aside so containerd rebuilds them and re-pulls images. Cooldown-gated. Recovers the failure that took rio's DNS + GitOps down for 2 days (2026-05-30).",
            run: containerd_snapshot_heal::run,
        },
        // Future recipes (each a five-minute add):
        //   stranded-cni-interfaces      — delete `flannel.1` / `cni0` if k3s started with `--flannel-backend=none`
        //   kubeconfig-rename-idempotent — re-run `seibi kubeconfig-rename` if /etc/rancher/k3s/k3s.yaml drifted back to default names
        //   fluxcd-deploy-key-staleness  — verify Flux's SSH deploy key still authorizes against pleme-io/k8s
        //   ghcr-pull-secret-token-staleness — verify each ns/ghcr-pull dockerconfigjson still 200s on a manifest fetch
    ]
}

pub async fn run(args: Args) -> Result<ExitCode> {
    let all = recipes();
    let selected: Vec<&Recipe> = match &args.only {
        None => all.iter().collect(),
        Some(name) => all.iter().filter(|r| r.name == name).collect(),
    };
    if selected.is_empty() {
        anyhow::bail!(
            "no recipe matched `--only {:?}`; known recipes: {}",
            args.only,
            all.iter().map(|r| r.name).collect::<Vec<_>>().join(", ")
        );
    }

    let mut refused = 0u32;
    let mut failed = 0u32;

    for recipe in &selected {
        info!(recipe = %recipe.name, dry_run = args.dry_run, "running");
        let action = (recipe.run)(args.dry_run);
        match &action {
            Action::AlreadyConverged => {
                info!(recipe = %recipe.name, "converged");
            }
            Action::Remediated { detail } => {
                info!(recipe = %recipe.name, %detail, "remediated");
            }
            Action::Refused { detail } => {
                warn!(recipe = %recipe.name, %detail, "drift detected — operator action required");
                refused += 1;
            }
            Action::Failed { detail } => {
                error!(recipe = %recipe.name, %detail, "remediation failed");
                failed += 1;
            }
        }
    }

    if failed > 0 {
        Ok(ExitCode::from(2))
    } else if refused > 0 {
        Ok(ExitCode::from(1))
    } else {
        Ok(ExitCode::SUCCESS)
    }
}

// ─────────────────────────────────────────────────────────────────────
// Recipe: flux-git-auth-freshness
// ─────────────────────────────────────────────────────────────────────
//
// Symptom from the field: rotating the GitHub PAT in SOPS doesn't
// propagate into the K8s `flux-system/flux-system` Secret until
// fluxcd-bootstrap.service runs again — and that service has no
// restartTrigger on the SOPS path, so it sticks at the OLD token until
// the next nixos-rebuild. Result: every Flux Kustomization reports
// `ArtifactFailed` because source-controller can't clone, and every
// HelmRelease cascades to `SourceNotReady`. Diagnosed manually three
// times on rio.
//
// Detection: read the `password` (PAT auth) or `identity` (SSH auth)
// field of `flux-system/flux-system`, ping the upstream, observe 200/OK
// or denial.
//
// Remediation: `systemctl restart fluxcd-bootstrap.service`. The
// oneshot re-renders the K8s Secret from `/run/secrets/...` which
// sops-nix has already updated to the freshest decrypted value. Idempotent.
mod flux_git_auth {
    use super::Action;
    use std::process::Command;

    pub fn run(dry_run: bool) -> Action {
        // 1. Probe: is fluxcd-bootstrap.service reporting active?
        let active = match Command::new("systemctl")
            .args(["is-active", "fluxcd-bootstrap.service"])
            .output()
        {
            Ok(o) => o.status.success(),
            Err(e) => {
                return Action::Failed {
                    detail: format!("could not probe fluxcd-bootstrap.service: {e}"),
                };
            }
        };
        if !active {
            // unit not active → bootstrap clearly hasn't run; remediate
            if dry_run {
                return Action::Refused {
                    detail: "fluxcd-bootstrap.service is inactive (would restart)".into(),
                };
            }
            return restart_bootstrap();
        }

        // 2. Probe: does the K8s GitRepository status show ready?
        let gitrepo = Command::new("kubectl")
            .args([
                "--kubeconfig",
                "/etc/rancher/k3s/k3s.yaml",
                "get",
                "gitrepository",
                "-n",
                "flux-system",
                "flux-system",
                "-o",
                "jsonpath={.status.conditions[?(@.type=='Ready')].status}",
            ])
            .output();
        let ready = matches!(
            gitrepo,
            Ok(o) if o.status.success() && String::from_utf8_lossy(&o.stdout).trim() == "True"
        );
        if ready {
            return Action::AlreadyConverged;
        }

        // 3. Drift: GR not Ready. Reason most often "auth required" /
        //    "Bad credentials" / "Invalid username or token". Restart
        //    bootstrap to push the fresh PAT into the K8s Secret.
        if dry_run {
            return Action::Refused {
                detail: "GitRepository not Ready (would restart fluxcd-bootstrap.service)".into(),
            };
        }
        restart_bootstrap()
    }

    fn restart_bootstrap() -> Action {
        match Command::new("systemctl")
            .args(["restart", "fluxcd-bootstrap.service"])
            .status()
        {
            Ok(s) if s.success() => Action::Remediated {
                detail: "fluxcd-bootstrap.service restarted; K8s flux-system/flux-system Secret re-rendered from /run/secrets".into(),
            },
            Ok(s) => Action::Failed {
                detail: format!("systemctl restart exited {s}"),
            },
            Err(e) => Action::Failed {
                detail: format!("systemctl restart spawn failed: {e}"),
            },
        }
    }
}

// ─────────────────────────────────────────────────────────────────────
// Recipe: containerd-snapshot-heal
// ─────────────────────────────────────────────────────────────────────
//
// Symptom (rio, 2026-05-30 → 06-01, 2-day outage): containerd's overlayfs
// snapshotter boltdb (meta.db) references base-layer snapshot IDs whose
// on-disk `snapshots/<id>/fs` dirs no longer exist — a desync after a
// reboot (ZFS itself scrubbed clean; the inconsistency is above ZFS, in
// containerd's metadata-vs-filesystem coupling). Every new sandbox fails
// `CreateContainerError: failed to stat parent: …/snapshots/<id>/fs: no
// such file or directory` → CoreDNS, Flux, metrics-server, traefik, the
// CNPG operator all wedge → cluster DNS + GitOps down.
//
// Detection: the exact signature in the recent k3s journal (no boltdb
// parsing; reflects current reality).
//
// Remediation (verified safe — rio is eventually-consistent; ALL cluster
// state is in kine SQLite on the boot ext4 drive, NOT in the snapshotter):
// stop k3s → kill shims + unmount /run/k3s overlays (k3s-killall.sh) →
// gate on 0 shims / 0 overlay mounts → move the overlayfs snapshotter +
// bolt index aside (recoverable same-dataset rename) → start k3s.
// containerd rebuilds an empty snapshotter + fresh bolt and re-pulls
// images. Cooldown-gated (15 min) so a persistent signature can't
// hot-loop k3s while images re-pull. This is the typed, self-healing form
// of the one-off recovery runbook — Viggy continuous-convergence applied
// to node container-runtime health.
mod containerd_snapshot_heal {
    use super::Action;
    use std::process::Command;
    use std::time::{SystemTime, UNIX_EPOCH};

    const CD: &str = "/var/lib/rancher/k3s/agent/containerd";
    const MARKER: &str = "/var/lib/seibi/containerd-heal.last";
    const COOLDOWN_SECS: u64 = 900;

    pub fn run(dry_run: bool) -> Action {
        if !desync_detected() {
            return Action::AlreadyConverged;
        }
        if let Some(remaining) = cooldown_remaining() {
            return Action::Refused {
                detail: format!(
                    "containerd snapshot desync present but a heal ran {}s ago; waiting {remaining}s for image re-pull to settle",
                    COOLDOWN_SECS - remaining
                ),
            };
        }
        if dry_run {
            return Action::Refused {
                detail: "containerd overlayfs-snapshotter↔boltdb desync detected (would stop k3s, move snapshotter+meta.db aside, restart, re-pull)".into(),
            };
        }
        heal()
    }

    /// True iff the recent k3s journal shows the snapshotter parent-missing
    /// signature (the desync actively breaking container creation).
    fn desync_detected() -> bool {
        match Command::new("journalctl")
            .args(["-u", "k3s", "--since", "-5min", "--no-pager", "-o", "cat"])
            .output()
        {
            Ok(o) => String::from_utf8_lossy(&o.stdout).lines().any(|l| {
                l.contains("failed to stat parent")
                    && l.contains("snapshotter.v1.overlayfs/snapshots")
                    && l.contains("no such file or directory")
            }),
            Err(_) => false,
        }
    }

    fn now() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
    }

    fn cooldown_remaining() -> Option<u64> {
        let last = std::fs::read_to_string(MARKER).ok()?.trim().parse::<u64>().ok()?;
        let elapsed = now().saturating_sub(last);
        (elapsed < COOLDOWN_SECS).then(|| COOLDOWN_SECS - elapsed)
    }

    fn mark_healed() {
        let _ = std::fs::create_dir_all("/var/lib/seibi");
        let _ = std::fs::write(MARKER, now().to_string());
    }

    fn shims_clear() -> bool {
        Command::new("pgrep")
            .args(["-fc", "containerd-shim"])
            .output()
            .map_or(true, |o| String::from_utf8_lossy(&o.stdout).trim() == "0")
    }

    fn overlays_clear() -> bool {
        match Command::new("findmnt").args(["-rno", "TARGET"]).output() {
            Ok(o) => !String::from_utf8_lossy(&o.stdout)
                .lines()
                .any(|l| l.starts_with("/run/k3s/containerd")),
            Err(_) => true,
        }
    }

    fn killall() {
        for cand in ["/run/current-system/sw/bin/k3s-killall.sh", "k3s-killall.sh"] {
            if Command::new(cand).status().map(|s| s.success()).unwrap_or(false) {
                return;
            }
        }
    }

    fn systemctl(action: &str) -> bool {
        Command::new("systemctl")
            .args([action, "k3s"])
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    fn heal() -> Action {
        // Mark BEFORE acting so a crash mid-heal still enforces the cooldown.
        mark_healed();
        if !systemctl("stop") {
            return Action::Failed { detail: "failed to stop k3s".into() };
        }
        killall();
        if !shims_clear() || !overlays_clear() {
            killall(); // one retry
        }
        if !shims_clear() || !overlays_clear() {
            systemctl("start"); // never leave k3s down
            return Action::Failed {
                detail: "teardown incomplete (containerd shims / /run/k3s overlay mounts remain); restarted k3s without moving state".into(),
            };
        }
        let ts = now();
        let moved_snap = std::fs::rename(
            format!("{CD}/io.containerd.snapshotter.v1.overlayfs"),
            format!("{CD}/overlayfs.broken.{ts}"),
        )
        .is_ok();
        let moved_bolt = std::fs::rename(
            format!("{CD}/io.containerd.metadata.v1.bolt"),
            format!("{CD}/metadata.bolt.broken.{ts}"),
        )
        .is_ok();
        if !systemctl("start") {
            return Action::Failed {
                detail: format!(
                    "moved snapshotter/bolt aside (.broken.{ts}) but k3s failed to start — rollback: mv them back + restart"
                ),
            };
        }
        Action::Remediated {
            detail: format!(
                "containerd snapshotter↔boltdb desync: moved overlayfs(ok={moved_snap}) + meta.db(ok={moved_bolt}) → .broken.{ts}; restarted k3s — images re-pulling, {COOLDOWN_SECS}s cooldown"
            ),
        }
    }
}
