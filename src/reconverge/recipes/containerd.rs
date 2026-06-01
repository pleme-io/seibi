//! `ContainerdHeal` — the containerd overlayfs-snapshotter↔boltdb desync
//! reconciler. Migrated verbatim from the legacy `containerd_snapshot_heal`
//! recipe; the `MARKER`/`COOLDOWN_SECS`/`cooldown_remaining`/`mark_healed`
//! code is DELETED (the queue's `RateLimiter` + `min_interval()=900s` replace
//! it). Sources: `JournaldTail{k3s, containerd_desync_line, Critical}` (reacts
//! in ms) + a `PollTicker` backstop; the two coalesce to one Critical heal.
//!
//! Symptom (rio, 2026-05-30 → 06-01, 2-day outage): containerd's overlayfs
//! snapshotter boltdb (meta.db) references base-layer snapshot IDs whose
//! on-disk `snapshots/<id>/fs` dirs no longer exist — a desync after a reboot
//! (ZFS scrubbed clean; the inconsistency is above ZFS, in containerd's
//! metadata-vs-filesystem coupling). Every new sandbox fails
//! `CreateContainerError: failed to stat parent: …/snapshots/<id>/fs: no such
//! file or directory` → CoreDNS, Flux, metrics-server, traefik all wedge.
//!
//! Remediation (verified safe — rio is eventually-consistent; ALL cluster
//! state is in kine SQLite on the boot drive, NOT in the snapshotter): stop
//! k3s → kill shims + unmount /run/k3s overlays → gate on 0 shims / 0 overlay
//! mounts → move the overlayfs snapshotter + bolt index aside → start k3s.

use std::process::Command;
use std::time::Duration;

use async_trait::async_trait;
use convergence_trait::types::{Constraint, Declaration, Drift, DriftSeverity};

use crate::reconverge::reconciler::{Observed, ReconcileError, Reconciled, Reconciler};
use crate::reconverge::signal::ReconvergeSignal;

pub const KIND: &str = "reconverge.containerd-heal";
const CD: &str = "/var/lib/rancher/k3s/agent/containerd";

/// The single desync signature. SHARED between `JournaldTail::predicate`
/// (fire instantly) and `observe` (confirm at reconcile time) — solve-once,
/// so detection and confirmation can never drift.
#[must_use]
pub fn containerd_desync_line(line: &str) -> bool {
    line.contains("failed to stat parent")
        && line.contains("snapshotter.v1.overlayfs/snapshots")
        && line.contains("no such file or directory")
}

pub struct ContainerdHeal;

#[async_trait]
impl Reconciler for ContainerdHeal {
    const KIND: &'static str = KIND;

    fn declaration(&self) -> Declaration {
        Declaration {
            name: "containerd-snapshot-coherence".into(),
            intent: "containerd's overlayfs snapshotter boltdb stays coherent with on-disk \
                     snapshot dirs so new sandboxes can be created"
                .into(),
            constraints: vec![Constraint::Invariant(
                "no 'failed to stat parent …/overlayfs/snapshots/<id>/fs: no such file' in the \
                 k3s journal"
                    .into(),
            )],
        }
    }

    fn min_interval(&self) -> Duration {
        // 15 min — a persistent signature can't hot-loop k3s while images re-pull.
        Duration::from_secs(900)
    }

    async fn observe(&self, signal: &ReconvergeSignal) -> Result<Observed, ReconcileError> {
        // Evidence (a journald-matched line) means we got here in ms — but we
        // still re-observe reality: the probe is sub-second.
        if let Some(ev) = &signal.evidence {
            tracing::debug!(evidence = %ev, "containerd-heal observe triggered by journald match");
        }
        let desync = tokio::task::spawn_blocking(desync_detected)
            .await
            .map_err(|e| ReconcileError::new(format!("observe join: {e}")))?;
        Ok(serde_json::json!({ "desync": desync }))
    }

    fn diff(&self, observed: &Observed, _decl: &Declaration) -> Vec<Drift> {
        if observed.get("desync").and_then(serde_json::Value::as_bool) == Some(true) {
            vec![Drift {
                resource: "containerd/io.containerd.snapshotter.v1.overlayfs".into(),
                expected: serde_json::json!("snapshotter↔boltdb coherent"),
                actual: serde_json::json!("meta.db references missing snapshot fs dirs"),
                severity: DriftSeverity::Critical,
            }]
        } else {
            vec![]
        }
    }

    async fn act(&self, _drift: &[Drift], dry_run: bool) -> Result<Reconciled, ReconcileError> {
        if dry_run {
            return Ok(Reconciled::Refused {
                detail: "containerd overlayfs-snapshotter↔boltdb desync detected (would stop k3s, \
                         move snapshotter+meta.db aside, restart, re-pull)"
                    .into(),
            });
        }
        tokio::task::spawn_blocking(heal_blocking)
            .await
            .map_err(|e| ReconcileError::new(format!("heal join: {e}")))?
    }
}

/// True iff the recent k3s journal shows the snapshotter parent-missing
/// signature (the desync actively breaking container creation).
fn desync_detected() -> bool {
    match Command::new("journalctl")
        .args(["-u", "k3s", "--since", "-5min", "--no-pager", "-o", "cat"])
        .output()
    {
        Ok(o) => String::from_utf8_lossy(&o.stdout)
            .lines()
            .any(containerd_desync_line),
        Err(_) => false,
    }
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

/// Run the bundled k3s-killall.sh (kills shims + unmounts /run/k3s). NixOS
/// doesn't put it on PATH, so search known spots + the nix store. Returns true
/// iff one ran successfully.
fn run_killall() -> bool {
    let mut cands: Vec<String> = vec![
        "/run/current-system/sw/bin/k3s-killall.sh".into(),
        "/usr/local/bin/k3s-killall.sh".into(),
    ];
    if let Ok(o) = Command::new("sh").args(["-c", "command -v k3s-killall.sh"]).output() {
        let p = String::from_utf8_lossy(&o.stdout).trim().to_string();
        if !p.is_empty() {
            cands.insert(0, p);
        }
    }
    if let Ok(o) = Command::new("find")
        .args(["/nix/store", "-maxdepth", "3", "-name", "k3s-killall.sh", "-type", "f"])
        .output()
    {
        if let Some(p) = String::from_utf8_lossy(&o.stdout).lines().next() {
            if !p.is_empty() {
                cands.push(p.to_string());
            }
        }
    }
    cands.iter().any(|c| Command::new(c).status().map(|s| s.success()).unwrap_or(false))
}

/// Fallback when k3s-killall.sh isn't found: the shims reparent to init on
/// `stop` and keep the overlay mounts pinned — kill them and lazy-unmount
/// every /run/k3s mount (deepest-first).
fn manual_teardown() {
    let _ = Command::new("pkill").args(["-TERM", "-f", "containerd-shim"]).status();
    std::thread::sleep(Duration::from_secs(2));
    let _ = Command::new("pkill").args(["-KILL", "-f", "containerd-shim"]).status();
    if let Ok(o) = Command::new("findmnt").args(["-rno", "TARGET"]).output() {
        let s = String::from_utf8_lossy(&o.stdout);
        let mut targets: Vec<String> =
            s.lines().filter(|l| l.starts_with("/run/k3s")).map(str::to_string).collect();
        targets.sort_by_key(|t| std::cmp::Reverse(t.len())); // deepest first
        for t in &targets {
            let _ = Command::new("umount").args(["-l", t]).status();
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

/// The verbatim legacy `heal()` body, returning the typed `Reconciled`/`Err`
/// instead of the flat `Action`. Transient failures become `Err`
/// (shigoto-retry owns backoff); success is `Remediated`.
fn heal_blocking() -> Result<Reconciled, ReconcileError> {
    if !systemctl("stop") {
        return Err(ReconcileError::new("failed to stop k3s"));
    }
    if !run_killall() {
        manual_teardown();
    }
    if !shims_clear() || !overlays_clear() {
        manual_teardown(); // retry the teardown
    }
    if !shims_clear() || !overlays_clear() {
        systemctl("start"); // never leave k3s down
        return Err(ReconcileError::new(
            "teardown incomplete (containerd shims / /run/k3s overlay mounts remain); restarted \
             k3s without moving state",
        ));
    }
    // Same-second tag for the aside dirs (process-local monotonic-ish; only
    // needs to be unique-per-heal, and heals are 15min-rate-limited).
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
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
        return Err(ReconcileError::new(format!(
            "moved snapshotter/bolt aside (.broken.{ts}) but k3s failed to start — rollback: mv \
             them back + restart"
        )));
    }
    Ok(Reconciled::Remediated {
        detail: format!(
            "containerd snapshotter↔boltdb desync: moved overlayfs(ok={moved_snap}) + \
             meta.db(ok={moved_bolt}) → .broken.{ts}; restarted k3s — images re-pulling, 900s \
             cooldown"
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn desync_predicate_matches_real_signature() {
        let real = "time=\"...\" level=error msg=\"failed to stat parent \
                    /var/lib/rancher/k3s/agent/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/593/fs: \
                    no such file or directory\"";
        assert!(containerd_desync_line(real));
        assert!(!containerd_desync_line("level=info msg=\"started k3s\""));
    }
}
