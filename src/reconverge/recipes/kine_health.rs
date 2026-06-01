//! `KineHealth` — the k3s datastore (kine SQLite) health reconciler.
//!
//! kine is an MVCC append store on SQLite: every K8s write is a new revision
//! row. Compaction deletes old revisions but SQLite never shrinks the FILE
//! (freed pages stay allocated), so on a high-churn cluster `state.db` grows
//! unbounded — and on a slow disk a bloated DB makes every Range/LIST query
//! (which discovery + every controller LIST needs) crawl, until the apiserver
//! times out and discovery wedges. Incident 2026-06-01: `state.db` reached
//! 4.3GB on rio's QLC boot disk; a build storm saturating the same disk tipped
//! it over → apiserver discovery wedged → control plane down for ~1h.
//!
//! Heal = stop k3s (releases the kine lock; container shims survive, so
//! workloads keep running) → integrity-check → VACUUM (reclaim free pages;
//! NON-destructive, every revision row preserved) → start k3s. Same
//! stop→fix→start shape as `containerd-snapshot-heal`, cooldown-gated to 6h
//! because the VACUUM blinks the control plane.

use std::process::Command;
use std::time::Duration;

use async_trait::async_trait;
use convergence_trait::types::{Constraint, Declaration, Drift, DriftSeverity};

use crate::reconverge::reconciler::{Observed, ReconcileError, Reconciled, Reconciler};
use crate::reconverge::signal::ReconvergeSignal;

pub const KIND: &str = "reconverge.kine-health";

/// kine's SQLite datastore on a k3s server node.
const DB: &str = "/var/lib/rancher/k3s/server/db/state.db";
/// Bloat threshold — a healthy single-node kine is well under this. Above it,
/// queries on the slow boot disk start timing out under any IO pressure.
const SIZE_THRESHOLD_BYTES: u64 = 1_000_000_000; // ~1 GB
/// Sustained `Slow SQL` lines in the recent journal ⇒ the datastore is already
/// struggling even below the size threshold (react early, not after the wedge).
const SLOW_SQL_THRESHOLD: u64 = 10;

/// The k3s journal signature for a struggling kine — SHARED between the
/// `JournaldTail` source (fire the moment kine thrashes) and `observe`
/// (confirm at reconcile time). Solve-once: detection + confirmation can't drift.
#[must_use]
pub fn kine_slow_sql_line(line: &str) -> bool {
    line.contains("Slow SQL") && line.contains("kine")
}

pub struct KineHealth;

#[async_trait]
impl Reconciler for KineHealth {
    const KIND: &'static str = KIND;

    fn declaration(&self) -> Declaration {
        Declaration {
            name: "kine-datastore-health".into(),
            intent: "the k3s kine SQLite datastore stays compact + query-fast so the apiserver \
                     never starves on Range/LIST (discovery) queries"
                .into(),
            constraints: vec![Constraint::Invariant(
                "kine state.db < ~1GB and no sustained 'Slow SQL' in the k3s journal".into(),
            )],
        }
    }

    fn min_interval(&self) -> Duration {
        // 6h — the VACUUM blinks the control plane, so heal rarely. The poll
        // backstop still re-observes every tick; this only rate-limits the act.
        Duration::from_secs(6 * 3600)
    }

    async fn observe(&self, signal: &ReconvergeSignal) -> Result<Observed, ReconcileError> {
        if let Some(ev) = &signal.evidence {
            tracing::debug!(evidence = %ev, "kine-health observe triggered by Slow-SQL journal match");
        }
        let size = db_size();
        let slow = tokio::task::spawn_blocking(slow_sql_count)
            .await
            .map_err(|e| ReconcileError::new(format!("observe join: {e}")))?;
        Ok(serde_json::json!({ "size_bytes": size, "slow_sql_2m": slow }))
    }

    fn diff(&self, observed: &Observed, _decl: &Declaration) -> Vec<Drift> {
        let size = observed.get("size_bytes").and_then(serde_json::Value::as_u64).unwrap_or(0);
        let slow = observed.get("slow_sql_2m").and_then(serde_json::Value::as_u64).unwrap_or(0);
        if size > SIZE_THRESHOLD_BYTES || slow >= SLOW_SQL_THRESHOLD {
            vec![Drift {
                resource: "k3s/kine/state.db".into(),
                expected: serde_json::json!(format!(
                    "< {SIZE_THRESHOLD_BYTES} bytes and no sustained Slow SQL"
                )),
                actual: serde_json::json!({ "size_bytes": size, "slow_sql_2m": slow }),
                severity: DriftSeverity::High,
            }]
        } else {
            vec![]
        }
    }

    async fn act(&self, _drift: &[Drift], dry_run: bool) -> Result<Reconciled, ReconcileError> {
        if dry_run {
            return Ok(Reconciled::Refused {
                detail: format!(
                    "kine state.db is {} bytes (would stop k3s, integrity-check, VACUUM, start k3s)",
                    db_size()
                ),
            });
        }
        tokio::task::spawn_blocking(vacuum_blocking)
            .await
            .map_err(|e| ReconcileError::new(format!("vacuum join: {e}")))?
    }
}

fn db_size() -> u64 {
    std::fs::metadata(DB).map(|m| m.len()).unwrap_or(0)
}

/// Count `Slow SQL` lines mentioning kine in the last 2 minutes of the k3s journal.
fn slow_sql_count() -> u64 {
    match Command::new("journalctl")
        .args(["-u", "k3s", "--since", "-2min", "--no-pager", "-o", "cat"])
        .output()
    {
        Ok(o) => u64::try_from(
            String::from_utf8_lossy(&o.stdout)
                .lines()
                .filter(|l| kine_slow_sql_line(l))
                .count(),
        )
        .unwrap_or(u64::MAX),
        Err(_) => 0,
    }
}

fn systemctl(action: &str) -> bool {
    Command::new("systemctl")
        .args([action, "k3s"])
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Locate the sqlite3 binary. The reconverge.nix `path` declares `pkgs.sqlite`,
/// so it's normally on PATH; resolve it explicitly to be resilient.
fn sqlite3_bin() -> String {
    Command::new("sh")
        .args(["-c", "command -v sqlite3"])
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|p| !p.is_empty())
        .unwrap_or_else(|| "sqlite3".into())
}

/// stop k3s → integrity-check → VACUUM → start k3s. Non-destructive (preserves
/// every revision row; only reclaims SQLite free pages). Transient failures
/// become `Err` (engine retries / deadletters); success is `Remediated`.
fn vacuum_blocking() -> Result<Reconciled, ReconcileError> {
    let sqlite = sqlite3_bin();
    let before = db_size();

    if !systemctl("stop") {
        return Err(ReconcileError::new("failed to stop k3s for kine VACUUM"));
    }
    // Let kine release the SQLite lock.
    std::thread::sleep(Duration::from_secs(5));

    // Integrity-check FIRST: VACUUMing a corrupt DB can lose data. If corrupt,
    // restart k3s + surface the error (operator restores from an etcd snapshot).
    let ic = Command::new(&sqlite)
        .args([DB, "PRAGMA integrity_check;"])
        .output()
        .map(|o| {
            String::from_utf8_lossy(&o.stdout)
                .lines()
                .next()
                .unwrap_or("")
                .trim()
                .to_string()
        })
        .unwrap_or_else(|e| format!("integrity probe failed: {e}"));
    if ic != "ok" {
        systemctl("start"); // never leave the control plane down
        return Err(ReconcileError::new(format!(
            "kine integrity_check != ok ({ic}); restarted k3s WITHOUT VACUUM — restore from an etcd snapshot"
        )));
    }

    // VACUUM (the long step). 180s busy-timeout in case anything lingers.
    let vac = Command::new(&sqlite)
        .args([DB, "PRAGMA busy_timeout=180000; VACUUM;"])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    let after = db_size();

    if !systemctl("start") {
        return Err(ReconcileError::new(format!(
            "VACUUM done (ok={vac}, {before}->{after} bytes) but k3s failed to start — rollback: systemctl start k3s"
        )));
    }
    if !vac {
        return Err(ReconcileError::new(format!(
            "VACUUM did not complete cleanly (size {before}->{after}); k3s restarted"
        )));
    }

    let reclaimed_pct = if before > 0 { 100 - (after.saturating_mul(100) / before) } else { 0 };
    Ok(Reconciled::Remediated {
        detail: format!(
            "kine state.db VACUUMed {before}->{after} bytes ({reclaimed_pct}% reclaimed); k3s restarted — apiserver discovery recovered"
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slow_sql_predicate_matches_real_signature() {
        let real = "Slow SQL (started: ...) (total time: 2.530765858s): INSERT INTO kine(name, \
                    created, deleted, ...) values(?, ?, ?, ?, ?, ?, ?, ?)";
        assert!(kine_slow_sql_line(real));
        assert!(!kine_slow_sql_line("level=info msg=\"started k3s\""));
        assert!(!kine_slow_sql_line("Slow SQL on some other table"));
    }
}
