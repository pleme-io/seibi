//! `FlakeLockBudget` (I6 — protect the cycle-break win) — a READ-ONLY guard
//! that alarms when a configured `flake.lock`'s node count exceeds its budget
//! or grows past a recorded baseline by more than `max_delta`.
//!
//! Why: we just killed an unbounded substrate↔gen flake-input growth (nix lock
//! 220 nodes, substrate 50). A future cyclic / transitively-unfollowed input
//! could silently reintroduce it; this guard makes that regression LOUD before
//! it lands in everyone's rebuild closure. It NEVER auto-fixes — a flake.lock
//! blowup needs an operator to find + `follows`-pin the offending input, not a
//! machine to rewrite the lock. So `act` always `Refused`s with the overage.

use std::time::Duration;

use async_trait::async_trait;
use convergence_trait::types::{Constraint, Declaration, Drift, DriftSeverity};

use crate::reconverge::reconciler::{Observed, ReconcileError, Reconciled, Reconciler};
use crate::reconverge::recipes::rebuild_efficiency::{Env, FlakeLockBudget as Budget, RealEnv, RebuildEfficiencyConfig};
use crate::reconverge::signal::ReconvergeSignal;

pub const KIND: &str = "reconverge.flake-lock-budget";

/// Count the `nodes` in a parsed flake.lock JSON. `flake.lock` is
/// `{ "nodes": { ... }, "root": "...", "version": 7 }`; the node count is the
/// number of keys under `nodes`. Pure + directly unit-tested.
#[must_use]
pub fn count_nodes(lock_json: &serde_json::Value) -> Option<usize> {
    lock_json.get("nodes").and_then(serde_json::Value::as_object).map(serde_json::Map::len)
}

/// Pure budget evaluation: given a measured node count + a budget, is it over?
/// `Some(reason)` = over budget (ceiling OR baseline-delta); `None` = within.
#[must_use]
pub fn over_budget(count: usize, b: &Budget) -> Option<String> {
    if count > b.max_nodes {
        return Some(format!(
            "{} nodes > ceiling {} (over by {})",
            count,
            b.max_nodes,
            count - b.max_nodes
        ));
    }
    if let Some(base) = b.baseline_nodes {
        if count > base.saturating_add(b.max_delta) {
            return Some(format!(
                "{} nodes grew {} over baseline {} (max_delta {})",
                count,
                count - base,
                base,
                b.max_delta
            ));
        }
    }
    None
}

pub struct FlakeLockBudgetGuard {
    config: RebuildEfficiencyConfig,
    env: std::sync::Arc<dyn Env>,
}

impl FlakeLockBudgetGuard {
    #[must_use]
    pub fn new(config: RebuildEfficiencyConfig) -> Self {
        Self { config, env: std::sync::Arc::new(RealEnv) }
    }

    #[must_use]
    pub fn with_env(config: RebuildEfficiencyConfig, env: std::sync::Arc<dyn Env>) -> Self {
        Self { config, env }
    }
}

#[async_trait]
impl Reconciler for FlakeLockBudgetGuard {
    const KIND: &'static str = KIND;

    fn declaration(&self) -> Declaration {
        Declaration {
            name: "flake-lock-budget".into(),
            intent: "configured flake.lock files stay within their committed \
                     node-count budget so a cyclic input can't silently \
                     reintroduce unbounded closure growth"
                .into(),
            constraints: vec![Constraint::Invariant(
                "each configured flake.lock node count <= max_nodes AND <= baseline + max_delta".into(),
            )],
        }
    }

    fn min_interval(&self) -> Duration {
        Duration::from_secs(300)
    }

    async fn observe(&self, _signal: &ReconvergeSignal) -> Result<Observed, ReconcileError> {
        // Measure every budgeted lock. A missing/unparseable lock is recorded
        // as a null count (loud in diff), never silently skipped.
        let mut rows = Vec::new();
        for b in &self.config.flake_lock_budgets {
            let count = self
                .env
                .read(&b.lock_path)
                .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
                .and_then(|v| count_nodes(&v));
            rows.push(serde_json::json!({
                "lock": b.lock_path.display().to_string(),
                "count": count,
                "over": count.and_then(|c| over_budget(c, b)),
            }));
        }
        Ok(serde_json::Value::Array(rows))
    }

    fn diff(&self, observed: &Observed, _decl: &Declaration) -> Vec<Drift> {
        observed
            .as_array()
            .map(|rows| {
                rows.iter()
                    .filter_map(|row| {
                        let over = row.get("over")?.as_str()?;
                        Some(Drift {
                            resource: row.get("lock")?.as_str()?.to_string(),
                            expected: serde_json::json!("node count within budget"),
                            actual: serde_json::json!(over),
                            severity: DriftSeverity::High,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    async fn act(&self, drift: &[Drift], _dry_run: bool) -> Result<Reconciled, ReconcileError> {
        // READ-ONLY GUARD: never auto-fixes. Always Refused with the overage.
        let alarms = drift
            .iter()
            .map(|d| format!("{}: {}", d.resource, d.actual.as_str().unwrap_or("over budget")))
            .collect::<Vec<_>>()
            .join("; ");
        tracing::error!(
            count = drift.len(),
            alarms = %alarms,
            "FLAKE-LOCK BUDGET EXCEEDED — a flake input likely lost its \
             `follows` pin and reintroduced closure growth. Find the new input \
             (`nix flake metadata`), pin it, re-lock. This guard does NOT auto-fix."
        );
        Ok(Reconciled::Refused {
            detail: format!("flake.lock budget exceeded: {alarms}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn budget(max: usize, base: Option<usize>, delta: usize) -> Budget {
        Budget {
            lock_path: PathBuf::from("/tmp/flake.lock"),
            max_nodes: max,
            baseline_nodes: base,
            max_delta: delta,
        }
    }

    #[test]
    fn under_ceiling_and_within_delta_is_ok() {
        let b = budget(250, Some(220), 20);
        assert_eq!(over_budget(220, &b), None, "exactly baseline is fine");
        assert_eq!(over_budget(235, &b), None, "+15 within max_delta 20");
        assert_eq!(over_budget(240, &b), None, "+20 == max_delta, not over");
    }

    #[test]
    fn over_ceiling_fires() {
        let b = budget(250, Some(220), 20);
        let reason = over_budget(251, &b).expect("over ceiling");
        assert!(reason.contains("> ceiling 250"), "{reason}");
    }

    #[test]
    fn baseline_delta_fires_below_ceiling() {
        // 245 < ceiling 250 BUT 245 > 220 + 20 → delta breach.
        let b = budget(250, Some(220), 20);
        let reason = over_budget(245, &b).expect("delta breach");
        assert!(reason.contains("over baseline 220"), "{reason}");
    }

    #[test]
    fn count_nodes_counts_the_nodes_object() {
        let lock = serde_json::json!({
            "nodes": { "root": {}, "nixpkgs": {}, "substrate": {} },
            "root": "root",
            "version": 7
        });
        assert_eq!(count_nodes(&lock), Some(3));
        assert_eq!(count_nodes(&serde_json::json!({"version": 7})), None);
    }

    #[tokio::test]
    async fn observe_diff_flags_over_budget_lock_and_act_refuses() {
        use crate::reconverge::recipes::rebuild_efficiency::Env;
        use std::collections::HashMap;
        use std::path::Path;
        use std::sync::Arc;

        // A flake.lock with 4 nodes against a ceiling of 2.
        let lock_path = PathBuf::from("/repo/flake.lock");
        let lock = serde_json::json!({
            "nodes": {"root":{}, "a":{}, "b":{}, "c":{}},
            "root":"root","version":7
        });

        struct MockEnv {
            files: HashMap<PathBuf, Vec<u8>>,
        }
        impl Env for MockEnv {
            fn read(&self, p: &Path) -> Option<Vec<u8>> {
                self.files.get(p).cloned()
            }
            fn git_tracked(&self, _r: &Path, _p: &Path) -> bool {
                false
            }
            fn gen_build_spec(&self, _r: &Path) -> Result<(), String> {
                Ok(())
            }
            fn git_add_force(&self, _r: &Path, _p: &Path) -> Result<(), String> {
                Ok(())
            }
            fn git_commit_path(&self, _r: &Path, _p: &Path, _m: &str) -> Result<(), String> {
                Ok(())
            }
        }

        let mut files = HashMap::new();
        files.insert(lock_path.clone(), serde_json::to_vec(&lock).unwrap());
        let env = Arc::new(MockEnv { files });

        let cfg = RebuildEfficiencyConfig {
            rebuild_input_repos: vec![],
            commit: false,
            flake_lock_budgets: vec![budget_at(&lock_path, 2)],
        };
        let guard = FlakeLockBudgetGuard::with_env(cfg, env);

        let observed = guard.observe(&dummy_signal()).await.unwrap();
        let drift = guard.diff(&observed, &guard.declaration());
        assert_eq!(drift.len(), 1, "4 nodes > ceiling 2 → drift");

        // Guard is read-only: even with dry_run=false it Refuses.
        match guard.act(&drift, false).await.unwrap() {
            Reconciled::Refused { detail } => assert!(detail.contains("flake.lock")),
            other => panic!("guard must Refuse, never act: {other:?}"),
        }
    }

    fn budget_at(path: &std::path::Path, max: usize) -> Budget {
        Budget {
            lock_path: path.to_path_buf(),
            max_nodes: max,
            baseline_nodes: None,
            max_delta: 20,
        }
    }

    fn dummy_signal() -> ReconvergeSignal {
        use crate::reconverge::signal::{SignalKey, Trigger};
        ReconvergeSignal::new(
            SignalKey::new(KIND, "node"),
            Trigger::Poll { source: "test", tick_seq: 0 },
        )
    }
}
