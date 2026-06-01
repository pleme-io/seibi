//! `BuildSpecFreshness` (I1 — the self-improving loop) — keeps every
//! configured rebuild-input Rust repo's committed `Cargo.build-spec.json`
//! present + fresh, so substrate's lockfile-builder always takes the committed
//! fast path instead of eval-time IFD (`gen build .` in a `__noChroot`
//! sandbox).
//!
//! Freshness, per gen's contract (`gen-cargo::build_spec::hash_cargo_lock`):
//! the spec is git-TRACKED **and** its `cargo_lock_sha256` field equals
//! `SHA-256(Cargo.lock bytes)` rendered as lowercase hex. Three states:
//!
//!   - `Missing` — spec not git-tracked (untracked or gitignored). The
//!     lockfile-builder finds a committed spec ONLY when it's in the tree, so
//!     an untracked spec is invisible to consumers → IFD.
//!   - `Stale`   — spec tracked but `cargo_lock_sha256` ≠ current Cargo.lock
//!     hash (operator ran `cargo update` without regen) → substrate traces
//!     `Drifted` + re-IFDs.
//!   - `Fresh`   — tracked AND hash matches → committed fast path. Converged.
//!
//! ## act() is DRY-RUN by default
//!
//! With `commit=false` (the default), `act` returns `Reconciled::Refused`
//! with a LOUD log naming exactly which repos WOULD be regenerated+committed
//! (and that it's a dry-run). Only `commit=true` regenerates the spec
//! (`gen build .`), force-adds it past `.gitignore`, and commits it
//! (path-filtered, idempotent). NEVER pushes — operator owns the push.
//!
//! Design risks honored: idempotent (a no-op commit is `Ok`); platform-
//! general spec (we never pass `--filter-platform`); loud-degrade (any git/gen
//! error becomes `Err` so it surfaces, never a silent skip).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use convergence_trait::types::{Constraint, Declaration, Drift, DriftSeverity};

use crate::reconverge::reconciler::{Observed, ReconcileError, Reconciled, Reconciler};
use crate::reconverge::recipes::rebuild_efficiency::{
    sha256_hex, Env, RealEnv, RebuildEfficiencyConfig, LOCK_FILE, SPEC_FILE,
};
use crate::reconverge::signal::ReconvergeSignal;

pub const KIND: &str = "reconverge.build-spec-freshness";

/// Per-repo freshness classification — the closed three-variant state of one
/// rebuild-input repo's committed spec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SpecState {
    /// Spec not git-tracked (untracked OR gitignored) → invisible to substrate.
    Missing,
    /// Spec tracked but `cargo_lock_sha256` ≠ current Cargo.lock hash.
    Stale,
    /// Spec tracked AND hash matches → committed fast path. Converged.
    Fresh,
    /// Repo has no Cargo.lock (not a buildable Rust workspace at this path).
    /// Loud-degrade: not silently treated as Fresh.
    NoLockfile,
}

impl SpecState {
    #[must_use]
    pub fn is_drifted(self) -> bool {
        matches!(self, SpecState::Missing | SpecState::Stale)
    }
}

/// Classify ONE repo from pure inputs — the testable core of `observe`. Reads
/// nothing itself; the caller supplies the bytes + tracked flag via `Env`, so
/// this is a pure function exercised directly by unit tests.
#[must_use]
pub fn classify(
    spec_tracked: bool,
    spec_bytes: Option<&[u8]>,
    cargo_lock_bytes: Option<&[u8]>,
) -> SpecState {
    let Some(lock) = cargo_lock_bytes else {
        return SpecState::NoLockfile;
    };
    if !spec_tracked {
        return SpecState::Missing;
    }
    let Some(spec) = spec_bytes else {
        // Tracked-but-unreadable is a degenerate Missing (the file the index
        // names isn't on disk). Treat as Missing so we regen it.
        return SpecState::Missing;
    };
    let embedded = match serde_json::from_slice::<serde_json::Value>(spec) {
        Ok(v) => v
            .get("cargo_lock_sha256")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        Err(_) => None,
    };
    let current = sha256_hex(lock);
    match embedded {
        Some(h) if h == current => SpecState::Fresh,
        // Missing-hash spec (legacy / hand-trimmed) or mismatch → Stale.
        _ => SpecState::Stale,
    }
}

/// Per-repo classification record carried through observe→diff→act.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RepoState {
    pub repo: PathBuf,
    pub state: SpecState,
}

pub struct BuildSpecFreshness {
    config: RebuildEfficiencyConfig,
    env: Arc<dyn Env>,
}

impl BuildSpecFreshness {
    /// Production constructor — `RealEnv` + the given config.
    #[must_use]
    pub fn new(config: RebuildEfficiencyConfig) -> Self {
        Self { config, env: Arc::new(RealEnv) }
    }

    /// Test/DI constructor — inject a mock `Env`.
    #[must_use]
    pub fn with_env(config: RebuildEfficiencyConfig, env: Arc<dyn Env>) -> Self {
        Self { config, env }
    }

    /// Classify every configured repo via the `Env` boundary. Shared by
    /// `observe`; pulled out so the daemon and tests drive the same path.
    fn classify_all(&self) -> Vec<RepoState> {
        self.config
            .rebuild_input_repos
            .iter()
            .map(|repo| {
                let spec_path = repo.join(SPEC_FILE);
                let lock_path = repo.join(LOCK_FILE);
                let tracked = self.env.git_tracked(repo, Path::new(SPEC_FILE));
                let spec_bytes = self.env.read(&spec_path);
                let lock_bytes = self.env.read(&lock_path);
                let state = classify(tracked, spec_bytes.as_deref(), lock_bytes.as_deref());
                RepoState { repo: repo.clone(), state }
            })
            .collect()
    }
}

#[async_trait]
impl Reconciler for BuildSpecFreshness {
    const KIND: &'static str = KIND;

    fn declaration(&self) -> Declaration {
        Declaration {
            name: "build-spec-freshness".into(),
            intent: "every rebuild-input Rust repo commits a fresh \
                     Cargo.build-spec.json so substrate's lockfile-builder takes \
                     the committed fast path (no eval-time IFD `gen build`)"
                .into(),
            constraints: vec![Constraint::Invariant(
                "each configured repo's Cargo.build-spec.json is git-tracked AND \
                 its cargo_lock_sha256 == SHA-256(Cargo.lock)"
                    .into(),
            )],
        }
    }

    fn min_interval(&self) -> Duration {
        // 10 min — regen+commit is cheap, but a persistent drift shouldn't
        // hot-loop `gen build` while a `cargo update` settles.
        Duration::from_secs(600)
    }

    async fn observe(&self, _signal: &ReconvergeSignal) -> Result<Observed, ReconcileError> {
        let states = self.classify_all();
        serde_json::to_value(&states)
            .map_err(|e| ReconcileError::new(format!("serialize repo states: {e}")))
    }

    fn diff(&self, observed: &Observed, _decl: &Declaration) -> Vec<Drift> {
        let states: Vec<RepoState> = serde_json::from_value(observed.clone()).unwrap_or_default();
        states
            .into_iter()
            .filter(|rs| rs.state.is_drifted())
            .map(|rs| Drift {
                resource: format!("{}/{SPEC_FILE}", rs.repo.display()),
                expected: serde_json::json!("tracked && fresh (cargo_lock_sha256 == SHA-256(Cargo.lock))"),
                actual: serde_json::json!(format!("{:?}", rs.state)),
                // Functional, not security: a missing spec degrades rebuild
                // speed, it doesn't break correctness. Medium.
                severity: DriftSeverity::Medium,
            })
            .collect()
    }

    async fn act(&self, drift: &[Drift], dry_run: bool) -> Result<Reconciled, ReconcileError> {
        // The drifted repos, recovered from the Drift `resource` strings.
        let repos: Vec<PathBuf> = drift
            .iter()
            .filter_map(|d| d.resource.strip_suffix(&format!("/{SPEC_FILE}")).map(PathBuf::from))
            .collect();
        let names = repos.iter().map(|p| p.display().to_string()).collect::<Vec<_>>().join(", ");

        // Dry-run gate: engine-level `dry_run` OR config-level `commit=false`.
        if dry_run || !self.config.commit {
            tracing::warn!(
                repos = %names,
                count = repos.len(),
                commit = self.config.commit,
                dry_run,
                "BUILD-SPEC DRIFT (DRY-RUN): these repos would have their \
                 Cargo.build-spec.json regenerated (`gen build .`), un-gitignored, \
                 and committed — NO push. Set commit=true (and not --dry-run) to act."
            );
            return Ok(Reconciled::Refused {
                detail: format!(
                    "build-spec drift in {} repo(s) [{names}] (DRY-RUN: would regen+commit; \
                     commit={})",
                    repos.len(),
                    self.config.commit
                ),
            });
        }

        // commit=true: regen + force-add + commit each drifted repo. Loud-
        // degrade — the FIRST error aborts with a typed Err (shigoto-retry /
        // poll re-observes), never a silent partial.
        let env = Arc::clone(&self.env);
        let repos2 = repos.clone();
        let result = tokio::task::spawn_blocking(move || remediate_blocking(&*env, &repos2))
            .await
            .map_err(|e| ReconcileError::new(format!("remediate join: {e}")))?;
        result.map(|_| Reconciled::Remediated {
            detail: format!("regenerated + committed Cargo.build-spec.json for {} repo(s): {names}", repos.len()),
        })
    }
}

/// The blocking remediation body: for each drifted repo regen the spec, force
/// it past `.gitignore`, and commit it (path-filtered, idempotent). Returns the
/// first error loudly. NEVER pushes.
fn remediate_blocking(env: &dyn Env, repos: &[PathBuf]) -> Result<(), ReconcileError> {
    for repo in repos {
        let spec_rel = Path::new(SPEC_FILE);
        env.gen_build_spec(repo)
            .map_err(|e| ReconcileError::new(format!("gen build {}: {e}", repo.display())))?;
        env.git_add_force(repo, spec_rel)
            .map_err(|e| ReconcileError::new(format!("git add -f {}/{SPEC_FILE}: {e}", repo.display())))?;
        let msg = format!("chore({SPEC_FILE}): regenerate fresh build-spec (seibi build-spec-freshness)");
        env.git_commit_path(repo, spec_rel, &msg)
            .map_err(|e| ReconcileError::new(format!("git commit {}/{SPEC_FILE}: {e}", repo.display())))?;
        tracing::info!(repo = %repo.display(), "regenerated + committed fresh build-spec");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    // A fresh spec for a given lock: embed cargo_lock_sha256 = SHA-256(lock).
    fn fresh_spec_for(lock: &[u8]) -> Vec<u8> {
        format!("{{\"cargo_lock_sha256\":\"{}\"}}", sha256_hex(lock)).into_bytes()
    }

    #[test]
    fn classify_missing_when_untracked() {
        let lock = b"[[package]]\n";
        assert_eq!(classify(false, Some(&fresh_spec_for(lock)), Some(lock)), SpecState::Missing);
    }

    #[test]
    fn classify_fresh_when_tracked_and_hash_matches() {
        let lock = b"name = \"x\"\nversion = \"1\"\n";
        let spec = fresh_spec_for(lock);
        assert_eq!(classify(true, Some(&spec), Some(lock)), SpecState::Fresh);
    }

    #[test]
    fn classify_stale_when_hash_mismatches() {
        let lock_old = b"version = \"1\"\n";
        let lock_new = b"version = \"2\"\n"; // operator ran cargo update
        let spec = fresh_spec_for(lock_old); // spec still embeds OLD hash
        assert_eq!(classify(true, Some(&spec), Some(lock_new)), SpecState::Stale);
    }

    #[test]
    fn classify_stale_when_hash_absent() {
        let lock = b"version = \"1\"\n";
        let spec = b"{\"version\":1}"; // legacy spec, no cargo_lock_sha256
        assert_eq!(classify(true, Some(spec), Some(lock)), SpecState::Stale);
    }

    #[test]
    fn classify_no_lockfile_is_loud_not_fresh() {
        // No Cargo.lock at the path → NoLockfile (degrade loud), never Fresh.
        assert_eq!(classify(true, Some(b"{}"), None), SpecState::NoLockfile);
        assert!(!SpecState::NoLockfile.is_drifted(), "NoLockfile isn't actionable drift");
    }

    // ── A mock Env exercising observe→diff→act end-to-end ───────────────

    #[derive(Default)]
    struct MockEnv {
        files: HashMap<PathBuf, Vec<u8>>,
        tracked: std::collections::HashSet<PathBuf>,
        gen_calls: Mutex<Vec<PathBuf>>,
        commit_calls: Mutex<Vec<PathBuf>>,
    }

    impl Env for MockEnv {
        fn read(&self, path: &Path) -> Option<Vec<u8>> {
            self.files.get(path).cloned()
        }
        fn git_tracked(&self, repo: &Path, path: &Path) -> bool {
            self.tracked.contains(&repo.join(path))
        }
        fn gen_build_spec(&self, repo: &Path) -> Result<(), String> {
            self.gen_calls.lock().unwrap().push(repo.to_path_buf());
            Ok(())
        }
        fn git_add_force(&self, _repo: &Path, _path: &Path) -> Result<(), String> {
            Ok(())
        }
        fn git_commit_path(&self, repo: &Path, _path: &Path, _message: &str) -> Result<(), String> {
            self.commit_calls.lock().unwrap().push(repo.to_path_buf());
            Ok(())
        }
    }

    fn cfg_for(repos: Vec<PathBuf>, commit: bool) -> RebuildEfficiencyConfig {
        RebuildEfficiencyConfig {
            rebuild_input_repos: repos,
            commit,
            flake_lock_budgets: vec![],
        }
    }

    #[tokio::test]
    async fn dry_run_refuses_and_names_drifted_repos() {
        let repo = PathBuf::from("/repos/fleet");
        let lock = b"version = \"1\"\n".to_vec();
        let mut env = MockEnv::default();
        // Lock present, but spec UNTRACKED → Missing → drift.
        env.files.insert(repo.join(LOCK_FILE), lock);
        let env = Arc::new(env);

        let rec = BuildSpecFreshness::with_env(cfg_for(vec![repo.clone()], false), env.clone());
        let observed = rec.observe(&dummy_signal()).await.unwrap();
        let drift = rec.diff(&observed, &rec.declaration());
        assert_eq!(drift.len(), 1, "one missing spec → one drift");

        let outcome = rec.act(&drift, false).await.unwrap();
        match outcome {
            Reconciled::Refused { detail } => assert!(detail.contains("fleet"), "names the repo"),
            other => panic!("expected Refused in dry-run, got {other:?}"),
        }
        assert!(env.gen_calls.lock().unwrap().is_empty(), "dry-run must NOT call gen");
        assert!(env.commit_calls.lock().unwrap().is_empty(), "dry-run must NOT commit");
    }

    #[tokio::test]
    async fn commit_true_regenerates_and_commits_drifted() {
        let repo = PathBuf::from("/repos/tend");
        let lock = b"version = \"1\"\n".to_vec();
        let mut env = MockEnv::default();
        env.files.insert(repo.join(LOCK_FILE), lock);
        // spec untracked → Missing → drift
        let env = Arc::new(env);

        let rec = BuildSpecFreshness::with_env(cfg_for(vec![repo.clone()], true), env.clone());
        let observed = rec.observe(&dummy_signal()).await.unwrap();
        let drift = rec.diff(&observed, &rec.declaration());
        let outcome = rec.act(&drift, false).await.unwrap();
        match outcome {
            Reconciled::Remediated { detail } => assert!(detail.contains("tend")),
            other => panic!("expected Remediated with commit=true, got {other:?}"),
        }
        assert_eq!(env.gen_calls.lock().unwrap().as_slice(), &[repo.clone()], "gen called once");
        assert_eq!(env.commit_calls.lock().unwrap().as_slice(), &[repo], "committed once");
    }

    #[tokio::test]
    async fn fresh_repo_produces_no_drift() {
        let repo = PathBuf::from("/repos/frost");
        let lock = b"version = \"1\"\n".to_vec();
        let spec = fresh_spec_for(&lock);
        let mut env = MockEnv::default();
        env.files.insert(repo.join(LOCK_FILE), lock);
        env.files.insert(repo.join(SPEC_FILE), spec);
        env.tracked.insert(repo.join(SPEC_FILE)); // tracked + fresh
        let env = Arc::new(env);

        let rec = BuildSpecFreshness::with_env(cfg_for(vec![repo], false), env);
        let observed = rec.observe(&dummy_signal()).await.unwrap();
        let drift = rec.diff(&observed, &rec.declaration());
        assert!(drift.is_empty(), "a fresh tracked spec is converged");
    }

    fn dummy_signal() -> ReconvergeSignal {
        use crate::reconverge::signal::{SignalKey, Trigger};
        ReconvergeSignal::new(
            SignalKey::new(KIND, "node"),
            Trigger::Poll { source: "test", tick_seq: 0 },
        )
    }
}
