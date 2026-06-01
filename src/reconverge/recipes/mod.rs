//! The concrete reconcilers + the production wiring that assembles them with
//! their sources into a runnable [`Engine`].

pub mod build_spec_freshness;
pub mod containerd;
pub mod flake_lock_budget;
pub mod flux_git_auth;
pub mod kine_health;
pub mod rebuild_efficiency;

use std::path::PathBuf;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::engine::Engine;
use super::signal::{Priority, SignalKey};
use super::source::{JournaldTail, PollTicker};

/// Subject for all rio node-health signals (single node today; widen when a
/// second node onboards).
pub const NODE_SUBJECT: &str = "node";

/// Build the production engine: all reconcilers + their sources, fully wired.
///
/// - `containerd-heal`     ← `JournaldTail` on k3s (reacts in ms) + the poll
///   backstop. The two coalesce to one Critical heal.
/// - `flux-git-auth`       ← the poll backstop only (a stale PAT has no event;
///   M1 adds an `Inotify` source on the SOPS token path).
/// - `build-spec-freshness`← the poll backstop only — each tick re-classifies
///   the rebuild-input repos; `min_interval()=600s` rate-limits actual
///   regen+commit. (I1: keeps the committed-spec fast path warm.)
/// - `flake-lock-budget`   ← the poll backstop only — a read-only alarm when a
///   committed flake.lock node count blows past budget. (I6.)
///
/// The two rebuild-efficiency reconcilers read their config from
/// [`rebuild_efficiency::RebuildEfficiencyConfig`]; `wire` loads it from the
/// conventional `~/.config/seibi/rebuild-efficiency.yaml` (falling back to the
/// fleet-prescribed default), so the daemon needs no extra plumbing.
#[must_use]
pub fn wire(dry_run: bool, audit_path: Option<PathBuf>, cancel: CancellationToken) -> Engine {
    let containerd_key = SignalKey::new(containerd::KIND, NODE_SUBJECT);
    let flux_key = SignalKey::new(flux_git_auth::KIND, NODE_SUBJECT);
    let build_spec_key = SignalKey::new(build_spec_freshness::KIND, NODE_SUBJECT);
    let lock_budget_key = SignalKey::new(flake_lock_budget::KIND, NODE_SUBJECT);
    let kine_key = SignalKey::new(kine_health::KIND, NODE_SUBJECT);

    let re_config = rebuild_efficiency::RebuildEfficiencyConfig::load(&rebuild_efficiency_config_path())
        .unwrap_or_else(|e| {
            tracing::warn!(error = %e, "rebuild-efficiency config load failed; using fleet default");
            rebuild_efficiency::RebuildEfficiencyConfig::default()
        });

    let mut b = Engine::builder()
        .dry_run(dry_run)
        .reconciler(containerd::ContainerdHeal)
        .reconciler(flux_git_auth::FluxGitAuth)
        .reconciler(build_spec_freshness::BuildSpecFreshness::new(re_config.clone()))
        .reconciler(flake_lock_budget::FlakeLockBudgetGuard::new(re_config))
        .reconciler(kine_health::KineHealth)
        .source(JournaldTail::new(
            "k3s.service",
            containerd::containerd_desync_line,
            containerd_key.clone(),
            Priority::Critical,
        ))
        // kine struggling shows as `Slow SQL ... kine` in the k3s journal —
        // react the moment it thrashes (Elevated), not after the apiserver wedges.
        .source(JournaldTail::new(
            "k3s.service",
            kine_health::kine_slow_sql_line,
            kine_key.clone(),
            Priority::Elevated,
        ))
        .source(PollTicker::new(
            "poll-1m",
            Duration::from_secs(60),
            vec![containerd_key, flux_key, build_spec_key, lock_budget_key, kine_key],
        ));
    if let Some(p) = audit_path {
        b = b.audit_path(p);
    }
    b.build(cancel)
}

/// Conventional rebuild-efficiency config path: `~/.config/seibi/rebuild-efficiency.yaml`.
fn rebuild_efficiency_config_path() -> PathBuf {
    std::env::var_os("HOME")
        .map_or_else(|| PathBuf::from(".config"), PathBuf::from)
        .join(".config/seibi/rebuild-efficiency.yaml")
}
