//! The concrete reconcilers + the production wiring that assembles them with
//! their sources into a runnable [`Engine`].

pub mod containerd;
pub mod flux_git_auth;

use std::path::PathBuf;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::engine::Engine;
use super::signal::{Priority, SignalKey};
use super::source::{JournaldTail, PollTicker};

/// Subject for all rio node-health signals (single node today; widen when a
/// second node onboards).
pub const NODE_SUBJECT: &str = "node";

/// Build the production engine: both reconcilers + their sources, fully wired.
///
/// - `containerd-heal` ← `JournaldTail` on k3s (reacts in ms) + the poll
///   backstop. The two coalesce to one Critical heal.
/// - `flux-git-auth`   ← the poll backstop only (a stale PAT has no event;
///   M1 adds an `Inotify` source on the SOPS token path).
#[must_use]
pub fn wire(dry_run: bool, audit_path: Option<PathBuf>, cancel: CancellationToken) -> Engine {
    let containerd_key = SignalKey::new(containerd::KIND, NODE_SUBJECT);
    let flux_key = SignalKey::new(flux_git_auth::KIND, NODE_SUBJECT);

    let mut b = Engine::builder()
        .dry_run(dry_run)
        .reconciler(containerd::ContainerdHeal)
        .reconciler(flux_git_auth::FluxGitAuth)
        .source(JournaldTail::new(
            "k3s.service",
            containerd::containerd_desync_line,
            containerd_key.clone(),
            Priority::Critical,
        ))
        .source(PollTicker::new(
            "poll-1m",
            Duration::from_secs(60),
            vec![containerd_key, flux_key],
        ));
    if let Some(p) = audit_path {
        b = b.audit_path(p);
    }
    b.build(cancel)
}
