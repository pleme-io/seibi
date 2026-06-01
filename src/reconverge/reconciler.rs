//! The node-health `Reconciler` — the runtime profile of convergence.
//!
//! Sibling (NOT subtype) of `convergence_trait::ConvergenceController`:
//! same `Drift`/`Declaration` vocabulary, same observe→diff→decide→act
//! semantics, but no render/deploy phase — on a node the mutation IS the
//! act. `ConvergenceController::verify` is sync + needs a `DeploymentHandle`
//! + `converge()` renders-but-never-deploys (a build-time generation
//! pipeline); node-health has nothing to render. So we consume the shared
//! data types and express the same semantics, without a fake adapter impl
//! that wouldn't compile. The compounding destination (3rd consumer): promote
//! this seam to a `convergence_trait::RuntimeController` peer trait upstream.

use std::time::Duration;

use async_trait::async_trait;
use convergence_trait::types::{Declaration, Drift};

use super::signal::ReconvergeSignal;

/// Opaque observed state. Boxed JSON keeps the trait object-safe-adjacent and
/// lets each reconciler carry its own probe shape with no generic parameter.
pub type Observed = serde_json::Value;

/// Beat 3+4 outcome — the typed peer of the legacy flat `Action` enum,
/// preserving its exit-code + journal semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reconciled {
    /// Reality already matched intent (was `Action::AlreadyConverged`).
    Converged,
    /// Drift detected and fixed (was `Action::Remediated`).
    Remediated { detail: String },
    /// Drift detected but the reconciler declined to act — needs an operator
    /// or a dry-run (was `Action::Refused`).
    Refused { detail: String },
}

impl Reconciled {
    /// Legacy exit-code parity: 0 converged/remediated, 1 refused.
    #[must_use]
    pub fn exit_class(&self) -> u8 {
        match self {
            Reconciled::Converged | Reconciled::Remediated { .. } => 0,
            Reconciled::Refused { .. } => 1,
        }
    }
}

/// Transient failures (was `Action::Failed`) become the Job's `Err`, so
/// shigoto-retry owns backoff; a persistent failure auto-deadletters.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{0}")]
pub struct ReconcileError(pub String);

impl ReconcileError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

impl shigoto_types::JobError for ReconcileError {}

/// A node-health Reconciler. The author writes ONLY these methods; `JobId`
/// assembly, coalescing, budget, retry, output capture, and the tick loop
/// are provided by the engine.
///
/// Not object-safe (it carries an associated `const KIND`); the engine holds
/// reconcilers behind the object-safe [`super::engine::ErasedReconciler`]
/// wrapper, which the blanket impl provides for free.
#[async_trait]
pub trait Reconciler: Send + Sync + 'static {
    /// Stable kind. == `SignalKey::reconciler_kind` (source routing) ==
    /// shigoto `JobKindId` (budget/retry/gate key). One identity end-to-end.
    const KIND: &'static str;

    /// Static declaration of what this reconciler keeps converged.
    fn declaration(&self) -> Declaration;

    /// Rate-limit window after a successful Act (replaces the legacy
    /// per-recipe `COOLDOWN_SECS` const). containerd-heal → 900s;
    /// flux-git-auth → 30s.
    fn min_interval(&self) -> Duration;

    /// Retry policy for transient `ReconcileError`s. Default `NoRetry` — the
    /// poll backstop re-observes next tick, so most reconcilers needn't retry
    /// in-dispatch. Override for genuinely transient probe failures.
    fn retry_policy(&self) -> shigoto_retry::RetryPolicy {
        shigoto_retry::RetryPolicy::NoRetry
    }

    /// BEAT 1 OBSERVE — probe reality. The triggering signal is passed so an
    /// event-driven reconcile MAY fast-path off `signal.evidence` (but MUST
    /// still confirm against reality). Pure read; no mutation.
    async fn observe(&self, signal: &ReconvergeSignal) -> Result<Observed, ReconcileError>;

    /// BEAT 2 DIFF — observed vs declaration → typed `Drift` set (empty =
    /// converged). The inverse of `ConvergenceController::verify` (which
    /// returns `Err(drift)`), reusing the canonical `Drift` type.
    fn diff(&self, observed: &Observed, decl: &Declaration) -> Vec<Drift>;

    /// BEAT 3+4 DECIDE+ACT — given non-empty drift, close the gap. `dry_run`
    /// short-circuits to `Refused`. Idempotent (may run after a coalesced
    /// burst). Classify (recurrence), Attest (sink) and Tick (rate-limit
    /// window) are the engine's job, not the author's.
    async fn act(&self, drift: &[Drift], dry_run: bool) -> Result<Reconciled, ReconcileError>;
}
