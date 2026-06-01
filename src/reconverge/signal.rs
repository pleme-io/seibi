//! The typed reconvergence trigger. Every Source — poll or interrupt —
//! emits a [`ReconvergeSignal`]; the engine never branches on transport.

use std::time::Instant;

/// A typed reconvergence trigger. Named `ReconvergeSignal` (not
/// `ConvergenceSignal`) to avoid colliding with pleme-io/convergence-signal
/// (the liveness-PROOF service) — this is the trigger-side peer.
#[derive(Debug, Clone)]
pub struct ReconvergeSignal {
    /// Coalescing + routing identity. Maps 1:1 to a shigoto `JobId`.
    pub key: SignalKey,
    /// What physically fired this. Typed-exhaustive: adding a Source forces
    /// a new variant + every match arm to decide.
    pub trigger: Trigger,
    /// Scheduling weight. Critical node-health jumps ahead of routine poll.
    pub priority: Priority,
    /// When the SOURCE observed the underlying event (not enqueue time).
    pub observed_at: Instant,
    /// Optional evidence a reconciler MAY read to short-circuit its probe
    /// (e.g. the matched journald line). It MUST still re-observe reality —
    /// evidence is a hint, not authority.
    pub evidence: Option<String>,
}

impl ReconvergeSignal {
    /// Convenience constructor: priority defaults from the trigger kind,
    /// `observed_at` is now, no evidence.
    #[must_use]
    pub fn new(key: SignalKey, trigger: Trigger) -> Self {
        let priority = trigger.default_priority();
        Self { key, trigger, priority, observed_at: Instant::now(), evidence: None }
    }

    #[must_use]
    pub fn with_priority(mut self, p: Priority) -> Self {
        self.priority = p;
        self
    }

    #[must_use]
    pub fn with_evidence(mut self, e: impl Into<String>) -> Self {
        self.evidence = Some(e.into());
        self
    }
}

/// Coalescing + routing identity. `(reconciler_kind, subject)` — all
/// "containerd-heal for THIS node" signals are one. `reconciler_kind` ==
/// `Reconciler::KIND` == its shigoto `JobKindId`, so the queue routes by
/// kind with no side table.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SignalKey {
    pub reconciler_kind: &'static str,
    pub subject: String,
}

impl SignalKey {
    #[must_use]
    pub fn new(reconciler_kind: &'static str, subject: impl Into<String>) -> Self {
        Self { reconciler_kind, subject: subject.into() }
    }
}

/// The evidence half — typed-exhaustive union of every Source's output.
#[derive(Debug, Clone)]
pub enum Trigger {
    /// PollTicker fired — periodic full re-observe (the eventual-convergence
    /// safety net even if every event was missed).
    Poll { source: &'static str, tick_seq: u64 },
    /// A journald entry matched a Source predicate.
    JournalMatch { unit: &'static str, message: String },
    /// systemd reported a unit entered `failed`.
    UnitFailed { unit: String, result: String },
    /// kube watch delivered an object whose readiness diverged.
    K8sNotReady { gvk: &'static str, namespace: Option<String>, name: String },
    /// inotify fired on a watched path (e.g. a rotated SOPS-rendered PAT).
    PathChanged { path: std::path::PathBuf },
}

/// Scheduling weight. Higher drains first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    /// Routine poll; coalesces aggressively.
    Background = 0,
    /// A watched resource drifted (k8s / inotify / path).
    Elevated = 1,
    /// Node-health critical (containerd desync / unit-failed) — jumps queue.
    Critical = 2,
}

impl Trigger {
    #[must_use]
    pub fn default_priority(&self) -> Priority {
        match self {
            Trigger::Poll { .. } => Priority::Background,
            Trigger::PathChanged { .. }
            | Trigger::K8sNotReady { .. }
            | Trigger::JournalMatch { .. } => Priority::Elevated,
            Trigger::UnitFailed { .. } => Priority::Critical,
        }
    }

    /// The coalescing signature for recurrence tracking. Poll triggers share
    /// one signature; event triggers signature their payload via
    /// [`anomaly_signature`], so "same desync line 40×" counts as 40 of ONE
    /// recurrence, not 40 distinct ones.
    #[must_use]
    pub fn signature(&self) -> String {
        match self {
            Trigger::Poll { source, .. } => format!("poll:{source}"),
            Trigger::JournalMatch { message, .. } => anomaly_signature(message),
            Trigger::UnitFailed { unit, .. } => format!("unit-failed:{unit}"),
            Trigger::K8sNotReady { gvk, name, .. } => format!("k8s:{gvk}:{name}"),
            Trigger::PathChanged { path } => format!("path:{}", path.display()),
        }
    }
}

/// Stable signature for recurrence: collapse each *run* of digits to a single
/// `#` (so `593` and `28` both become `#` — length-independent), then BLAKE3 →
/// 12 hex. So `snapshots/593/fs` and `snapshots/28/fs` collapse to one
/// signature. Copied from pangea-operator anomaly_tracker for M0; extract a
/// shared `anomaly-signature` crate at the 3rd consumer.
#[must_use]
pub fn anomaly_signature(line: &str) -> String {
    let mut stripped = String::with_capacity(line.len());
    let mut prev_digit = false;
    for c in line.chars() {
        if c.is_ascii_digit() {
            if !prev_digit {
                stripped.push('#');
            }
            prev_digit = true;
        } else {
            stripped.push(c);
            prev_digit = false;
        }
    }
    blake3::hash(stripped.as_bytes()).to_hex()[..12].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poll_is_background_event_is_higher() {
        assert_eq!(Trigger::Poll { source: "t", tick_seq: 0 }.default_priority(), Priority::Background);
        assert_eq!(
            Trigger::UnitFailed { unit: "k3s".into(), result: "failed".into() }.default_priority(),
            Priority::Critical
        );
        assert!(Priority::Critical > Priority::Background);
    }

    #[test]
    fn same_desync_line_modulo_ids_shares_signature() {
        let a = Trigger::JournalMatch { unit: "k3s", message: "stat parent snapshots/593/fs no such file".into() };
        let b = Trigger::JournalMatch { unit: "k3s", message: "stat parent snapshots/28/fs no such file".into() };
        assert_eq!(a.signature(), b.signature(), "digit-stripped signatures must collapse");
    }
}
