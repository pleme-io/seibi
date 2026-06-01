//! Coalescing priority queue for reconvergence signals: dedup by
//! [`SignalKey`], priority-ordered drain, per-key rate-limit (the typed,
//! in-process replacement for the per-recipe cooldown marker files).

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};
use std::time::{Duration, Instant};

use super::signal::{Priority, ReconvergeSignal, SignalKey};

/// One coalesced pending trigger (mirrors anomaly_tracker's Recurrence:
/// a key with a count + first/last seen).
#[derive(Debug, Clone)]
pub struct Pending {
    pub signal: ReconvergeSignal,
    pub coalesced: u32,
    pub first_seen: Instant,
    pub last_seen: Instant,
}

/// What the queue drains to the engine — carries the coalesced + recurrence
/// counts so the reconcile can attest "healed N coalesced triggers".
#[derive(Debug, Clone)]
pub struct ReadySignal {
    pub signal: ReconvergeSignal,
    pub coalesced: u32,
    pub recurrence: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OfferOutcome {
    Enqueued,
    Coalesced,
}

/// Heap ordering: highest priority first; within a priority, oldest
/// `observed_at` first (FIFO). `BinaryHeap` is a max-heap, so "older =
/// greater" via `Reverse(observed_at)`.
#[derive(Debug, Clone)]
struct HeapEntry {
    key: SignalKey,
    priority: Priority,
    observed_at: Instant,
}
impl PartialEq for HeapEntry {
    fn eq(&self, o: &Self) -> bool {
        self.priority == o.priority && self.observed_at == o.observed_at
    }
}
impl Eq for HeapEntry {}
impl Ord for HeapEntry {
    fn cmp(&self, o: &Self) -> Ordering {
        self.priority
            .cmp(&o.priority)
            .then_with(|| Reverse(self.observed_at).cmp(&Reverse(o.observed_at)))
    }
}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}

/// Per-key rate limiter: a key is `ready` once `window` has elapsed since
/// the last reconcile.
#[derive(Debug, Default, Clone)]
struct RateLimiter {
    until: Option<Instant>,
}
impl RateLimiter {
    fn ready(&self, now: Instant) -> bool {
        self.until.is_none_or(|u| now >= u)
    }
    fn reconciled(&mut self, at: Instant, window: Duration) {
        self.until = Some(at + window);
    }
    fn eligible_in(&self, now: Instant) -> Option<Duration> {
        self.until.and_then(|u| u.checked_duration_since(now))
    }
}

/// Coalescing priority queue. Enqueue merges by `SignalKey`; dequeue pops
/// the highest-priority / oldest key past its rate-limit window.
///
/// Invariants (property-tested below):
///  - DEDUP   — ≤1 pending per `SignalKey`.
///  - COALESCE— merge keeps max priority + freshest `observed_at` + ++recurrence.
///  - PRIORITY— Critical drains before Background.
///  - RATE-LIMIT — a cooling key is DEFERRED (re-considered later), never DROPPED.
#[derive(Default)]
pub struct SignalQueue {
    pending: HashMap<SignalKey, Pending>,
    heap: BinaryHeap<HeapEntry>,
    limiters: HashMap<SignalKey, RateLimiter>,
    recurrence: HashMap<(SignalKey, String), u32>,
}

impl SignalQueue {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Offer a signal from a Source. Dedup/coalesce applied here; rate-limit
    /// is enforced at drain (we never DROP a real drift signal — we defer it
    /// past the cooldown).
    pub fn offer(&mut self, sig: ReconvergeSignal) -> OfferOutcome {
        let now = Instant::now();
        *self
            .recurrence
            .entry((sig.key.clone(), sig.trigger.signature()))
            .or_insert(0) += 1;

        if let Some(p) = self.pending.get_mut(&sig.key) {
            p.coalesced += 1;
            p.last_seen = now;
            if sig.priority > p.signal.priority {
                p.signal.priority = sig.priority;
            }
            if sig.observed_at > p.signal.observed_at {
                p.signal.observed_at = sig.observed_at;
            }
            if p.signal.evidence.is_none() {
                p.signal.evidence = sig.evidence;
            }
            // Priority may have risen — push a fresh heap entry; the stale
            // lower one is filtered at drain by the `pending` membership check.
            self.heap.push(HeapEntry {
                key: p.signal.key.clone(),
                priority: p.signal.priority,
                observed_at: p.signal.observed_at,
            });
            return OfferOutcome::Coalesced;
        }
        self.heap.push(HeapEntry {
            key: sig.key.clone(),
            priority: sig.priority,
            observed_at: sig.observed_at,
        });
        self.pending.insert(
            sig.key.clone(),
            Pending { signal: sig, coalesced: 0, first_seen: now, last_seen: now },
        );
        OfferOutcome::Enqueued
    }

    /// Drain the next ready signal: highest priority, oldest-within-priority,
    /// whose key is past its rate-limit window. Cooling keys are buffered and
    /// re-pushed so they're reconsidered later. None ⇒ empty or all cooling.
    pub fn next_ready(&mut self, now: Instant) -> Option<ReadySignal> {
        let mut deferred = Vec::new();
        let result = loop {
            let Some(top) = self.heap.pop() else { break None };
            if !self.pending.contains_key(&top.key) {
                continue; // stale (already drained or superseded)
            }
            if !self.limiters.get(&top.key).is_none_or(|l| l.ready(now)) {
                deferred.push(top); // cooling — reconsider after the window
                continue;
            }
            let Some(p) = self.pending.remove(&top.key) else { continue };
            let recurrence = self
                .recurrence
                .get(&(p.signal.key.clone(), p.signal.trigger.signature()))
                .copied()
                .unwrap_or(1);
            break Some(ReadySignal { signal: p.signal, coalesced: p.coalesced, recurrence });
        };
        for d in deferred {
            self.heap.push(d);
        }
        result
    }

    /// Start the per-key rate-limit window after a reconcile. Critical halves
    /// the window (a recurring node-down after a heal deserves faster retry).
    pub fn mark_reconciled(
        &mut self,
        key: &SignalKey,
        at: Instant,
        min_interval: Duration,
        prio: Priority,
    ) {
        let w = if prio == Priority::Critical { min_interval / 2 } else { min_interval };
        self.limiters.entry(key.clone()).or_default().reconciled(at, w);
    }

    /// Soonest a rate-limited *pending* key becomes eligible — the engine
    /// sleeps until then instead of busy-waiting. None ⇒ nothing on a cooldown.
    #[must_use]
    pub fn next_eligible_in(&self, now: Instant) -> Option<Duration> {
        self.pending
            .keys()
            .filter_map(|k| self.limiters.get(k).and_then(|l| l.eligible_in(now)))
            .min()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reconverge::signal::Trigger;

    fn poll(kind: &'static str) -> ReconvergeSignal {
        ReconvergeSignal::new(SignalKey::new(kind, "rio"), Trigger::Poll { source: "t", tick_seq: 0 })
    }
    fn critical(kind: &'static str) -> ReconvergeSignal {
        ReconvergeSignal::new(
            SignalKey::new(kind, "rio"),
            Trigger::UnitFailed { unit: "k3s".into(), result: "failed".into() },
        )
    }

    #[test]
    fn dedup_one_pending_per_key() {
        let mut q = SignalQueue::new();
        assert_eq!(q.offer(poll("a")), OfferOutcome::Enqueued);
        assert_eq!(q.offer(poll("a")), OfferOutcome::Coalesced);
        assert_eq!(q.offer(poll("a")), OfferOutcome::Coalesced);
        let r = q.next_ready(Instant::now()).expect("one ready");
        assert_eq!(r.coalesced, 2, "3 offers → 1 enqueue + 2 coalesced");
        assert!(q.next_ready(Instant::now()).is_none(), "only one pending");
    }

    #[test]
    fn coalesce_keeps_max_priority() {
        let mut q = SignalQueue::new();
        q.offer(poll("a")); // Background
        q.offer(critical("a")); // raises to Critical (same key)
        let r = q.next_ready(Instant::now()).expect("ready");
        assert_eq!(r.signal.priority, Priority::Critical);
    }

    #[test]
    fn critical_drains_before_background() {
        let mut q = SignalQueue::new();
        q.offer(poll("bg"));
        q.offer(critical("crit"));
        let first = q.next_ready(Instant::now()).expect("first");
        assert_eq!(first.signal.priority, Priority::Critical, "Critical jumps the queue");
        let second = q.next_ready(Instant::now()).expect("second");
        assert_eq!(second.signal.priority, Priority::Background);
    }

    #[test]
    fn rate_limited_key_is_deferred_not_dropped() {
        let mut q = SignalQueue::new();
        let now = Instant::now();
        q.offer(poll("a"));
        q.mark_reconciled(&SignalKey::new("a", "rio"), now, Duration::from_secs(900), Priority::Background);
        // re-offer after reconcile: stays pending but is cooling
        q.offer(poll("a"));
        assert!(q.next_ready(now).is_none(), "cooling key deferred");
        assert!(!q.is_empty(), "but still pending (not dropped)");
        assert!(q.next_eligible_in(now).is_some(), "has a cooldown remaining");
        // past the window it drains
        let later = now + Duration::from_secs(901);
        assert!(q.next_ready(later).is_some(), "eligible after the window");
    }
}
