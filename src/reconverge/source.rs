//! Signal sources — poll AND event, one trait, indistinguishable downstream.
//!
//! A `Source` is a long-lived task: it owns its cadence (timer interval /
//! journald follow cursor / future kube-watch reconnect) and pushes
//! [`ReconvergeSignal`]s until cancelled. It MUST self-heal internally; a
//! `run` that returns `Err` means the source is dead and
//! [`supervise_source`] restarts it with backoff — so a wedged event source
//! degrades to poll-only LOUDLY, never silently.

use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;

use super::signal::{Priority, ReconvergeSignal, SignalKey, Trigger};

#[derive(thiserror::Error, Debug)]
pub enum SourceError {
    #[error("source io: {0}")]
    Io(#[from] std::io::Error),
    #[error("source backend: {0}")]
    Backend(String),
}

/// A producer of `ReconvergeSignal`s. Poll-tickers and OS event taps
/// implement the SAME trait; the engine sees one uniform stream.
#[async_trait]
pub trait Source: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    async fn run(
        &self,
        tx: Sender<ReconvergeSignal>,
        cancel: CancellationToken,
    ) -> Result<(), SourceError>;
}

/// Restart-with-backoff supervisor. A source that returns (Ok OR Err) without
/// the token being cancelled is restarted; backoff doubles to a 60s cap. The
/// loud-degrade guarantee: a dead event source is logged at each restart, and
/// the `PollTicker` backstop keeps eventual convergence regardless.
pub async fn supervise_source(
    src: Box<dyn Source>,
    tx: Sender<ReconvergeSignal>,
    cancel: CancellationToken,
) {
    const MAX_BACKOFF: Duration = Duration::from_secs(60);
    let mut backoff = Duration::from_secs(1);
    loop {
        if cancel.is_cancelled() {
            return;
        }
        match src.run(tx.clone(), cancel.clone()).await {
            Ok(()) if cancel.is_cancelled() => return,
            Ok(()) => {
                tracing::warn!(source = src.name(), "source exited cleanly but unexpectedly; restarting");
            }
            Err(e) => {
                tracing::error!(
                    source = src.name(),
                    error = %e,
                    backoff_s = backoff.as_secs(),
                    "source failed; restarting with backoff (degrade to poll is LOUD)"
                );
            }
        }
        tokio::select! {
            () = cancel.cancelled() => return,
            () = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// The safety net — preserves today's 1-minute eventual convergence. Each
/// tick emits a `Trigger::Poll` (Background priority) for every target key;
/// the queue coalesces a poll into any already-pending event for that key.
pub struct PollTicker {
    pub name: &'static str,
    pub interval: Duration,
    pub targets: Vec<SignalKey>,
}

impl PollTicker {
    #[must_use]
    pub fn new(name: &'static str, interval: Duration, targets: Vec<SignalKey>) -> Self {
        Self { name, interval, targets }
    }
}

#[async_trait]
impl Source for PollTicker {
    fn name(&self) -> &'static str {
        self.name
    }

    async fn run(
        &self,
        tx: Sender<ReconvergeSignal>,
        cancel: CancellationToken,
    ) -> Result<(), SourceError> {
        let mut ticker = tokio::time::interval(self.interval);
        // First tick fires immediately → an initial sweep on startup.
        let mut seq = 0u64;
        loop {
            tokio::select! {
                () = cancel.cancelled() => return Ok(()),
                _ = ticker.tick() => {
                    for key in &self.targets {
                        let sig = ReconvergeSignal::new(
                            key.clone(),
                            Trigger::Poll { source: self.name, tick_seq: seq },
                        );
                        // Poll is lowest-value: drop on a full queue — the next
                        // tick re-observes. Never block the ticker.
                        let _ = tx.try_send(sig);
                    }
                    seq = seq.wrapping_add(1);
                }
            }
        }
    }
}

/// The headline win: `journalctl -fu <unit>` follow → predicate match → a
/// Critical signal in milliseconds (vs up to a full poll interval). The
/// `predicate` is SHARED with the reconciler's `observe` (solve-once), so
/// detection and confirmation can never drift.
pub struct JournaldTail {
    pub unit: &'static str,
    pub predicate: fn(&str) -> bool,
    pub key: SignalKey,
    pub priority: Priority,
}

impl JournaldTail {
    #[must_use]
    pub fn new(
        unit: &'static str,
        predicate: fn(&str) -> bool,
        key: SignalKey,
        priority: Priority,
    ) -> Self {
        Self { unit, predicate, key, priority }
    }
}

#[async_trait]
impl Source for JournaldTail {
    fn name(&self) -> &'static str {
        self.unit
    }

    async fn run(
        &self,
        tx: Sender<ReconvergeSignal>,
        cancel: CancellationToken,
    ) -> Result<(), SourceError> {
        // ≤3-line shell glue (NO-SHELL exception): spawn the follow. The
        // typed-clean upgrade is the `systemd` crate's sd_journal seek_tail +
        // wait — a named M1+ follow-up. Seek-to-tail (`-n 0`): we only react
        // to NEW entries; missing one just defers detection to the next poll.
        let mut child = tokio::process::Command::new("journalctl")
            .args(["-fu", self.unit, "-n", "0", "-o", "cat"])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| SourceError::Backend("journalctl produced no stdout".into()))?;
        let mut lines = BufReader::new(stdout).lines();
        loop {
            tokio::select! {
                () = cancel.cancelled() => {
                    let _ = child.start_kill();
                    return Ok(());
                }
                line = lines.next_line() => match line {
                    Ok(Some(l)) => {
                        if (self.predicate)(&l) {
                            let sig = ReconvergeSignal::new(
                                self.key.clone(),
                                Trigger::JournalMatch { unit: self.unit, message: l.clone() },
                            )
                            .with_priority(self.priority)
                            .with_evidence(l);
                            // Critical: never drop. Block until the engine takes it
                            // (or is gone, in which case we're shutting down).
                            if tx.send(sig).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                    Ok(None) => {
                        return Err(SourceError::Backend(format!(
                            "journalctl -fu {} exited (log rotation / unit restart) — supervisor will reattach",
                            self.unit
                        )));
                    }
                    Err(e) => return Err(SourceError::Io(e)),
                }
            }
        }
    }
}
