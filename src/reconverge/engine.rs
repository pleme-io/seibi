//! The engine — drains the queue and dispatches each ready signal through a
//! FRESH 1-node shigoto Dag (the tend-proven re-arm pattern). shigoto's FSM
//! has NO `Succeeded → Pending` edge (verified in `shigoto-types::advance`),
//! so a resident sticky scheduler would run each reconciler ONCE then go
//! dark. Rebuilding the Dag + scheduler per dispatch makes every trigger a
//! clean run while still inheriting `RetryPolicy` + `AuditFileEmitter`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use shigoto_dag::Dag;
use shigoto_emit::{AuditFileEmitter, NullEmitter, TransitionEmitter};
use shigoto_retry::RetryPolicy;
use shigoto_scheduler::{InProcessScheduler, Scheduler};
use shigoto_types::{ErasedJob, JobKindId, JobScope, JobSubject, OutputSink, RecordingJob};
use tokio_util::sync::CancellationToken;

use super::queue::{ReadySignal, SignalQueue};
use super::reconciler::{ReconcileError, Reconciled, Reconciler};
use super::signal::ReconvergeSignal;
use super::source::{supervise_source, Source};

/// Wraps a `Reconciler` + its triggering signal into a shigoto `RecordingJob`.
/// The blanket `impl<T: RecordingJob> Job for T` (verified in shigoto-types)
/// makes this a schedulable Job with no extra plumbing.
pub struct ReconcileJob<R: Reconciler> {
    reconciler: Arc<R>,
    signal: ReconvergeSignal,
    coalesced: u32,
    dry_run: bool,
    sink: Option<Arc<dyn OutputSink<Reconciled>>>,
}

#[async_trait::async_trait]
impl<R: Reconciler> RecordingJob for ReconcileJob<R> {
    type Output = Reconciled;
    type Error = ReconcileError;
    const KIND: &'static str = R::KIND;

    fn scope(&self) -> JobScope {
        JobScope::Global
    }

    fn subject(&self) -> JobSubject {
        JobSubject::Pinned(self.signal.key.subject.clone())
    }

    fn output_sink(&self) -> Option<&Arc<dyn OutputSink<Self::Output>>> {
        self.sink.as_ref()
    }

    /// The seven-beat tick, fused into one execute. Beats 1-4 here; Beat 5
    /// (Attest) = the OutputSink record + AuditFileEmitter transitions; Beat 6
    /// (Tick) = the engine starting the rate-limit window after dispatch.
    async fn execute_body(&self) -> Result<Reconciled, ReconcileError> {
        let decl = self.reconciler.declaration(); // declaration
        let observed = self.reconciler.observe(&self.signal).await?; // BEAT 1
        let drift = self.reconciler.diff(&observed, &decl); // BEAT 2
        if drift.is_empty() {
            return Ok(Reconciled::Converged);
        }
        tracing::debug!(
            kind = R::KIND,
            coalesced = self.coalesced,
            drift = drift.len(),
            "drift confirmed; acting"
        );
        self.reconciler.act(&drift, self.dry_run).await // BEAT 3+4
    }
}

/// Object-safe wrapper over a `Reconciler` (the trait itself carries an
/// associated `const KIND`, so it isn't object-safe). The blanket impl for
/// `Arc<R>` provides this for free; [`erase`] is the constructor.
pub trait ErasedReconciler: Send + Sync + 'static {
    fn kind(&self) -> &'static str;
    fn min_interval(&self) -> Duration;
    fn retry_policy(&self) -> RetryPolicy;
    fn make_job(
        &self,
        signal: ReconvergeSignal,
        coalesced: u32,
        dry_run: bool,
        sink: Option<Arc<dyn OutputSink<Reconciled>>>,
    ) -> Arc<dyn ErasedJob>;
}

impl<R: Reconciler> ErasedReconciler for Arc<R> {
    fn kind(&self) -> &'static str {
        R::KIND
    }
    fn min_interval(&self) -> Duration {
        (**self).min_interval()
    }
    fn retry_policy(&self) -> RetryPolicy {
        (**self).retry_policy()
    }
    fn make_job(
        &self,
        signal: ReconvergeSignal,
        coalesced: u32,
        dry_run: bool,
        sink: Option<Arc<dyn OutputSink<Reconciled>>>,
    ) -> Arc<dyn ErasedJob> {
        Arc::new(ReconcileJob {
            reconciler: Arc::clone(self),
            signal,
            coalesced,
            dry_run,
            sink,
        }) as Arc<dyn ErasedJob>
    }
}

/// Erase a concrete reconciler into the object-safe handle the engine stores.
#[must_use]
pub fn erase<R: Reconciler>(r: R) -> Arc<dyn ErasedReconciler> {
    Arc::new(Arc::new(r))
}

/// Bounded tick cap per dispatch (a 1-node Dag terminates in a handful of
/// ticks; the cap is a runaway backstop, matching tend's `MAX_TICKS`).
const MAX_TICKS: usize = 64;

/// Builder for [`Engine`]. Register reconcilers + sources, then `build`.
#[derive(Default)]
pub struct EngineBuilder {
    sources: Vec<Box<dyn Source>>,
    reconcilers: HashMap<&'static str, Arc<dyn ErasedReconciler>>,
    dry_run: bool,
    audit_path: Option<PathBuf>,
}

impl EngineBuilder {
    #[must_use]
    pub fn reconciler<R: Reconciler>(mut self, r: R) -> Self {
        self.reconcilers.insert(R::KIND, erase(r));
        self
    }

    #[must_use]
    pub fn source(mut self, s: impl Source) -> Self {
        self.sources.push(Box::new(s));
        self
    }

    #[must_use]
    pub fn dry_run(mut self, v: bool) -> Self {
        self.dry_run = v;
        self
    }

    #[must_use]
    pub fn audit_path(mut self, p: impl Into<PathBuf>) -> Self {
        self.audit_path = Some(p.into());
        self
    }

    #[must_use]
    pub fn build(self, cancel: CancellationToken) -> Engine {
        Engine {
            sources: self.sources,
            reconcilers: self.reconcilers,
            queue: SignalQueue::new(),
            dry_run: self.dry_run,
            audit_path: self.audit_path,
            cancel,
        }
    }
}

/// The reconvergence engine. Spawns every source, drains the coalescing queue,
/// and dispatches each ready signal to its reconciler.
pub struct Engine {
    sources: Vec<Box<dyn Source>>,
    reconcilers: HashMap<&'static str, Arc<dyn ErasedReconciler>>,
    queue: SignalQueue,
    dry_run: bool,
    audit_path: Option<PathBuf>,
    cancel: CancellationToken,
}

impl Engine {
    #[must_use]
    pub fn builder() -> EngineBuilder {
        EngineBuilder::default()
    }

    /// Run until cancelled. Event-driven when events flow (mpsc recv),
    /// poll-driven via the `PollTicker` source, and cooldown-driven via
    /// `next_eligible_in` — never busy-waiting.
    pub async fn run(mut self) -> anyhow::Result<()> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ReconvergeSignal>(4096);

        // 1. Spawn every Source under the restart-with-backoff supervisor —
        //    poll AND event share one lifecycle.
        for src in self.sources.drain(..) {
            let tx = tx.clone();
            let cancel = self.cancel.clone();
            tokio::spawn(supervise_source(src, tx, cancel));
        }
        drop(tx); // engine holds no producer; rx closes when all sources end.

        tracing::info!(
            reconcilers = self.reconcilers.len(),
            dry_run = self.dry_run,
            "reconverge engine started"
        );

        // 2. Main loop.
        loop {
            tokio::select! {
                () = self.cancel.cancelled() => break,
                maybe = rx.recv() => match maybe {
                    Some(sig) => {
                        self.queue.offer(sig);
                        // Drain any burst already queued → coalesce in one pass.
                        while let Ok(s) = rx.try_recv() {
                            self.queue.offer(s);
                        }
                    }
                    None => break, // all sources gone
                },
                () = sleep_opt(self.queue.next_eligible_in(Instant::now())) => {}
            }
            // Drain every ready (off-cooldown, priority-ordered) signal.
            while let Some(ready) = self.queue.next_ready(Instant::now()) {
                self.dispatch(ready).await;
            }
        }
        tracing::info!("reconverge engine stopped");
        Ok(())
    }

    /// Dispatch ONE drained signal through a fresh Dag + scheduler.
    async fn dispatch(&mut self, ready: ReadySignal) {
        let key = ready.signal.key.clone();
        let Some(reconciler) = self.reconcilers.get(key.reconciler_kind).cloned() else {
            tracing::warn!(kind = key.reconciler_kind, "no reconciler registered; dropping signal");
            return;
        };

        let scheduler =
            InProcessScheduler::new("reconverge").with_emitter(self.audit_emitter());
        scheduler
            .register_retry_policy(JobKindId::new(key.reconciler_kind), reconciler.retry_policy())
            .await;

        let job = reconciler.make_job(ready.signal.clone(), ready.coalesced, self.dry_run, None);
        let id = job.id();
        scheduler.register_job(job).await;

        let mut dag = Dag::new();
        dag.ensure_node(id.clone());
        // Drive to terminal: tick until no transition fired (tend pattern).
        for _ in 0..MAX_TICKS {
            match scheduler.tick(&mut dag).await {
                Ok(receipt) if receipt.transitions_this_tick.is_empty() => break,
                Ok(_) => {}
                Err(e) => {
                    tracing::error!(kind = key.reconciler_kind, error = %e, "scheduler tick failed");
                    break;
                }
            }
        }
        let phase = scheduler.snapshot(&dag).await.phases.get(&id).cloned();
        tracing::info!(
            kind = key.reconciler_kind,
            ?phase,
            coalesced = ready.coalesced,
            recurrence = ready.recurrence,
            "reconcile dispatched"
        );

        // BEAT 6 TICK — start the rate-limit window (replaces the marker file).
        self.queue.mark_reconciled(
            &key,
            Instant::now(),
            reconciler.min_interval(),
            ready.signal.priority,
        );
    }

    fn audit_emitter(&self) -> Arc<dyn TransitionEmitter> {
        match &self.audit_path {
            Some(p) => match AuditFileEmitter::new(p) {
                Ok(e) => Arc::new(e),
                Err(err) => {
                    tracing::warn!(error = %err, path = %p.display(), "audit emitter open failed; using null");
                    Arc::new(NullEmitter)
                }
            },
            None => Arc::new(NullEmitter),
        }
    }
}

/// Sleep for `Some(d)`, or park forever for `None` (nothing on cooldown).
async fn sleep_opt(d: Option<Duration>) {
    match d {
        Some(d) => tokio::time::sleep(d).await,
        None => std::future::pending::<()>().await,
    }
}
