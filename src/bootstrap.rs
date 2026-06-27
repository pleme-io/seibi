//! `seibi bootstrap` — the idempotent, declare-and-observe cluster bootstrap
//! orchestrator. ONE shigoto Dag drives the deterministic-secret pipeline end
//! to end:
//!
//! ```text
//!   ensure-secrets → seed-ssm → verify-seed → declare-cluster
//!                                    → observe-cluster → observe-flux
//! ```
//!
//! The secrets plane (ensure/seed/verify) runs fully here — SOPS is the single
//! source of truth, projected to SSM SecureString and read back to prove no
//! drift, so cluster ups/downs reproduce the SAME identity. The cluster/flux
//! plane is declare-and-observe (★★ PLATFORM-MEDIATED): seibi NEVER plans or
//! applies — those steps report a typed `Deferred` outcome naming the GitOps
//! action (commit `spec.suspend:false` on the InfrastructureTemplate; observe
//! via the rio pangea-operator) until cross-cluster access is wired (M2). They
//! are typed-Deferred — never a silent Ok, never a panic (★★ TYPED-SPEC).
//!
//! A custom [`AllUpstreamsSucceeded`] gate makes a downstream step terminal-
//! `Skipped` when any upstream did not succeed — so a failed seed can never let
//! `declare-cluster` run against a half-seeded cluster (the scheduler's implicit
//! `AllUpstreamsTerminal` would otherwise proceed past a Deadlettered upstream,
//! since it treats Deadlettered as "terminal").
//!
//! Idempotent by construction: re-running a converged cluster seeds nothing
//! (read-before-write in [`ssm_bootstrap::seed`]), verifies clean, and the
//! receipt reports `AlreadyReady`.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use clap::Args as ClapArgs;
use tracing::{info, warn};

use shigoto_dag::Dag;
use shigoto_emit::{AuditFileEmitter, NullEmitter, TransitionEmitter};
use shigoto_gate::{Gate, GateContext, GateOutcome};
use shigoto_retry::RetryPolicy;
use shigoto_scheduler::{InProcessScheduler, Scheduler};
use shigoto_types::{
    JobId, JobKindId, JobPhase, JobScope, JobSubject, OutputSink, RecordingJob, SkipReason,
};

use crate::ssm_bootstrap::{self, SsmConfig};

/// Bounded tick cap — the 6-node linear chain terminates in a handful of ticks;
/// the cap is a runaway backstop (matches tend/reconverge's MAX_TICKS).
const MAX_TICKS: usize = 64;

// ── Typed step outcome + report ──────────────────────────────────────

/// What a step actually did. `Converged` = already in the desired state, no
/// change. `Applied` = made a change. `Deferred` = a declare-and-observe step
/// whose realization is owned elsewhere (a GitOps commit + the pangea-operator),
/// not executed in-process by seibi. `Failed` = the step errored (the reason is
/// carried in the report detail so the receipt can surface WHY).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepOutcome {
    Converged,
    Applied,
    Deferred,
    Failed,
}

impl StepOutcome {
    fn label(self) -> &'static str {
        match self {
            StepOutcome::Converged => "converged",
            StepOutcome::Applied => "applied",
            StepOutcome::Deferred => "deferred",
            StepOutcome::Failed => "failed",
        }
    }
}

/// One step's typed result, captured into the run ledger (a side channel — the
/// scheduler discards typed Job outputs at its erased boundary, and an
/// `OutputSink` only fires on success, so the ledger is how the receipt sees
/// failures too).
#[derive(Debug, Clone)]
pub struct StepReport {
    pub step: &'static str,
    pub outcome: StepOutcome,
    pub detail: String,
}

/// Append one step's typed result to the shared run ledger.
fn record(ledger: &Ledger, step: &'static str, outcome: StepOutcome, detail: String) {
    ledger
        .lock()
        .expect("bootstrap ledger mutex poisoned")
        .push(StepReport { step, outcome, detail });
}

/// Shared, ordered record of every step's typed outcome.
type Ledger = Arc<Mutex<Vec<StepReport>>>;

#[derive(thiserror::Error, Debug)]
pub enum BootstrapError {
    #[error("{step}: {msg}")]
    Step { step: &'static str, msg: String },
}

impl BootstrapError {
    fn step(step: &'static str, msg: impl Into<String>) -> Self {
        BootstrapError::Step {
            step,
            msg: msg.into(),
        }
    }
}

// ── Gate: every upstream SUCCEEDED (not merely terminal) ─────────────

/// Refuses (Skip) a job whose any direct DAG predecessor did not reach
/// `Succeeded`. Unlike the scheduler's implicit `AllUpstreamsTerminal` — which
/// treats a Deadlettered/Skipped upstream as terminal and lets the downstream
/// proceed — this gate makes a downstream of a FAILED step terminal-`Skipped`,
/// so we never declare or observe a cluster whose secrets failed to seed.
struct AllUpstreamsSucceeded;

impl Gate for AllUpstreamsSucceeded {
    fn name(&self) -> &'static str {
        "all-upstreams-succeeded"
    }

    fn evaluate(&self, ctx: &GateContext) -> GateOutcome {
        for pred in ctx.dag.predecessors(ctx.job_id) {
            let phase = ctx
                .snapshot
                .phases
                .get(&pred)
                .cloned()
                .unwrap_or(JobPhase::Pending);
            match phase {
                JobPhase::Succeeded => {}
                JobPhase::Deadlettered | JobPhase::Skipped(_) => {
                    return GateOutcome::Skip(SkipReason::Other(format!(
                        "upstream {} did not succeed ({})",
                        pred.kind.0,
                        phase.kind()
                    )));
                }
                _ => return GateOutcome::Wait,
            }
        }
        GateOutcome::Pass
    }
}

/// Build a step's `JobId` — the same coordinates every job reports via the
/// blanket `Job::id()` (scope Global, kind = KIND, subject = the cluster), so
/// the Dag edges line up with the registered jobs.
fn job_id(kind: &str, cluster: &str) -> JobId {
    JobId {
        scope: JobScope::Global,
        kind: JobKindId::new(kind),
        subject: JobSubject::Pinned(cluster.to_string()),
    }
}

// ── Jobs ─────────────────────────────────────────────────────────────
//
// Every job is a RecordingJob with no OutputSink (Output = ()): it records its
// typed StepReport directly to the ledger — success AND failure — then returns
// Ok/Err so the FSM advances. `output_sink()` is None; capture is the ledger.

/// `ensure-secrets` — the SOPS source must hold all boot secrets BEFORE any AWS
/// write. Pure SOPS read (no AWS); fail-fast if the deterministic source is
/// incomplete.
struct EnsureSecretsJob {
    cfg: Arc<SsmConfig>,
    ledger: Ledger,
}

#[async_trait::async_trait]
impl RecordingJob for EnsureSecretsJob {
    type Output = ();
    type Error = BootstrapError;
    const KIND: &'static str = "bootstrap.ensure-secrets";

    fn scope(&self) -> JobScope {
        JobScope::Global
    }
    fn subject(&self) -> JobSubject {
        JobSubject::Pinned(self.cfg.cluster.clone())
    }
    fn output_sink(&self) -> Option<&Arc<dyn OutputSink<Self::Output>>> {
        None
    }

    async fn execute_body(&self) -> Result<(), BootstrapError> {
        let missing = ssm_bootstrap::sops_missing(&self.cfg);
        if !missing.is_empty() {
            let msg = format!(
                "{} of {} boot secret(s) absent from SOPS: {} — add them (e.g. `seibi pki-bootstrap`) before bootstrapping",
                missing.len(),
                ssm_bootstrap::SECRET_COUNT,
                missing.join(", ")
            );
            record(&self.ledger, Self::KIND, StepOutcome::Failed, msg.clone());
            return Err(BootstrapError::step(Self::KIND, msg));
        }
        record(
            &self.ledger,
            Self::KIND,
            StepOutcome::Converged,
            format!("{} boot secrets present in SOPS", ssm_bootstrap::SECRET_COUNT),
        );
        Ok(())
    }
}

/// `seed-ssm` — project SOPS → SSM SecureString, idempotently (read-before-
/// write: a parameter already equal to SOPS is left untouched).
struct SeedSsmJob {
    cfg: Arc<SsmConfig>,
    client: aws_sdk_ssm::Client,
    dry_run: bool,
    ledger: Ledger,
}

#[async_trait::async_trait]
impl RecordingJob for SeedSsmJob {
    type Output = ();
    type Error = BootstrapError;
    const KIND: &'static str = "bootstrap.seed-ssm";

    fn scope(&self) -> JobScope {
        JobScope::Global
    }
    fn subject(&self) -> JobSubject {
        JobSubject::Pinned(self.cfg.cluster.clone())
    }
    fn output_sink(&self) -> Option<&Arc<dyn OutputSink<Self::Output>>> {
        None
    }

    async fn execute_body(&self) -> Result<(), BootstrapError> {
        let summary = match ssm_bootstrap::seed(&self.cfg, &self.client, self.dry_run).await {
            Ok(s) => s,
            Err(e) => {
                record(&self.ledger, Self::KIND, StepOutcome::Failed, e.to_string());
                return Err(BootstrapError::step(Self::KIND, e.to_string()));
            }
        };
        if !summary.missing.is_empty() {
            let msg = format!(
                "{} secret(s) missing from SOPS: {}",
                summary.missing.len(),
                summary.missing.join(", ")
            );
            record(&self.ledger, Self::KIND, StepOutcome::Failed, msg.clone());
            return Err(BootstrapError::step(Self::KIND, msg));
        }
        let outcome = if summary.written.is_empty() {
            StepOutcome::Converged
        } else {
            StepOutcome::Applied
        };
        let prefix = if self.dry_run { "DRY-RUN: " } else { "" };
        let detail = format!(
            "{}{} written, {} unchanged",
            prefix,
            summary.written.len(),
            summary.unchanged.len()
        );
        record(&self.ledger, Self::KIND, outcome, detail);
        Ok(())
    }
}

/// `verify-seed` — read each SecureString back and assert it equals SOPS. In
/// apply mode this is the hard determinism gate (drift → fail). In dry-run it
/// is advisory (reports what would change; never fails the plan).
struct VerifySeedJob {
    cfg: Arc<SsmConfig>,
    client: aws_sdk_ssm::Client,
    dry_run: bool,
    ledger: Ledger,
}

#[async_trait::async_trait]
impl RecordingJob for VerifySeedJob {
    type Output = ();
    type Error = BootstrapError;
    const KIND: &'static str = "bootstrap.verify-seed";

    fn scope(&self) -> JobScope {
        JobScope::Global
    }
    fn subject(&self) -> JobSubject {
        JobSubject::Pinned(self.cfg.cluster.clone())
    }
    fn output_sink(&self) -> Option<&Arc<dyn OutputSink<Self::Output>>> {
        None
    }

    async fn execute_body(&self) -> Result<(), BootstrapError> {
        let v = match ssm_bootstrap::verify(&self.cfg, &self.client).await {
            Ok(v) => v,
            Err(e) => {
                record(&self.ledger, Self::KIND, StepOutcome::Failed, e.to_string());
                return Err(BootstrapError::step(Self::KIND, e.to_string()));
            }
        };
        if self.dry_run {
            let detail = if v.drift.is_empty() {
                format!("{} secrets already match SOPS (would stay converged)", v.verified.len())
            } else {
                format!("{} match, {} would change on apply", v.verified.len(), v.drift.len())
            };
            record(&self.ledger, Self::KIND, StepOutcome::Deferred, detail);
            return Ok(());
        }
        if !v.drift.is_empty() {
            let msg = format!("{} secret(s) drifted: {}", v.drift.len(), v.drift.join(", "));
            record(&self.ledger, Self::KIND, StepOutcome::Failed, msg.clone());
            return Err(BootstrapError::step(Self::KIND, msg));
        }
        record(
            &self.ledger,
            Self::KIND,
            StepOutcome::Converged,
            format!("{} secrets verified == SOPS", v.verified.len()),
        );
        Ok(())
    }
}

/// Macro for the declare-and-observe steps: each is a typed `Deferred` no-op
/// that names the GitOps action it stands for. seibi declares + observes; it
/// never plans or applies (★★ PLATFORM-MEDIATED). The body is computed from the
/// cluster so the receipt is concrete.
macro_rules! deferred_job {
    ($name:ident, $kind:literal, $detail:expr) => {
        struct $name {
            cfg: Arc<SsmConfig>,
            ledger: Ledger,
        }

        #[async_trait::async_trait]
        impl RecordingJob for $name {
            type Output = ();
            type Error = BootstrapError;
            const KIND: &'static str = $kind;

            fn scope(&self) -> JobScope {
                JobScope::Global
            }
            fn subject(&self) -> JobSubject {
                JobSubject::Pinned(self.cfg.cluster.clone())
            }
            fn output_sink(&self) -> Option<&Arc<dyn OutputSink<Self::Output>>> {
                None
            }

            async fn execute_body(&self) -> Result<(), BootstrapError> {
                // Bind the (non-capturing) closure to a fn pointer first, then
                // call it — calling the closure literal inline parses the trailing
                // args as part of the closure body (E0618).
                let make: fn(&str) -> String = $detail;
                let detail = make(self.cfg.cluster.as_str());
                record(&self.ledger, Self::KIND, StepOutcome::Deferred, detail);
                Ok(())
            }
        }
    };
}

deferred_job!(
    DeclareClusterJob,
    "bootstrap.declare-cluster",
    |cluster: &str| format!(
        "declare: commit spec.suspend=false on the InfrastructureTemplate for {cluster}; the rio pangea-operator plans+applies via magma (seibi never plans/applies)"
    )
);
deferred_job!(
    ObserveClusterJob,
    "bootstrap.observe-cluster",
    |cluster: &str| format!(
        "observe: InfrastructureTemplate status.lastCycle for {cluster} via the rio operator / grafana-rio MCP (cross-cluster; M2)"
    )
);
deferred_job!(
    ObserveFluxJob,
    "bootstrap.observe-flux",
    |cluster: &str| format!(
        "observe: the downstream FluxCD reconcile for {cluster} (cross-cluster; M2)"
    )
);

// ── CLI ──────────────────────────────────────────────────────────────

#[derive(ClapArgs)]
pub struct Args {
    /// Cluster name (e.g. akeyless-dev). Substituted into SOPS paths + the
    /// default SSM prefix, and the JobSubject of every step.
    #[arg(long)]
    cluster: String,

    /// SSM parameter prefix. Defaults to `/pangea/<cluster>/secrets`.
    #[arg(long)]
    prefix: Option<String>,

    /// AWS region for the SSM parameters.
    #[arg(long, default_value = "us-east-1")]
    region: String,

    /// SOPS-encrypted secrets file (defaults to the nix repo's secrets.yaml).
    #[arg(long, env = "SEIBI_SECRETS_FILE")]
    secrets_file: Option<PathBuf>,

    /// SOPS age key file for decryption.
    #[arg(long, env = "SOPS_AGE_KEY_FILE")]
    age_key_file: Option<PathBuf>,

    /// Plan only: seed writes are skipped (verify reads back what is live),
    /// and the declare/observe steps stay deferred. Nothing in AWS is mutated.
    #[arg(long)]
    dry_run: bool,

    /// Append every shigoto transition as a JSONL line to this path (the
    /// resumable audit trail; `jq`-readable like every other seibi audit log).
    #[arg(long)]
    audit_log: Option<PathBuf>,
}

pub async fn run(args: Args) -> Result<ExitCode> {
    let secrets_file = args.secrets_file.unwrap_or_else(|| {
        crate::common::find_git_root()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("secrets.yaml")
    });
    if !secrets_file.exists() {
        anyhow::bail!(
            "secrets file not found: {} (set --secrets-file or run from the nix repo on cid)",
            secrets_file.display()
        );
    }
    let age_key_file = args.age_key_file.unwrap_or_else(crate::common::default_key_file);
    if !age_key_file.exists() {
        anyhow::bail!(
            "age key not found at {} — run on the host with the SOPS age key (cid)",
            age_key_file.display()
        );
    }
    let prefix = args
        .prefix
        .unwrap_or_else(|| format!("/pangea/{}/secrets", args.cluster));
    let cfg = Arc::new(SsmConfig {
        cluster: args.cluster.clone(),
        prefix,
        region: args.region.clone(),
        secrets_file,
        age_key_file,
    });

    info!(
        cluster = %cfg.cluster,
        prefix = %cfg.prefix,
        region = %cfg.region,
        dry_run = args.dry_run,
        "seibi bootstrap: driving the cluster bootstrap Dag"
    );

    let client = ssm_bootstrap::make_client(&cfg.region).await;
    let ledger: Ledger = Arc::new(Mutex::new(Vec::new()));

    // Emitter: append the transition trail as JSONL when --audit-log is set.
    let emitter: Arc<dyn TransitionEmitter> = match &args.audit_log {
        Some(p) => match AuditFileEmitter::new(p) {
            Ok(e) => Arc::new(e),
            Err(err) => {
                warn!(error = %err, path = %p.display(), "audit emitter open failed; using null");
                Arc::new(NullEmitter::new())
            }
        },
        None => Arc::new(NullEmitter::new()),
    };

    let scheduler = InProcessScheduler::new("seibi-bootstrap").with_emitter(emitter);

    // The success-gate on every step (a root with no predecessors trivially
    // passes). This is what makes a failed upstream SKIP its descendants
    // (terminal), rather than the implicit AllUpstreamsTerminal letting them run.
    let kinds = [
        EnsureSecretsJob::KIND,
        SeedSsmJob::KIND,
        VerifySeedJob::KIND,
        DeclareClusterJob::KIND,
        ObserveClusterJob::KIND,
        ObserveFluxJob::KIND,
    ];
    for kind in kinds {
        scheduler
            .register_gate(JobKindId::new(kind), Arc::new(AllUpstreamsSucceeded))
            .await;
    }
    // Small immediate retry on the AWS-touching steps (transient throttling);
    // delay 0 so the single-Dag drive loop never stalls on a backoff window.
    // The whole command is idempotent, so re-running is the outer retry.
    for kind in [SeedSsmJob::KIND, VerifySeedJob::KIND] {
        scheduler
            .register_retry_policy(
                JobKindId::new(kind),
                RetryPolicy::Fixed {
                    attempts: 3,
                    delay_ms: 0,
                },
            )
            .await;
    }

    // Build + register the six jobs (RecordingJob → Job → ErasedJob via blanket
    // impls; coerce each Arc to the erased dispatch surface the scheduler holds).
    let ensure = Arc::new(EnsureSecretsJob {
        cfg: cfg.clone(),
        ledger: ledger.clone(),
    });
    let seed = Arc::new(SeedSsmJob {
        cfg: cfg.clone(),
        client: client.clone(),
        dry_run: args.dry_run,
        ledger: ledger.clone(),
    });
    let verify = Arc::new(VerifySeedJob {
        cfg: cfg.clone(),
        client: client.clone(),
        dry_run: args.dry_run,
        ledger: ledger.clone(),
    });
    let declare = Arc::new(DeclareClusterJob {
        cfg: cfg.clone(),
        ledger: ledger.clone(),
    });
    let obs_cluster = Arc::new(ObserveClusterJob {
        cfg: cfg.clone(),
        ledger: ledger.clone(),
    });
    let obs_flux = Arc::new(ObserveFluxJob {
        cfg: cfg.clone(),
        ledger: ledger.clone(),
    });

    scheduler
        .register_job(ensure as Arc<dyn shigoto_types::ErasedJob>)
        .await;
    scheduler
        .register_job(seed as Arc<dyn shigoto_types::ErasedJob>)
        .await;
    scheduler
        .register_job(verify as Arc<dyn shigoto_types::ErasedJob>)
        .await;
    scheduler
        .register_job(declare as Arc<dyn shigoto_types::ErasedJob>)
        .await;
    scheduler
        .register_job(obs_cluster as Arc<dyn shigoto_types::ErasedJob>)
        .await;
    scheduler
        .register_job(obs_flux as Arc<dyn shigoto_types::ErasedJob>)
        .await;

    // The linear bootstrap chain.
    let id_ensure = job_id(EnsureSecretsJob::KIND, &cfg.cluster);
    let id_seed = job_id(SeedSsmJob::KIND, &cfg.cluster);
    let id_verify = job_id(VerifySeedJob::KIND, &cfg.cluster);
    let id_declare = job_id(DeclareClusterJob::KIND, &cfg.cluster);
    let id_obs_cluster = job_id(ObserveClusterJob::KIND, &cfg.cluster);
    let id_obs_flux = job_id(ObserveFluxJob::KIND, &cfg.cluster);

    let mut dag = Dag::new();
    dag.add_edge(id_ensure.clone(), id_seed.clone());
    dag.add_edge(id_seed.clone(), id_verify.clone());
    dag.add_edge(id_verify.clone(), id_declare.clone());
    dag.add_edge(id_declare.clone(), id_obs_cluster.clone());
    dag.add_edge(id_obs_cluster.clone(), id_obs_flux.clone());

    // Drive to terminal: tick until a tick fires no transition (tend pattern).
    for _ in 0..MAX_TICKS {
        let receipt = scheduler.tick(&mut dag).await?;
        if receipt.transitions_this_tick.is_empty() {
            break;
        }
    }

    let snapshot = scheduler.snapshot(&dag).await;
    let captured = ledger.lock().expect("bootstrap ledger mutex poisoned").clone();

    // ── Receipt ──────────────────────────────────────────────────────
    let order: [(&'static str, &JobId); 6] = [
        (EnsureSecretsJob::KIND, &id_ensure),
        (SeedSsmJob::KIND, &id_seed),
        (VerifySeedJob::KIND, &id_verify),
        (DeclareClusterJob::KIND, &id_declare),
        (ObserveClusterJob::KIND, &id_obs_cluster),
        (ObserveFluxJob::KIND, &id_obs_flux),
    ];

    let mut failed = false;
    let mut any_applied = false;
    for (kind, id) in order {
        let phase = snapshot
            .phases
            .get(id)
            .cloned()
            .unwrap_or(JobPhase::Pending);
        let report = captured.iter().find(|r| r.step == kind);
        let outcome = report.map_or("-", |r| r.outcome.label());
        let detail = report.map_or("", |r| r.detail.as_str());
        match phase {
            JobPhase::Succeeded => {
                if matches!(report.map(|r| r.outcome), Some(StepOutcome::Applied)) {
                    any_applied = true;
                }
                info!(step = kind, phase = "succeeded", outcome, detail, "step ok");
            }
            JobPhase::Skipped(_) => {
                failed = true;
                warn!(step = kind, phase = "skipped", "step skipped — an upstream did not succeed");
            }
            JobPhase::Deadlettered => {
                failed = true;
                warn!(step = kind, phase = "deadlettered", detail, "step FAILED");
            }
            other => {
                failed = true;
                warn!(step = kind, phase = other.kind(), "step did not reach a terminal phase");
            }
        }
    }

    if failed {
        warn!(cluster = %cfg.cluster, "bootstrap INCOMPLETE — see failed/skipped steps above");
        return Ok(ExitCode::FAILURE);
    }

    let verdict = if any_applied {
        "bootstrapped (changes applied)"
    } else {
        "AlreadyReady (no changes)"
    };
    info!(
        cluster = %cfg.cluster,
        dry_run = args.dry_run,
        secrets = verdict,
        "secrets plane converged; cluster+flux deferred to the pangea-operator (declare+observe)"
    );
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use shigoto_types::Snapshot;
    use std::collections::HashMap;

    #[test]
    fn gate_passes_only_when_all_upstreams_succeeded() {
        let up = job_id("up", "c");
        let down = job_id("down", "c");
        let mut dag = Dag::new();
        dag.add_edge(up.clone(), down.clone());

        let eval = |phase: JobPhase| {
            let mut phases = HashMap::new();
            phases.insert(up.clone(), phase);
            let snapshot = Snapshot { phases };
            AllUpstreamsSucceeded.evaluate(&GateContext {
                job_id: &down,
                snapshot: &snapshot,
                dag: &dag,
            })
        };

        assert_eq!(eval(JobPhase::Succeeded), GateOutcome::Pass);
        assert!(matches!(eval(JobPhase::Pending), GateOutcome::Wait));
        assert!(matches!(eval(JobPhase::Running), GateOutcome::Wait));
        assert!(matches!(eval(JobPhase::Deadlettered), GateOutcome::Skip(_)));
        assert!(matches!(
            eval(JobPhase::Skipped(SkipReason::GateRejected)),
            GateOutcome::Skip(_)
        ));
    }

    #[test]
    fn root_with_no_upstreams_passes() {
        let root = job_id("root", "c");
        let dag = Dag::new();
        let snapshot = Snapshot {
            phases: HashMap::new(),
        };
        assert_eq!(
            AllUpstreamsSucceeded.evaluate(&GateContext {
                job_id: &root,
                snapshot: &snapshot,
                dag: &dag,
            }),
            GateOutcome::Pass
        );
    }

    #[test]
    fn step_outcome_labels_are_stable() {
        assert_eq!(StepOutcome::Converged.label(), "converged");
        assert_eq!(StepOutcome::Applied.label(), "applied");
        assert_eq!(StepOutcome::Deferred.label(), "deferred");
        assert_eq!(StepOutcome::Failed.label(), "failed");
    }

    #[test]
    fn job_ids_are_distinct_per_step_same_cluster() {
        let ids = [
            job_id(EnsureSecretsJob::KIND, "akeyless-dev"),
            job_id(SeedSsmJob::KIND, "akeyless-dev"),
            job_id(VerifySeedJob::KIND, "akeyless-dev"),
            job_id(DeclareClusterJob::KIND, "akeyless-dev"),
            job_id(ObserveClusterJob::KIND, "akeyless-dev"),
            job_id(ObserveFluxJob::KIND, "akeyless-dev"),
        ];
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique.len(), 6, "each step must have a distinct JobId");
    }
}
