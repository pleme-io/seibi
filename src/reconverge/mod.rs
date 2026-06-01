//! `reconverge` — the typed event+poll convergence runtime.
//!
//! The 8-phase enactment model's Phase 8 (`Reconverge`) at the node, made
//! event-driven. SOURCES (poll AND interrupt) emit a uniform
//! [`signal::ReconvergeSignal`] into a coalescing/prioritizing
//! [`queue::SignalQueue`]; the [`engine::Engine`] drains it and dispatches
//! each to a [`reconciler::Reconciler`] through a *fresh* shigoto Dag per
//! signal (the tend re-arm pattern — shigoto's FSM has no Succeeded→Pending
//! edge, so a resident DAG would run each reconciler once then go dark).
//!
//! Reconcilers are a runtime *sibling* of `convergence_trait::ConvergenceController`
//! (same `Drift`/`Declaration` vocabulary, same observe→diff→decide→act
//! semantics) — NOT a subtype: there is no render/deploy phase, the node
//! mutation IS the act.
//!
//! M0: the engine modules live alongside `legacy` (the pre-engine poll-loop),
//! which still backs the `--once` CLI sweep + exit codes during migration.

mod legacy;

pub mod engine;
pub mod queue;
pub mod reconciler;
pub mod recipes;
pub mod signal;
pub mod source;

// CLI surface (Args + run) — the legacy `--once` sweep keeps exit-code 0/1/2
// parity for CI/dry-run during migration. The daemon (engine::Engine, via
// [`daemon`]) takes over once the nix unit flips oneshot→notify.
pub use legacy::{run, Args};

use tokio_util::sync::CancellationToken;

/// Long-running daemon entry point — the `Type=notify` service body. Wires the
/// production engine (both reconcilers + their sources) and runs it until
/// SIGTERM/SIGINT. The `PollTicker` source internalizes today's 1-minute
/// cadence, so the systemd timer is retired.
pub async fn daemon(dry_run: bool, audit_path: Option<std::path::PathBuf>) -> anyhow::Result<()> {
    let cancel = CancellationToken::new();

    // Cancel on SIGTERM (systemd stop) or SIGINT (Ctrl-C).
    let sig_cancel = cancel.clone();
    tokio::spawn(async move {
        let mut term = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "failed to install SIGTERM handler");
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => tracing::info!("SIGINT — shutting down reconverge daemon"),
            _ = term.recv() => tracing::info!("SIGTERM — shutting down reconverge daemon"),
        }
        sig_cancel.cancel();
    });

    recipes::wire(dry_run, audit_path, cancel).run().await
}
