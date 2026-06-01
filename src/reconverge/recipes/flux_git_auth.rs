//! `FluxGitAuth` — the Flux git-auth freshness reconciler. Migrated verbatim
//! from the legacy `flux_git_auth` recipe.
//!
//! Symptom: rotating the GitHub PAT in SOPS doesn't propagate into the K8s
//! `flux-system/flux-system` Secret until `fluxcd-bootstrap.service` runs
//! again — and that service has no restartTrigger on the SOPS path, so it
//! sticks at the OLD token until the next nixos-rebuild. Every Flux
//! Kustomization then reports `ArtifactFailed` (source-controller can't
//! clone) and every HelmRelease cascades to `SourceNotReady`.
//!
//! Sources: `PollTicker` (a stale PAT has no event) + (M1) `Inotify` on the
//! SOPS token path. `min_interval()=30s`.

use std::process::Command;
use std::time::Duration;

use async_trait::async_trait;
use convergence_trait::types::{Constraint, Declaration, Drift, DriftSeverity};

use crate::reconverge::reconciler::{Observed, ReconcileError, Reconciled, Reconciler};
use crate::reconverge::signal::ReconvergeSignal;

pub const KIND: &str = "reconverge.flux-git-auth";

pub struct FluxGitAuth;

#[async_trait]
impl Reconciler for FluxGitAuth {
    const KIND: &'static str = KIND;

    fn declaration(&self) -> Declaration {
        Declaration {
            name: "flux-git-auth-freshness".into(),
            intent: "the K8s Secret Flux source-controller uses still authenticates against the \
                     upstream git remote (GitRepository Ready=True)"
                .into(),
            constraints: vec![Constraint::Invariant(
                "flux-system/flux-system GitRepository reports Ready=True".into(),
            )],
        }
    }

    fn min_interval(&self) -> Duration {
        Duration::from_secs(30)
    }

    async fn observe(&self, _signal: &ReconvergeSignal) -> Result<Observed, ReconcileError> {
        tokio::task::spawn_blocking(probe)
            .await
            .map_err(|e| ReconcileError::new(format!("observe join: {e}")))?
    }

    fn diff(&self, observed: &Observed, _decl: &Declaration) -> Vec<Drift> {
        let active = observed.get("bootstrap_active").and_then(serde_json::Value::as_bool);
        let ready = observed.get("gitrepo_ready").and_then(serde_json::Value::as_bool);
        // Drift if bootstrap is inactive, OR active-but-GitRepository-not-Ready.
        let drifted = active != Some(true) || ready != Some(true);
        if drifted {
            vec![Drift {
                resource: "flux-system/flux-system (GitRepository + bootstrap)".into(),
                expected: serde_json::json!("bootstrap active && GitRepository Ready=True"),
                actual: serde_json::json!({ "bootstrap_active": active, "gitrepo_ready": ready }),
                severity: DriftSeverity::High,
            }]
        } else {
            vec![]
        }
    }

    async fn act(&self, _drift: &[Drift], dry_run: bool) -> Result<Reconciled, ReconcileError> {
        if dry_run {
            return Ok(Reconciled::Refused {
                detail: "flux git-auth drift (would restart fluxcd-bootstrap.service to re-render \
                         the K8s Secret from current SOPS state)"
                    .into(),
            });
        }
        tokio::task::spawn_blocking(restart_bootstrap)
            .await
            .map_err(|e| ReconcileError::new(format!("restart join: {e}")))?
    }
}

/// Probe reality: is `fluxcd-bootstrap.service` active, and does the
/// GitRepository report Ready=True? A probe-invocation failure is a transient
/// `Err` (retry/poll re-observes), not a silent false.
fn probe() -> Result<Observed, ReconcileError> {
    let active = match Command::new("systemctl")
        .args(["is-active", "fluxcd-bootstrap.service"])
        .output()
    {
        Ok(o) => o.status.success(),
        Err(e) => return Err(ReconcileError::new(format!("could not probe fluxcd-bootstrap.service: {e}"))),
    };

    // Only meaningful if bootstrap is active; if inactive we already know it drifted.
    let gitrepo_ready = if active {
        let gr = Command::new("kubectl")
            .args([
                "--kubeconfig",
                "/etc/rancher/k3s/k3s.yaml",
                "get",
                "gitrepository",
                "-n",
                "flux-system",
                "flux-system",
                "-o",
                "jsonpath={.status.conditions[?(@.type=='Ready')].status}",
            ])
            .output();
        matches!(gr, Ok(o) if o.status.success() && String::from_utf8_lossy(&o.stdout).trim() == "True")
    } else {
        false
    };

    Ok(serde_json::json!({ "bootstrap_active": active, "gitrepo_ready": gitrepo_ready }))
}

/// Restart `fluxcd-bootstrap.service` — the oneshot re-renders the K8s Secret
/// from `/run/secrets/...` which sops-nix has already updated. Idempotent.
fn restart_bootstrap() -> Result<Reconciled, ReconcileError> {
    match Command::new("systemctl")
        .args(["restart", "fluxcd-bootstrap.service"])
        .status()
    {
        Ok(s) if s.success() => Ok(Reconciled::Remediated {
            detail: "fluxcd-bootstrap.service restarted; flux-system/flux-system Secret \
                     re-rendered from /run/secrets"
                .into(),
        }),
        Ok(s) => Err(ReconcileError::new(format!("systemctl restart exited {s}"))),
        Err(e) => Err(ReconcileError::new(format!("systemctl restart spawn failed: {e}"))),
    }
}
