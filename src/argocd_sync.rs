//! `seibi argocd-sync` — trigger manual sync on one or more ArgoCD Applications.
//!
//! Use case: feature-branch ApplicationSets have been kubectl-applied directly
//! into the central ArgoCD's `argocd` namespace (bypassing the
//! master-targeted appset-sync) and the generated Applications need a manual
//! sync to converge resources onto the destination cluster. This is the
//! pre-merge smoke-test path for ArgoCD-driven substrate work — common across
//! akeyless environments where the central ArgoCD lives at one cluster and
//! ApplicationSets fan out to N destination clusters via cluster-generator
//! selectors.
//!
//! Why this lives in seibi: every operator-side "infra maintenance" knob ends
//! up here. ArgoCD app sync is one such knob. Generic across akeyless ArgoCD
//! work; not specific to any one feature branch or cluster.
//!
//! What we DON'T do here:
//!   - Apply ApplicationSets themselves (use `kubectl apply -f` for that —
//!     it's a one-time bootstrap, not a recurring op).
//!   - Watch sync until convergence (separate `--watch` flag could land here
//!     later; today the operator polls `kubectl get application` themselves).
//!   - Drift recovery on prod (different ergonomics — `--prune` should be
//!     true, with eyeball on the diff first).

use anyhow::{Context, Result, bail};
use clap::Args as ClapArgs;
use std::process::{Command, ExitCode};
use tracing::{info, warn};

#[derive(ClapArgs)]
pub struct Args {
    /// kubectl context for the cluster hosting the central ArgoCD.
    /// Default targets the akeylesslabs cicd cluster which hosts
    /// argocd.akeyless.io. Override for non-akeyless ArgoCD instances.
    #[arg(long, default_value = "us-east-1-cicd-eks")]
    cluster: String,

    /// Namespace where the Applications live in the central ArgoCD.
    #[arg(long, default_value = "argocd")]
    namespace: String,

    /// Path to a kubeconfig file. If omitted, inherits from $KUBECONFIG /
    /// the kubectl default chain. Useful for one-shot invocations against a
    /// temp kubeconfig produced by `aws eks update-kubeconfig`.
    #[arg(long)]
    kubeconfig: Option<String>,

    /// Whether the sync should prune resources not present in the
    /// Application's source. Safe (and the right answer) for first-sync of
    /// a freshly-applied ApplicationSet. Set true for drift-recovery syncs
    /// against established Applications — but eyeball the diff first.
    #[arg(long, default_value_t = false)]
    prune: bool,

    /// Whether to use Server-Side Apply for the sync. Recommended (the
    /// default) — preserves field ownership across writers, matches what
    /// most pleme-io / akeylesslabs ApplicationSets already opt into via
    /// their own syncOptions.
    #[arg(long, default_value_t = true)]
    server_side_apply: bool,

    /// Don't actually patch — print what would be patched and exit. Useful
    /// for confirming app names + cluster context before committing to the
    /// real sync.
    #[arg(long, default_value_t = false)]
    dry_run: bool,

    /// One or more Application names to sync. Order is informational only —
    /// ArgoCD respects sync-wave annotations on the underlying resources, so
    /// passing all three at once is fine even when sync waves matter.
    #[arg(required = true)]
    apps: Vec<String>,
}

pub async fn run(args: Args) -> Result<ExitCode> {
    let patch_body = build_patch_body(args.prune, args.server_side_apply);
    info!(
        cluster = %args.cluster,
        namespace = %args.namespace,
        app_count = args.apps.len(),
        prune = args.prune,
        server_side_apply = args.server_side_apply,
        dry_run = args.dry_run,
        "argocd-sync"
    );

    let mut failed = 0usize;
    for app in &args.apps {
        if args.dry_run {
            info!(app = %app, "would patch");
            continue;
        }

        match patch_one(&args.cluster, &args.namespace, args.kubeconfig.as_deref(), app, &patch_body) {
            Ok(()) => info!(app = %app, "sync triggered"),
            Err(e) => {
                warn!(app = %app, error = %e, "sync trigger failed");
                failed += 1;
            }
        }
    }

    if failed == 0 {
        info!("all {} app(s) triggered", args.apps.len());
        Ok(ExitCode::SUCCESS)
    } else {
        warn!("{} of {} app(s) failed to trigger", failed, args.apps.len());
        Ok(ExitCode::FAILURE)
    }
}

fn build_patch_body(prune: bool, server_side_apply: bool) -> String {
    let mut sync_options: Vec<&str> = Vec::new();
    if server_side_apply {
        sync_options.push("ServerSideApply=true");
    }
    let opts_json = serde_json::to_string(&sync_options).expect("sync option list serializes");

    format!(r#"{{"operation":{{"sync":{{"prune":{prune},"syncOptions":{opts_json}}}}}}}"#)
}

fn patch_one(
    cluster: &str,
    namespace: &str,
    kubeconfig: Option<&str>,
    app: &str,
    patch_body: &str,
) -> Result<()> {
    let mut cmd = Command::new("kubectl");
    if let Some(kc) = kubeconfig {
        cmd.arg("--kubeconfig").arg(kc);
    }
    cmd.args([
        "--context",
        cluster,
        "-n",
        namespace,
        "patch",
        "application",
        app,
        "--type",
        "merge",
        "-p",
        patch_body,
    ]);

    let output = cmd
        .output()
        .with_context(|| format!("spawn kubectl patch for {app}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("kubectl patch {app} failed: {}", stderr.trim());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patch_body_default_no_prune_with_ssa() {
        let body = build_patch_body(false, true);
        assert!(body.contains(r#""prune":false"#));
        assert!(body.contains(r#""ServerSideApply=true""#));
    }

    #[test]
    fn patch_body_prune_true() {
        let body = build_patch_body(true, true);
        assert!(body.contains(r#""prune":true"#));
    }

    #[test]
    fn patch_body_no_ssa_omits_option() {
        let body = build_patch_body(false, false);
        assert!(body.contains(r#""syncOptions":[]"#));
    }
}
