//! Render the *direct-apiserver* kubeconfig from k3s.yaml.
//!
//! k3s writes `/etc/rancher/k3s/k3s.yaml` pointing every client at the k3s
//! supervisor load-balancer on `127.0.0.1:6443`. On a single-node server that
//! LB is a no-value proxy hop (nothing to balance) and periodically FLAPS —
//! wedging external kubectl and tripping the reconverge `flux-git-auth` probe
//! into a false `fluxcd-bootstrap.service` restart. The apiserver itself
//! listens directly on `127.0.0.1:6444` (localhost-only, no proxy) and is
//! reliable.
//!
//! This derives the direct kubeconfig by a TYPED transform: parse k3s.yaml as
//! structured YAML and set each `clusters[].cluster.server` port via a typed
//! `url::Url`. It does NOT text-pattern the file (a `sed`/`String::replace`
//! that silently no-ops the day k3s changes the server's host or formatting).
//! k3s.yaml is generated at runtime (live client certs), so this runs as a
//! node oneshot rather than at build time.

use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;
use tracing::info;

use crate::kubeconfig::{set_perms, wait_for_file};

#[derive(ClapArgs)]
pub struct Args {
    /// Source kubeconfig — k3s writes this pointing at the :6443 supervisor LB.
    #[arg(long, default_value = "/etc/rancher/k3s/k3s.yaml")]
    from: PathBuf,

    /// Output kubeconfig, pointing at the direct apiserver.
    #[arg(long, default_value = "/etc/rancher/k3s/k3s-direct.yaml")]
    to: PathBuf,

    /// Apiserver port every cluster `server` is re-pointed at.
    #[arg(long, default_value = "6444")]
    port: u16,

    /// Seconds to wait for the source kubeconfig to appear (k3s writes it a
    /// moment after the unit goes active).
    #[arg(long, default_value = "300")]
    timeout: u64,
}

/// Wait for k3s.yaml, derive the direct kubeconfig, write it `0600` (it embeds
/// the cluster-admin client cert — root-only).
pub async fn run(args: Args) -> Result<ExitCode> {
    wait_for_file(&args.from, Duration::from_secs(args.timeout)).await?;

    let src = std::fs::read_to_string(&args.from)
        .with_context(|| format!("reading {}", args.from.display()))?;
    let direct = rewrite_server_port(&src, args.port)?;
    std::fs::write(&args.to, &direct)
        .with_context(|| format!("writing {}", args.to.display()))?;
    set_perms(&args.to, 0o600)?;

    info!(
        from = %args.from.display(),
        to = %args.to.display(),
        port = args.port,
        "direct-apiserver kubeconfig rendered"
    );
    Ok(ExitCode::SUCCESS)
}

/// Set the port of every `clusters[].cluster.server` to `port`, preserving
/// scheme + host + path via a typed `url::Url`. Errors (rather than silently
/// no-ops) if the document has no cluster server to rewrite — a structural
/// guarantee a text substitution can't make.
fn rewrite_server_port(yaml: &str, port: u16) -> Result<String> {
    let mut doc: serde_yaml::Value =
        serde_yaml::from_str(yaml).context("parsing kubeconfig YAML")?;

    let clusters = doc
        .get_mut("clusters")
        .and_then(serde_yaml::Value::as_sequence_mut)
        .context("kubeconfig has no clusters[] sequence")?;

    let mut rewritten = 0usize;
    for entry in clusters.iter_mut() {
        let Some(server) = entry.get_mut("cluster").and_then(|c| c.get_mut("server")) else {
            continue;
        };
        let Some(current) = server.as_str() else {
            continue;
        };
        let mut url =
            url::Url::parse(current).with_context(|| format!("parsing cluster server url `{current}`"))?;
        url.set_port(Some(port))
            .map_err(|()| anyhow::anyhow!("cannot set port on cluster server url `{current}`"))?;
        *server = serde_yaml::Value::String(url.as_str().trim_end_matches('/').to_string());
        rewritten += 1;
    }

    anyhow::ensure!(rewritten > 0, "no clusters[].cluster.server field found to rewrite");
    serde_yaml::to_string(&doc).context("serializing direct kubeconfig YAML")
}

#[cfg(test)]
mod tests {
    use super::rewrite_server_port;

    const SAMPLE: &str = "\
apiVersion: v1
kind: Config
clusters:
- cluster:
    certificate-authority-data: QQ==
    server: https://127.0.0.1:6443
  name: default
contexts:
- context:
    cluster: default
    user: default
  name: default
current-context: default
users:
- name: default
  user:
    client-certificate-data: QQ==
    client-key-data: QQ==
";

    #[test]
    fn rewrites_supervisor_port_to_direct() {
        let out = rewrite_server_port(SAMPLE, 6444).unwrap();
        assert!(out.contains("server: https://127.0.0.1:6444"), "got: {out}");
        assert!(!out.contains(":6443"), "port :6443 should be gone: {out}");
    }

    #[test]
    fn preserves_host_and_other_fields() {
        let out = rewrite_server_port(SAMPLE, 6444).unwrap();
        // Structure + the client certs are untouched — only the port moved.
        assert!(out.contains("127.0.0.1"));
        assert!(out.contains("client-key-data"));
        assert!(out.contains("current-context: default"));
    }

    #[test]
    fn rewrites_non_localhost_host() {
        let yaml = SAMPLE.replace("127.0.0.1:6443", "rio.example:6443");
        let out = rewrite_server_port(&yaml, 6444).unwrap();
        assert!(out.contains("server: https://rio.example:6444"), "got: {out}");
    }

    #[test]
    fn idempotent_when_already_direct() {
        let once = rewrite_server_port(SAMPLE, 6444).unwrap();
        let twice = rewrite_server_port(&once, 6444).unwrap();
        assert!(twice.contains(":6444"));
        assert!(!twice.contains(":6443"));
    }

    #[test]
    fn errors_when_no_cluster_server() {
        let yaml = "apiVersion: v1\nkind: Config\nclusters: []\n";
        assert!(rewrite_server_port(yaml, 6444).is_err());
    }
}
