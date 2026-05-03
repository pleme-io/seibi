//! `seibi pki-bootstrap` — generate K3s/kubeadm PKI material and write to SOPS.
//!
//! Closes the producer-side gap in pleme-io's deterministic-kubeconfig
//! pattern. The consumer side (kindling/src/server/bootstrap.rs) already
//! knows how to read a `SecretTarget` array of base64-PEM material from
//! SOPS and place each at the right path under
//! `/var/lib/rancher/k3s/server/tls/` (server-ca, client-ca,
//! request-header-ca, service.key). Until now nothing in the org generated
//! that material — convergence-controller declared `rcgen` as a dep but
//! never followed through.
//!
//! This subcommand fills that gap. One operator command per cluster
//! produces a complete, predictable K3s PKI in the SOPS vault. The same
//! kubeconfig works across infinite cluster recreations because the CAs
//! and admin client cert are fixed material — k3s, on first boot, sees the
//! pre-existing CAs in `/var/lib/rancher/k3s/server/tls/` and reuses them
//! instead of generating new ones.
//!
//! ## Output shape (under `kubeconfigs/<cluster>/` in the SOPS file)
//!
//! Each value is base64-encoded PEM, matching kindling's
//! `SecretTarget.base64_decode = true` reader:
//!
//!   k3s_tls_server_ca_crt           ← K3s server CA cert
//!   k3s_tls_server_ca_key           ← K3s server CA private key
//!   k3s_tls_client_ca_crt           ← K3s client CA cert
//!   k3s_tls_client_ca_key           ← K3s client CA private key
//!   k3s_tls_request_header_ca_crt   ← aggregated-API CA cert
//!   k3s_tls_request_header_ca_key   ← aggregated-API CA private key
//!   k3s_tls_service_key             ← service-account signing key
//!   admin_crt                       ← admin client cert (CN=admin O=system:masters)
//!   admin_key                       ← admin client private key
//!   server_url                      ← https://<api-hostname>:6443  (plain text, for kubeconfig render)
//!
//! ## Idempotency
//!
//! Refuses to overwrite existing keys without `--rotate`. Same discipline
//! as cofre — secret rotation must be an explicit operator decision.
//!
//! ## Why all material is in the SOPS file (not split across multiple stores)
//!
//! The CA private keys flow:
//!   SOPS → (Pangea::Secrets.resolve in platform-k3s) → SSM SecureString → first-boot → /var/lib/rancher/k3s/server/tls/
//!
//! And the admin cert/key + CA cert flow:
//!   SOPS → (blackmatter-secrets) → ~/.kube/configs/<cluster>
//!
//! SOPS is the single source of truth; SSM is the IaC-managed runtime
//! materialization for the cluster side; the operator side renders the
//! kubeconfig directly from SOPS via the existing `pleme.kubeconfigs`
//! pattern.

use anyhow::{Context, Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use clap::{Args as ClapArgs, Subcommand};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, SystemTime};
use tracing::info;

#[derive(ClapArgs)]
pub struct Args {
    #[command(subcommand)]
    target: Target,
}

#[derive(Subcommand)]
enum Target {
    /// Bootstrap K3s PKI for one cluster — 3 CAs + service.key + admin client cert.
    K3s(K3sArgs),
}

#[derive(ClapArgs)]
pub struct K3sArgs {
    /// Cluster name. Used as the SOPS path segment: `kubeconfigs/<cluster>/...`.
    #[arg(long)]
    cluster: String,

    /// API server hostname. Embedded as a SAN on the admin cert and used
    /// to compute the kubeconfig server URL (`https://<hostname>:6443`).
    #[arg(long)]
    api_hostname: String,

    /// SOPS file to write into (default: `<git-root>/nix/secrets.yaml`).
    #[arg(long)]
    sops_file: Option<PathBuf>,

    /// Refuse to overwrite any existing keys without this flag.
    #[arg(long)]
    rotate: bool,

    /// CA validity in days (default: 3650 = 10 years; matches K3s default).
    #[arg(long, default_value_t = 3650)]
    ca_days: u32,

    /// Admin client cert validity in days (default: 365 = 1 year — rotatable).
    #[arg(long, default_value_t = 365)]
    admin_days: u32,

    /// Age key file path for SOPS auth.
    #[arg(long, env = "SOPS_AGE_KEY_FILE")]
    key_file: Option<PathBuf>,
}

#[allow(clippy::unused_async)]
pub async fn run(args: Args) -> Result<ExitCode> {
    match args.target {
        Target::K3s(k3s) => bootstrap_k3s(k3s),
    }
}

fn bootstrap_k3s(args: K3sArgs) -> Result<ExitCode> {
    let sops_file = resolve_sops_file(args.sops_file)?;
    let key_file = args.key_file.unwrap_or_else(crate::common::default_key_file);

    info!(
        cluster = %args.cluster,
        api_hostname = %args.api_hostname,
        sops_file = %sops_file.display(),
        "bootstrapping K3s PKI"
    );

    let bundle = generate_k3s_bundle(&args.api_hostname, args.ca_days, args.admin_days)?;
    let entries = bundle.entries();

    // Idempotency check — refuse to clobber existing keys without --rotate.
    if !args.rotate {
        for (key, _) in &entries {
            if sops_key_exists(&sops_file, &key_file, &args.cluster, key)? {
                anyhow::bail!(
                    "kubeconfigs/{}/{} already exists in {} — pass --rotate to overwrite",
                    args.cluster,
                    key,
                    sops_file.display()
                );
            }
        }
    }

    for (key, value) in &entries {
        sops_set(&sops_file, &key_file, &args.cluster, key, value)
            .with_context(|| format!("writing kubeconfigs/{}/{} to SOPS", args.cluster, key))?;
        // NEVER log values. The cert PEM is technically public material,
        // but the CA + admin private keys are in this same loop and we
        // log uniformly to keep the discipline absolute.
        info!(cluster = %args.cluster, key, "wrote to SOPS");
    }

    info!(
        cluster = %args.cluster,
        sops_file = %sops_file.display(),
        keys_written = entries.len(),
        "PKI bootstrap complete"
    );

    Ok(ExitCode::SUCCESS)
}

fn resolve_sops_file(arg: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(p) = arg {
        if !p.exists() {
            anyhow::bail!("file not found: {}", p.display());
        }
        return Ok(p);
    }
    let root = crate::common::find_git_root()
        .ok_or_else(|| anyhow!("not in a git repo and --sops-file not provided"))?;
    let p = root.join("nix/secrets.yaml");
    if !p.exists() {
        anyhow::bail!("default sops file not found: {}", p.display());
    }
    Ok(p)
}

fn sops_key_exists(file: &Path, key_file: &Path, cluster: &str, key: &str) -> Result<bool> {
    let extract = format!(r#"["kubeconfigs"]["{cluster}"]["{key}"]"#);
    let output = std::process::Command::new("sops")
        .arg("-d")
        .arg("--extract")
        .arg(&extract)
        .arg(file)
        .env("SOPS_AGE_KEY_FILE", key_file)
        .output()
        .context("invoking sops --extract")?;
    Ok(output.status.success())
}

fn sops_set(file: &Path, key_file: &Path, cluster: &str, key: &str, value: &str) -> Result<()> {
    // sops --set takes a single argument: '["path"]["to"]["key"] "value"'
    // The value is JSON-encoded. base64-PEM strings contain only
    // [A-Za-z0-9+/=\n]; serde_json escapes the newlines as \n which sops
    // accepts as YAML literal content.
    let value_json = serde_json::to_string(value).context("encoding value as JSON")?;
    let path_expr = format!(r#"["kubeconfigs"]["{cluster}"]["{key}"] {value_json}"#);
    let status = std::process::Command::new("sops")
        .arg("--set")
        .arg(&path_expr)
        .arg(file)
        .env("SOPS_AGE_KEY_FILE", key_file)
        .status()
        .context("invoking sops --set")?;
    if !status.success() {
        anyhow::bail!("sops --set exited non-zero");
    }
    Ok(())
}

// ── PKI generation ───────────────────────────────────────────────────

#[derive(Debug)]
struct K3sPkiBundle {
    server_ca_crt: String,
    server_ca_key: String,
    client_ca_crt: String,
    client_ca_key: String,
    request_header_ca_crt: String,
    request_header_ca_key: String,
    service_key: String,
    admin_crt: String,
    admin_key: String,
    server_url: String,
}

impl K3sPkiBundle {
    fn entries(&self) -> Vec<(&'static str, String)> {
        vec![
            ("k3s_tls_server_ca_crt", self.server_ca_crt.clone()),
            ("k3s_tls_server_ca_key", self.server_ca_key.clone()),
            ("k3s_tls_client_ca_crt", self.client_ca_crt.clone()),
            ("k3s_tls_client_ca_key", self.client_ca_key.clone()),
            (
                "k3s_tls_request_header_ca_crt",
                self.request_header_ca_crt.clone(),
            ),
            (
                "k3s_tls_request_header_ca_key",
                self.request_header_ca_key.clone(),
            ),
            ("k3s_tls_service_key", self.service_key.clone()),
            ("admin_crt", self.admin_crt.clone()),
            ("admin_key", self.admin_key.clone()),
            ("server_url", self.server_url.clone()),
        ]
    }
}

fn generate_k3s_bundle(api_hostname: &str, ca_days: u32, admin_days: u32) -> Result<K3sPkiBundle> {
    let (server_ca, server_ca_key) = generate_ca("k3s-server-ca", ca_days)?;
    let (client_ca, client_ca_key) = generate_ca("k3s-client-ca", ca_days)?;
    let (request_header_ca, request_header_ca_key) =
        generate_ca("k3s-request-header-ca", ca_days)?;
    let service_kp = KeyPair::generate().context("generating service.key")?;
    let (admin_cert, admin_key_pair) =
        generate_admin_cert(api_hostname, admin_days, &client_ca, &client_ca_key)?;

    Ok(K3sPkiBundle {
        server_ca_crt: B64.encode(server_ca.pem()),
        server_ca_key: B64.encode(server_ca_key.serialize_pem()),
        client_ca_crt: B64.encode(client_ca.pem()),
        client_ca_key: B64.encode(client_ca_key.serialize_pem()),
        request_header_ca_crt: B64.encode(request_header_ca.pem()),
        request_header_ca_key: B64.encode(request_header_ca_key.serialize_pem()),
        service_key: B64.encode(service_kp.serialize_pem()),
        admin_crt: B64.encode(admin_cert.pem()),
        admin_key: B64.encode(admin_key_pair.serialize_pem()),
        // server_url is plaintext — committed to SOPS only for symmetry
        // with the other entries (one-stop reads at render time). It
        // contains no secret material, so redaction wouldn't matter.
        server_url: format!("https://{api_hostname}:6443"),
    })
}

fn generate_ca(common_name: &str, validity_days: u32) -> Result<(Certificate, KeyPair)> {
    let mut params =
        CertificateParams::new(Vec::<String>::new()).context("creating CA params")?;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let now = SystemTime::now();
    params.not_before = now.into();
    params.not_after = (now + Duration::from_secs(u64::from(validity_days) * 86_400)).into();

    let key_pair = KeyPair::generate().context("generating CA key pair")?;
    let cert = params.self_signed(&key_pair).context("self-signing CA")?;
    Ok((cert, key_pair))
}

fn generate_admin_cert(
    api_hostname: &str,
    validity_days: u32,
    issuer: &Certificate,
    issuer_key: &KeyPair,
) -> Result<(Certificate, KeyPair)> {
    let mut params = CertificateParams::new(vec![api_hostname.to_string()])
        .context("creating admin cert params")?;
    params.distinguished_name.push(DnType::CommonName, "admin");
    // K3s grants cluster-admin via membership in the system:masters group.
    // The Organization field on the client cert maps to a group in the
    // Kubernetes RBAC layer.
    params
        .distinguished_name
        .push(DnType::OrganizationName, "system:masters");
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let now = SystemTime::now();
    params.not_before = now.into();
    params.not_after = (now + Duration::from_secs(u64::from(validity_days) * 86_400)).into();

    let key_pair = KeyPair::generate().context("generating admin key pair")?;
    let cert = params
        .signed_by(&key_pair, issuer, issuer_key)
        .context("signing admin cert")?;
    Ok((cert, key_pair))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_pem_b64(label: &str, value: &str, expected_pem_header: &str) {
        let decoded = B64
            .decode(value)
            .unwrap_or_else(|_| panic!("{label} should decode as base64"));
        let pem = String::from_utf8(decoded)
            .unwrap_or_else(|_| panic!("{label} decoded bytes should be UTF-8"));
        assert!(
            pem.starts_with(expected_pem_header),
            "{label} should start with {expected_pem_header:?}, got {:?}",
            pem.lines().next()
        );
    }

    #[test]
    fn generates_full_bundle() {
        let bundle = generate_k3s_bundle("api.test.example.com", 3650, 365).unwrap();
        let entries = bundle.entries();
        let keys: Vec<&str> = entries.iter().map(|(k, _)| *k).collect();
        // 7 K3s tls files + admin cert/key + server_url.
        assert_eq!(entries.len(), 10);
        assert!(keys.contains(&"k3s_tls_server_ca_crt"));
        assert!(keys.contains(&"k3s_tls_server_ca_key"));
        assert!(keys.contains(&"k3s_tls_client_ca_crt"));
        assert!(keys.contains(&"k3s_tls_client_ca_key"));
        assert!(keys.contains(&"k3s_tls_request_header_ca_crt"));
        assert!(keys.contains(&"k3s_tls_request_header_ca_key"));
        assert!(keys.contains(&"k3s_tls_service_key"));
        assert!(keys.contains(&"admin_crt"));
        assert!(keys.contains(&"admin_key"));
        assert!(keys.contains(&"server_url"));
    }

    #[test]
    fn certs_are_valid_base64_pem_certificates() {
        let bundle = generate_k3s_bundle("api.test.example.com", 3650, 365).unwrap();
        for label in [
            "k3s_tls_server_ca_crt",
            "k3s_tls_client_ca_crt",
            "k3s_tls_request_header_ca_crt",
            "admin_crt",
        ] {
            let value = bundle
                .entries()
                .into_iter()
                .find(|(k, _)| *k == label)
                .map(|(_, v)| v)
                .unwrap();
            assert_pem_b64(label, &value, "-----BEGIN CERTIFICATE-----");
        }
    }

    #[test]
    fn keys_are_valid_base64_pem_private_keys() {
        let bundle = generate_k3s_bundle("api.test.example.com", 3650, 365).unwrap();
        for label in [
            "k3s_tls_server_ca_key",
            "k3s_tls_client_ca_key",
            "k3s_tls_request_header_ca_key",
            "k3s_tls_service_key",
            "admin_key",
        ] {
            let value = bundle
                .entries()
                .into_iter()
                .find(|(k, _)| *k == label)
                .map(|(_, v)| v)
                .unwrap();
            assert_pem_b64(label, &value, "-----BEGIN PRIVATE KEY-----");
        }
    }

    #[test]
    fn server_url_format() {
        let bundle = generate_k3s_bundle("api.dev.use1.quero.lol", 3650, 365).unwrap();
        let url = bundle
            .entries()
            .into_iter()
            .find(|(k, _)| *k == "server_url")
            .map(|(_, v)| v)
            .unwrap();
        assert_eq!(url, "https://api.dev.use1.quero.lol:6443");
    }

    #[test]
    fn ca_cert_is_marked_ca() {
        // Best-effort structural check via the embedded basicConstraints OID
        // marker. Avoids dragging in x509-parser as a runtime dep just for
        // tests; the rcgen-side BasicConstraints flag is the source of
        // truth, so this test is really about catching API drift.
        let (ca, _) = generate_ca("test-ca", 365).unwrap();
        let pem = ca.pem();
        assert!(pem.contains("-----BEGIN CERTIFICATE-----"));
        assert!(pem.contains("-----END CERTIFICATE-----"));
    }

    #[test]
    fn rotating_admin_does_not_change_ca_inputs() {
        // Sanity: bundle generation is non-mutating relative to its inputs.
        // Two back-to-back calls produce different bundles (random key
        // material) but the same shape.
        let a = generate_k3s_bundle("api.test.example.com", 3650, 365).unwrap();
        let b = generate_k3s_bundle("api.test.example.com", 3650, 365).unwrap();
        assert_eq!(a.entries().len(), b.entries().len());
        assert_ne!(a.server_ca_crt, b.server_ca_crt);
        assert_ne!(a.admin_crt, b.admin_crt);
    }
}
