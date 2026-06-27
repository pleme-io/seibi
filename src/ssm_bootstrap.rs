//! `seibi ssm-bootstrap` — seed a cluster's bootstrap secrets from SOPS into
//! AWS SSM Parameter Store as SecureString, so the node fetches them at boot
//! via its instance role (the W3b secret-free path) instead of receiving them
//! in cloud-init. Run ONCE on the host that holds the age key (cid).
//!
//! Reads the same SOPS paths the akeyless-dev workspace used to decrypt at
//! synth, and writes one SecureString per secret under
//! `<prefix>/<suffix>` (default prefix `/pangea/<cluster>/secrets`). Secret
//! values are passed to the `aws` CLI via stdin (`--cli-input-json`), never on
//! argv, so they don't appear in the process table.

use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use tracing::{info, warn};

/// One bootstrap secret: where it lives in SOPS and its SSM name suffix.
struct SsmSecretDef {
    /// SSM parameter name suffix — written under `<prefix>/<suffix>`.
    ssm_suffix: &'static str,
    /// SOPS extract paths to try in order (first non-empty wins).
    /// `{cluster}` is substituted at runtime.
    sops_paths: &'static [&'static str],
    /// Appended to the decrypted value before writing. The k3s admin
    /// password is seeded already-formatted as a k3s basic-auth line.
    suffix: &'static str,
}

/// The bootstrap secrets a k3s NixOS node fetches at boot. Keys match
/// kindling's BOOTSTRAP_SECRET_TARGETS via the workspace's ssm_secret_refs
/// map; the 7 TLS values are stored base64-encoded (kindling base64-decodes).
/// `nix-github-token` is intentionally absent — the workspace points it at
/// the `flux-github-token` parameter.
const SECRET_DEFS: &[SsmSecretDef] = &[
    SsmSecretDef {
        ssm_suffix: "sops-age-key",
        sops_paths: &[r#"["clusters"]["{cluster}"]["sops-age-key"]"#],
        suffix: "",
    },
    SsmSecretDef {
        ssm_suffix: "flux-github-token",
        sops_paths: &[r#"["clusters"]["{cluster}"]["flux-github-token"]"#],
        suffix: "",
    },
    SsmSecretDef {
        ssm_suffix: "vpn-private-key",
        sops_paths: &[r#"["clusters"]["{cluster}"]["wireguard"]["private-key"]"#],
        suffix: "",
    },
    SsmSecretDef {
        ssm_suffix: "vpn-psk",
        sops_paths: &[r#"["ryn"]["wireguard"]["ryn-{cluster}"]["psk"]"#],
        suffix: "",
    },
    SsmSecretDef {
        ssm_suffix: "k3s-server-token",
        sops_paths: &[r#"["clusters"]["{cluster}"]["server-token"]"#],
        suffix: "",
    },
    SsmSecretDef {
        ssm_suffix: "k3s-admin-password",
        sops_paths: &[r#"["clusters"]["{cluster}"]["admin-password"]"#],
        // k3s static-token-auth line: <password>,<user>,<uid>,<groups>
        suffix: ",admin,admin,system:masters",
    },
    SsmSecretDef {
        ssm_suffix: "tls-server-ca-crt",
        sops_paths: &[r#"["clusters"]["{cluster}"]["tls"]["server-ca-crt"]"#],
        suffix: "",
    },
    SsmSecretDef {
        ssm_suffix: "tls-server-ca-key",
        sops_paths: &[r#"["clusters"]["{cluster}"]["tls"]["server-ca-key"]"#],
        suffix: "",
    },
    SsmSecretDef {
        ssm_suffix: "tls-client-ca-crt",
        sops_paths: &[r#"["clusters"]["{cluster}"]["tls"]["client-ca-crt"]"#],
        suffix: "",
    },
    SsmSecretDef {
        ssm_suffix: "tls-client-ca-key",
        sops_paths: &[r#"["clusters"]["{cluster}"]["tls"]["client-ca-key"]"#],
        suffix: "",
    },
    SsmSecretDef {
        ssm_suffix: "tls-request-header-ca-crt",
        sops_paths: &[r#"["clusters"]["{cluster}"]["tls"]["request-header-ca-crt"]"#],
        suffix: "",
    },
    SsmSecretDef {
        ssm_suffix: "tls-request-header-ca-key",
        sops_paths: &[r#"["clusters"]["{cluster}"]["tls"]["request-header-ca-key"]"#],
        suffix: "",
    },
    SsmSecretDef {
        ssm_suffix: "tls-service-key",
        sops_paths: &[r#"["clusters"]["{cluster}"]["tls"]["service-key"]"#],
        suffix: "",
    },
];

#[derive(ClapArgs)]
pub struct Args {
    /// Cluster name (e.g. akeyless-dev). Substituted into SOPS paths + the
    /// default SSM prefix.
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

    /// Print what would be written without calling AWS.
    #[arg(long)]
    dry_run: bool,

    /// After seeding, read each SecureString back and assert it matches the
    /// SOPS source — the drift gate that turns "re-run safe" into "provably
    /// identical state": same SOPS → same SSM → same cluster identity across
    /// every up/down. Combine with --dry-run for a pure read-back drift check
    /// (no writes).
    #[arg(long)]
    verify: bool,
}

fn default_key_file() -> PathBuf {
    crate::common::default_key_file()
}

fn find_git_root() -> Option<PathBuf> {
    crate::common::find_git_root()
}

/// Decrypt + extract one SOPS value. Returns None if every path is empty/absent.
fn extract_sops(secrets_file: &Path, age_key_file: &Path, paths: &[&str], cluster: &str) -> Option<String> {
    for path_template in paths {
        let path = path_template.replace("{cluster}", cluster);
        let output = std::process::Command::new("sops")
            .args(["--decrypt", "--extract", &path])
            .arg(secrets_file)
            .env("SOPS_AGE_KEY_FILE", age_key_file)
            .output()
            .ok()?;
        if output.status.success() {
            let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !value.is_empty() {
                return Some(value);
            }
        }
    }
    None
}

/// The exact value a secret must hold in SSM = the SOPS source plus any def
/// suffix (only the k3s admin-password carries one). Used by BOTH the seed
/// and the verify pass so they can never compute a different "intended" value
/// — the determinism contract lives in one function.
fn intended_value(raw: String, def: &SsmSecretDef) -> String {
    let mut value = raw;
    if !def.suffix.is_empty() {
        value.push_str(def.suffix);
    }
    value
}

/// Write one SecureString to SSM via the typed aws-sdk-ssm client. The value
/// is passed in-process — never argv, a temp file, or a CLI — and the SDK
/// serializes the request. This is the de-shelled path (one client, no
/// per-call CLI cold-start), so 13 puts complete in well under a second.
async fn put_ssm_parameter(
    client: &aws_sdk_ssm::Client,
    name: &str,
    value: &str,
    dry_run: bool,
) -> Result<()> {
    if dry_run {
        info!(param = %name, bytes = value.len(), "DRY-RUN: would put SecureString");
        return Ok(());
    }

    client
        .put_parameter()
        .name(name)
        .value(value)
        .r#type(aws_sdk_ssm::types::ParameterType::SecureString)
        .overwrite(true)
        .send()
        .await
        .with_context(|| format!("aws ssm put-parameter {name}"))?;
    Ok(())
}

/// Resolved configuration for one cluster's bootstrap-secret projection.
/// The reusable seed/verify core takes this so both the CLI (`run`) and the
/// `seibi bootstrap` orchestrator drive ONE implementation — no duplicated
/// SOPS→SSM logic, no chance of the two paths diverging.
pub struct SsmConfig {
    pub cluster: String,
    pub prefix: String,
    pub region: String,
    pub secrets_file: PathBuf,
    pub age_key_file: PathBuf,
}

/// Outcome of one `seed` pass. `written` = parameters absent-or-differing from
/// SOPS that were (re)written; `unchanged` = already byte-identical (the
/// idempotent no-op path); `missing` = absent from SOPS (the node would fail to
/// boot without these).
#[derive(Debug, Default, Clone)]
pub struct SeedSummary {
    pub written: Vec<String>,
    pub unchanged: Vec<String>,
    pub missing: Vec<String>,
}

/// Outcome of one `verify` pass — the read-back drift gate as data.
#[derive(Debug, Default, Clone)]
pub struct VerifySummary {
    pub verified: Vec<String>,
    pub drift: Vec<String>,
}

/// Build one typed SSM client for `region`. Credentials resolve from the
/// ambient chain (AWS_PROFILE / SSO / env), the same as the aws CLI.
pub async fn make_client(region: &str) -> aws_sdk_ssm::Client {
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_sdk_ssm::config::Region::new(region.to_string()))
        .load()
        .await;
    aws_sdk_ssm::Client::new(&sdk_config)
}

/// Read one SecureString's current decrypted value, or `None` when the
/// parameter does not exist (the expected first-seed case). Any other error
/// propagates.
async fn get_ssm_value(client: &aws_sdk_ssm::Client, name: &str) -> Result<Option<String>> {
    match client
        .get_parameter()
        .name(name)
        .with_decryption(true)
        .send()
        .await
    {
        Ok(out) => Ok(out.parameter.and_then(|p| p.value)),
        Err(e) => {
            if e.as_service_error().map(|se| se.is_parameter_not_found()) == Some(true) {
                Ok(None)
            } else {
                Err(e).with_context(|| format!("aws ssm get-parameter {name}"))
            }
        }
    }
}

/// Seed every boot secret from SOPS into SSM, idempotently: a parameter whose
/// live value already equals the SOPS source is left untouched (no new SSM
/// version), so a converged cluster re-seeds as a pure no-op. Returns the typed
/// summary; the caller decides policy on `missing`.
pub async fn seed(
    cfg: &SsmConfig,
    client: &aws_sdk_ssm::Client,
    dry_run: bool,
) -> Result<SeedSummary> {
    let mut summary = SeedSummary::default();
    for def in SECRET_DEFS {
        let name = format!("{}/{}", cfg.prefix, def.ssm_suffix);
        match extract_sops(&cfg.secrets_file, &cfg.age_key_file, def.sops_paths, &cfg.cluster) {
            Some(raw) => {
                let value = intended_value(raw, def);
                let current = get_ssm_value(client, &name).await?;
                if current.as_deref() == Some(value.as_str()) {
                    info!(param = %name, "unchanged (SSM already matches SOPS)");
                    summary.unchanged.push(def.ssm_suffix.to_string());
                } else {
                    put_ssm_parameter(client, &name, &value, dry_run)
                        .await
                        .with_context(|| format!("seeding {name}"))?;
                    info!(param = %name, "seeded");
                    summary.written.push(def.ssm_suffix.to_string());
                }
            }
            None => {
                warn!(param = %name, "secret not found in SOPS — skipping");
                summary.missing.push(def.ssm_suffix.to_string());
            }
        }
    }
    Ok(summary)
}

/// Read each SecureString back and compare to its SOPS source — the drift gate
/// as a reusable function. Returns the typed summary; the caller decides policy
/// on `drift`.
pub async fn verify(cfg: &SsmConfig, client: &aws_sdk_ssm::Client) -> Result<VerifySummary> {
    let mut summary = VerifySummary::default();
    for def in SECRET_DEFS {
        let name = format!("{}/{}", cfg.prefix, def.ssm_suffix);
        let Some(raw) =
            extract_sops(&cfg.secrets_file, &cfg.age_key_file, def.sops_paths, &cfg.cluster)
        else {
            summary.drift.push(format!("{} (absent from SOPS)", def.ssm_suffix));
            continue;
        };
        let expected = intended_value(raw, def);
        match get_ssm_value(client, &name).await {
            Ok(Some(got)) if got == expected => {
                info!(param = %name, "verified == SOPS");
                summary.verified.push(def.ssm_suffix.to_string());
            }
            Ok(Some(_)) => summary.drift.push(format!("{} (SSM != SOPS)", def.ssm_suffix)),
            Ok(None) => summary.drift.push(format!("{} (absent from SSM)", def.ssm_suffix)),
            Err(e) => summary.drift.push(format!("{} ({e})", def.ssm_suffix)),
        }
    }
    Ok(summary)
}

/// Number of boot-critical secrets a k3s node fetches (for receipts).
pub const SECRET_COUNT: usize = SECRET_DEFS.len();

/// SOPS-only presence check (no AWS): the SSM suffixes whose SOPS source is
/// absent or empty. The fail-fast precondition before any AWS write — if the
/// deterministic source is incomplete, the node would fail to boot, so the
/// orchestrator refuses to proceed.
#[must_use]
pub fn sops_missing(cfg: &SsmConfig) -> Vec<String> {
    SECRET_DEFS
        .iter()
        .filter(|def| {
            extract_sops(&cfg.secrets_file, &cfg.age_key_file, def.sops_paths, &cfg.cluster)
                .is_none()
        })
        .map(|def| def.ssm_suffix.to_string())
        .collect()
}

pub async fn run(args: Args) -> Result<ExitCode> {
    let secrets_file = args.secrets_file.unwrap_or_else(|| {
        find_git_root().unwrap_or_else(|| PathBuf::from(".")).join("secrets.yaml")
    });
    if !secrets_file.exists() {
        anyhow::bail!(
            "secrets file not found: {} (set --secrets-file or run from the nix repo on cid)",
            secrets_file.display()
        );
    }

    let age_key_file = args.age_key_file.unwrap_or_else(default_key_file);
    if !age_key_file.exists() {
        anyhow::bail!(
            "age key not found at {} — this command must run on the host with the SOPS age key (cid)",
            age_key_file.display()
        );
    }

    let prefix = args
        .prefix
        .unwrap_or_else(|| format!("/pangea/{}/secrets", args.cluster));

    let cfg = SsmConfig {
        cluster: args.cluster,
        prefix,
        region: args.region,
        secrets_file,
        age_key_file,
    };

    info!(
        cluster = %cfg.cluster,
        prefix = %cfg.prefix,
        region = %cfg.region,
        dry_run = args.dry_run,
        "seeding bootstrap secrets to SSM SecureString"
    );

    // One typed client for the whole run (credentials resolved from the
    // ambient chain — AWS_PROFILE / SSO / env — the same as the aws CLI).
    let client = make_client(&cfg.region).await;

    let summary = seed(&cfg, &client, args.dry_run).await?;
    info!(
        cluster = %cfg.cluster,
        written = summary.written.len(),
        unchanged = summary.unchanged.len(),
        "SSM seeding complete"
    );

    if !summary.missing.is_empty() {
        // Fail-hard: an incompletely-seeded cluster will fail-hard at node
        // boot when kindling can't fetch a required secret. Surface it now.
        anyhow::bail!(
            "{} secret(s) missing from SOPS for cluster {}: {}. The node would \
             fail to boot without these — check the SOPS paths.",
            summary.missing.len(),
            cfg.cluster,
            summary.missing.join(", ")
        );
    }

    // ── Drift gate (--verify): read each SecureString back and assert it
    // matches the SOPS source. Turns idempotency from "re-run without error"
    // into "provably identical state" — the guarantee that cluster ups/downs
    // reproduce the SAME identity. With --dry-run (writes skipped) this is a
    // pure read-back drift check against whatever is already seeded.
    if args.verify {
        info!(cluster = %cfg.cluster, "verifying SSM SecureStrings match SOPS (drift gate)");
        let v = verify(&cfg, &client).await?;
        if !v.drift.is_empty() {
            anyhow::bail!(
                "{} secret(s) DRIFTED for cluster {}: {}. SSM does not match SOPS — \
                 re-seed (drop --dry-run) to converge; otherwise the node boots a \
                 DIFFERENT identity than SOPS intends.",
                v.drift.len(),
                cfg.cluster,
                v.drift.join(", ")
            );
        }
        info!(
            cluster = %cfg.cluster,
            verified = v.verified.len(),
            "all secrets match SOPS — deterministic, no drift"
        );
    }

    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defs_cover_the_13_boot_secrets() {
        assert_eq!(SECRET_DEFS.len(), 13, "expected 13 boot-critical SSM secrets");
    }

    #[test]
    fn every_def_has_a_sops_path_and_suffix_is_known() {
        for def in SECRET_DEFS {
            assert!(!def.sops_paths.is_empty(), "{} has no SOPS path", def.ssm_suffix);
            assert!(!def.ssm_suffix.is_empty());
        }
    }

    #[test]
    fn only_admin_password_carries_a_value_suffix() {
        let suffixed: Vec<_> = SECRET_DEFS.iter().filter(|d| !d.suffix.is_empty()).collect();
        assert_eq!(suffixed.len(), 1);
        assert_eq!(suffixed[0].ssm_suffix, "k3s-admin-password");
        assert_eq!(suffixed[0].suffix, ",admin,admin,system:masters");
    }

    #[test]
    fn cluster_substitution_in_sops_paths() {
        let p = r#"["ryn"]["wireguard"]["ryn-{cluster}"]["psk"]"#.replace("{cluster}", "akeyless-dev");
        assert_eq!(p, r#"["ryn"]["wireguard"]["ryn-akeyless-dev"]["psk"]"#);
    }

    #[test]
    fn intended_value_applies_suffix_consistently() {
        // The admin-password def carries the k3s static-token suffix; the seed
        // and verify passes both compute the intended value through this one
        // function, so the drift gate can never false-positive on the suffix.
        let admin = SECRET_DEFS
            .iter()
            .find(|d| d.ssm_suffix == "k3s-admin-password")
            .unwrap();
        assert_eq!(
            intended_value("hunter2".to_string(), admin),
            "hunter2,admin,admin,system:masters"
        );
        let age = SECRET_DEFS
            .iter()
            .find(|d| d.ssm_suffix == "sops-age-key")
            .unwrap();
        assert_eq!(intended_value("AGE-KEY".to_string(), age), "AGE-KEY");
    }

    #[test]
    fn ssm_suffixes_are_unique() {
        let mut seen = std::collections::HashSet::new();
        for def in SECRET_DEFS {
            assert!(seen.insert(def.ssm_suffix), "duplicate SSM suffix: {}", def.ssm_suffix);
        }
    }
}
