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
use std::io::Write;
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

/// Write one SecureString to SSM via the `aws` CLI. The value is passed on
/// stdin (`--cli-input-json file:///dev/stdin`), never argv, so it does not
/// appear in the process table.
fn put_ssm_parameter(region: &str, name: &str, value: &str, dry_run: bool) -> Result<()> {
    if dry_run {
        info!(param = %name, bytes = value.len(), "DRY-RUN: would put SecureString");
        return Ok(());
    }

    let input = serde_json::json!({
        "Name": name,
        "Value": value,
        "Type": "SecureString",
        "Overwrite": true,
    });

    let mut child = std::process::Command::new("aws")
        .args(["ssm", "put-parameter", "--region", region, "--cli-input-json", "file:///dev/stdin"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .context("spawning `aws ssm put-parameter`")?;

    child
        .stdin
        .take()
        .context("capturing aws stdin")?
        .write_all(serde_json::to_string(&input)?.as_bytes())
        .context("writing put-parameter input")?;

    let out = child.wait_with_output().context("awaiting `aws ssm put-parameter`")?;
    if !out.status.success() {
        anyhow::bail!(
            "aws ssm put-parameter for {name} failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(())
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

    info!(
        cluster = %args.cluster,
        prefix = %prefix,
        region = %args.region,
        dry_run = args.dry_run,
        "seeding bootstrap secrets to SSM SecureString"
    );

    let mut written = 0usize;
    let mut missing: Vec<String> = Vec::new();

    for def in SECRET_DEFS {
        let name = format!("{}/{}", prefix, def.ssm_suffix);
        match extract_sops(&secrets_file, &age_key_file, def.sops_paths, &args.cluster) {
            Some(mut value) => {
                if !def.suffix.is_empty() {
                    value.push_str(def.suffix);
                }
                put_ssm_parameter(&args.region, &name, &value, args.dry_run)
                    .with_context(|| format!("seeding {name}"))?;
                info!(param = %name, "seeded");
                written += 1;
            }
            None => {
                warn!(param = %name, "secret not found in SOPS — skipping");
                missing.push(def.ssm_suffix.to_string());
            }
        }
    }

    info!(cluster = %args.cluster, written, "SSM seeding complete");

    if !missing.is_empty() {
        // Fail-hard: an incompletely-seeded cluster will fail-hard at node
        // boot when kindling can't fetch a required secret. Surface it now.
        anyhow::bail!(
            "{} secret(s) missing from SOPS for cluster {}: {}. The node would \
             fail to boot without these — check the SOPS paths.",
            missing.len(),
            args.cluster,
            missing.join(", ")
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
    fn ssm_suffixes_are_unique() {
        let mut seen = std::collections::HashSet::new();
        for def in SECRET_DEFS {
            assert!(seen.insert(def.ssm_suffix), "duplicate SSM suffix: {}", def.ssm_suffix);
        }
    }
}
