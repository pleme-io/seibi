//! Generate a self-signed TLS certificate (idempotent) via rcgen — no openssl
//! subprocess.
//!
//! Replaces the `vault-cert` shell oneshot in pleme-io/nix's
//! `modules/nixos/vaultwarden/default.nix` (openssl req -x509). Writes the cert
//! (0644) + key (0600) only when the cert is absent, so it is safe to run on
//! every boot before nginx. CN and SAN are both set to `--cn` (modern TLS
//! wants the SAN; the CN preserves the openssl `-subj /CN=` behaviour).

use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::ExitCode;
use tracing::{info, warn};

#[derive(ClapArgs)]
pub struct Args {
    /// Common Name + Subject Alternative Name for the certificate.
    #[arg(long)]
    cn: String,

    /// Where the PEM certificate is written.
    #[arg(long)]
    cert: PathBuf,

    /// Where the PEM private key is written.
    #[arg(long)]
    key: PathBuf,

    /// Validity in days.
    #[arg(long, default_value_t = 365)]
    days: i64,
}

/// Render the (cert PEM, key PEM) pair for a CN. Pure (no I/O) — unit-tested.
fn make_cert(cn: &str, days: i64) -> Result<(String, String)> {
    let mut params =
        rcgen::CertificateParams::new(vec![cn.to_string()]).context("building cert params")?;
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, cn);
    let now = time::OffsetDateTime::now_utc();
    params.not_before = now;
    params.not_after = now + time::Duration::days(days);

    let key = rcgen::KeyPair::generate().context("generating key pair")?;
    let cert = params.self_signed(&key).context("self-signing cert")?;
    Ok((cert.pem(), key.serialize_pem()))
}

fn write_mode(path: &PathBuf, contents: &str, mode: u32) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    fs::write(path, contents).with_context(|| format!("writing {}", path.display()))?;
    let mut perms = fs::metadata(path)?.permissions();
    perms.set_mode(mode);
    fs::set_permissions(path, perms)?;
    Ok(())
}

pub fn run(args: &Args) -> Result<ExitCode> {
    if args.cert.exists() {
        info!(cert = %args.cert.display(), "certificate already present — skipping");
        return Ok(ExitCode::SUCCESS);
    }

    let (cert_pem, key_pem) = make_cert(&args.cn, args.days)?;
    write_mode(&args.key, &key_pem, 0o600).context("writing key")?;
    write_mode(&args.cert, &cert_pem, 0o644).context("writing cert")?;

    if args.days > 825 {
        warn!(days = args.days, "validity exceeds the 825-day CA/Browser baseline");
    }
    info!(
        cn = %args.cn,
        cert = %args.cert.display(),
        key = %args.key.display(),
        days = args.days,
        "self-signed certificate generated"
    );
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_valid_pem_pair() {
        let (cert, key) = make_cert("vault.example.com", 365).unwrap();
        assert!(cert.starts_with("-----BEGIN CERTIFICATE-----"));
        assert!(cert.trim_end().ends_with("-----END CERTIFICATE-----"));
        assert!(key.contains("PRIVATE KEY-----"));
        // Body between the markers is non-trivial base64 (a real DER cert).
        let body = cert
            .lines()
            .filter(|l| !l.starts_with("-----"))
            .collect::<String>();
        assert!(body.len() > 100, "cert body should be a real DER payload");
    }

    #[test]
    fn distinct_invocations_distinct_keys() {
        let (_, k1) = make_cert("a.example", 365).unwrap();
        let (_, k2) = make_cert("a.example", 365).unwrap();
        assert_ne!(k1, k2, "each cert should get a fresh key");
    }
}
