use anyhow::Result;
use clap::Args as ClapArgs;
use std::path::PathBuf;
use std::process::ExitCode;
use tracing::info;

/// Bootstrap secret definitions: SOPS path variants and the env var to export.
struct SecretDef {
    env_var: &'static str,
    /// SOPS extract paths to try in order (first non-empty wins).
    sops_paths: &'static [&'static str],
}

/// Secrets we know how to extract for any cluster.
/// `{cluster}` is replaced at runtime.
const SECRET_DEFS: &[SecretDef] = &[
    SecretDef {
        env_var: "SOPS_CLUSTER_AGE_KEY",
        sops_paths: &[
            r#"["clusters"]["{cluster}"]["sops-age-key"]"#,
            r#"["clusters"]["{cluster}"]["age-key"]"#,
        ],
    },
    SecretDef {
        env_var: "FLUX_GITHUB_TOKEN",
        sops_paths: &[
            r#"["clusters"]["{cluster}"]["flux-github-token"]"#,
            r#"["fluxcd"]["kube-clusters"]["pat"]"#,
        ],
    },
];

#[derive(ClapArgs)]
pub struct Args {
    /// Cluster name (e.g., akeyless-dev, ryn-k3s)
    #[arg(long)]
    cluster: String,

    /// Path to SOPS-encrypted secrets file
    #[arg(long, env = "SEIBI_SECRETS_FILE")]
    secrets_file: Option<PathBuf>,

    /// SOPS age key file for decryption
    #[arg(long, env = "SOPS_AGE_KEY_FILE")]
    age_key_file: Option<PathBuf>,
}

fn default_key_file() -> PathBuf {
    crate::common::default_key_file()
}

fn find_git_root() -> Option<PathBuf> {
    crate::common::find_git_root()
}

/// Try each SOPS extract path, return the first non-empty value.
async fn try_extract(
    secrets_file: &PathBuf,
    age_key_file: &PathBuf,
    paths: &[&str],
    cluster: &str,
) -> Option<String> {
    for path_template in paths {
        let path = path_template.replace("{cluster}", cluster);

        let output = tokio::process::Command::new("sops")
            .args(["--decrypt", "--extract", &path])
            .arg(secrets_file)
            .env("SOPS_AGE_KEY_FILE", age_key_file)
            .output()
            .await
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

/// Shell-escape a value for safe inclusion in `export VAR='...'`.
fn shell_escape(s: &str) -> String {
    // Single-quote escaping: replace ' with '\''
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Extract cluster bootstrap secrets from a SOPS-encrypted file and print shell exports.
pub async fn run(args: Args) -> Result<ExitCode> {
    let secrets_file = args.secrets_file.unwrap_or_else(|| {
        let root = find_git_root().unwrap_or_else(|| PathBuf::from("."));
        root.join("secrets.yaml")
    });

    if !secrets_file.exists() {
        anyhow::bail!(
            "secrets file not found: {} (set --secrets-file or run from nix repo)",
            secrets_file.display()
        );
    }

    let age_key_file = args.age_key_file.unwrap_or_else(default_key_file);

    if !age_key_file.exists() {
        anyhow::bail!(
            "age key not found at {} (run seibi sops-key sync first)",
            age_key_file.display()
        );
    }

    info!(
        cluster = %args.cluster,
        secrets_file = %secrets_file.display(),
        "extracting cluster bootstrap secrets"
    );

    let mut extracted = 0;

    for def in SECRET_DEFS {
        match try_extract(&secrets_file, &age_key_file, def.sops_paths, &args.cluster).await {
            Some(value) => {
                println!("export {}={}", def.env_var, shell_escape(&value));
                info!(var = def.env_var, "extracted");
                extracted += 1;
            }
            None => {
                info!(var = def.env_var, "not found (skipping)");
            }
        }
    }

    info!(cluster = %args.cluster, count = extracted, "bootstrap secrets ready");

    if extracted == 0 {
        tracing::warn!(
            cluster = %args.cluster,
            "no bootstrap secrets found — check SOPS paths"
        );
    }

    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shell_escape_simple() {
        assert_eq!(shell_escape("hello"), "'hello'");
    }

    #[test]
    fn shell_escape_with_quotes() {
        assert_eq!(shell_escape("it's"), "'it'\\''s'");
    }

    #[test]
    fn shell_escape_empty() {
        assert_eq!(shell_escape(""), "''");
    }

    #[test]
    fn shell_escape_special_chars() {
        assert_eq!(shell_escape("a$b`c"), "'a$b`c'");
    }

    #[test]
    fn shell_escape_with_newline() {
        assert_eq!(shell_escape("line1\nline2"), "'line1\nline2'");
    }

    #[test]
    fn shell_escape_with_multiple_quotes() {
        assert_eq!(shell_escape("it's a 'test'"), "'it'\\''s a '\\''test'\\'''");
    }

    #[test]
    fn shell_escape_with_spaces() {
        assert_eq!(shell_escape("hello world"), "'hello world'");
    }

    #[test]
    fn shell_escape_with_backslash() {
        assert_eq!(shell_escape("a\\b"), "'a\\b'");
    }

    #[test]
    fn sops_path_template_substitution() {
        let template = r#"["clusters"]["{cluster}"]["flux-github-token"]"#;
        let result = template.replace("{cluster}", "akeyless-dev");
        assert_eq!(
            result,
            r#"["clusters"]["akeyless-dev"]["flux-github-token"]"#
        );
    }

    #[test]
    fn secret_defs_all_have_at_least_one_sops_path() {
        for def in SECRET_DEFS {
            assert!(
                !def.sops_paths.is_empty(),
                "{} has no SOPS paths",
                def.env_var
            );
        }
    }

    #[test]
    fn secret_defs_env_vars_are_uppercase() {
        for def in SECRET_DEFS {
            assert_eq!(
                def.env_var,
                def.env_var.to_uppercase(),
                "env var should be uppercase: {}",
                def.env_var
            );
        }
    }

    #[test]
    fn sops_paths_contain_cluster_placeholder() {
        for def in SECRET_DEFS {
            let has_cluster_placeholder = def
                .sops_paths
                .iter()
                .any(|p| p.contains("{cluster}"));
            if def.env_var == "SOPS_CLUSTER_AGE_KEY" {
                assert!(has_cluster_placeholder, "SOPS_CLUSTER_AGE_KEY should have {{cluster}} in paths");
            }
        }
    }
}
