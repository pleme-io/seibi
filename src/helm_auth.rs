use anyhow::{Context, Result};
use base64::Engine;
use clap::Args as ClapArgs;
use serde_json::json;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::ExitCode;
use tracing::info;

#[derive(ClapArgs)]
pub struct Args {
    /// Path to registry token file
    #[arg(long, env = "SEIBI_HELM_TOKEN_FILE")]
    token_file: PathBuf,

    /// Registry username
    #[arg(long, env = "SEIBI_HELM_USERNAME")]
    username: String,

    /// Registry URL
    #[arg(long, default_value = "ghcr.io")]
    registry: String,

    /// Output path for config.json
    #[arg(long, env = "SEIBI_HELM_OUTPUT")]
    output: PathBuf,
}

pub fn run(args: &Args) -> Result<ExitCode> {
    let token = fs::read_to_string(&args.token_file)
        .with_context(|| format!("reading token from {}", args.token_file.display()))?;
    let token = token.trim();

    let auth = base64::engine::general_purpose::STANDARD
        .encode(format!("{}:{token}", args.username));

    let config = json!({
        "auths": {
            &args.registry: {
                "auth": auth
            }
        }
    });

    if let Some(parent) = args.output.parent() {
        fs::create_dir_all(parent)?;
    }

    fs::write(&args.output, serde_json::to_string_pretty(&config)?)?;
    fs::set_permissions(&args.output, fs::Permissions::from_mode(0o600))?;

    info!(
        registry = %args.registry,
        output = %args.output.display(),
        "helm auth config written"
    );
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_generates_valid_auth_config() {
        let dir = std::env::temp_dir().join("seibi-test-helm-auth");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let token_file = dir.join("token");
        fs::write(&token_file, "ghp_testtoken123\n").unwrap();
        let output = dir.join("config.json");

        let args = Args {
            token_file,
            username: "testuser".into(),
            registry: "ghcr.io".into(),
            output: output.clone(),
        };

        let result = run(&args).unwrap();
        assert_eq!(result, ExitCode::SUCCESS);

        let content = fs::read_to_string(&output).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();

        assert!(parsed["auths"]["ghcr.io"]["auth"].is_string());

        let auth_b64 = parsed["auths"]["ghcr.io"]["auth"].as_str().unwrap();
        let decoded = String::from_utf8(
            base64::engine::general_purpose::STANDARD
                .decode(auth_b64)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(decoded, "testuser:ghp_testtoken123");

        let meta = fs::metadata(&output).unwrap();
        assert_eq!(meta.permissions().mode() & 0o777, 0o600);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_custom_registry() {
        let dir = std::env::temp_dir().join("seibi-test-helm-auth-custom");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let token_file = dir.join("token");
        fs::write(&token_file, "mytoken").unwrap();
        let output = dir.join("config.json");

        let args = Args {
            token_file,
            username: "admin".into(),
            registry: "registry.example.com".into(),
            output: output.clone(),
        };

        let result = run(&args).unwrap();
        assert_eq!(result, ExitCode::SUCCESS);

        let content = fs::read_to_string(&output).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert!(parsed["auths"]["registry.example.com"]["auth"].is_string());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_missing_token_file_returns_error() {
        let dir = std::env::temp_dir().join("seibi-test-helm-auth-missing");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let args = Args {
            token_file: dir.join("nonexistent"),
            username: "user".into(),
            registry: "ghcr.io".into(),
            output: dir.join("config.json"),
        };

        let result = run(&args);
        assert!(result.is_err());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_creates_parent_directories() {
        let dir = std::env::temp_dir().join("seibi-test-helm-auth-nested");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let token_file = dir.join("token");
        fs::write(&token_file, "tok").unwrap();
        let output = dir.join("nested/deep/config.json");

        let args = Args {
            token_file,
            username: "u".into(),
            registry: "ghcr.io".into(),
            output: output.clone(),
        };

        let result = run(&args).unwrap();
        assert_eq!(result, ExitCode::SUCCESS);
        assert!(output.exists());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_trims_token_whitespace() {
        let dir = std::env::temp_dir().join("seibi-test-helm-auth-trim");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let token_file = dir.join("token");
        fs::write(&token_file, "  tok123  \n").unwrap();
        let output = dir.join("config.json");

        let args = Args {
            token_file,
            username: "user".into(),
            registry: "ghcr.io".into(),
            output: output.clone(),
        };

        run(&args).unwrap();

        let content = fs::read_to_string(&output).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        let auth_b64 = parsed["auths"]["ghcr.io"]["auth"].as_str().unwrap();
        let decoded = String::from_utf8(
            base64::engine::general_purpose::STANDARD
                .decode(auth_b64)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(decoded, "user:tok123");

        let _ = fs::remove_dir_all(&dir);
    }
}
