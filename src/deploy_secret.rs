use clap::Args as ClapArgs;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::ExitCode;
use tracing::info;

#[derive(Debug, thiserror::Error)]
enum DeployError {
    #[error("creating directory {path}: {source}")]
    MkdirFailed {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("copying {src} → {dst}: {source}")]
    CopyFailed {
        src: PathBuf,
        dst: PathBuf,
        source: std::io::Error,
    },

    #[error("parsing mode '{mode}'")]
    InvalidMode {
        mode: String,
        source: std::num::ParseIntError,
    },

    #[error("chmod {mode} {path}: {source}")]
    ChmodFailed {
        mode: String,
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("chown {owner} {path} failed")]
    ChownFailed { owner: String, path: PathBuf },

    #[error("running chown {owner} {path}: {source}")]
    ChownExec {
        owner: String,
        path: PathBuf,
        source: std::io::Error,
    },
}

#[derive(ClapArgs)]
pub struct Args {
    /// Source file path
    #[arg(long)]
    source: PathBuf,

    /// Destination file path
    #[arg(long)]
    dest: PathBuf,

    /// File mode in octal (e.g., 0600)
    #[arg(long, default_value = "0600")]
    mode: String,

    /// Owner in user:group format (runs chown)
    #[arg(long)]
    owner: Option<String>,
}

/// Parse an octal mode string (e.g. "0600", "644") into a `u32`.
fn parse_octal_mode(s: &str) -> Result<u32, std::num::ParseIntError> {
    u32::from_str_radix(s.trim_start_matches('0'), 8)
}

/// Copy a secret file to its destination with specified permissions and optional ownership.
pub fn run(args: &Args) -> Result<ExitCode, anyhow::Error> {
    if let Some(parent) = args.dest.parent() {
        fs::create_dir_all(parent).map_err(|source| DeployError::MkdirFailed {
            path: parent.to_path_buf(),
            source,
        })?;
    }

    fs::copy(&args.source, &args.dest).map_err(|source| DeployError::CopyFailed {
        src: args.source.clone(),
        dst: args.dest.clone(),
        source,
    })?;

    let mode = parse_octal_mode(&args.mode).map_err(|source| DeployError::InvalidMode {
        mode: args.mode.clone(),
        source,
    })?;
    fs::set_permissions(&args.dest, fs::Permissions::from_mode(mode)).map_err(|source| {
        DeployError::ChmodFailed {
            mode: args.mode.clone(),
            path: args.dest.clone(),
            source,
        }
    })?;

    if let Some(ref owner) = args.owner {
        let status = std::process::Command::new("chown")
            .arg(owner)
            .arg(&args.dest)
            .status()
            .map_err(|source| DeployError::ChownExec {
                owner: owner.clone(),
                path: args.dest.clone(),
                source,
            })?;
        if !status.success() {
            return Err(DeployError::ChownFailed {
                owner: owner.clone(),
                path: args.dest.clone(),
            }
            .into());
        }
    }

    info!(
        source = %args.source.display(),
        dest = %args.dest.display(),
        mode = %args.mode,
        owner = ?args.owner,
        "secret deployed"
    );
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_copies_file_and_sets_permissions() {
        let dir = std::env::temp_dir().join("seibi-test-deploy-basic");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let source = dir.join("secret.txt");
        fs::write(&source, "supersecret").unwrap();
        let dest = dir.join("deployed.txt");

        let result = run(&Args {
            source: source.clone(),
            dest: dest.clone(),
            mode: "0600".into(),
            owner: None,
        });

        assert!(result.is_ok());
        assert_eq!(fs::read_to_string(&dest).unwrap(), "supersecret");
        let perms = fs::metadata(&dest).unwrap().permissions();
        assert_eq!(perms.mode() & 0o777, 0o600);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_creates_parent_dirs() {
        let dir = std::env::temp_dir().join("seibi-test-deploy-nested");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let source = dir.join("src.txt");
        fs::write(&source, "data").unwrap();
        let dest = dir.join("a/b/c/dest.txt");

        let result = run(&Args {
            source,
            dest: dest.clone(),
            mode: "0644".into(),
            owner: None,
        });

        assert!(result.is_ok());
        assert!(dest.exists());
        let perms = fs::metadata(&dest).unwrap().permissions();
        assert_eq!(perms.mode() & 0o777, 0o644);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_mode_400() {
        let dir = std::env::temp_dir().join("seibi-test-deploy-400");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let source = dir.join("src.txt");
        fs::write(&source, "readonly").unwrap();
        let dest = dir.join("dest.txt");

        let result = run(&Args {
            source,
            dest: dest.clone(),
            mode: "0400".into(),
            owner: None,
        });

        assert!(result.is_ok());
        let perms = fs::metadata(&dest).unwrap().permissions();
        assert_eq!(perms.mode() & 0o777, 0o400);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_mode_without_leading_zero() {
        let dir = std::env::temp_dir().join("seibi-test-deploy-nolead");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let source = dir.join("src.txt");
        fs::write(&source, "data").unwrap();
        let dest = dir.join("dest.txt");

        let result = run(&Args {
            source,
            dest: dest.clone(),
            mode: "600".into(),
            owner: None,
        });

        assert!(result.is_ok());
        let perms = fs::metadata(&dest).unwrap().permissions();
        assert_eq!(perms.mode() & 0o777, 0o600);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_invalid_mode_returns_error() {
        let dir = std::env::temp_dir().join("seibi-test-deploy-badmode");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let source = dir.join("src.txt");
        fs::write(&source, "data").unwrap();
        let dest = dir.join("dest.txt");

        let result = run(&Args {
            source,
            dest,
            mode: "xyz".into(),
            owner: None,
        });

        assert!(result.is_err());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_missing_source_returns_error() {
        let dir = std::env::temp_dir().join("seibi-test-deploy-nosrc");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let result = run(&Args {
            source: dir.join("nonexistent"),
            dest: dir.join("dest.txt"),
            mode: "0600".into(),
            owner: None,
        });

        assert!(result.is_err());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn parse_octal_mode_standard() {
        assert_eq!(parse_octal_mode("0600").unwrap(), 0o600);
        assert_eq!(parse_octal_mode("0644").unwrap(), 0o644);
        assert_eq!(parse_octal_mode("0755").unwrap(), 0o755);
        assert_eq!(parse_octal_mode("0400").unwrap(), 0o400);
    }

    #[test]
    fn parse_octal_mode_without_leading_zero() {
        assert_eq!(parse_octal_mode("600").unwrap(), 0o600);
        assert_eq!(parse_octal_mode("644").unwrap(), 0o644);
    }

    #[test]
    fn parse_octal_mode_multiple_leading_zeros() {
        assert_eq!(parse_octal_mode("00600").unwrap(), 0o600);
    }

    #[test]
    fn parse_octal_mode_invalid() {
        assert!(parse_octal_mode("xyz").is_err());
        assert!(parse_octal_mode("").is_err());
        assert!(parse_octal_mode("999").is_err());
    }
}
