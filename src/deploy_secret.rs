use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::ExitCode;
use tracing::info;

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

/// Copy a secret file to its destination with specified permissions and optional ownership.
pub fn run(args: &Args) -> Result<ExitCode> {
    if let Some(parent) = args.dest.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating directory {}", parent.display()))?;
    }

    fs::copy(&args.source, &args.dest).with_context(|| {
        format!(
            "copying {} → {}",
            args.source.display(),
            args.dest.display()
        )
    })?;

    let mode = u32::from_str_radix(args.mode.trim_start_matches('0'), 8)
        .with_context(|| format!("parsing mode '{}'", args.mode))?;
    fs::set_permissions(&args.dest, fs::Permissions::from_mode(mode))
        .with_context(|| format!("chmod {} {}", args.mode, args.dest.display()))?;

    if let Some(ref owner) = args.owner {
        let status = std::process::Command::new("chown")
            .arg(owner)
            .arg(&args.dest)
            .status()
            .with_context(|| format!("running chown {owner} {}", args.dest.display()))?;
        if !status.success() {
            anyhow::bail!("chown {owner} {} failed", args.dest.display());
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
}
