use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use tracing::info;

#[derive(ClapArgs)]
pub struct Args {
    /// Directory containing .app bundles to sync (e.g., ~/Applications/Home Manager Apps)
    #[arg(long, default_value = "~/Applications/Home Manager Apps")]
    source: String,

    /// Directory to create macOS aliases in (Spotlight-indexed)
    #[arg(long, default_value = "~/Applications/Nix")]
    target: String,

    /// Force Spotlight re-index after sync
    #[arg(long, default_value = "true")]
    reindex: bool,
}

pub async fn run(args: Args) -> Result<ExitCode> {
    let home = std::env::var("HOME").context("HOME not set")?;
    let source = expand_tilde(&args.source, &home);
    let target = expand_tilde(&args.target, &home);

    // Create target directory
    std::fs::create_dir_all(&target)
        .with_context(|| format!("creating {}", target.display()))?;

    // Remove stale aliases
    if target.exists() {
        for entry in std::fs::read_dir(&target).context("reading target dir")? {
            let entry = entry?;
            let _ = std::fs::remove_file(entry.path());
            let _ = std::fs::remove_dir_all(entry.path());
        }
        info!(target = %target.display(), "cleared stale aliases");
    }

    // Find all .app bundles in source directories
    let mut app_count = 0;
    let sources = collect_sources(&source, &home);

    for src_dir in &sources {
        if !src_dir.exists() {
            continue;
        }

        let entries = std::fs::read_dir(src_dir)
            .with_context(|| format!("reading {}", src_dir.display()))?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();

            if !name.ends_with(".app") {
                continue;
            }

            // Create macOS alias via osascript (the only reliable way to create
            // Finder aliases that Spotlight indexes)
            let status = tokio::process::Command::new("/usr/bin/osascript")
                .args([
                    "-e",
                    &format!(
                        "tell application \"Finder\" to make alias file to POSIX file \"{}\" at POSIX file \"{}\"",
                        path.display(),
                        target.display()
                    ),
                ])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .await
                .with_context(|| format!("creating alias for {name}"))?;

            if status.success() {
                info!(app = %name, "aliased");
                app_count += 1;
            }
        }
    }

    // Force Spotlight to re-index
    if args.reindex {
        let _ = tokio::process::Command::new("/usr/bin/mdimport")
            .arg(&target)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await;
        info!("Spotlight re-index triggered");
    }

    info!(count = app_count, "Spotlight sync complete");
    Ok(ExitCode::SUCCESS)
}

fn expand_tilde(path: &str, home: &str) -> PathBuf {
    if path.starts_with("~/") {
        PathBuf::from(home).join(&path[2..])
    } else {
        PathBuf::from(path)
    }
}

fn collect_sources(primary: &Path, home: &str) -> Vec<PathBuf> {
    vec![
        primary.to_path_buf(),
        PathBuf::from(format!("{home}/Applications")),
    ]
}
