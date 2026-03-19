use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use tracing::info;

#[derive(ClapArgs)]
pub struct Args {
    /// Base directories to scan for Rust target/ directories
    #[arg(long, required = true, num_args = 1..)]
    paths: Vec<String>,

    /// Also clean ~/.cargo/registry/cache and ~/.cargo/registry/src
    #[arg(long)]
    cargo_cache: bool,

    /// Only report what would be deleted, don't actually delete
    #[arg(long)]
    dry_run: bool,

    /// Maximum directory depth to walk looking for target/ dirs
    #[arg(long, default_value_t = 4)]
    max_depth: u32,
}

pub async fn run(args: Args) -> Result<ExitCode> {
    let home = std::env::var("HOME").context("HOME not set")?;
    let mut total_freed: u64 = 0;
    let mut targets_found: u32 = 0;

    for raw_path in &args.paths {
        let base = expand_tilde(raw_path, &home);
        if !base.exists() {
            info!(path = %base.display(), "skipping — path does not exist");
            continue;
        }

        let mut targets = Vec::new();
        find_rust_targets(&base, 0, args.max_depth, &mut targets);

        for target_dir in targets {
            let size = dir_size(&target_dir);
            let size_mb = size / (1024 * 1024);
            info!(
                path = %target_dir.display(),
                size_mb,
                "found target/ directory"
            );

            if !args.dry_run {
                std::fs::remove_dir_all(&target_dir)
                    .with_context(|| format!("removing {}", target_dir.display()))?;
                info!(path = %target_dir.display(), "removed");
            }

            total_freed += size;
            targets_found += 1;
        }
    }

    if args.cargo_cache {
        let registry = PathBuf::from(&home).join(".cargo/registry");
        for subdir in &["cache", "src"] {
            let dir = registry.join(subdir);
            if !dir.exists() {
                continue;
            }

            let size = dir_size(&dir);
            let size_mb = size / (1024 * 1024);
            info!(
                path = %dir.display(),
                size_mb,
                "found cargo registry directory"
            );

            if !args.dry_run {
                // Remove contents but keep the directory itself
                let entries = std::fs::read_dir(&dir)
                    .with_context(|| format!("reading {}", dir.display()))?;
                for entry in entries {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_dir() {
                        std::fs::remove_dir_all(&path)
                            .with_context(|| format!("removing {}", path.display()))?;
                    } else {
                        std::fs::remove_file(&path)
                            .with_context(|| format!("removing {}", path.display()))?;
                    }
                }
                info!(path = %dir.display(), "cleaned");
            }

            total_freed += size;
        }
    }

    let total_mb = total_freed / (1024 * 1024);

    if args.dry_run {
        info!(
            targets = targets_found,
            total_mb,
            "dry run complete — would free {total_mb} MB"
        );
    } else {
        info!(
            targets = targets_found,
            total_mb,
            "cleanup complete — freed {total_mb} MB"
        );
    }

    Ok(ExitCode::SUCCESS)
}

/// Recursively find directories named `target` that have a sibling `Cargo.toml`.
fn find_rust_targets(dir: &Path, depth: u32, max_depth: u32, results: &mut Vec<PathBuf>) {
    if depth > max_depth {
        return;
    }

    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        let name = entry.file_name();
        if name == "target" {
            // Check for sibling Cargo.toml to confirm this is a Rust project
            let cargo_toml = dir.join("Cargo.toml");
            if cargo_toml.exists() {
                results.push(path);
            }
        } else {
            // Don't recurse into target directories or hidden directories
            let name_str = name.to_string_lossy();
            if !name_str.starts_with('.') {
                find_rust_targets(&path, depth + 1, max_depth, results);
            }
        }
    }
}

/// Calculate total size of a directory tree using `std::fs::metadata`.
fn dir_size(dir: &Path) -> u64 {
    let mut total: u64 = 0;

    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return 0,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            total += dir_size(&path);
        } else if let Ok(meta) = std::fs::metadata(&path) {
            total += meta.len();
        }
    }

    total
}

fn expand_tilde(path: &str, home: &str) -> PathBuf {
    if path.starts_with("~/") {
        PathBuf::from(home).join(&path[2..])
    } else {
        PathBuf::from(path)
    }
}
