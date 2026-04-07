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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn expand_tilde_with_home_prefix() {
        let result = expand_tilde("~/projects", "/home/alice");
        assert_eq!(result, PathBuf::from("/home/alice/projects"));
    }

    #[test]
    fn expand_tilde_absolute_path_unchanged() {
        let result = expand_tilde("/var/data", "/home/alice");
        assert_eq!(result, PathBuf::from("/var/data"));
    }

    #[test]
    fn expand_tilde_relative_path_unchanged() {
        let result = expand_tilde("relative/path", "/home/alice");
        assert_eq!(result, PathBuf::from("relative/path"));
    }

    #[test]
    fn find_rust_targets_detects_target_with_cargo_toml() {
        let dir = std::env::temp_dir().join("seibi-test-find-targets");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("myproject/target")).unwrap();
        fs::write(dir.join("myproject/Cargo.toml"), "[package]\nname = \"test\"").unwrap();

        let mut results = Vec::new();
        find_rust_targets(&dir, 0, 4, &mut results);
        assert_eq!(results.len(), 1);
        assert!(results[0].ends_with("target"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn find_rust_targets_ignores_target_without_cargo_toml() {
        let dir = std::env::temp_dir().join("seibi-test-find-no-cargo");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("random/target")).unwrap();

        let mut results = Vec::new();
        find_rust_targets(&dir, 0, 4, &mut results);
        assert!(results.is_empty());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn find_rust_targets_skips_hidden_directories() {
        let dir = std::env::temp_dir().join("seibi-test-find-hidden");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join(".hidden/project/target")).unwrap();
        fs::write(dir.join(".hidden/project/Cargo.toml"), "[package]").unwrap();

        let mut results = Vec::new();
        find_rust_targets(&dir, 0, 4, &mut results);
        assert!(results.is_empty());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn find_rust_targets_respects_max_depth() {
        let dir = std::env::temp_dir().join("seibi-test-find-depth");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("a/b/c/project/target")).unwrap();
        fs::write(dir.join("a/b/c/project/Cargo.toml"), "[package]").unwrap();

        let mut results = Vec::new();
        find_rust_targets(&dir, 0, 2, &mut results);
        assert!(results.is_empty(), "should not find target beyond max_depth=2");

        let mut results = Vec::new();
        find_rust_targets(&dir, 0, 5, &mut results);
        assert_eq!(results.len(), 1, "should find target with sufficient depth");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn dir_size_empty_dir() {
        let dir = std::env::temp_dir().join("seibi-test-dirsize-empty");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        assert_eq!(dir_size(&dir), 0);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn dir_size_with_files() {
        let dir = std::env::temp_dir().join("seibi-test-dirsize-files");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("a.txt"), "hello").unwrap();
        fs::write(dir.join("b.txt"), "world!").unwrap();

        let size = dir_size(&dir);
        assert_eq!(size, 11); // 5 + 6 bytes

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn dir_size_recursive() {
        let dir = std::env::temp_dir().join("seibi-test-dirsize-recursive");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("sub")).unwrap();
        fs::write(dir.join("a.txt"), "abc").unwrap();
        fs::write(dir.join("sub/b.txt"), "defgh").unwrap();

        let size = dir_size(&dir);
        assert_eq!(size, 8); // 3 + 5 bytes

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn dir_size_nonexistent() {
        let dir = std::env::temp_dir().join("seibi-test-dirsize-nonexistent");
        assert_eq!(dir_size(&dir), 0);
    }
}
