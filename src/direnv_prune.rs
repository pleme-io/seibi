use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, SystemTime};
use tracing::info;

#[derive(ClapArgs)]
pub struct Args {
    /// Base directories to scan for `.direnv` flake-profile GC roots.
    #[arg(long, required = true, num_args = 1..)]
    paths: Vec<String>,

    /// Remove flake-profile/flake-inputs entries whose mtime is older than this many days.
    #[arg(long, default_value_t = 30)]
    older_than_days: u32,

    /// Don't actually delete — just report what would be removed.
    #[arg(long)]
    dry_run: bool,

    /// Maximum directory depth to walk while searching for `.direnv` directories.
    #[arg(long, default_value_t = 6)]
    max_depth: u32,
}

/// Walk each `path` and remove stale `.direnv/flake-profile-*` and
/// `.direnv/flake-inputs/` entries. Each is a Nix GC root that pins an entire
/// flake closure; removing them releases the GC root so the next
/// `nix-collect-garbage` can free those store paths. The next `direnv allow`
/// in the workspace will rebuild whatever is still needed.
pub fn run(args: &Args) -> Result<ExitCode> {
    let home = std::env::var("HOME").context("HOME not set")?;
    let cutoff = SystemTime::now() - Duration::from_secs(u64::from(args.older_than_days) * 86_400);

    let mut entries_removed: u32 = 0;
    let mut bytes_freed: u64 = 0;

    for raw in &args.paths {
        let base = expand_tilde(raw, &home);
        if !base.exists() {
            info!(path = %base.display(), "skipping — path does not exist");
            continue;
        }

        let mut direnv_dirs = Vec::new();
        find_direnv_dirs(&base, 0, args.max_depth, &mut direnv_dirs);

        for dd in direnv_dirs {
            for entry in collect_stale_entries(&dd, cutoff) {
                let size = path_size(&entry);
                let size_kb = size / 1024;
                info!(
                    path = %entry.display(),
                    size_kb,
                    "stale direnv entry"
                );
                if !args.dry_run {
                    remove_entry(&entry)
                        .with_context(|| format!("removing {}", entry.display()))?;
                }
                entries_removed += 1;
                bytes_freed += size;
            }
        }
    }

    let freed_mb = bytes_freed / (1024 * 1024);
    if args.dry_run {
        info!(
            entries_removed,
            freed_mb, "dry run complete — would free {freed_mb} MB"
        );
    } else {
        info!(
            entries_removed,
            freed_mb, "direnv-prune complete — freed {freed_mb} MB"
        );
    }
    Ok(ExitCode::SUCCESS)
}

/// Walk `dir` looking for directories named `.direnv`. Skip hidden dirs (other than
/// `.direnv` itself), `node_modules`, and `target` to keep the scan cheap.
fn find_direnv_dirs(dir: &Path, depth: u32, max_depth: u32, out: &mut Vec<PathBuf>) {
    if depth > max_depth {
        return;
    }
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(meta) = entry.metadata() else { continue };
        if !meta.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name == ".direnv" {
            out.push(path);
            continue;
        }
        if name_str.starts_with('.') || name_str == "node_modules" || name_str == "target" {
            continue;
        }
        find_direnv_dirs(&path, depth + 1, max_depth, out);
    }
}

/// Return entries inside `direnv_dir` that look like flake-profile GC roots and
/// whose mtime is older than `cutoff`.
fn collect_stale_entries(direnv_dir: &Path, cutoff: SystemTime) -> Vec<PathBuf> {
    let mut stale = Vec::new();
    let Ok(entries) = std::fs::read_dir(direnv_dir) else {
        return stale;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        let is_flake_root = name_str.starts_with("flake-profile-") || name_str == "flake-inputs";
        if !is_flake_root {
            continue;
        }
        let path = entry.path();
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        let Ok(modified) = meta.modified() else {
            continue;
        };
        if modified < cutoff {
            stale.push(path);
        }
    }
    stale
}

/// Remove a path that may be a symlink, file, or directory.
fn remove_entry(path: &Path) -> std::io::Result<()> {
    let meta = std::fs::symlink_metadata(path)?;
    let ft = meta.file_type();
    if ft.is_symlink() || ft.is_file() {
        std::fs::remove_file(path)
    } else {
        std::fs::remove_dir_all(path)
    }
}

/// Disk size of a path. Symlinks contribute their own length (not the target's).
fn path_size(path: &Path) -> u64 {
    let Ok(meta) = std::fs::symlink_metadata(path) else {
        return 0;
    };
    if meta.file_type().is_symlink() || meta.is_file() {
        return meta.len();
    }
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    let mut total = 0u64;
    for entry in entries.flatten() {
        total += path_size(&entry.path());
    }
    total
}

fn expand_tilde(path: &str, home: &str) -> PathBuf {
    crate::common::expand_tilde(path, home)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{Duration, SystemTime};

    fn fresh_tmp(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(name);
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn touch_old(path: &Path, days_ago: u64) {
        let when = SystemTime::now() - Duration::from_secs(days_ago * 86_400);
        let f = fs::File::options().write(true).open(path).unwrap();
        f.set_modified(when).unwrap();
    }

    #[test]
    fn finds_dot_direnv_dirs_and_skips_node_modules() {
        let dir = fresh_tmp("seibi-test-direnv-find");
        fs::create_dir_all(dir.join("workspace-a/.direnv")).unwrap();
        fs::create_dir_all(dir.join("workspace-a/node_modules/.direnv")).unwrap();
        fs::create_dir_all(dir.join("workspace-b/.direnv")).unwrap();

        let mut out = Vec::new();
        find_direnv_dirs(&dir, 0, 4, &mut out);
        out.sort();
        assert_eq!(out.len(), 2, "expected 2 dirs, got {out:?}");
        assert!(out.iter().all(|p| p.ends_with(".direnv")));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn collect_stale_filters_by_mtime_and_name() {
        let dir = fresh_tmp("seibi-test-direnv-stale");
        let direnv = dir.join(".direnv");
        fs::create_dir_all(&direnv).unwrap();

        // Old flake-profile (eligible)
        let old_profile = direnv.join("flake-profile-deadbeef");
        fs::write(&old_profile, b"").unwrap();
        touch_old(&old_profile, 60);

        // Fresh flake-profile (not eligible)
        let fresh_profile = direnv.join("flake-profile-cafebabe");
        fs::write(&fresh_profile, b"").unwrap();

        // Random file (not eligible regardless of mtime)
        let other = direnv.join("rc.cache");
        fs::write(&other, b"").unwrap();
        touch_old(&other, 90);

        let cutoff = SystemTime::now() - Duration::from_secs(30 * 86_400);
        let stale = collect_stale_entries(&direnv, cutoff);
        assert_eq!(stale.len(), 1);
        assert!(stale[0].ends_with("flake-profile-deadbeef"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_dry_run_leaves_files_in_place() {
        let dir = fresh_tmp("seibi-test-direnv-dryrun");
        let direnv = dir.join(".direnv");
        fs::create_dir_all(&direnv).unwrap();

        let stale = direnv.join("flake-profile-old");
        fs::write(&stale, b"").unwrap();
        touch_old(&stale, 60);

        unsafe { std::env::set_var("HOME", dir.to_string_lossy().as_ref()) };

        let result = run(&Args {
            paths: vec![dir.to_string_lossy().into_owned()],
            older_than_days: 30,
            dry_run: true,
            max_depth: 4,
        });
        assert!(result.is_ok());
        assert!(stale.exists(), "dry-run must not delete");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_real_removes_stale_only() {
        let dir = fresh_tmp("seibi-test-direnv-real");
        let direnv = dir.join(".direnv");
        fs::create_dir_all(&direnv).unwrap();

        let stale = direnv.join("flake-profile-old");
        let fresh = direnv.join("flake-profile-new");
        let bystander = direnv.join("rc.cache");
        fs::write(&stale, b"").unwrap();
        fs::write(&fresh, b"").unwrap();
        fs::write(&bystander, b"").unwrap();
        touch_old(&stale, 60);

        unsafe { std::env::set_var("HOME", dir.to_string_lossy().as_ref()) };

        let result = run(&Args {
            paths: vec![dir.to_string_lossy().into_owned()],
            older_than_days: 30,
            dry_run: false,
            max_depth: 4,
        });
        assert!(result.is_ok());
        assert!(!stale.exists(), "stale entry must be removed");
        assert!(fresh.exists(), "fresh entry must be preserved");
        assert!(bystander.exists(), "non-flake entry must be preserved");

        let _ = fs::remove_dir_all(&dir);
    }
}
