use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::process::{Command, ExitCode};
use tracing::{info, warn};

#[derive(ClapArgs)]
pub struct Args {
    /// Don't actually delete — propagated as `--dry-run` to every subcommand.
    #[arg(long)]
    dry_run: bool,

    /// Subcommands to skip (comma-separated): direnv-prune, rust-cleanup,
    /// claude-vm-prune, podman-prune, nix-gc.
    #[arg(long, value_delimiter = ',')]
    skip: Vec<String>,

    /// Path passed to `direnv-prune --paths` and `rust-cleanup --paths`.
    /// Defaults to `~/code`. Pass multiple by repeating the flag.
    #[arg(long, default_values_t = vec!["~/code".to_owned()])]
    code_paths: Vec<String>,
}

/// Run every cleanup subcommand sequentially in dependency order.
///
/// Order is intentional:
///   1. `direnv-prune`     — release stale `.direnv/flake-profile-*` GC roots
///   2. `rust-cleanup`     — drop Rust target/ caches under `--code-paths`
///   3. `claude-vm-prune`  — reap stale Claude Desktop VM bundles
///   4. `podman-prune`     — drop unused images, containers, volumes
///   5. `nix-gc`           — collect generations now that pins are released
///
/// Each step is invoked as a subprocess of *this* binary (`std::env::current_exe`),
/// so behaviour is identical to running each `seibi <subcommand>` by hand. The
/// sweep continues even if a step fails; the final exit code is non-zero iff
/// at least one step failed.
pub fn run(args: &Args) -> Result<ExitCode> {
    let me = std::env::current_exe().context("locating seibi binary")?;

    let steps: [Step; 5] = [
        Step {
            name: "direnv-prune",
            extra_args: vec_with_paths("--paths", &args.code_paths),
        },
        Step {
            name: "rust-cleanup",
            extra_args: vec_with_paths("--paths", &args.code_paths),
        },
        Step {
            name: "claude-vm-prune",
            extra_args: vec![],
        },
        Step {
            name: "podman-prune",
            extra_args: vec![],
        },
        Step {
            name: "nix-gc",
            extra_args: vec![],
        },
    ];

    let mut all_ok = true;
    let mut ran = 0u32;
    let mut skipped = 0u32;

    for step in &steps {
        if args.skip.iter().any(|s| s == step.name) {
            info!(subcommand = step.name, "skipping (--skip)");
            skipped += 1;
            continue;
        }

        let mut cmd = Command::new(&me);
        cmd.arg(step.name);
        if args.dry_run {
            cmd.arg("--dry-run");
        }
        cmd.args(&step.extra_args);

        info!(subcommand = step.name, dry_run = args.dry_run, "▶ running");
        let status = cmd
            .status()
            .with_context(|| format!("spawning {}", step.name))?;
        ran += 1;
        if !status.success() {
            warn!(
                subcommand = step.name,
                code = ?status.code(),
                "step exited non-zero"
            );
            all_ok = false;
        }
    }

    info!(ran, skipped, ok = all_ok, "sweep complete");
    if all_ok {
        Ok(ExitCode::SUCCESS)
    } else {
        Ok(ExitCode::from(2))
    }
}

struct Step {
    name: &'static str,
    extra_args: Vec<String>,
}

fn vec_with_paths(flag: &str, paths: &[String]) -> Vec<String> {
    if paths.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(1 + paths.len());
    out.push(flag.to_owned());
    out.extend(paths.iter().cloned());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vec_with_paths_emits_flag_then_values() {
        let v = vec_with_paths("--paths", &["~/code".to_owned(), "/tmp/x".to_owned()]);
        assert_eq!(v, vec!["--paths", "~/code", "/tmp/x"]);
    }

    #[test]
    fn vec_with_paths_empty_yields_empty() {
        let v = vec_with_paths("--paths", &[]);
        assert!(v.is_empty());
    }

    #[test]
    fn skip_list_filters_named_steps() {
        // Construct args manually — sweep never spawns when skip covers everything,
        // but we don't actually invoke run() here (would need a real seibi binary).
        // Instead, just verify the comparison logic the loop relies on.
        let skip = vec!["nix-gc".to_owned(), "podman-prune".to_owned()];
        assert!(skip.iter().any(|s| s == "nix-gc"));
        assert!(skip.iter().any(|s| s == "podman-prune"));
        assert!(!skip.iter().any(|s| s == "direnv-prune"));
    }
}
