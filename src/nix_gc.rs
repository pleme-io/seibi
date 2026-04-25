use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::process::{Command, ExitCode};
use tracing::{info, warn};

#[derive(ClapArgs)]
pub struct Args {
    /// Keep generations newer than this many days. 0 means "keep nothing — delete all old".
    #[arg(long, default_value_t = 14)]
    keep_days: u32,

    /// Don't actually delete — just print what would be removed.
    #[arg(long)]
    dry_run: bool,
}

/// Wrap `nix-collect-garbage`. With `--keep-days N`, only generations older
/// than N days are eligible. With `--keep-days 0` falls back to plain `-d`
/// (delete every old generation).
pub fn run(args: &Args) -> Result<ExitCode> {
    let mut cmd = build_command(args);
    info!(
        keep_days = args.keep_days,
        dry_run = args.dry_run,
        "running nix-collect-garbage"
    );
    let status = cmd.status().context("running nix-collect-garbage")?;
    if !status.success() {
        warn!(code = ?status.code(), "nix-collect-garbage failed");
        return Ok(ExitCode::from(2));
    }
    info!("nix-gc complete");
    Ok(ExitCode::SUCCESS)
}

fn build_command(args: &Args) -> Command {
    let mut cmd = Command::new("nix-collect-garbage");
    if args.dry_run {
        cmd.arg("--dry-run");
    }
    if args.keep_days > 0 {
        cmd.arg("--delete-older-than")
            .arg(format!("{}d", args.keep_days));
    } else {
        cmd.arg("-d");
    }
    cmd
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args_of(cmd: &Command) -> Vec<String> {
        cmd.get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn defaults_translate_to_delete_older_than_14d() {
        let cmd = build_command(&Args {
            keep_days: 14,
            dry_run: false,
        });
        assert_eq!(cmd.get_program(), "nix-collect-garbage");
        assert_eq!(args_of(&cmd), vec!["--delete-older-than", "14d"]);
    }

    #[test]
    fn keep_zero_means_delete_all_old() {
        let cmd = build_command(&Args {
            keep_days: 0,
            dry_run: false,
        });
        assert_eq!(args_of(&cmd), vec!["-d"]);
    }

    #[test]
    fn dry_run_adds_flag_before_age_arg() {
        let cmd = build_command(&Args {
            keep_days: 7,
            dry_run: true,
        });
        assert_eq!(
            args_of(&cmd),
            vec!["--dry-run", "--delete-older-than", "7d"]
        );
    }
}
