use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::process::{Command, ExitCode, Stdio};
use tracing::{info, warn};

#[derive(ClapArgs)]
pub struct Args {
    /// Don't actually prune — print `podman system df` and exit.
    #[arg(long)]
    dry_run: bool,

    /// Skip volumes (default: also prune dangling volumes).
    #[arg(long)]
    keep_volumes: bool,

    /// Skip if any container is currently running.
    #[arg(long, default_value_t = true)]
    skip_if_running: bool,
}

/// Wrap `podman system prune --all --force` (with `--volumes` by default).
/// If podman is absent, this is a no-op (`SUCCESS`) so the scheduled task
/// stays green on hosts where podman isn't installed.
pub fn run(args: &Args) -> Result<ExitCode> {
    if !command_exists("podman") {
        warn!("podman not in PATH — skipping prune");
        return Ok(ExitCode::SUCCESS);
    }

    if args.skip_if_running && containers_running()? {
        info!("podman has running containers — skipping prune");
        return Ok(ExitCode::SUCCESS);
    }

    if args.dry_run {
        let df = Command::new("podman")
            .args(["system", "df"])
            .output()
            .context("running podman system df")?;
        info!(
            df = %String::from_utf8_lossy(&df.stdout).trim(),
            "podman dry-run — printing system df"
        );
        return Ok(ExitCode::SUCCESS);
    }

    let mut cmd = build_prune_command(args);
    info!(
        keep_volumes = args.keep_volumes,
        "running podman system prune"
    );
    let output = cmd.output().context("running podman system prune")?;
    if !output.status.success() {
        warn!(
            stderr = %String::from_utf8_lossy(&output.stderr).trim(),
            code = ?output.status.code(),
            "podman prune failed"
        );
        return Ok(ExitCode::from(2));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let summary = stdout
        .lines()
        .find(|l| l.contains("reclaimed"))
        .unwrap_or("(no reclaim line in output)");
    info!(summary = summary.trim(), "podman prune complete");
    Ok(ExitCode::SUCCESS)
}

fn build_prune_command(args: &Args) -> Command {
    let mut cmd = Command::new("podman");
    cmd.args(["system", "prune", "--all", "--force"]);
    if !args.keep_volumes {
        cmd.arg("--volumes");
    }
    cmd
}

fn command_exists(name: &str) -> bool {
    Command::new("which")
        .arg(name)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

fn containers_running() -> Result<bool> {
    let out = Command::new("podman")
        .args(["ps", "--quiet"])
        .output()
        .context("running podman ps")?;
    if !out.status.success() {
        // If podman ps fails (daemon not started, machine off, etc.), be conservative
        // and treat it as "nothing running" so the prune still gets a chance.
        return Ok(false);
    }
    Ok(!out.stdout.iter().all(u8::is_ascii_whitespace))
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
    fn default_includes_volumes() {
        let cmd = build_prune_command(&Args {
            dry_run: false,
            keep_volumes: false,
            skip_if_running: true,
        });
        assert_eq!(cmd.get_program(), "podman");
        assert_eq!(
            args_of(&cmd),
            vec!["system", "prune", "--all", "--force", "--volumes"]
        );
    }

    #[test]
    fn keep_volumes_omits_volumes_flag() {
        let cmd = build_prune_command(&Args {
            dry_run: false,
            keep_volumes: true,
            skip_if_running: true,
        });
        assert_eq!(args_of(&cmd), vec!["system", "prune", "--all", "--force"]);
    }
}
