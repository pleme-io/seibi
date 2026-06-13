//! Timestamped tar.gz backup of a directory, with retention + optional
//! stop/start of a systemd unit around it.
//!
//! Replaces the `vaultwarden-backup` shell oneshot in pleme-io/nix's
//! `modules/nixos/vaultwarden/default.nix`. NO-SHELL: `tar` and `systemctl`
//! are typed `Command` wrappers (not a shell interpreter); the timestamp,
//! retention prune, and stop/restart ordering are Rust. The unit is restarted
//! even if the archive step fails (the shell version left it stopped on error).

use anyhow::{bail, Context, Result};
use clap::Args as ClapArgs;
use std::fs;
use std::path::PathBuf;
use std::process::{Command, ExitCode};
use tracing::{info, warn};

#[derive(ClapArgs)]
pub struct Args {
    /// Directory whose contents are archived.
    #[arg(long)]
    source: PathBuf,

    /// Directory the timestamped archive is written to.
    #[arg(long)]
    dest: PathBuf,

    /// Archive name prefix (`<prefix>-<YYYYmmdd-HHMMSS>.tar.gz`).
    #[arg(long)]
    prefix: String,

    /// Number of newest archives to retain (older are deleted).
    #[arg(long, default_value_t = 7)]
    keep: usize,

    /// Optional systemd unit to stop before / start after the backup (for a
    /// consistent snapshot).
    #[arg(long)]
    stop_unit: Option<String>,
}

fn timestamp() -> String {
    let now = time::OffsetDateTime::now_utc();
    format!(
        "{:04}{:02}{:02}-{:02}{:02}{:02}",
        now.year(),
        now.month() as u8,
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    )
}

/// Given the existing archive file names + a retention count, return the names
/// to delete (everything older than the newest `keep`). Pure — unit-tested.
/// Timestamped names sort lexicographically == chronologically.
fn to_prune(mut files: Vec<String>, keep: usize) -> Vec<String> {
    files.sort();
    files.reverse(); // newest first
    files.into_iter().skip(keep).collect()
}

fn systemctl(verb: &str, unit: &str) -> Result<()> {
    let status = Command::new("systemctl")
        .args([verb, unit])
        .status()
        .with_context(|| format!("invoking systemctl {verb} {unit}"))?;
    if !status.success() {
        bail!("systemctl {verb} {unit} failed: {status}");
    }
    Ok(())
}

fn archive(source: &PathBuf, out: &PathBuf) -> Result<()> {
    // tar -czf <out> -C <source> .  — archive the directory's contents.
    let status = Command::new("tar")
        .arg("-czf")
        .arg(out)
        .arg("-C")
        .arg(source)
        .arg(".")
        .status()
        .context("invoking tar")?;
    if !status.success() {
        bail!("tar exited with {status}");
    }
    Ok(())
}

fn prune(dest: &PathBuf, prefix: &str, keep: usize) -> Result<usize> {
    let suffix = ".tar.gz";
    let mut names: Vec<String> = Vec::new();
    for entry in fs::read_dir(dest).with_context(|| format!("reading {}", dest.display()))? {
        let name = entry?.file_name().to_string_lossy().into_owned();
        if name.starts_with(&format!("{prefix}-")) && name.ends_with(suffix) {
            names.push(name);
        }
    }
    let mut removed = 0;
    for name in to_prune(names, keep) {
        let path = dest.join(&name);
        fs::remove_file(&path).with_context(|| format!("removing {}", path.display()))?;
        removed += 1;
    }
    Ok(removed)
}

pub async fn run(args: &Args) -> Result<ExitCode> {
    fs::create_dir_all(&args.dest)
        .with_context(|| format!("creating {}", args.dest.display()))?;

    let out = args.dest.join(format!("{}-{}.tar.gz", args.prefix, timestamp()));

    // Stop the unit for a consistent snapshot, if requested.
    if let Some(unit) = &args.stop_unit {
        systemctl("stop", unit).with_context(|| format!("stopping {unit}"))?;
    }

    // Archive + prune; capture the result so we ALWAYS restart the unit.
    let result: Result<usize> = (|| {
        archive(&args.source, &out)?;
        prune(&args.dest, &args.prefix, args.keep)
    })();

    if let Some(unit) = &args.stop_unit {
        if let Err(e) = systemctl("start", unit) {
            warn!(unit, error = %e, "failed to restart unit after backup");
        }
    }

    let removed = result?;
    info!(
        archive = %out.display(),
        pruned = removed,
        keep = args.keep,
        "backup complete"
    );
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prunes_oldest_beyond_keep() {
        let files = vec![
            "vw-20260601-000000.tar.gz".to_string(),
            "vw-20260603-000000.tar.gz".to_string(),
            "vw-20260602-000000.tar.gz".to_string(),
            "vw-20260605-000000.tar.gz".to_string(),
            "vw-20260604-000000.tar.gz".to_string(),
        ];
        let prune = to_prune(files, 2);
        // keep the 2 newest (06-05, 06-04); prune 06-03, 06-02, 06-01.
        assert_eq!(prune.len(), 3);
        assert!(prune.contains(&"vw-20260601-000000.tar.gz".to_string()));
        assert!(prune.contains(&"vw-20260602-000000.tar.gz".to_string()));
        assert!(prune.contains(&"vw-20260603-000000.tar.gz".to_string()));
        assert!(!prune.contains(&"vw-20260605-000000.tar.gz".to_string()));
        assert!(!prune.contains(&"vw-20260604-000000.tar.gz".to_string()));
    }

    #[test]
    fn keep_more_than_present_prunes_nothing() {
        let files = vec![
            "vw-20260601-000000.tar.gz".to_string(),
            "vw-20260602-000000.tar.gz".to_string(),
        ];
        assert!(to_prune(files, 7).is_empty());
    }

    #[test]
    fn timestamp_is_fixed_width() {
        let ts = timestamp();
        // YYYYmmdd-HHMMSS == 15 chars.
        assert_eq!(ts.len(), 15);
        assert_eq!(ts.as_bytes()[8], b'-');
        assert!(ts.chars().enumerate().all(|(i, c)| i == 8 || c.is_ascii_digit()));
    }
}
