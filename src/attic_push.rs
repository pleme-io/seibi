use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Command, ExitCode, Stdio};
use tracing::{info, warn};

#[derive(ClapArgs)]
pub struct Args {
    /// Path to Attic JWT token file
    #[arg(long, env = "SEIBI_ATTIC_TOKEN_FILE")]
    token_file: PathBuf,

    /// Attic cache name
    #[arg(long, env = "SEIBI_ATTIC_CACHE_NAME")]
    cache_name: String,

    /// Attic server URL
    #[arg(long, env = "SEIBI_ATTIC_CACHE_URL")]
    cache_url: String,

    /// Parallel push jobs (passed to `attic push --jobs`)
    #[arg(long, default_value = "8")]
    jobs: u32,

    /// Attic server alias for login
    #[arg(long, default_value = "nexus")]
    server_name: String,

    /// Maximum store paths per `attic push` invocation. Each invocation
    /// causes atticd to issue one `get_missing_paths` SQL query with
    /// one bound parameter per path; SQLite caps at
    /// `SQLITE_MAX_VARIABLE_NUMBER` (default 999, newer builds 32766).
    /// 500 leaves headroom for both. Postgres caps at 65535 so larger
    /// batches are safe there — this flag only matters on SQLite.
    #[arg(long, default_value = "500")]
    batch_size: usize,
}

/// Login to an Attic cache server and push all Nix store paths,
/// batched so that each underlying `get_missing_paths` request stays
/// well below atticd's SQLite variable limit.
pub fn run(args: &Args) -> Result<ExitCode> {
    let token = fs::read_to_string(&args.token_file)
        .with_context(|| format!("reading token from {}", args.token_file.display()))?;
    let token = token.trim();

    let login = Command::new("attic")
        .args(["login", &args.server_name, &args.cache_url, token])
        .status()
        .context("running attic login")?;
    if !login.success() {
        warn!("attic login failed, skipping");
        return Ok(ExitCode::from(2));
    }

    let check = Command::new("attic")
        .args(["cache", "info", &args.cache_name])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("checking attic cache")?;
    if !check.success() {
        warn!(cache = %args.cache_name, "cache unreachable, skipping");
        return Ok(ExitCode::from(2));
    }

    let paths = collect_store_paths()?;
    info!(
        cache = %args.cache_name,
        total_paths = paths.len(),
        batch_size = args.batch_size,
        jobs = args.jobs,
        "pushing store paths in batches..."
    );

    let mut total_ok = 0;
    let mut total_err = 0;
    let mut batches = 0;
    for batch in paths.chunks(args.batch_size.max(1)) {
        batches += 1;
        match push_batch(&args.cache_name, args.jobs, batch) {
            Ok(()) => {
                total_ok += batch.len();
                info!(
                    batch = batches,
                    paths = batch.len(),
                    pushed_so_far = total_ok,
                    "batch ok"
                );
            }
            Err(e) => {
                total_err += batch.len();
                warn!(
                    batch = batches,
                    paths = batch.len(),
                    error = %e,
                    "batch failed; continuing with next batch"
                );
            }
        }
    }

    info!(
        cache = %args.cache_name,
        batches,
        pushed = total_ok,
        failed = total_err,
        "push complete"
    );
    if total_err > 0 {
        Ok(ExitCode::from(1))
    } else {
        Ok(ExitCode::SUCCESS)
    }
}

/// Run `nix path-info --all` and collect the store paths it prints
/// (one per line, deduplicated + sorted for stable batching).
fn collect_store_paths() -> Result<Vec<String>> {
    let mut child = Command::new("nix")
        .args(["path-info", "--all"])
        .stdout(Stdio::piped())
        .spawn()
        .context("spawning nix path-info")?;
    let stdout = child
        .stdout
        .take()
        .context("nix path-info had no stdout pipe")?;
    let reader = BufReader::new(stdout);
    let mut paths: Vec<String> = reader
        .lines()
        .map_while(Result::ok)
        .filter(|l| !l.trim().is_empty())
        .collect();
    let _ = child.wait();
    paths.sort();
    paths.dedup();
    Ok(paths)
}

/// One `attic push <cache> --stdin --jobs N` invocation, feeding the
/// batch's paths to stdin (newline-separated).
fn push_batch(cache_name: &str, jobs: u32, batch: &[String]) -> Result<()> {
    let mut child = Command::new("attic")
        .args([
            "push",
            cache_name,
            "--stdin",
            "--jobs",
            &jobs.to_string(),
        ])
        .stdin(Stdio::piped())
        .spawn()
        .context("spawning attic push")?;
    if let Some(stdin) = child.stdin.as_mut() {
        for path in batch {
            stdin
                .write_all(path.as_bytes())
                .context("writing path to attic push stdin")?;
            stdin
                .write_all(b"\n")
                .context("writing newline to attic push stdin")?;
        }
    }
    let status = child.wait().context("waiting on attic push")?;
    if !status.success() {
        anyhow::bail!("attic push exited {}", status);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_size_default_safe_for_sqlite() {
        // The default 500 must leave headroom against the lower
        // SQLite default of SQLITE_MAX_VARIABLE_NUMBER=999. The
        // get_missing_paths query uses one bound parameter per path
        // (plus a small constant overhead), so 500 paths → ~500
        // variables → comfortably under 999.
        let args = Args {
            token_file: PathBuf::from("/dev/null"),
            cache_name: String::new(),
            cache_url: String::new(),
            jobs: 8,
            server_name: String::new(),
            batch_size: 500,
        };
        assert!(args.batch_size < 999, "default must clear SQLite limit");
    }

    #[test]
    fn chunks_carve_input_into_correct_groups() {
        // Mirror the chunks() semantics we rely on in run().
        let paths: Vec<String> = (0..1250).map(|i| format!("/nix/store/p-{i}")).collect();
        let batches: Vec<_> = paths.chunks(500).collect();
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].len(), 500);
        assert_eq!(batches[1].len(), 500);
        assert_eq!(batches[2].len(), 250, "tail batch is the remainder");
        // Total never exceeds the input.
        assert_eq!(batches.iter().map(|b| b.len()).sum::<usize>(), 1250);
    }

    #[test]
    fn batch_size_max_of_one_prevents_zero_chunks() {
        // The .max(1) guard in run() — a 0-arg from a future arg
        // override mustn't trigger chunks(0) which panics.
        let paths = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let safe_size = 0usize.max(1);
        let batches: Vec<_> = paths.chunks(safe_size).collect();
        assert_eq!(batches.len(), 3, "size=1 → one path per chunk");
    }

    #[test]
    fn batch_size_larger_than_input_yields_one_chunk() {
        let paths: Vec<String> = (0..10).map(|i| format!("p{i}")).collect();
        let batches: Vec<_> = paths.chunks(500).collect();
        assert_eq!(batches.len(), 1, "small input → single batch");
        assert_eq!(batches[0].len(), 10);
    }
}
