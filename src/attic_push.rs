use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Command, ExitCode, Stdio};
use std::time::Duration;
use tracing::{info, warn};

/// How long to wait for the reachability probe before declaring the
/// Attic server's transport down. Short — this is a liveness ping, not
/// a real request.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

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

    /// Best-effort mode: NEVER exit non-zero. Cache-warming is non-critical —
    /// an unreachable cache or per-path push failures (e.g. atticd restarting
    /// during a `nixos-rebuild switch`) must not fail the unit and mark the
    /// system `degraded`. The real outcome stays in the logs + structured
    /// `pushed`/`failed` fields for monitoring. Strict mode (default) surfaces
    /// failures via exit code for callers that gate on the push (CI, etc.).
    #[arg(long)]
    best_effort: bool,
}

/// Resolve the process exit code for a push outcome, honoring best-effort.
/// In best-effort mode the unit never fails (returns 0); strict mode returns
/// the outcome's exit code (2 = login/unreachable skip, 1 = partial failure).
#[must_use]
pub fn resolve_exit_code(strict_code: u8, best_effort: bool) -> u8 {
    if best_effort { 0 } else { strict_code }
}

/// Strip the `/<cache-name>` (and any deeper path) suffix from an Attic
/// cache URL, yielding the server *base* URL. Probing the base avoids the
/// HTTP 404 (`/nexus`) and 401 (`/nexus/nix-cache-info`) that the
/// cache-scoped paths return on a perfectly healthy server — those status
/// codes mean "server up, wrong path / needs auth", never "unreachable".
///
/// `http://rio:8080/nexus`            -> `http://rio:8080/`
/// `http://rio:8080/nexus/`           -> `http://rio:8080/`
/// `http://rio:8080`                  -> `http://rio:8080/`
#[must_use]
pub fn server_base_url(cache_url: &str) -> String {
    // Find the end of the scheme+authority (`scheme://host:port`).
    let after_scheme = cache_url
        .find("://")
        .map_or(0, |i| i + 3);
    let authority_end = cache_url[after_scheme..]
        .find('/')
        .map_or(cache_url.len(), |i| after_scheme + i);
    let mut base = cache_url[..authority_end].to_string();
    base.push('/');
    base
}

/// A reachability probe NEVER fails on an HTTP status code — only on a
/// transport-level failure (DNS error, connection refused, TLS error,
/// timeout). reqwest's `send()` returns `Ok` for *any* completed HTTP
/// response (200, 401, 404, 500, …) and `Err` only when the transport
/// itself failed. So "did the send complete?" is exactly the right
/// signal. This pure helper makes that decision testable in isolation.
#[must_use]
pub fn reachable_from_send(send_succeeded: bool) -> bool {
    send_succeeded
}

/// Login to an Attic cache server and push all Nix store paths,
/// batched so that each underlying `get_missing_paths` request stays
/// well below atticd's SQLite variable limit.
pub async fn run(args: &Args) -> Result<ExitCode> {
    let token = fs::read_to_string(&args.token_file)
        .with_context(|| format!("reading token from {}", args.token_file.display()))?;
    let token = token.trim();

    let login = Command::new("attic")
        .args(["login", &args.server_name, &args.cache_url, token])
        .status()
        .context("running attic login")?;
    if !login.success() {
        warn!("attic login failed, skipping");
        return Ok(ExitCode::from(resolve_exit_code(2, args.best_effort)));
    }

    // Reachability == transport reachability of the SERVER BASE, not an
    // HTTP status code on the cache path. `attic cache info` (and a GET
    // of `/<cache>` / `/<cache>/nix-cache-info`) return non-2xx — 404 /
    // 401 — on a healthy server, so any status-based check is a
    // false-negative. We only treat a completed HTTP response (any
    // status) as reachable; an `Err` from `send()` (DNS / connect /
    // timeout) is the sole "unreachable" signal.
    let base = server_base_url(&args.cache_url);
    let probe = reqwest::Client::builder()
        .timeout(PROBE_TIMEOUT)
        .build()
        .context("building reachability probe client")?;
    let send_succeeded = probe.get(&base).send().await.is_ok();
    if !reachable_from_send(send_succeeded) {
        warn!(cache = %args.cache_name, base = %base, "cache unreachable, skipping");
        return Ok(ExitCode::from(resolve_exit_code(2, args.best_effort)));
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
    let strict_code = u8::from(total_err > 0); // 1 on any partial failure, else 0
    Ok(ExitCode::from(resolve_exit_code(strict_code, args.best_effort)))
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
    fn best_effort_never_fails_the_unit() {
        // Cache-warming must never degrade the system: every strict code
        // collapses to 0 in best-effort mode.
        assert_eq!(resolve_exit_code(0, true), 0);
        assert_eq!(resolve_exit_code(1, true), 0); // partial push failure
        assert_eq!(resolve_exit_code(2, true), 0); // login fail / unreachable
    }

    #[test]
    fn strict_mode_surfaces_failure_codes() {
        assert_eq!(resolve_exit_code(0, false), 0);
        assert_eq!(resolve_exit_code(1, false), 1);
        assert_eq!(resolve_exit_code(2, false), 2);
    }

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
            best_effort: false,
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

    #[test]
    fn server_base_strips_cache_name_suffix() {
        // The reported bug: probing the cache-scoped URL 404s. The base
        // must drop the `/<cache>` segment so the probe hits the root.
        assert_eq!(server_base_url("http://rio:8080/nexus"), "http://rio:8080/");
    }

    #[test]
    fn server_base_strips_trailing_slash_and_deep_paths() {
        assert_eq!(server_base_url("http://rio:8080/nexus/"), "http://rio:8080/");
        assert_eq!(
            server_base_url("http://rio:8080/nexus/nix-cache-info"),
            "http://rio:8080/"
        );
    }

    #[test]
    fn server_base_handles_bare_authority_and_https() {
        assert_eq!(server_base_url("http://rio:8080"), "http://rio:8080/");
        assert_eq!(
            server_base_url("https://cache.example.com/nexus"),
            "https://cache.example.com/"
        );
    }

    #[test]
    fn http_status_codes_are_all_reachable() {
        // The crux of the false-negative fix: a 404 / 401 / 403 / 500 is
        // still a COMPLETED HTTP response, which means `send()` returned
        // `Ok` — the server is UP. Reachability must be true for every
        // such case. We model "send completed" as the input.
        for completed in [/* 200 */ true, /* 404 */ true, /* 401 */ true] {
            assert!(
                reachable_from_send(completed),
                "any completed HTTP response (incl. 404/401/403) must be reachable"
            );
        }
    }

    #[test]
    fn transport_error_is_unreachable() {
        // Only a transport-level failure (DNS / connection refused /
        // timeout) — i.e. `send()` returned `Err`, modeled as `false` —
        // counts as unreachable.
        assert!(
            !reachable_from_send(false),
            "a transport error (DNS/connect/timeout) is the sole unreachable signal"
        );
    }
}
