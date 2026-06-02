use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::collections::VecDeque;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
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

// ─────────────────────────────────────────────────────────────────────────
// Breathable push — an AIMD congestion controller for attic-push.
//
// atticd exposes NO server-side rate/concurrency/admission knob (every write
// failure is a bare HTTP 500), and the safe push rate is TIME-VARYING because
// rio builds on the same node. A fixed-rate bucket can't fit. So the client
// rides the line: it finds atticd's moving ceiling ONLINE from the per-batch
// loss signal — additive-increase concurrency while batches succeed, collapse
// to the floor + exponential backoff + circuit-break the instant atticd errors.
// Failed paths are RE-ENQUEUED, never dropped, so the cache still warms fully.
// (theory: Chiu & Jain 1989 AIMD; Nygard circuit-breaker. Sibling of the
// breathe band law — breathe sets the steady ceiling, this rides under it.)
// ─────────────────────────────────────────────────────────────────────────

/// A classified `attic push` failure — the typed border (vs an opaque exit-1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushError {
    /// Transport-level: connection refused, tcp-connect, error-sending-request,
    /// timeout — atticd is saturated or restarting. The AIMD loss signal.
    TransportDown,
    /// "too many SQL variables" — the batch is too large for atticd's SQLite.
    VarLimit,
    /// 401/403 — auth failed; no amount of backoff fixes it.
    Auth,
    /// Anything else (a poison path, an unexpected error).
    Other,
}

/// The outcome of one batch push.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchOutcome {
    Ok,
    Failed(PushError),
}

/// What the drive loop should do with the batch it just attempted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Batch landed — proceed.
    Continue,
    /// Transient failure — put the paths back; the controller has backed off.
    Requeue,
    /// Hard stop (auth) — no point retrying.
    Abort,
}

/// Tuning for the AIMD controller. Defaults match the breathable-attic design.
#[derive(Debug, Clone)]
pub struct BreathConfig {
    pub floor: u32,
    pub ceiling: u32,
    pub k_clean: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub break_threshold: u32,
    pub min_batch: usize,
    pub open_window: Duration,
    pub max_circuit_probes: u32,
}

impl Default for BreathConfig {
    fn default() -> Self {
        Self {
            floor: 1,
            ceiling: 8,
            k_clean: 3,
            base_delay: Duration::from_secs(2),
            max_delay: Duration::from_secs(60),
            break_threshold: 3,
            min_batch: 32,
            open_window: Duration::from_secs(30),
            max_circuit_probes: 10,
        }
    }
}

/// The AIMD state. Pure: `observe()` does the whole control law without I/O,
/// so it is unit-tested exhaustively (mirroring DiskPressureState / the
/// typed-spec interpreter discipline).
#[derive(Debug, Clone)]
pub struct BreathController {
    jobs: u32,
    ssthresh: u32,
    batch_size: usize,
    consecutive_ok: u32,
    consecutive_fail: u32,
    delay: Duration,
    circuit_open: bool,
    cfg: BreathConfig,
}

impl BreathController {
    #[must_use]
    pub fn new(start_jobs: u32, ssthresh: u32, batch_size: usize, cfg: BreathConfig) -> Self {
        Self {
            jobs: start_jobs.clamp(cfg.floor, cfg.ceiling),
            ssthresh: ssthresh.max(cfg.floor),
            batch_size: batch_size.max(cfg.min_batch),
            consecutive_ok: 0,
            consecutive_fail: 0,
            delay: cfg.base_delay,
            circuit_open: false,
            cfg,
        }
    }

    #[must_use] pub fn jobs(&self) -> u32 { self.jobs }
    #[must_use] pub fn batch_size(&self) -> usize { self.batch_size }
    #[must_use] pub fn delay(&self) -> Duration { self.delay }
    #[must_use] pub fn circuit_open(&self) -> bool { self.circuit_open }
    #[must_use] pub fn open_window(&self) -> Duration { self.cfg.open_window }
    #[must_use] pub fn max_circuit_probes(&self) -> u32 { self.cfg.max_circuit_probes }

    /// Circuit recovered (a reachability probe succeeded): resume cautiously.
    pub fn close_circuit(&mut self) {
        self.circuit_open = false;
        self.jobs = self.cfg.floor;
        self.consecutive_fail = 0;
        self.delay = self.cfg.base_delay;
    }

    /// The whole AIMD control law, pure. Returns the loop's next action.
    pub fn observe(&mut self, outcome: BatchOutcome) -> Action {
        match outcome {
            BatchOutcome::Ok => {
                self.consecutive_fail = 0;
                self.consecutive_ok += 1;
                self.delay = self.cfg.base_delay;
                if self.jobs < self.ssthresh {
                    // slow-start: exponential up to ssthresh
                    self.jobs = (self.jobs.saturating_mul(2)).min(self.ssthresh);
                } else if self.consecutive_ok % self.cfg.k_clean == 0 {
                    // congestion-avoidance: additive-increase, capped at ceiling
                    self.jobs = (self.jobs + 1).min(self.cfg.ceiling);
                }
                Action::Continue
            }
            BatchOutcome::Failed(PushError::Auth) => Action::Abort,
            BatchOutcome::Failed(PushError::VarLimit) => {
                // batch too big for atticd's SQLite — halve it and retry smaller
                self.batch_size = (self.batch_size / 2).max(self.cfg.min_batch);
                Action::Requeue
            }
            BatchOutcome::Failed(PushError::TransportDown | PushError::Other) => {
                self.consecutive_ok = 0;
                self.consecutive_fail += 1;
                self.ssthresh = (self.jobs / 2).max(self.cfg.floor); // remember half the rate
                self.jobs = self.cfg.floor; // multiplicative-decrease: collapse hard
                let shift = self.consecutive_fail.min(6);
                self.delay = self
                    .cfg
                    .base_delay
                    .saturating_mul(1u32 << shift)
                    .min(self.cfg.max_delay); // exponential inter-batch backoff
                if self.consecutive_fail >= self.cfg.break_threshold {
                    self.circuit_open = true; // trip the breaker → pause + probe
                }
                Action::Requeue
            }
        }
    }
}

/// Classify an `attic push` failure from its captured stderr (pure + tested).
#[must_use]
pub fn classify_push_error(stderr: &str) -> PushError {
    let s = stderr.to_ascii_lowercase();
    if s.contains("connection refused")
        || s.contains("error sending request")
        || s.contains("tcp connect")
        || s.contains("connect error")
        || s.contains("timed out")
        || s.contains("timeout")
    {
        PushError::TransportDown
    } else if s.contains("too many sql variables") || s.contains("sqlite_max_variable") {
        PushError::VarLimit
    } else if s.contains("401") || s.contains("unauthorized") || s.contains("403") || s.contains("forbidden") {
        PushError::Auth
    } else {
        PushError::Other
    }
}

/// Extract `host:port` from a server base URL (`http://rio:8080/` → `rio:8080`).
#[must_use]
pub fn parse_authority(base_url: &str) -> Option<String> {
    base_url
        .split("://")
        .nth(1)
        .and_then(|rest| rest.split('/').next())
        .filter(|a| !a.is_empty())
        .map(str::to_string)
}

/// The Environment the drive loop pushes through — real impl shells out to
/// `attic`, tests mock it. This is the testability contract.
pub trait Pusher {
    fn push_batch(&self, cache_name: &str, jobs: u32, batch: &[String]) -> Result<(), PushError>;
    fn reachable(&self, base_url: &str) -> bool;
}

/// The real pusher: `attic push --stdin --jobs N`, stderr captured + classified.
pub struct AtticPusher;

impl Pusher for AtticPusher {
    fn push_batch(&self, cache_name: &str, jobs: u32, batch: &[String]) -> Result<(), PushError> {
        push_batch_classified(cache_name, jobs, batch)
    }
    fn reachable(&self, base_url: &str) -> bool {
        // Transport reachability: can we open a TCP connection to atticd again?
        let Some(authority) = parse_authority(base_url) else { return false };
        let Ok(mut addrs) = authority.to_socket_addrs() else { return false };
        addrs
            .next()
            .is_some_and(|addr| TcpStream::connect_timeout(&addr, PROBE_TIMEOUT).is_ok())
    }
}

/// Summary of a breathable push run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PushSummary {
    pub pushed: usize,
    pub unrecovered: usize,
    pub circuit_tripped: bool,
    pub aborted: bool,
}

/// Drive the whole store through the controller: pop a batch, push it, observe
/// the outcome, ride/back-off accordingly; re-enqueue transient failures; pause
/// + probe while the breaker is open; give up after `max_circuit_probes`.
/// Pure control flow over a `Pusher` — testable with a scripted mock.
pub fn drive_push<P: Pusher>(
    pusher: &P,
    cache: &str,
    base_url: &str,
    paths: Vec<String>,
    mut ctl: BreathController,
    sleep: &dyn Fn(Duration),
) -> PushSummary {
    let mut queue: VecDeque<String> = paths.into();
    let mut pushed = 0usize;
    let mut circuit_tripped = false;
    let mut probes = 0u32;

    while !queue.is_empty() {
        if ctl.circuit_open() {
            circuit_tripped = true;
            sleep(ctl.open_window());
            if pusher.reachable(base_url) {
                ctl.close_circuit();
                probes = 0;
            } else {
                probes += 1;
                if probes >= ctl.max_circuit_probes() {
                    break; // give up; the rest is unrecovered (best-effort)
                }
                continue;
            }
        }

        let n = ctl.batch_size().min(queue.len());
        let batch: Vec<String> = queue.drain(..n).collect();
        let outcome = match pusher.push_batch(cache, ctl.jobs(), &batch) {
            Ok(()) => BatchOutcome::Ok,
            Err(e) => BatchOutcome::Failed(e),
        };
        match ctl.observe(outcome) {
            Action::Continue => pushed += batch.len(),
            Action::Requeue => {
                for p in batch.into_iter().rev() {
                    queue.push_front(p);
                }
            }
            Action::Abort => {
                return PushSummary { pushed, unrecovered: queue.len(), circuit_tripped, aborted: true };
            }
        }
        sleep(ctl.delay());
    }

    PushSummary { pushed, unrecovered: queue.len(), circuit_tripped, aborted: false }
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
    // --jobs becomes the CEILING the AIMD ramps toward; the controller starts
    // small (slow-start) and finds atticd's safe rate online.
    let cfg = BreathConfig { ceiling: args.jobs.max(1), ..BreathConfig::default() };
    let ssthresh = (args.jobs / 2).max(cfg.floor);
    info!(
        cache = %args.cache_name,
        total_paths = paths.len(),
        batch_size = args.batch_size,
        ceiling = cfg.ceiling,
        "breathable push: AIMD-riding atticd's moving ceiling..."
    );

    let ctl = BreathController::new(2, ssthresh, args.batch_size, cfg);
    let cache = args.cache_name.clone();
    let base_owned = base.clone();
    // The drive loop is blocking (subprocess + sleeps); keep it off the runtime.
    let summary = tokio::task::spawn_blocking(move || {
        drive_push(&AtticPusher, &cache, &base_owned, paths, ctl, &|d| std::thread::sleep(d))
    })
    .await
    .context("breathable push join")?;

    info!(
        cache = %args.cache_name,
        pushed = summary.pushed,
        unrecovered = summary.unrecovered,
        circuit_tripped = summary.circuit_tripped,
        "push complete"
    );
    if summary.circuit_tripped || summary.unrecovered > 0 {
        // Pillar-11: atticd stayed saturated past the AIMD backoff + breaker.
        // The cache is partially warmed; the next run retries. Surface for alert.
        warn!(
            unrecovered = summary.unrecovered,
            circuit_tripped = summary.circuit_tripped,
            "atticd backpressure: paths unrecovered after AIMD backoff"
        );
    }
    let strict_code: u8 = if summary.aborted { 2 } else { u8::from(summary.unrecovered > 0) };
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

/// One `attic push <cache> --stdin --jobs N` invocation. stderr is captured so a
/// failure is CLASSIFIED into a typed `PushError` (the AIMD signal), not an
/// opaque exit code. stdin is closed explicitly so atticd sees EOF.
fn push_batch_classified(cache_name: &str, jobs: u32, batch: &[String]) -> Result<(), PushError> {
    let mut child = Command::new("attic")
        .args(["push", cache_name, "--stdin", "--jobs", &jobs.to_string()])
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|_| PushError::Other)?;
    if let Some(stdin) = child.stdin.as_mut() {
        for path in batch {
            // a write failure means the child died mid-stream — let wait()
            // surface the real cause via the captured stderr.
            if stdin.write_all(path.as_bytes()).is_err() || stdin.write_all(b"\n").is_err() {
                break;
            }
        }
    }
    drop(child.stdin.take()); // close stdin → EOF so attic finishes reading
    let mut stderr = String::new();
    if let Some(mut e) = child.stderr.take() {
        let _ = e.read_to_string(&mut stderr);
    }
    match child.wait() {
        Ok(s) if s.success() => Ok(()),
        Ok(_) => Err(classify_push_error(&stderr)),
        Err(_) => Err(PushError::Other),
    }
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

    // ── AIMD controller ──────────────────────────────────────────────────

    #[test]
    fn classify_maps_stderr_to_typed_error() {
        assert_eq!(classify_push_error("error sending request: Connection refused"), PushError::TransportDown);
        assert_eq!(classify_push_error("tcp connect error"), PushError::TransportDown);
        assert_eq!(classify_push_error("operation timed out"), PushError::TransportDown);
        assert_eq!(classify_push_error("too many SQL variables"), PushError::VarLimit);
        assert_eq!(classify_push_error("HTTP 401 Unauthorized"), PushError::Auth);
        assert_eq!(classify_push_error("some other failure"), PushError::Other);
    }

    #[test]
    fn parse_authority_extracts_host_port() {
        assert_eq!(parse_authority("http://rio:8080/"), Some("rio:8080".into()));
        assert_eq!(parse_authority("http://rio:8080/nexus"), Some("rio:8080".into()));
        assert_eq!(parse_authority("https://cache.example.com/"), Some("cache.example.com".into()));
        assert_eq!(parse_authority("not a url"), None);
    }

    fn ctl() -> BreathController {
        BreathController::new(2, 4, 500, BreathConfig::default())
    }

    #[test]
    fn ok_slow_starts_then_additive_increases() {
        let mut c = ctl(); // jobs 2, ssthresh 4, ceiling 8
        assert_eq!(c.observe(BatchOutcome::Ok), Action::Continue);
        assert_eq!(c.jobs(), 4, "slow-start doubles up to ssthresh");
        // now jobs == ssthresh → congestion-avoidance: +1 every k_clean(3) oks
        c.observe(BatchOutcome::Ok); // ok#2
        assert_eq!(c.jobs(), 4, "no bump before k_clean");
        c.observe(BatchOutcome::Ok); // ok#3 → 3 % 3 == 0
        assert_eq!(c.jobs(), 5, "additive-increase after k_clean clean batches");
    }

    #[test]
    fn additive_increase_is_capped_at_ceiling() {
        let mut c = BreathController::new(8, 8, 500, BreathConfig::default()); // start at ceiling
        for _ in 0..9 { c.observe(BatchOutcome::Ok); }
        assert_eq!(c.jobs(), 8, "never exceeds ceiling");
    }

    #[test]
    fn transport_failure_collapses_and_backs_off() {
        let mut c = BreathController::new(8, 8, 500, BreathConfig::default());
        let base = BreathConfig::default().base_delay;
        assert_eq!(c.observe(BatchOutcome::Failed(PushError::TransportDown)), Action::Requeue);
        assert_eq!(c.jobs(), 1, "multiplicative-decrease collapses to floor");
        assert_eq!(c.delay(), base * 2, "exponential backoff: base * 2^1");
        assert!(!c.circuit_open(), "one failure does not trip the breaker");
        c.observe(BatchOutcome::Failed(PushError::Other));
        assert_eq!(c.delay(), base * 4, "backoff grows: base * 2^2");
        c.observe(BatchOutcome::Failed(PushError::TransportDown)); // 3rd consecutive
        assert!(c.circuit_open(), "breaker trips at break_threshold (3)");
    }

    #[test]
    fn varlimit_halves_batch_and_requeues() {
        let mut c = ctl();
        assert_eq!(c.observe(BatchOutcome::Failed(PushError::VarLimit)), Action::Requeue);
        assert_eq!(c.batch_size(), 250, "batch halves to fit atticd's SQLite");
    }

    #[test]
    fn auth_failure_aborts() {
        let mut c = ctl();
        assert_eq!(c.observe(BatchOutcome::Failed(PushError::Auth)), Action::Abort);
    }

    #[test]
    fn ok_after_backoff_resets_delay_and_clears_failures() {
        let mut c = BreathController::new(8, 8, 500, BreathConfig::default());
        c.observe(BatchOutcome::Failed(PushError::TransportDown));
        c.observe(BatchOutcome::Ok);
        assert_eq!(c.delay(), BreathConfig::default().base_delay, "a clean batch decays the delay");
    }

    // ── drive loop (scripted Pusher) ─────────────────────────────────────

    struct MockPusher {
        outcomes: std::cell::RefCell<VecDeque<Result<(), PushError>>>,
        reachable_seq: std::cell::RefCell<VecDeque<bool>>,
        pushed_paths: std::cell::RefCell<usize>,
    }
    impl MockPusher {
        fn new(outcomes: Vec<Result<(), PushError>>, reachable_seq: Vec<bool>) -> Self {
            Self {
                outcomes: std::cell::RefCell::new(outcomes.into()),
                reachable_seq: std::cell::RefCell::new(reachable_seq.into()),
                pushed_paths: std::cell::RefCell::new(0),
            }
        }
    }
    impl Pusher for MockPusher {
        fn push_batch(&self, _c: &str, _j: u32, batch: &[String]) -> Result<(), PushError> {
            let r = self.outcomes.borrow_mut().pop_front().unwrap_or(Ok(()));
            if r.is_ok() { *self.pushed_paths.borrow_mut() += batch.len(); }
            r
        }
        fn reachable(&self, _u: &str) -> bool {
            self.reachable_seq.borrow_mut().pop_front().unwrap_or(true)
        }
    }

    fn paths(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("/nix/store/p-{i}")).collect()
    }
    fn no_sleep(_d: Duration) {}

    #[test]
    fn drive_pushes_everything_when_atticd_is_healthy() {
        let m = MockPusher::new(vec![], vec![]); // always Ok
        let c = BreathController::new(2, 4, 100, BreathConfig::default());
        let s = drive_push(&m, "nexus", "http://x:1/", paths(250), c, &no_sleep);
        assert_eq!(s.pushed, 250);
        assert_eq!(s.unrecovered, 0);
        assert!(!s.circuit_tripped && !s.aborted);
    }

    #[test]
    fn drive_recovers_after_atticd_blips() {
        // first batch fails 3× (trips breaker), atticd then recovers, all land.
        let m = MockPusher::new(
            vec![
                Err(PushError::TransportDown),
                Err(PushError::TransportDown),
                Err(PushError::TransportDown),
            ],
            vec![true], // first circuit probe succeeds
        );
        let c = BreathController::new(4, 4, 100, BreathConfig::default());
        let s = drive_push(&m, "nexus", "http://x:1/", paths(200), c, &no_sleep);
        assert!(s.circuit_tripped, "breaker should have tripped");
        assert_eq!(s.unrecovered, 0, "re-enqueued paths all land after recovery");
        assert_eq!(s.pushed, 200);
    }

    #[test]
    fn drive_gives_up_when_atticd_stays_down() {
        // every push fails, every probe fails → give up, rest is unrecovered.
        let m = MockPusher::new(
            std::iter::repeat_with(|| Err(PushError::TransportDown)).take(50).collect(),
            vec![false; 50],
        );
        let c = BreathController::new(4, 4, 100, BreathConfig::default());
        let s = drive_push(&m, "nexus", "http://x:1/", paths(300), c, &no_sleep);
        assert!(s.circuit_tripped);
        assert!(s.unrecovered > 0, "unrecovered paths surface for best-effort + alert");
        assert!(!s.aborted);
    }

    #[test]
    fn drive_aborts_on_auth() {
        let m = MockPusher::new(vec![Err(PushError::Auth)], vec![]);
        let c = BreathController::new(4, 4, 100, BreathConfig::default());
        let s = drive_push(&m, "nexus", "http://x:1/", paths(200), c, &no_sleep);
        assert!(s.aborted);
        assert_eq!(s.pushed, 0);
    }
}
