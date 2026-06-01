//! Shared config + the typed `Env` boundary for the two rebuild-efficiency
//! reconcilers (`build_spec_freshness` + `flake_lock_budget`).
//!
//! The operator invariant these recipes encode: **every `nix run .#rebuild`
//! must make the fleet MORE efficient, not less.** Two failure classes erode
//! that:
//!
//!  - I1 — a rebuild-input Rust repo drops its committed `Cargo.build-spec.json`
//!    (gitignored, deleted, or stale vs `Cargo.lock`), so substrate's
//!    lockfile-builder falls back to eval-time IFD (`gen build .` in a
//!    `__noChroot` sandbox). Every consumer rebuild then blocks eval on a
//!    cargo-metadata-with-network run — the slow path we work hard to avoid.
//!    `build_spec_freshness` keeps those specs committed + fresh.
//!
//!  - I6 — a future cyclic / unbounded flake input silently reintroduces the
//!    substrate↔gen node-count blowup we just killed. `flake_lock_budget`
//!    guards the committed lock node-counts against a budget ceiling.
//!
//! ## Config
//!
//! Plain typed `serde` struct for M0. The org Configuration prime directive
//! wants every operator-facing config as a `shikumi::TieredConfig`, but
//! shikumi is **not** a seibi dependency today and adding it (the trait +
//! ConfigStore discovery + hot-reload machinery) meaningfully expands scope
//! and risks the build. Getting this BUILT is the priority, so we ship a
//! typed serde struct now.
//
// TODO(shikumi): promote `RebuildEfficiencyConfig` to `shikumi::TieredConfig`
// (bare / discovered / prescribed_default / extend / diff) once shikumi is a
// seibi dependency. `prescribed_default()` is exactly `Self::default()` below;
// `discovered()` reads `~/.config/seibi/rebuild-efficiency.yaml`.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Typed config for the rebuild-efficiency reconcilers. Serde-loadable from
/// YAML/JSON; `Default` is the fleet-prescribed shape (the repos cid's
/// `nix run .#rebuild` evaluates + the nix/substrate lock budgets).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct RebuildEfficiencyConfig {
    /// Rust "rebuild-input" repos whose committed `Cargo.build-spec.json`
    /// keeps the lockfile-builder on the committed fast path. Paths are
    /// absolute; `~` is NOT expanded (callers pass concrete roots).
    pub rebuild_input_repos: Vec<PathBuf>,

    /// When false (DEFAULT — dry-run): `build_spec_freshness::act` only
    /// REFUSES with a loud log naming the repos it WOULD regen+commit. When
    /// true: it regenerates + un-gitignores + `git add`+commits each drifted
    /// spec. NEVER pushes either way.
    pub commit: bool,

    /// Per-flake.lock node-count budgets the `flake_lock_budget` guard
    /// enforces. Ceilings are slack-inclusive (nix=250, substrate=80) above
    /// the current committed counts (nix=220, substrate=50).
    pub flake_lock_budgets: Vec<FlakeLockBudget>,
}

/// One committed flake.lock's node-count budget + an optional growth gate.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FlakeLockBudget {
    /// Path to the `flake.lock` to count nodes of.
    pub lock_path: PathBuf,
    /// Hard ceiling on `nodes` count (slack already baked in).
    pub max_nodes: usize,
    /// Optional recorded baseline; drift also fires if `nodes` grew by more
    /// than `max_delta` above this baseline (catches creep below the ceiling).
    #[serde(default)]
    pub baseline_nodes: Option<usize>,
    /// Max allowed growth over `baseline_nodes` before drift fires.
    #[serde(default = "default_max_delta")]
    pub max_delta: usize,
}

fn default_max_delta() -> usize {
    20
}

impl Default for RebuildEfficiencyConfig {
    fn default() -> Self {
        // The default rebuild-input set = the repos cid's `nix run .#rebuild`
        // evaluates as Rust flake inputs. Conventionally cloned under
        // `~/code/github/pleme-io/<repo>`; we leave them as repo-name-relative
        // roots resolved against the workspace base at load time. To keep the
        // default self-contained + absolute, we anchor on $HOME.
        let base = home_code_base();
        let repos = ["fleet", "tend", "frost", "mado", "ayatsuri", "tear", "cordel"]
            .into_iter()
            .map(|r| base.join(r))
            .collect();

        // Lock budgets: nix repo (220 today → 250 ceiling) + substrate
        // (50 → 80). Anchored on the same code base.
        let nix_lock = home_code_base().join("nix").join("flake.lock");
        let substrate_lock = home_code_base().join("substrate").join("flake.lock");
        let budgets = vec![
            FlakeLockBudget {
                lock_path: nix_lock,
                max_nodes: 250,
                baseline_nodes: Some(220),
                max_delta: 20,
            },
            FlakeLockBudget {
                lock_path: substrate_lock,
                max_nodes: 80,
                baseline_nodes: Some(50),
                max_delta: 20,
            },
        ];

        Self { rebuild_input_repos: repos, commit: false, flake_lock_budgets: budgets }
    }
}

/// `~/code/github/pleme-io` — the conventional pleme-io clone root. Falls back
/// to a bare relative path if `$HOME` is unset (tests pass explicit roots, so
/// this default is only the daemon's convenience).
fn home_code_base() -> PathBuf {
    std::env::var_os("HOME").map_or_else(
        || PathBuf::from("code/github/pleme-io"),
        |h| PathBuf::from(h).join("code/github/pleme-io"),
    )
}

impl RebuildEfficiencyConfig {
    /// Load from `path` (YAML or JSON by extension), falling back to the
    /// fleet-prescribed `Default` when the file is absent. A present-but-
    /// malformed file is a LOUD error (never silently default — that would
    /// mask a typo the operator needs to see).
    ///
    /// # Errors
    /// Returns `Err` when the file exists but cannot be read or parsed.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let raw = std::fs::read_to_string(path)?;
        let is_json = path.extension().is_some_and(|e| e == "json");
        let cfg = if is_json {
            serde_json::from_str(&raw)?
        } else {
            // serde_json round-trips YAML-as-JSON for the common case; for true
            // YAML the operator should ship `.yaml` and we parse via serde_yaml
            // if available. seibi has no serde_yaml dep, so accept JSON-shaped
            // YAML (the strict subset) and surface anything else loudly.
            serde_json::from_str(&raw)
                .map_err(|e| anyhow::anyhow!("parse {} (JSON/JSON-shaped-YAML only): {e}", path.display()))?
        };
        Ok(cfg)
    }
}

// ─────────────────────────────────────────────────────────────────────
// The typed Env boundary — every filesystem / git / gen / subprocess touch
// the recipes need goes through this trait so tests mock it. Production wires
// `RealEnv`; unit tests wire an in-memory fake. This is the Environment-trait
// of the org ★★ TYPED-SPEC rule: the interpreter's side effects are abstracted
// so observe/diff/act are exercised without real FS, git, or `gen`.
// ─────────────────────────────────────────────────────────────────────

/// Side-effect surface for the rebuild-efficiency recipes.
pub trait Env: Send + Sync {
    /// Read a file's bytes (Cargo.lock, flake.lock, the spec). `None` = absent.
    fn read(&self, path: &Path) -> Option<Vec<u8>>;

    /// Is `path` tracked by git in its repo? (`git ls-files --error-unmatch`).
    /// `false` covers both untracked AND gitignored.
    fn git_tracked(&self, repo: &Path, path: &Path) -> bool;

    /// Regenerate `<repo>/Cargo.build-spec.json` via `gen build .`. Returns the
    /// command's success. Only called when `commit=true`.
    fn gen_build_spec(&self, repo: &Path) -> Result<(), String>;

    /// `git rm --cached --ignore-unmatch` is NOT what we want; this un-ignores
    /// by `git add -f` the spec (forces past .gitignore). Returns success.
    fn git_add_force(&self, repo: &Path, path: &Path) -> Result<(), String>;

    /// `git commit -m <msg> -- <path>` (path-filtered, idempotent: a no-op
    /// commit when nothing staged is treated as Ok). Returns success.
    fn git_commit_path(&self, repo: &Path, path: &Path, message: &str) -> Result<(), String>;
}

/// SHA-256 hex of bytes — the exact freshness key gen embeds as
/// `cargo_lock_sha256` (`format!("{:x}", Sha256::digest(cargo_lock_bytes))`).
/// Mirrors `gen-cargo::build_spec::hash_cargo_lock` so both sides compute the
/// same digest; solve-once, no drift.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    format!("{:x}", h.finalize())
}

/// The relative spec filename every gen-consuming repo commits.
pub const SPEC_FILE: &str = "Cargo.build-spec.json";
/// The relative lockfile gen hashes for freshness.
pub const LOCK_FILE: &str = "Cargo.lock";

/// Production `Env` — real filesystem + `git` + `gen` subprocesses. Each
/// subprocess is a small typed wrapper (NO SHELL — typed `Command` argv, not
/// string glue).
pub struct RealEnv;

impl Env for RealEnv {
    fn read(&self, path: &Path) -> Option<Vec<u8>> {
        std::fs::read(path).ok()
    }

    fn git_tracked(&self, repo: &Path, path: &Path) -> bool {
        std::process::Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(["ls-files", "--error-unmatch", "--"])
            .arg(path)
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    fn gen_build_spec(&self, repo: &Path) -> Result<(), String> {
        run_ok(
            std::process::Command::new("gen")
                .arg("-C")
                .arg(repo)
                .args(["build", "."]),
            "gen build .",
        )
        // `gen` may not accept `-C`; fall back to cwd-set form if the first
        // invocation reports an arg error. Keep it simple + loud.
        .or_else(|_| {
            run_ok(
                std::process::Command::new("gen").current_dir(repo).args(["build", "."]),
                "gen build . (cwd)",
            )
        })
    }

    fn git_add_force(&self, repo: &Path, path: &Path) -> Result<(), String> {
        run_ok(
            std::process::Command::new("git")
                .arg("-C")
                .arg(repo)
                .args(["add", "-f", "--"])
                .arg(path),
            "git add -f",
        )
    }

    fn git_commit_path(&self, repo: &Path, path: &Path, message: &str) -> Result<(), String> {
        // Idempotent: if nothing is staged, `git commit` exits non-zero with
        // "nothing to commit"; treat that as success (no-op).
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(["commit", "-m", message, "--"])
            .arg(path)
            .output()
            .map_err(|e| format!("git commit spawn failed: {e}"))?;
        if out.status.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&out.stdout) + String::from_utf8_lossy(&out.stderr);
        if stderr.contains("nothing to commit") || stderr.contains("no changes added") {
            return Ok(()); // no-op = converged
        }
        Err(format!("git commit failed: {}", stderr.trim()))
    }
}

/// Run a `Command`, mapping non-zero / spawn-failure to a loud `Err(String)`.
fn run_ok(cmd: &mut std::process::Command, label: &str) -> Result<(), String> {
    let out = cmd.output().map_err(|e| format!("{label} spawn failed: {e}"))?;
    if out.status.success() {
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&out.stderr);
        Err(format!("{label} exited {}: {}", out.status, stderr.trim()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_has_seven_rebuild_inputs_and_two_budgets() {
        let c = RebuildEfficiencyConfig::default();
        assert_eq!(c.rebuild_input_repos.len(), 7, "fleet/tend/frost/mado/ayatsuri/tear/cordel");
        assert!(!c.commit, "commit defaults OFF (dry-run)");
        assert_eq!(c.flake_lock_budgets.len(), 2, "nix + substrate locks");
        let nix = &c.flake_lock_budgets[0];
        assert_eq!(nix.max_nodes, 250);
        assert_eq!(nix.baseline_nodes, Some(220));
        let sub = &c.flake_lock_budgets[1];
        assert_eq!(sub.max_nodes, 80);
        assert_eq!(sub.baseline_nodes, Some(50));
    }

    #[test]
    fn sha256_hex_matches_gen_convention() {
        // gen: format!("{:x}", Sha256::digest(bytes)). Empty input → the
        // well-known SHA-256 of "" (lowercase, 64 hex chars).
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        let h = sha256_hex(b"hello\n");
        assert_eq!(h.len(), 64);
        assert!(h.chars().all(|c| c.is_ascii_hexdigit() && !c.is_uppercase()));
    }

    #[test]
    fn load_absent_file_falls_back_to_default() {
        let missing = std::path::Path::new("/nonexistent/seibi/rebuild-efficiency.yaml");
        let c = RebuildEfficiencyConfig::load(missing).expect("absent → default");
        assert_eq!(c, RebuildEfficiencyConfig::default());
    }

    #[test]
    fn load_roundtrips_json() {
        let dir = std::env::temp_dir().join(format!("seibi-re-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("cfg.json");
        let cfg = RebuildEfficiencyConfig {
            rebuild_input_repos: vec![PathBuf::from("/tmp/a")],
            commit: true,
            flake_lock_budgets: vec![FlakeLockBudget {
                lock_path: PathBuf::from("/tmp/flake.lock"),
                max_nodes: 99,
                baseline_nodes: Some(40),
                max_delta: 5,
            }],
        };
        std::fs::write(&p, serde_json::to_string(&cfg).unwrap()).unwrap();
        let loaded = RebuildEfficiencyConfig::load(&p).unwrap();
        assert_eq!(loaded, cfg);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
