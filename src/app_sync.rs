//! `app-sync` — heal macOS launch references for nix-managed `.app` bundles.
//!
//! home-manager symlinks `~/Applications/Home Manager Apps/<App>.app` →
//! `/nix/store/<hash>-…/Applications/<App>.app`. When the macOS Dock pins an
//! app it follows that symlink and caches the *resolved* store path (both a
//! `_CFURLString` and a binary security-scoped `book` bookmark). A later
//! `nix-collect-garbage` deletes that store path, so the cached reference
//! dangles and clicking the Dock icon bounces-and-fails — even though the
//! app itself is healthy and the stable symlink still resolves to the new
//! generation's bundle.
//!
//! This verb is the reconciler for that drift. It rewrites every Dock
//! `persistent-apps` entry whose target path no longer exists back to the
//! stable symlink under `--source`, strips the stale `book` bookmark (and the
//! cached mod-dates) so macOS re-resolves by path, re-registers the live
//! bundles with `LaunchServices`, then restarts the Dock. Entries whose path
//! still exists (system apps like Activity Monitor) are left untouched, so
//! the operation is idempotent and safe to run on every home-manager switch.
//!
//! Pairs with `spotlight-sync` (which makes the same bundles discoverable via
//! Spotlight): that verb owns *findability*, this verb owns *launchability*.

use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use plist::Value;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use tracing::{info, warn};

/// `lsregister` lives deep inside the `LaunchServices` framework.
const LSREGISTER: &str = "/System/Library/Frameworks/CoreServices.framework\
    /Versions/A/Frameworks/LaunchServices.framework\
    /Versions/A/Support/lsregister";

/// Characters macOS percent-encodes inside a `_CFURLString` path. `/` is left
/// alone so it keeps working as the path separator (matches how macOS itself
/// writes e.g. `Activity%20Monitor.app`).
const URL_PATH: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

#[derive(ClapArgs)]
pub struct Args {
    /// Directory of nix-managed `.app` symlinks (stable paths) used to
    /// re-target dangling Dock entries.
    #[arg(long, default_value = "~/Applications/Home Manager Apps")]
    source: String,

    /// Report intended changes without writing the Dock plist, touching
    /// `LaunchServices`, or restarting the Dock.
    #[arg(long)]
    dry_run: bool,

    /// Skip re-registering the live bundles with `LaunchServices`.
    #[arg(long)]
    no_register: bool,

    /// Skip the `cfprefsd`/`Dock` restart after rewriting the plist.
    #[arg(long)]
    no_restart_dock: bool,
}

/// A planned re-target of one Dock entry from a dead path to a stable one.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Retarget {
    app_name: String,
    old_url: String,
    new_url: String,
}

pub async fn run(args: Args) -> Result<ExitCode> {
    let home = std::env::var("HOME").context("HOME not set")?;
    let source = crate::common::expand_tilde(&args.source, &home);
    let dock_plist = PathBuf::from(&home).join("Library/Preferences/com.apple.dock.plist");

    let exists = |p: &Path| p.exists();

    // ── Dock reconcile ──────────────────────────────────────────────
    let changes = if dock_plist.exists() {
        reconcile_dock(&dock_plist, &source, args.dry_run, &exists)
            .with_context(|| format!("reconciling {}", dock_plist.display()))?
    } else {
        warn!(plist = %dock_plist.display(), "no Dock plist — skipping Dock reconcile");
        Vec::new()
    };

    for c in &changes {
        info!(app = %c.app_name, from = %c.old_url, to = %c.new_url, "re-targeted Dock entry");
    }

    // ── LaunchServices reconcile ────────────────────────────────────
    // Re-register the live bundles so the current path wins over any stale
    // registration left behind by an old `~/Applications/*` layout.
    if !args.no_register && !args.dry_run {
        register_bundles(&source).await;
    }

    // ── restart Dock ────────────────────────────────────────────────
    // Only when we actually rewrote the plist. Flush the per-user prefs
    // cache first so cfprefsd doesn't clobber our on-disk edit, then bounce
    // the Dock so it re-reads from disk.
    if !changes.is_empty() && !args.dry_run && !args.no_restart_dock {
        run_quiet("/usr/bin/killall", &["cfprefsd"]).await;
        run_quiet("/usr/bin/killall", &["Dock"]).await;
        info!("restarted Dock");
    }

    info!(retargeted = changes.len(), "app-sync complete");
    Ok(ExitCode::SUCCESS)
}

/// Rewrite dangling `persistent-apps` entries in the Dock plist. Returns the
/// list of re-targets performed (or that would be performed under `dry_run`).
/// `exists` is injected so the decision logic is unit-testable without a real
/// filesystem or Dock.
fn reconcile_dock(
    dock_plist: &Path,
    source: &Path,
    dry_run: bool,
    exists: &impl Fn(&Path) -> bool,
) -> Result<Vec<Retarget>> {
    let mut root = Value::from_file(dock_plist)
        .with_context(|| format!("reading {}", dock_plist.display()))?;

    let mut changes = Vec::new();
    {
        let Some(apps) = root
            .as_dictionary_mut()
            .and_then(|d| d.get_mut("persistent-apps"))
            .and_then(Value::as_array_mut)
        else {
            return Ok(changes);
        };

        for app in apps.iter_mut() {
            let Some(cfurl) = entry_cfurl(app) else {
                continue;
            };
            let Some(plan) = plan_retarget(&cfurl, source, exists) else {
                continue;
            };
            if !dry_run {
                apply_retarget(app, &plan.new_url);
            }
            changes.push(plan);
        }
    }

    if !dry_run && !changes.is_empty() {
        root.to_file_binary(dock_plist)
            .with_context(|| format!("writing {}", dock_plist.display()))?;
    }
    Ok(changes)
}

/// Read `tile-data.file-data._CFURLString` from one persistent-apps entry.
fn entry_cfurl(app: &Value) -> Option<String> {
    app.as_dictionary()?
        .get("tile-data")?
        .as_dictionary()?
        .get("file-data")?
        .as_dictionary()?
        .get("_CFURLString")?
        .as_string()
        .map(str::to_owned)
}

/// Point one entry at `new_url` and drop the cached resolution so macOS
/// re-resolves from the (stable) path on next launch.
fn apply_retarget(app: &mut Value, new_url: &str) {
    let Some(tile) = app
        .as_dictionary_mut()
        .and_then(|d| d.get_mut("tile-data"))
        .and_then(Value::as_dictionary_mut)
    else {
        return;
    };
    if let Some(fd) = tile.get_mut("file-data").and_then(Value::as_dictionary_mut) {
        fd.insert("_CFURLString".into(), Value::String(new_url.to_owned()));
        // 15 == absolute POSIX path URL (kCFURLPOSIXPathStyle).
        fd.insert("_CFURLStringType".into(), Value::Integer(plist::Integer::from(15_i64)));
    }
    tile.remove("book");
    tile.remove("file-mod-date");
    tile.remove("parent-mod-date");
}

/// Decide whether a Dock entry needs re-targeting.
///
/// Returns `Some` only for a dangling `.app` whose basename has a live
/// counterpart under `source`. Healthy entries (path exists) and apps we
/// don't manage are left alone.
fn plan_retarget(cfurl: &str, source: &Path, exists: &impl Fn(&Path) -> bool) -> Option<Retarget> {
    let target = url_to_path(cfurl)?;
    if exists(&target) {
        return None; // healthy — e.g. /System/Applications/Utilities/Activity Monitor.app
    }
    let app_name = target.file_name()?.to_string_lossy().into_owned();
    if !app_name.to_ascii_lowercase().ends_with(".app") {
        return None; // only heal .app bundles
    }
    let candidate = source.join(&app_name);
    if !exists(&candidate) {
        return None; // not a nix-managed app we can re-target
    }
    let new_url = path_to_file_url(&candidate, true);
    if new_url == cfurl {
        return None; // already correct
    }
    Some(Retarget {
        app_name,
        old_url: cfurl.to_owned(),
        new_url,
    })
}

/// Parse a `file://` URL into a filesystem path (percent-decoded, trailing
/// slash trimmed). Returns `None` for non-`file` URLs.
fn url_to_path(cfurl: &str) -> Option<PathBuf> {
    let rest = cfurl.strip_prefix("file://")?;
    let decoded = percent_encoding::percent_decode_str(rest)
        .decode_utf8()
        .ok()?;
    let trimmed = decoded.trim_end_matches('/');
    Some(PathBuf::from(if trimmed.is_empty() { "/" } else { trimmed }))
}

/// Build a `file://` URL for `path`, percent-encoding the way macOS does and
/// appending a trailing slash for directories (`.app` bundles).
fn path_to_file_url(path: &Path, is_dir: bool) -> String {
    let raw = path.to_string_lossy();
    let encoded = utf8_percent_encode(&raw, URL_PATH).to_string();
    let mut url = format!("file://{encoded}");
    if is_dir && !url.ends_with('/') {
        url.push('/');
    }
    url
}

/// Register every `.app` under `source` with `LaunchServices` (`lsregister -f`).
async fn register_bundles(source: &Path) {
    let Ok(entries) = std::fs::read_dir(source) else {
        warn!(source = %source.display(), "source dir unreadable — skipping LaunchServices register");
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_ascii_lowercase();
        if name.ends_with(".app") {
            run_quiet(LSREGISTER, &["-f", &entry.path().to_string_lossy()]).await;
        }
    }
}

/// Run a command, discarding output and ignoring failure (best-effort glue).
async fn run_quiet(program: &str, args: &[&str]) {
    let _ = tokio::process::Command::new(program)
        .args(args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// Build an `exists` closure backed by a fixed set of "present" paths.
    fn fake_fs(present: &[&str]) -> impl Fn(&Path) -> bool {
        let set: HashSet<PathBuf> = present.iter().map(PathBuf::from).collect();
        move |p: &Path| set.contains(p)
    }

    #[test]
    fn url_to_path_plain() {
        assert_eq!(
            url_to_path("file:///nix/store/abc/Applications/Ghostty.app"),
            Some(PathBuf::from("/nix/store/abc/Applications/Ghostty.app"))
        );
    }

    #[test]
    fn url_to_path_decodes_spaces_and_trims_slash() {
        assert_eq!(
            url_to_path("file:///System/Applications/Utilities/Activity%20Monitor.app/"),
            Some(PathBuf::from(
                "/System/Applications/Utilities/Activity Monitor.app"
            ))
        );
    }

    #[test]
    fn url_to_path_rejects_non_file_scheme() {
        assert_eq!(url_to_path("https://example.com/x"), None);
    }

    #[test]
    fn path_to_file_url_encodes_space_and_adds_trailing_slash() {
        assert_eq!(
            path_to_file_url(
                &PathBuf::from("/Users/luis.d/Applications/Home Manager Apps/Ghostty pleme.app"),
                true
            ),
            "file:///Users/luis.d/Applications/Home%20Manager%20Apps/Ghostty%20pleme.app/"
        );
    }

    #[test]
    fn path_to_file_url_preserves_slashes() {
        assert_eq!(
            path_to_file_url(&PathBuf::from("/a/b/c.app"), true),
            "file:///a/b/c.app/"
        );
    }

    #[test]
    fn plan_retarget_heals_dangling_nix_store_path() {
        let source = PathBuf::from("/Users/luis.d/Applications/Home Manager Apps");
        let exists = fake_fs(&["/Users/luis.d/Applications/Home Manager Apps/Ghostty.app"]);
        // The store path is gone (not in fake fs); the stable symlink exists.
        let plan = plan_retarget(
            "file:///nix/store/dead-hash/Applications/Ghostty.app",
            &source,
            &exists,
        );
        assert_eq!(
            plan,
            Some(Retarget {
                app_name: "Ghostty.app".to_owned(),
                old_url: "file:///nix/store/dead-hash/Applications/Ghostty.app".to_owned(),
                new_url:
                    "file:///Users/luis.d/Applications/Home%20Manager%20Apps/Ghostty.app/"
                        .to_owned(),
            })
        );
    }

    #[test]
    fn plan_retarget_leaves_healthy_system_app_untouched() {
        let source = PathBuf::from("/Users/luis.d/Applications/Home Manager Apps");
        // Activity Monitor exists on disk → must not be touched.
        let exists = fake_fs(&["/System/Applications/Utilities/Activity Monitor.app"]);
        let plan = plan_retarget(
            "file:///System/Applications/Utilities/Activity%20Monitor.app/",
            &source,
            &exists,
        );
        assert_eq!(plan, None);
    }

    #[test]
    fn plan_retarget_skips_dangling_app_with_no_source_counterpart() {
        let source = PathBuf::from("/Users/luis.d/Applications/Home Manager Apps");
        // Nothing exists — neither the dead path nor a stable counterpart.
        let exists = fake_fs(&[]);
        let plan = plan_retarget(
            "file:///Applications/SomethingElse.app/",
            &source,
            &exists,
        );
        assert_eq!(plan, None);
    }

    #[test]
    fn plan_retarget_heals_workspace_variant_with_spaces() {
        let source = PathBuf::from("/Users/luis.d/Applications/Home Manager Apps");
        let exists = fake_fs(&["/Users/luis.d/Applications/Home Manager Apps/Ghostty pleme.app"]);
        let plan = plan_retarget(
            "file:///nix/store/gone/Applications/Ghostty%20pleme.app/",
            &source,
            &exists,
        )
        .expect("workspace variant should be re-targeted");
        assert_eq!(plan.app_name, "Ghostty pleme.app");
        assert_eq!(
            plan.new_url,
            "file:///Users/luis.d/Applications/Home%20Manager%20Apps/Ghostty%20pleme.app/"
        );
    }

    #[test]
    fn plan_retarget_skips_non_app_dangling_entry() {
        let source = PathBuf::from("/Users/luis.d/Applications/Home Manager Apps");
        let exists = fake_fs(&[]);
        // A dangling non-.app path (e.g. a stale document) is not ours to heal.
        let plan = plan_retarget("file:///tmp/gone/notes.txt", &source, &exists);
        assert_eq!(plan, None);
    }

    #[test]
    fn apply_retarget_rewrites_url_and_strips_bookmark() {
        // Build a synthetic persistent-apps entry mirroring the real shape.
        let mut file_data = plist::Dictionary::new();
        file_data.insert(
            "_CFURLString".into(),
            Value::String("file:///nix/store/dead/Applications/Ghostty.app".into()),
        );
        file_data.insert("_CFURLStringType".into(), Value::Integer(15_i64.into()));

        let mut tile = plist::Dictionary::new();
        tile.insert("file-data".into(), Value::Dictionary(file_data));
        tile.insert("book".into(), Value::Data(vec![1, 2, 3, 4]));
        tile.insert("file-mod-date".into(), Value::Integer(123_i64.into()));
        tile.insert("parent-mod-date".into(), Value::Integer(456_i64.into()));
        tile.insert("bundle-identifier".into(), Value::String("com.mitchellh.ghostty".into()));

        let mut entry = plist::Dictionary::new();
        entry.insert("tile-data".into(), Value::Dictionary(tile));
        entry.insert("tile-type".into(), Value::String("file-tile".into()));
        let mut app = Value::Dictionary(entry);

        apply_retarget(
            &mut app,
            "file:///Users/luis.d/Applications/Home%20Manager%20Apps/Ghostty.app/",
        );

        let tile = app
            .as_dictionary()
            .unwrap()
            .get("tile-data")
            .unwrap()
            .as_dictionary()
            .unwrap();
        // bookmark + cached mod-dates gone
        assert!(tile.get("book").is_none());
        assert!(tile.get("file-mod-date").is_none());
        assert!(tile.get("parent-mod-date").is_none());
        // bundle-identifier preserved (we only strip cache keys)
        assert!(tile.get("bundle-identifier").is_some());
        // url rewritten, type still 15
        let fd = tile.get("file-data").unwrap().as_dictionary().unwrap();
        assert_eq!(
            fd.get("_CFURLString").unwrap().as_string().unwrap(),
            "file:///Users/luis.d/Applications/Home%20Manager%20Apps/Ghostty.app/"
        );
        assert_eq!(fd.get("_CFURLStringType").unwrap().as_signed_integer().unwrap(), 15);
    }

    #[test]
    fn entry_cfurl_extracts_nested_url() {
        let mut file_data = plist::Dictionary::new();
        file_data.insert("_CFURLString".into(), Value::String("file:///x/Y.app".into()));
        let mut tile = plist::Dictionary::new();
        tile.insert("file-data".into(), Value::Dictionary(file_data));
        let mut entry = plist::Dictionary::new();
        entry.insert("tile-data".into(), Value::Dictionary(tile));
        let app = Value::Dictionary(entry);
        assert_eq!(entry_cfurl(&app).as_deref(), Some("file:///x/Y.app"));
    }

    #[test]
    fn entry_cfurl_returns_none_for_malformed_entry() {
        let app = Value::Dictionary(plist::Dictionary::new());
        assert_eq!(entry_cfurl(&app), None);
    }
}
