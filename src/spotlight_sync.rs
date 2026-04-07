use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use tracing::info;

#[derive(ClapArgs)]
pub struct Args {
    /// Directory containing .app bundles to sync (e.g., ~/Applications/Home Manager Apps)
    #[arg(long, default_value = "~/Applications/Home Manager Apps")]
    source: String,

    /// Directory to create wrapper .app bundles in (Spotlight-indexed)
    #[arg(long, default_value = "~/Applications/Nix")]
    target: String,

    /// Force Spotlight re-index after sync
    #[arg(long)]
    reindex: bool,
}

pub async fn run(args: Args) -> Result<ExitCode> {
    let home = std::env::var("HOME").context("HOME not set")?;
    let source = expand_tilde(&args.source, &home);
    let target = expand_tilde(&args.target, &home);

    // Create target directory
    std::fs::create_dir_all(&target)
        .with_context(|| format!("creating {}", target.display()))?;

    // Remove stale wrapper bundles
    if target.exists() {
        for entry in std::fs::read_dir(&target).context("reading target dir")? {
            let entry = entry?;
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            if has_app_extension(&name) {
                let _ = std::fs::remove_dir_all(&path);
            } else {
                let _ = std::fs::remove_file(&path);
            }
        }
        info!(target = %target.display(), "cleared stale bundles");
    }

    // Find all .app bundles in source directories and create wrapper bundles
    let mut app_count: u32 = 0;
    let sources = collect_sources(&source, &home);

    for src_dir in &sources {
        if !src_dir.exists() {
            continue;
        }

        let entries = std::fs::read_dir(src_dir)
            .with_context(|| format!("reading {}", src_dir.display()))?;

        for entry in entries {
            let entry = entry?;
            let src_path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();

            if !has_app_extension(&name) {
                continue;
            }

            // Resolve the source .app to its real path (follows symlinks)
            let resolved = std::fs::canonicalize(&src_path)
                .unwrap_or_else(|_| src_path.clone());

            if create_wrapper_bundle(&resolved, &target, &name)? {
                info!(app = %name, "synced");
                app_count += 1;
            }
        }
    }

    // Register with Launch Services so Spotlight treats them as applications
    let lsregister = "/System/Library/Frameworks/CoreServices.framework\
        /Versions/A/Frameworks/LaunchServices.framework\
        /Versions/A/Support/lsregister";

    for entry in std::fs::read_dir(&target).into_iter().flatten().flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if has_app_extension(&name) {
            let _ = tokio::process::Command::new(lsregister)
                .args(["-f", &entry.path().to_string_lossy()])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .await;
        }
    }

    // Force Spotlight to re-index
    if args.reindex || app_count > 0 {
        let _ = tokio::process::Command::new("/usr/bin/mdimport")
            .arg(&target)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await;
        info!("Spotlight re-index triggered");
    }

    info!(count = app_count, "Spotlight sync complete");
    Ok(ExitCode::SUCCESS)
}

/// Create a real .app wrapper bundle that Spotlight indexes as an application.
///
/// Instead of a Finder alias (which Spotlight sees as `com.apple.alias-file`),
/// this creates a proper `.app` bundle with:
/// - `Contents/Info.plist` copied from the source
/// - `Contents/MacOS/<executable>` that execs the original
///
/// Spotlight indexes these as `com.apple.application-bundle`.
fn create_wrapper_bundle(source: &Path, target_dir: &Path, name: &str) -> Result<bool> {
    let target_app = target_dir.join(name);

    // Read source Info.plist
    let src_plist = source.join("Contents/Info.plist");
    if !src_plist.exists() {
        info!(app = %name, "skipped — no Info.plist");
        return Ok(false);
    }

    // Find the executable name from Info.plist
    let plist_data = std::fs::read_to_string(&src_plist)
        .with_context(|| format!("reading {}", src_plist.display()))?;
    let exec_name = extract_plist_value(&plist_data, "CFBundleExecutable")
        .unwrap_or_else(|| name.trim_end_matches(".app").to_owned());

    // Find the source executable
    let src_exec = source.join("Contents/MacOS").join(&exec_name);
    if !src_exec.exists() {
        info!(app = %name, "skipped — executable not found: {}", exec_name);
        return Ok(false);
    }

    // Resolve the source executable to its final target (follow all symlinks)
    let resolved_exec = std::fs::canonicalize(&src_exec)
        .unwrap_or_else(|_| src_exec.clone());

    // Create wrapper .app bundle
    let macos_dir = target_app.join("Contents/MacOS");
    std::fs::create_dir_all(&macos_dir)
        .with_context(|| format!("creating {}", macos_dir.display()))?;

    // Copy Info.plist
    std::fs::copy(&src_plist, target_app.join("Contents/Info.plist"))
        .with_context(|| format!("copying Info.plist for {name}"))?;

    // Copy icon if present
    let src_resources = source.join("Contents/Resources");
    if src_resources.exists() {
        let tgt_resources = target_app.join("Contents/Resources");
        let _ = std::fs::create_dir_all(&tgt_resources);
        // Copy .icns files for Spotlight icon display
        if let Ok(entries) = std::fs::read_dir(&src_resources) {
            for entry in entries.flatten() {
                let fname = entry.file_name().to_string_lossy().to_string();
                if has_icns_extension(&fname) {
                    let _ = std::fs::copy(entry.path(), tgt_resources.join(&fname));
                }
            }
        }
    }

    // Create trampoline executable that execs the real binary
    let trampoline = macos_dir.join(&exec_name);
    let script = format!(
        "#!/bin/bash\nexec \"{}\" \"$@\"\n",
        resolved_exec.display()
    );
    std::fs::write(&trampoline, &script)
        .with_context(|| format!("writing trampoline for {name}"))?;
    std::fs::set_permissions(&trampoline, std::fs::Permissions::from_mode(0o755))
        .with_context(|| format!("chmod trampoline for {name}"))?;

    Ok(true)
}

/// Extract a string value from a plist XML by key name.
/// Simple text parser — avoids adding a plist dependency to seibi.
fn extract_plist_value(xml: &str, key: &str) -> Option<String> {
    let key_tag = format!("<key>{key}</key>");
    let pos = xml.find(&key_tag)? + key_tag.len();
    let rest = &xml[pos..];
    let start = rest.find("<string>")? + 8;
    let end = rest[start..].find("</string>")?;
    Some(rest[start..start + end].to_owned())
}

fn has_app_extension(name: &str) -> bool {
    Path::new(name)
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("app"))
}

fn has_icns_extension(name: &str) -> bool {
    Path::new(name)
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("icns"))
}

fn expand_tilde(path: &str, home: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/") {
        PathBuf::from(home).join(rest)
    } else {
        PathBuf::from(path)
    }
}

fn collect_sources(primary: &Path, home: &str) -> Vec<PathBuf> {
    vec![
        primary.to_path_buf(),
        PathBuf::from(format!("{home}/Applications")),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_plist_value_basic() {
        let xml = r#"<?xml version="1.0"?>
<plist>
<dict>
    <key>CFBundleExecutable</key>
    <string>MyApp</string>
    <key>CFBundleName</key>
    <string>My Application</string>
</dict>
</plist>"#;
        assert_eq!(
            extract_plist_value(xml, "CFBundleExecutable"),
            Some("MyApp".to_owned())
        );
        assert_eq!(
            extract_plist_value(xml, "CFBundleName"),
            Some("My Application".to_owned())
        );
    }

    #[test]
    fn extract_plist_value_missing_key() {
        let xml = "<plist><dict><key>Other</key><string>val</string></dict></plist>";
        assert_eq!(extract_plist_value(xml, "CFBundleExecutable"), None);
    }

    #[test]
    fn extract_plist_value_empty_string() {
        let xml = "<plist><dict><key>CFBundleExecutable</key><string></string></dict></plist>";
        assert_eq!(
            extract_plist_value(xml, "CFBundleExecutable"),
            Some("".to_owned())
        );
    }

    #[test]
    fn extract_plist_value_no_string_tag() {
        let xml = "<plist><dict><key>CFBundleExecutable</key><integer>42</integer></dict></plist>";
        assert_eq!(extract_plist_value(xml, "CFBundleExecutable"), None);
    }

    #[test]
    fn extract_plist_value_empty_xml() {
        assert_eq!(extract_plist_value("", "CFBundleExecutable"), None);
    }

    #[test]
    fn expand_tilde_with_home_prefix() {
        let result = expand_tilde("~/Documents/test", "/home/user");
        assert_eq!(result, PathBuf::from("/home/user/Documents/test"));
    }

    #[test]
    fn expand_tilde_without_home_prefix() {
        let result = expand_tilde("/absolute/path", "/home/user");
        assert_eq!(result, PathBuf::from("/absolute/path"));
    }

    #[test]
    fn expand_tilde_only_tilde_slash() {
        let result = expand_tilde("~/", "/home/user");
        assert_eq!(result, PathBuf::from("/home/user/"));
    }

    #[test]
    fn expand_tilde_tilde_without_slash_is_literal() {
        let result = expand_tilde("~nope", "/home/user");
        assert_eq!(result, PathBuf::from("~nope"));
    }

    #[test]
    fn collect_sources_includes_primary_and_applications() {
        let primary = PathBuf::from("/custom/apps");
        let sources = collect_sources(&primary, "/home/user");
        assert_eq!(sources.len(), 2);
        assert_eq!(sources[0], PathBuf::from("/custom/apps"));
        assert_eq!(sources[1], PathBuf::from("/home/user/Applications"));
    }
}
