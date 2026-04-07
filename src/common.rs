use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

/// Expand a leading `~/` prefix to the given home directory.
///
/// Paths that don't start with `~/` are returned as-is.
#[must_use]
pub fn expand_tilde(path: &str, home: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/") {
        PathBuf::from(home).join(rest)
    } else {
        PathBuf::from(path)
    }
}

/// Return the default SOPS age key file path (`~/.config/sops/age/keys.txt`).
#[must_use]
pub fn default_key_file() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    PathBuf::from(home).join(".config/sops/age/keys.txt")
}

/// Discover the repository root via `git rev-parse --show-toplevel`.
#[must_use]
pub fn find_git_root() -> Option<PathBuf> {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .ok()?;
    if output.status.success() {
        Some(PathBuf::from(
            String::from_utf8_lossy(&output.stdout).trim(),
        ))
    } else {
        None
    }
}

/// Read a file and return its trimmed contents.
///
/// Suitable for token files, key files, and other single-value secrets.
pub fn read_trimmed_file(path: &Path) -> Result<String> {
    std::fs::read_to_string(path)
        .map(|s| s.trim().to_owned())
        .with_context(|| format!("reading {}", path.display()))
}

/// Shell-escape a value for safe inclusion in `export VAR='...'`.
///
/// Uses single-quote wrapping with `'\''` to escape embedded single quotes.
#[must_use]
pub fn shell_escape(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── expand_tilde ────────────────────────────────────────

    #[test]
    fn expand_tilde_with_home_prefix() {
        assert_eq!(
            expand_tilde("~/projects", "/home/alice"),
            PathBuf::from("/home/alice/projects"),
        );
    }

    #[test]
    fn expand_tilde_absolute_path_unchanged() {
        assert_eq!(
            expand_tilde("/var/data", "/home/alice"),
            PathBuf::from("/var/data"),
        );
    }

    #[test]
    fn expand_tilde_relative_path_unchanged() {
        assert_eq!(
            expand_tilde("relative/path", "/home/alice"),
            PathBuf::from("relative/path"),
        );
    }

    #[test]
    fn expand_tilde_only_tilde_slash() {
        assert_eq!(
            expand_tilde("~/", "/home/user"),
            PathBuf::from("/home/user/"),
        );
    }

    #[test]
    fn expand_tilde_tilde_without_slash_is_literal() {
        assert_eq!(expand_tilde("~nope", "/home/user"), PathBuf::from("~nope"));
    }

    // ── default_key_file ────────────────────────────────────

    #[test]
    fn default_key_file_ends_with_expected_path() {
        let path = default_key_file();
        assert!(
            path.ends_with(".config/sops/age/keys.txt"),
            "unexpected path: {path:?}",
        );
    }

    // ── read_trimmed_file ───────────────────────────────────

    #[test]
    fn read_trimmed_file_trims_whitespace() {
        let dir = std::env::temp_dir().join("seibi-test-common-trim");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let path = dir.join("token");
        std::fs::write(&path, "  my-token  \n").unwrap();

        let val = read_trimmed_file(&path).unwrap();
        assert_eq!(val, "my-token");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── shell_escape ─────────────────────────────────────────

    #[test]
    fn shell_escape_simple() {
        assert_eq!(shell_escape("hello"), "'hello'");
    }

    #[test]
    fn shell_escape_with_quotes() {
        assert_eq!(shell_escape("it's"), "'it'\\''s'");
    }

    #[test]
    fn shell_escape_empty() {
        assert_eq!(shell_escape(""), "''");
    }

    #[test]
    fn shell_escape_special_chars() {
        assert_eq!(shell_escape("a$b`c"), "'a$b`c'");
    }

    // ── read_trimmed_file ───────────────────────────────────

    #[test]
    fn read_trimmed_file_missing_returns_error() {
        let result = read_trimmed_file(Path::new("/nonexistent/file"));
        assert!(result.is_err());
    }

    #[test]
    fn read_trimmed_file_empty_returns_empty() {
        let dir = std::env::temp_dir().join("seibi-test-common-empty");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let path = dir.join("token");
        std::fs::write(&path, "").unwrap();

        let val = read_trimmed_file(&path).unwrap();
        assert_eq!(val, "");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
