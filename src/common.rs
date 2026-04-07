use std::path::PathBuf;

/// Default location for the SOPS age key file (`~/.config/sops/age/keys.txt`).
pub fn default_key_file() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    PathBuf::from(home).join(".config/sops/age/keys.txt")
}

/// Discover the git repository root via `git rev-parse --show-toplevel`.
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

/// Expand a leading `~/` in a path string to the given home directory.
pub fn expand_tilde(path: &str, home: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/") {
        PathBuf::from(home).join(rest)
    } else {
        PathBuf::from(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_key_file_under_home() {
        let path = default_key_file();
        let path_str = path.to_string_lossy();
        assert!(
            path_str.ends_with(".config/sops/age/keys.txt"),
            "unexpected path: {path_str}"
        );
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
    fn expand_tilde_relative_path_unchanged() {
        let result = expand_tilde("relative/path", "/home/user");
        assert_eq!(result, PathBuf::from("relative/path"));
    }

    #[test]
    fn expand_tilde_empty_string() {
        let result = expand_tilde("", "/home/user");
        assert_eq!(result, PathBuf::from(""));
    }

    #[test]
    fn expand_tilde_just_tilde() {
        let result = expand_tilde("~", "/home/user");
        assert_eq!(result, PathBuf::from("~"));
    }
}
