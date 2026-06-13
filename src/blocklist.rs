//! Assemble a dnsmasq ad/tracker blocklist from upstream hosts lists.
//!
//! Replaces the inline shell in pleme-io/nix's
//! `modules/pleme/nixos/edge-router.nix` (the `edge-router-blocklist-refresh`
//! oneshot). Fetches each `--url` best-effort (a single dead source never
//! fails the run), keeps only `0.0.0.0`/`127.0.0.1` hosts lines, normalizes
//! each to `<ip> <host>`, deduplicates + sorts (a `BTreeSet`), and writes the
//! result for dnsmasq's `addn-hosts`.

use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;
use tracing::{info, warn};

#[derive(ClapArgs)]
pub struct Args {
    /// Upstream hosts-list URL (repeatable). Each is fetched best-effort.
    #[arg(long = "url", required = true)]
    urls: Vec<String>,

    /// Where the assembled blocklist is written (dnsmasq addn-hosts).
    #[arg(long, default_value = "/var/lib/dnsmasq/blocklist.hosts")]
    out: PathBuf,
}

/// Keep only sinkhole lines (`0.0.0.0 host` / `127.0.0.1 host`), normalized to
/// `<ip> <host>` (first host field only — matches the awk `$1" "$2`), sorted +
/// deduplicated. Pure — the unit-tested core.
fn filter_blocklist(input: &str) -> BTreeSet<String> {
    input
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.starts_with("0.0.0.0") || line.starts_with("127.0.0.1") {
                let mut parts = line.split_whitespace();
                let ip = parts.next()?;
                let host = parts.next()?;
                // skip a bare "0.0.0.0" with no host, and inline-comment hosts.
                if host.starts_with('#') {
                    return None;
                }
                Some([ip, host].join(" "))
            } else {
                None
            }
        })
        .collect()
}

fn render(entries: &BTreeSet<String>) -> String {
    let mut s = entries.iter().cloned().collect::<Vec<_>>().join("\n");
    s.push('\n');
    s
}

pub async fn run(args: &Args) -> Result<ExitCode> {
    let client = reqwest::Client::builder()
        .user_agent("seibi-blocklist")
        .build()
        .context("building HTTP client")?;

    let mut combined = String::new();
    let mut fetched = 0usize;
    for url in &args.urls {
        match client.get(url).send().await {
            Ok(resp) => match resp.error_for_status() {
                Ok(resp) => match resp.text().await {
                    Ok(body) => {
                        combined.push_str(&body);
                        combined.push('\n');
                        fetched += 1;
                    }
                    Err(e) => warn!(url, error = %e, "blocklist body read failed — skipping"),
                },
                Err(e) => warn!(url, error = %e, "blocklist source returned error status — skipping"),
            },
            Err(e) => warn!(url, error = %e, "blocklist fetch failed — skipping"),
        }
    }

    let entries = filter_blocklist(&combined);
    let rendered = render(&entries);

    if let Some(parent) = args.out.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    fs::write(&args.out, &rendered)
        .with_context(|| format!("writing {}", args.out.display()))?;

    info!(
        out = %args.out.display(),
        sources = args.urls.len(),
        fetched,
        entries = entries.len(),
        "blocklist assembled"
    );
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_only_sinkhole_lines() {
        let input = "\
# comment line
0.0.0.0 ads.example.com
127.0.0.1 tracker.example.net
1.2.3.4 not-a-sinkhole.com
0.0.0.0 ads.example.com
   0.0.0.0 leading-ws.example.org
0.0.0.0
0.0.0.0 # inline-comment-host
";
        let got = filter_blocklist(input);
        assert!(got.contains("0.0.0.0 ads.example.com"));
        assert!(got.contains("127.0.0.1 tracker.example.net"));
        assert!(got.contains("0.0.0.0 leading-ws.example.org"));
        // not a sinkhole prefix
        assert!(!got.iter().any(|l| l.contains("not-a-sinkhole")));
        // dedup: ads.example.com appears twice in input, once in set
        assert_eq!(got.iter().filter(|l| l.contains("ads.example.com")).count(), 1);
        // bare "0.0.0.0" (no host) dropped; inline-comment host dropped
        assert!(!got.contains("0.0.0.0"));
        assert!(!got.iter().any(|l| l.contains('#')));
    }

    #[test]
    fn render_is_sorted_and_newline_terminated() {
        let input = "0.0.0.0 zeta.com\n0.0.0.0 alpha.com\n127.0.0.1 mid.com\n";
        let entries = filter_blocklist(input);
        let out = render(&entries);
        // BTreeSet => sorted; "0.0.0.0 alpha" < "0.0.0.0 zeta" < "127.0.0.1 mid"
        let alpha = out.find("alpha.com").unwrap();
        let zeta = out.find("zeta.com").unwrap();
        let mid = out.find("mid.com").unwrap();
        assert!(alpha < zeta && zeta < mid);
        assert!(out.ends_with('\n'));
    }

    #[test]
    fn empty_input_empty_output() {
        let entries = filter_blocklist("");
        assert!(entries.is_empty());
        assert_eq!(render(&entries), "\n");
    }
}
