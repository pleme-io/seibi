use std::process::Stdio;
use tokio::process::Command;

/// Outcome of a single health-check probe.
pub struct ProbeResult {
    pub healthy: bool,
    pub detail: String,
}

/// A health-check probe that can test network, wifi, or systemd unit status.
pub enum Probe {
    Ping { target: String },
    Wifi { interface: String },
    Systemd { unit: String },
}

impl Probe {
    pub fn name(&self) -> &str {
        match self {
            Self::Ping { .. } => "network",
            Self::Wifi { .. } => "wifi",
            Self::Systemd { unit } => unit,
        }
    }

    pub async fn check(&self) -> ProbeResult {
        match self {
            Self::Ping { target } => check_ping(target).await,
            Self::Wifi { interface } => check_wifi(interface).await,
            Self::Systemd { unit } => check_systemd(unit).await,
        }
    }
}

async fn check_ping(target: &str) -> ProbeResult {
    let ok = Command::new("ping")
        .args(["-c", "1", "-W", "5", target])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .is_ok_and(|s| s.success());

    ProbeResult {
        healthy: ok,
        detail: if ok {
            format!("ping {target} ok")
        } else {
            format!("ping {target} failed")
        },
    }
}

async fn check_wifi(interface: &str) -> ProbeResult {
    let output = Command::new("iw")
        .args([interface, "info"])
        .output()
        .await;

    match output {
        Ok(out) if out.status.success() => {
            let text = String::from_utf8_lossy(&out.stdout);
            let ssid = text
                .lines()
                .find_map(|l| l.trim().strip_prefix("ssid "))
                .map(String::from);

            match ssid {
                Some(s) => ProbeResult {
                    healthy: true,
                    detail: format!("connected to {s}"),
                },
                None => ProbeResult {
                    healthy: false,
                    detail: "no SSID".into(),
                },
            }
        }
        _ => ProbeResult {
            healthy: false,
            detail: format!("{interface} down"),
        },
    }
}

async fn check_systemd(unit: &str) -> ProbeResult {
    let ok = Command::new("systemctl")
        .args(["is-active", "--quiet", unit])
        .status()
        .await
        .is_ok_and(|s| s.success());

    ProbeResult {
        healthy: ok,
        detail: if ok {
            format!("{unit} active")
        } else {
            format!("{unit} inactive")
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_name_ping() {
        let probe = Probe::Ping {
            target: "8.8.8.8".into(),
        };
        assert_eq!(probe.name(), "network");
    }

    #[test]
    fn probe_name_wifi() {
        let probe = Probe::Wifi {
            interface: "wlan0".into(),
        };
        assert_eq!(probe.name(), "wifi");
    }

    #[test]
    fn probe_name_systemd_returns_unit_name() {
        let probe = Probe::Systemd {
            unit: "k3s.service".into(),
        };
        assert_eq!(probe.name(), "k3s.service");
    }

    #[test]
    fn probe_name_systemd_different_units() {
        let probe = Probe::Systemd {
            unit: "nginx.service".into(),
        };
        assert_eq!(probe.name(), "nginx.service");

        let probe = Probe::Systemd {
            unit: "docker.service".into(),
        };
        assert_eq!(probe.name(), "docker.service");
    }

    #[tokio::test]
    async fn check_ping_detail_contains_target() {
        let result = check_ping("127.0.0.1").await;
        assert!(
            result.detail.contains("127.0.0.1"),
            "detail should mention target: {}",
            result.detail
        );
    }

    #[tokio::test]
    async fn check_ping_unreachable_is_unhealthy() {
        let result = check_ping("192.0.2.1").await;
        assert!(!result.healthy);
        assert!(result.detail.contains("192.0.2.1"));
    }

    #[tokio::test]
    async fn check_systemd_nonexistent_unit_is_unhealthy() {
        let result = check_systemd("seibi-nonexistent-unit-12345.service").await;
        assert!(!result.healthy);
        assert!(
            result.detail.contains("inactive") || result.detail.contains("down"),
            "detail should indicate failure: {}",
            result.detail
        );
    }

    #[test]
    fn probe_result_fields() {
        let r = ProbeResult {
            healthy: true,
            detail: "all good".into(),
        };
        assert!(r.healthy);
        assert_eq!(r.detail, "all good");
    }
}
