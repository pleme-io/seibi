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
}
