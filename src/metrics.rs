pub struct SystemMetrics {
    pub uptime: String,
    pub load_avg: String,
    pub memory_used: String,
    pub memory_total: String,
    pub disk_used: String,
    pub disk_total: String,
    pub disk_percent: String,
    pub cpu_temp: String,
    pub battery_level: String,
    pub battery_status: String,
    pub wifi_status: String,
    pub wifi_ssid: String,
    pub ip_address: String,
}

impl SystemMetrics {
    #[cfg(target_os = "linux")]
    pub fn collect() -> Self {
        let (memory_used, memory_total) =
            read_memory().unwrap_or_else(|| ("N/A".into(), "N/A".into()));
        let (disk_used, disk_total, disk_percent) =
            read_disk().unwrap_or_else(|| ("N/A".into(), "N/A".into(), "N/A".into()));

        Self {
            uptime: read_uptime(),
            load_avg: read_load_avg(),
            memory_used,
            memory_total,
            disk_used,
            disk_total,
            disk_percent,
            cpu_temp: read_cpu_temp(),
            battery_level: read_battery_level(),
            battery_status: read_battery_status(),
            wifi_status: read_wifi_status(),
            wifi_ssid: read_wifi_ssid(),
            ip_address: read_ip_address(),
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub fn collect() -> Self {
        Self {
            uptime: "N/A".into(),
            load_avg: "N/A".into(),
            memory_used: "N/A".into(),
            memory_total: "N/A".into(),
            disk_used: "N/A".into(),
            disk_total: "N/A".into(),
            disk_percent: "N/A".into(),
            cpu_temp: "N/A".into(),
            battery_level: "N/A".into(),
            battery_status: "N/A".into(),
            wifi_status: "N/A".into(),
            wifi_ssid: "N/A".into(),
            ip_address: "N/A".into(),
        }
    }

    /// Returns a human-readable health label.
    pub fn health_assessment(&self) -> &str {
        if self.wifi_status.contains("Disconnected") {
            return "Network Down";
        }

        if let Some(level) = parse_battery_percent(&self.battery_level) {
            if level < 10 {
                return "Battery Critical";
            }
            if level < 20 {
                return "Low Battery";
            }
        }

        if let Some(load) = self.load_avg.split(',').next() {
            if let Ok(l) = load.trim().parse::<f64>() {
                if l > 4.0 {
                    return "High Load";
                }
            }
        }

        "Healthy"
    }
}

fn parse_battery_percent(s: &str) -> Option<u32> {
    s.trim_end_matches('%').trim().parse().ok()
}

// ── Linux /proc + /sys readers ──────────────────────────────

#[cfg(target_os = "linux")]
use std::fs;
#[cfg(target_os = "linux")]
use std::process::Command;

#[cfg(target_os = "linux")]
fn read_uptime() -> String {
    fs::read_to_string("/proc/uptime")
        .ok()
        .and_then(|s| s.split_whitespace().next().map(String::from))
        .and_then(|s| s.parse::<f64>().ok())
        .map(format_duration)
        .unwrap_or_else(|| "N/A".into())
}

#[cfg(target_os = "linux")]
fn read_load_avg() -> String {
    fs::read_to_string("/proc/loadavg")
        .ok()
        .map(|s| {
            let f: Vec<&str> = s.split_whitespace().collect();
            if f.len() >= 3 {
                format!("{}, {}, {}", f[0], f[1], f[2])
            } else {
                "N/A".into()
            }
        })
        .unwrap_or_else(|| "N/A".into())
}

#[cfg(target_os = "linux")]
fn read_memory() -> Option<(String, String)> {
    let content = fs::read_to_string("/proc/meminfo").ok()?;
    let mut total_kb: u64 = 0;
    let mut available_kb: u64 = 0;

    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            total_kb = parse_kb(rest);
        } else if let Some(rest) = line.strip_prefix("MemAvailable:") {
            available_kb = parse_kb(rest);
        }
    }

    if total_kb > 0 {
        let used_kb = total_kb.saturating_sub(available_kb);
        Some((format_bytes(used_kb * 1024), format_bytes(total_kb * 1024)))
    } else {
        None
    }
}

#[cfg(target_os = "linux")]
fn parse_kb(s: &str) -> u64 {
    s.split_whitespace()
        .next()
        .and_then(|n| n.parse().ok())
        .unwrap_or(0)
}

#[cfg(target_os = "linux")]
fn read_disk() -> Option<(String, String, String)> {
    let output = Command::new("df").args(["-B1", "/"]).output().ok()?;
    let text = String::from_utf8_lossy(&output.stdout);
    let line = text.lines().nth(1)?;
    let fields: Vec<&str> = line.split_whitespace().collect();
    if fields.len() >= 5 {
        let total: u64 = fields[1].parse().ok()?;
        let used: u64 = fields[2].parse().ok()?;
        Some((format_bytes(used), format_bytes(total), fields[4].to_owned()))
    } else {
        None
    }
}

#[cfg(target_os = "linux")]
fn read_cpu_temp() -> String {
    for i in 0..10 {
        let path = format!("/sys/class/thermal/thermal_zone{i}/temp");
        if let Ok(temp_str) = fs::read_to_string(&path) {
            if let Ok(millideg) = temp_str.trim().parse::<u64>() {
                return format!("{:.1}\u{00b0}C", millideg as f64 / 1000.0);
            }
        }
    }
    "N/A".into()
}

#[cfg(target_os = "linux")]
fn read_battery_level() -> String {
    for bat in &["BAT0", "BAT1"] {
        let path = format!("/sys/class/power_supply/{bat}/capacity");
        if let Ok(level) = fs::read_to_string(&path) {
            return format!("{}%", level.trim());
        }
    }
    "N/A".into()
}

#[cfg(target_os = "linux")]
fn read_battery_status() -> String {
    for bat in &["BAT0", "BAT1"] {
        let path = format!("/sys/class/power_supply/{bat}/status");
        if let Ok(status) = fs::read_to_string(&path) {
            return status.trim().to_owned();
        }
    }
    "N/A".into()
}

#[cfg(target_os = "linux")]
fn read_wifi_status() -> String {
    Command::new("ip")
        .args(["link", "show", "wlo1"])
        .output()
        .ok()
        .filter(|o| String::from_utf8_lossy(&o.stdout).contains("state UP"))
        .map_or_else(|| "Disconnected".into(), |_| "Connected".into())
}

#[cfg(target_os = "linux")]
fn read_wifi_ssid() -> String {
    Command::new("iw")
        .args(["wlo1", "info"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| {
            let text = String::from_utf8_lossy(&o.stdout).into_owned();
            text.lines()
                .find_map(|l| l.trim().strip_prefix("ssid ").map(String::from))
        })
        .unwrap_or_else(|| "N/A".into())
}

#[cfg(target_os = "linux")]
fn read_ip_address() -> String {
    Command::new("ip")
        .args(["-4", "addr", "show", "scope", "global"])
        .output()
        .ok()
        .and_then(|o| {
            let text = String::from_utf8_lossy(&o.stdout).into_owned();
            text.lines()
                .find_map(|line| {
                    let trimmed = line.trim();
                    trimmed
                        .strip_prefix("inet ")
                        .and_then(|rest| rest.split('/').next())
                        .map(String::from)
                })
        })
        .unwrap_or_else(|| "N/A".into())
}

// ── Formatting ──────────────────────────────────────────────

#[cfg(target_os = "linux")]
fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    for unit in UNITS {
        if size < 1024.0 {
            return format!("{size:.1} {unit}");
        }
        size /= 1024.0;
    }
    format!("{size:.1} PB")
}

#[cfg(target_os = "linux")]
fn format_duration(seconds: f64) -> String {
    let secs = seconds as u64;
    let days = secs / 86400;
    let hours = (secs % 86400) / 3600;
    let minutes = (secs % 3600) / 60;

    let mut parts = Vec::new();
    if days > 0 {
        parts.push(format!("{days}d"));
    }
    if hours > 0 {
        parts.push(format!("{hours}h"));
    }
    if minutes > 0 {
        parts.push(format!("{minutes}m"));
    }
    if parts.is_empty() {
        parts.push(format!("{secs}s"));
    }
    parts.join(" ")
}
