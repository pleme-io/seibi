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
    #[must_use]
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
    #[must_use]
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
    #[must_use]
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_metrics(overrides: impl FnOnce(&mut SystemMetrics)) -> SystemMetrics {
        let mut m = SystemMetrics {
            uptime: "1d".into(),
            load_avg: "0.5, 0.3, 0.1".into(),
            memory_used: "4.0 GB".into(),
            memory_total: "16.0 GB".into(),
            disk_used: "50.0 GB".into(),
            disk_total: "500.0 GB".into(),
            disk_percent: "10%".into(),
            cpu_temp: "45.0°C".into(),
            battery_level: "85%".into(),
            battery_status: "Charging".into(),
            wifi_status: "Connected".into(),
            wifi_ssid: "HomeNet".into(),
            ip_address: "192.168.1.100".into(),
        };
        overrides(&mut m);
        m
    }

    #[test]
    fn health_healthy_when_all_normal() {
        let m = make_metrics(|_| {});
        assert_eq!(m.health_assessment(), "Healthy");
    }

    #[test]
    fn health_network_down_when_wifi_disconnected() {
        let m = make_metrics(|m| m.wifi_status = "Disconnected".into());
        assert_eq!(m.health_assessment(), "Network Down");
    }

    #[test]
    fn health_battery_critical_below_10() {
        let m = make_metrics(|m| m.battery_level = "9%".into());
        assert_eq!(m.health_assessment(), "Battery Critical");
    }

    #[test]
    fn health_battery_critical_at_boundary() {
        let m = make_metrics(|m| m.battery_level = "10%".into());
        assert_ne!(m.health_assessment(), "Battery Critical");
    }

    #[test]
    fn health_low_battery_between_10_and_20() {
        let m = make_metrics(|m| m.battery_level = "15%".into());
        assert_eq!(m.health_assessment(), "Low Battery");

        let m = make_metrics(|m| m.battery_level = "19%".into());
        assert_eq!(m.health_assessment(), "Low Battery");
    }

    #[test]
    fn health_low_battery_boundary_at_20() {
        let m = make_metrics(|m| m.battery_level = "20%".into());
        assert_eq!(m.health_assessment(), "Healthy");
    }

    #[test]
    fn health_high_load() {
        let m = make_metrics(|m| m.load_avg = "5.0, 3.0, 2.0".into());
        assert_eq!(m.health_assessment(), "High Load");
    }

    #[test]
    fn health_load_at_boundary() {
        let m = make_metrics(|m| m.load_avg = "4.0, 3.0, 2.0".into());
        assert_eq!(m.health_assessment(), "Healthy");
    }

    #[test]
    fn health_load_just_above_boundary() {
        let m = make_metrics(|m| m.load_avg = "4.01, 3.0, 2.0".into());
        assert_eq!(m.health_assessment(), "High Load");
    }

    #[test]
    fn health_priority_network_over_battery() {
        let m = make_metrics(|m| {
            m.wifi_status = "Disconnected".into();
            m.battery_level = "5%".into();
        });
        assert_eq!(m.health_assessment(), "Network Down");
    }

    #[test]
    fn health_priority_battery_over_load() {
        let m = make_metrics(|m| {
            m.battery_level = "5%".into();
            m.load_avg = "10.0, 8.0, 6.0".into();
        });
        assert_eq!(m.health_assessment(), "Battery Critical");
    }

    #[test]
    fn health_unparseable_battery_falls_through() {
        let m = make_metrics(|m| m.battery_level = "N/A".into());
        assert_eq!(m.health_assessment(), "Healthy");
    }

    #[test]
    fn health_unparseable_load_falls_through() {
        let m = make_metrics(|m| m.load_avg = "N/A".into());
        assert_eq!(m.health_assessment(), "Healthy");
    }

    #[test]
    fn health_empty_load_avg() {
        let m = make_metrics(|m| m.load_avg = "".into());
        assert_eq!(m.health_assessment(), "Healthy");
    }

    #[test]
    fn parse_battery_percent_normal() {
        assert_eq!(parse_battery_percent("85%"), Some(85));
    }

    #[test]
    fn parse_battery_percent_no_percent_sign() {
        assert_eq!(parse_battery_percent("85"), Some(85));
    }

    #[test]
    fn parse_battery_percent_with_leading_whitespace() {
        assert_eq!(parse_battery_percent(" 85%"), Some(85));
    }

    #[test]
    fn parse_battery_percent_trailing_space_after_percent_fails() {
        // trim_end_matches('%') won't strip '%' when trailing whitespace follows,
        // so " 85% " parses to None — this is acceptable since the input is
        // always formatted as "XX%" by read_battery_level
        assert_eq!(parse_battery_percent(" 85% "), None);
    }

    #[test]
    fn parse_battery_percent_zero() {
        assert_eq!(parse_battery_percent("0%"), Some(0));
    }

    #[test]
    fn parse_battery_percent_hundred() {
        assert_eq!(parse_battery_percent("100%"), Some(100));
    }

    #[test]
    fn parse_battery_percent_invalid() {
        assert_eq!(parse_battery_percent("N/A"), None);
        assert_eq!(parse_battery_percent(""), None);
        assert_eq!(parse_battery_percent("abc%"), None);
    }

    #[test]
    fn collect_non_linux_returns_na() {
        #[cfg(not(target_os = "linux"))]
        {
            let m = SystemMetrics::collect();
            assert_eq!(m.uptime, "N/A");
            assert_eq!(m.load_avg, "N/A");
            assert_eq!(m.wifi_status, "N/A");
        }
    }

    #[cfg(target_os = "linux")]
    mod linux_tests {
        use super::super::*;

        #[test]
        fn format_bytes_zero() {
            assert_eq!(format_bytes(0), "0.0 B");
        }

        #[test]
        fn format_bytes_bytes_range() {
            assert_eq!(format_bytes(512), "512.0 B");
        }

        #[test]
        fn format_bytes_kilobytes() {
            assert_eq!(format_bytes(1024), "1.0 KB");
            assert_eq!(format_bytes(1536), "1.5 KB");
        }

        #[test]
        fn format_bytes_megabytes() {
            assert_eq!(format_bytes(1024 * 1024), "1.0 MB");
        }

        #[test]
        fn format_bytes_gigabytes() {
            assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0 GB");
        }

        #[test]
        fn format_bytes_terabytes() {
            assert_eq!(format_bytes(1024u64 * 1024 * 1024 * 1024), "1.0 TB");
        }

        #[test]
        fn format_bytes_petabytes() {
            assert_eq!(
                format_bytes(1024u64 * 1024 * 1024 * 1024 * 1024),
                "1.0 PB"
            );
        }

        #[test]
        fn format_duration_seconds_only() {
            assert_eq!(format_duration(30.0), "30s");
        }

        #[test]
        fn format_duration_minutes() {
            assert_eq!(format_duration(120.0), "2m");
        }

        #[test]
        fn format_duration_hours_and_minutes() {
            assert_eq!(format_duration(3660.0), "1h 1m");
        }

        #[test]
        fn format_duration_days() {
            assert_eq!(format_duration(90000.0), "1d 1h");
        }

        #[test]
        fn format_duration_zero() {
            assert_eq!(format_duration(0.0), "0s");
        }

        #[test]
        fn format_duration_fractional_truncates() {
            // format_duration only shows the largest units; seconds are omitted
            // when minutes are present (by design: no seconds branch in the fn)
            assert_eq!(format_duration(61.9), "1m");
        }

        #[test]
        fn parse_kb_normal() {
            assert_eq!(parse_kb("  16384 kB"), 16384);
        }

        #[test]
        fn parse_kb_empty() {
            assert_eq!(parse_kb(""), 0);
        }

        #[test]
        fn parse_kb_invalid() {
            assert_eq!(parse_kb("abc kB"), 0);
        }
    }
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
