use anyhow::Result;
use reqwest::Client;
use serde::Serialize;
use tracing::{info, warn};

// ── Discord embed colors ────────────────────────────────────

pub const GREEN: u32 = 0x2e_cc71;
pub const RED: u32 = 0xe7_4c3c;
pub const ORANGE: u32 = 0xe6_7e22;
pub const BLUE: u32 = 0x34_98db;

#[must_use]
pub fn event_color(event: &str) -> u32 {
    if event.ends_with("-up") || event == "boot" {
        GREEN
    } else if event.ends_with("-down") || event == "shutdown" {
        RED
    } else {
        BLUE
    }
}

#[must_use]
pub fn health_color(health: &str) -> u32 {
    if health == "Healthy" {
        GREEN
    } else if health.contains("Critical") || health.contains("Down") {
        RED
    } else {
        ORANGE
    }
}

// ── Webhook client ──────────────────────────────────────────

pub struct Webhook {
    client: Client,
    url: String,
    hostname: String,
}

impl Webhook {
    #[must_use]
    pub fn new(url: impl Into<String>, hostname: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            url: url.into(),
            hostname: hostname.into(),
        }
    }

    pub async fn send(&self, embed: EmbedBuilder) -> Result<()> {
        let payload = Payload {
            username: format!("{} monitor", self.hostname),
            embeds: vec![embed.build()],
        };

        info!(
            host = %self.hostname,
            title = %payload.embeds[0].title,
            "sending webhook"
        );

        let resp = self.client.post(&self.url).json(&payload).send().await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            warn!(%status, body, "webhook delivery failed");
        }

        Ok(())
    }

    pub async fn event(&self, event: &str, detail: &str) -> Result<()> {
        let embed = EmbedBuilder::new(format!("[{}] {event}", self.hostname))
            .description(detail)
            .color(event_color(event))
            .footer(format!("{} seibi", self.hostname));
        self.send(embed).await
    }
}

// ── Embed builder ───────────────────────────────────────────

pub struct EmbedBuilder {
    title: String,
    description: String,
    color: u32,
    fields: Vec<Field>,
    footer: Option<String>,
}

impl EmbedBuilder {
    #[must_use]
    pub fn new(title: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            description: String::new(),
            color: BLUE,
            fields: Vec::new(),
            footer: None,
        }
    }

    #[must_use]
    pub fn description(mut self, d: impl Into<String>) -> Self {
        self.description = d.into();
        self
    }

    #[must_use]
    pub fn color(mut self, c: u32) -> Self {
        self.color = c;
        self
    }

    #[must_use]
    pub fn field(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
        inline: bool,
    ) -> Self {
        self.fields.push(Field {
            name: name.into(),
            value: value.into(),
            inline,
        });
        self
    }

    #[must_use]
    pub fn footer(mut self, f: impl Into<String>) -> Self {
        self.footer = Some(f.into());
        self
    }

    fn build(self) -> Embed {
        Embed {
            title: self.title,
            description: self.description,
            color: self.color,
            timestamp: Some(now_rfc3339()),
            fields: self.fields,
            footer: self.footer.map(|text| Footer { text }),
        }
    }
}

// ── Serialization types ─────────────────────────────────────

#[derive(Serialize)]
struct Payload {
    username: String,
    embeds: Vec<Embed>,
}

#[derive(Serialize)]
struct Embed {
    title: String,
    description: String,
    color: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    fields: Vec<Field>,
    #[serde(skip_serializing_if = "Option::is_none")]
    footer: Option<Footer>,
}

#[derive(Serialize)]
struct Field {
    name: String,
    value: String,
    inline: bool,
}

#[derive(Serialize)]
struct Footer {
    text: String,
}

// ── Shared status embed builder ─────────────────────────────

/// Build a status embed with system metrics. Used by both `notify` and `monitor`.
#[must_use]
pub fn status_embed(
    hostname: &str,
    health: &str,
    color: u32,
    description: &str,
    metrics: &crate::metrics::SystemMetrics,
) -> EmbedBuilder {
    EmbedBuilder::new(format!("Status \u{2014} {hostname} \u{2014} {health}"))
        .description(description)
        .color(color)
        .field(
            "WiFi",
            format!("{} ({})", metrics.wifi_ssid, metrics.wifi_status),
            true,
        )
        .field("IP", &metrics.ip_address, true)
        .field("Load", &metrics.load_avg, true)
        .field(
            "Memory",
            format!("{} / {}", metrics.memory_used, metrics.memory_total),
            true,
        )
        .field(
            "Disk",
            format!(
                "{} / {} ({})",
                metrics.disk_used, metrics.disk_total, metrics.disk_percent
            ),
            true,
        )
        .field("CPU Temp", &metrics.cpu_temp, true)
        .field(
            "Battery",
            format!("{} ({})", metrics.battery_level, metrics.battery_status),
            true,
        )
        .field("Uptime", &metrics.uptime, true)
        .footer(format!("{hostname} seibi"))
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_color_up_suffix_returns_green() {
        assert_eq!(event_color("network-up"), GREEN);
        assert_eq!(event_color("wifi-up"), GREEN);
    }

    #[test]
    fn event_color_boot_returns_green() {
        assert_eq!(event_color("boot"), GREEN);
    }

    #[test]
    fn event_color_down_suffix_returns_red() {
        assert_eq!(event_color("network-down"), RED);
        assert_eq!(event_color("wifi-down"), RED);
    }

    #[test]
    fn event_color_shutdown_returns_red() {
        assert_eq!(event_color("shutdown"), RED);
    }

    #[test]
    fn event_color_unknown_returns_blue() {
        assert_eq!(event_color("status"), BLUE);
        assert_eq!(event_color("custom-event"), BLUE);
        assert_eq!(event_color(""), BLUE);
    }

    #[test]
    fn health_color_healthy_returns_green() {
        assert_eq!(health_color("Healthy"), GREEN);
    }

    #[test]
    fn health_color_critical_returns_red() {
        assert_eq!(health_color("Battery Critical"), RED);
        assert_eq!(health_color("Critical"), RED);
    }

    #[test]
    fn health_color_down_returns_red() {
        assert_eq!(health_color("Network Down"), RED);
    }

    #[test]
    fn health_color_other_returns_orange() {
        assert_eq!(health_color("Low Battery"), ORANGE);
        assert_eq!(health_color("High Load"), ORANGE);
        assert_eq!(health_color("Unknown"), ORANGE);
    }

    #[test]
    fn embed_builder_defaults() {
        let embed = EmbedBuilder::new("test title").build();
        assert_eq!(embed.title, "test title");
        assert_eq!(embed.description, "");
        assert_eq!(embed.color, BLUE);
        assert!(embed.fields.is_empty());
        assert!(embed.footer.is_none());
        assert!(embed.timestamp.is_some());
    }

    #[test]
    fn embed_builder_chaining() {
        let embed = EmbedBuilder::new("title")
            .description("desc")
            .color(RED)
            .field("f1", "v1", true)
            .field("f2", "v2", false)
            .footer("foot")
            .build();

        assert_eq!(embed.title, "title");
        assert_eq!(embed.description, "desc");
        assert_eq!(embed.color, RED);
        assert_eq!(embed.fields.len(), 2);
        assert_eq!(embed.fields[0].name, "f1");
        assert_eq!(embed.fields[0].value, "v1");
        assert!(embed.fields[0].inline);
        assert_eq!(embed.fields[1].name, "f2");
        assert!(!embed.fields[1].inline);
        assert_eq!(embed.footer.unwrap().text, "foot");
    }

    #[test]
    fn embed_serializes_without_empty_fields() {
        let embed = EmbedBuilder::new("t").build();
        let json = serde_json::to_string(&embed).unwrap();
        assert!(!json.contains("fields"));
        assert!(!json.contains("footer"));
    }

    #[test]
    fn embed_serializes_with_fields_and_footer() {
        let embed = EmbedBuilder::new("t")
            .field("k", "v", true)
            .footer("f")
            .build();
        let json = serde_json::to_string(&embed).unwrap();
        assert!(json.contains("\"fields\""));
        assert!(json.contains("\"footer\""));
    }

    #[test]
    fn status_embed_contains_all_metric_fields() {
        let metrics = crate::metrics::SystemMetrics {
            uptime: "1d 2h".into(),
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

        let embed = status_embed("myhost", "Healthy", GREEN, "test desc", &metrics);
        let built = embed.build();

        assert!(built.title.contains("myhost"));
        assert!(built.title.contains("Healthy"));
        assert_eq!(built.description, "test desc");
        assert_eq!(built.color, GREEN);
        assert_eq!(built.fields.len(), 8);
        assert_eq!(built.footer.unwrap().text, "myhost seibi");
    }

    #[test]
    fn now_rfc3339_produces_valid_timestamp() {
        let ts = now_rfc3339();
        assert!(!ts.is_empty());
        assert!(ts.contains('T'));
    }

    #[test]
    fn payload_serializes_correctly() {
        let payload = Payload {
            username: "test".into(),
            embeds: vec![EmbedBuilder::new("t").build()],
        };
        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains("\"username\":\"test\""));
        assert!(json.contains("\"embeds\""));
    }
}
