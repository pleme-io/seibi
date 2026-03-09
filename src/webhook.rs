use anyhow::Result;
use reqwest::Client;
use serde::Serialize;
use tracing::{info, warn};

// ── Discord embed colors ────────────────────────────────────

pub const GREEN: u32 = 0x2e_cc71;
pub const RED: u32 = 0xe7_4c3c;
pub const ORANGE: u32 = 0xe6_7e22;
pub const BLUE: u32 = 0x34_98db;

pub fn event_color(event: &str) -> u32 {
    if event.ends_with("-up") || event == "boot" {
        GREEN
    } else if event.ends_with("-down") || event == "shutdown" {
        RED
    } else {
        BLUE
    }
}

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
    pub fn new(url: &str, hostname: &str) -> Self {
        Self {
            client: Client::new(),
            url: url.to_owned(),
            hostname: hostname.to_owned(),
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

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}
