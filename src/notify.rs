use anyhow::Result;
use clap::Args as ClapArgs;
use std::process::ExitCode;
use tracing::info;

use crate::metrics::SystemMetrics;
use crate::webhook::{self, EmbedBuilder, Webhook, GREEN, RED};

#[derive(ClapArgs)]
pub struct Args {
    /// Discord webhook URL
    #[arg(long, env = "SEIBI_WEBHOOK_URL")]
    webhook_url: String,

    /// Node hostname
    #[arg(long, env = "HOSTNAME")]
    hostname: String,

    /// Event name (boot, shutdown, status, or custom)
    event: String,

    /// Optional message
    message: Option<String>,
}

pub async fn run(args: Args) -> Result<ExitCode> {
    let wh = Webhook::new(&args.webhook_url, &args.hostname);
    let metrics = SystemMetrics::collect();
    let health = metrics.health_assessment();
    let color = webhook::health_color(health);

    let embed = match args.event.as_str() {
        "boot" => EmbedBuilder::new(format!("Server Online \u{2014} {}", args.hostname))
            .description(format!(
                "**{} has booted successfully**\n\nHealth: **{health}**",
                args.hostname,
            ))
            .color(GREEN)
            .footer(format!("{} seibi", args.hostname)),

        "shutdown" => {
            EmbedBuilder::new(format!("Server Shutdown \u{2014} {}", args.hostname))
                .description(format!("**{} is shutting down**", args.hostname))
                .color(RED)
                .footer(format!("{} seibi", args.hostname))
        }

        "status" => webhook::status_embed(
            &args.hostname,
            health,
            color,
            args.message.as_deref().unwrap_or("Periodic health check"),
            &metrics,
        ),

        other => {
            let c = webhook::event_color(other);
            let mut b = EmbedBuilder::new(format!("[{}] {other}", args.hostname)).color(c);
            if let Some(msg) = &args.message {
                b = b.description(msg.as_str());
            }
            b.footer(format!("{} seibi", args.hostname))
        }
    };

    wh.send(embed).await?;
    info!(event = %args.event, host = %args.hostname, "notification sent");
    Ok(ExitCode::SUCCESS)
}
