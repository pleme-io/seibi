use anyhow::{bail, Context, Result};
use clap::Args as ClapArgs;
use nix::unistd::Uid;
use std::io::{self, BufRead, Write};
use std::path::Path;
use std::process::{Command, ExitCode};
use tracing::info;

#[derive(ClapArgs)]
pub struct Args {
    /// LUKS device UUID
    #[arg(long)]
    luks_uuid: String,

    /// TPM2 PCR values for binding (e.g., "0+7")
    #[arg(long, default_value = "0+7")]
    pcrs: String,
}

pub async fn run(args: Args) -> Result<ExitCode> {
    let device = format!("/dev/disk/by-uuid/{}", args.luks_uuid);
    let name = format!("luks-{}", args.luks_uuid);

    println!("=== LUKS Auto-Unlock Setup ===");
    println!();
    println!("Enroll TPM2 for automatic LUKS unlocking.");
    println!("You will need your current LUKS passphrase.");
    println!();
    println!("Device: {device}");
    println!("Name:   {name}");
    println!("PCRs:   {}", args.pcrs);
    println!();

    // Check root
    if !Uid::effective().is_root() {
        bail!("must run as root (sudo)");
    }

    // Check TPM2
    if !Path::new("/dev/tpmrm0").exists() {
        bail!("TPM2 device not found (/dev/tpmrm0). Check BIOS settings.");
    }

    // Check LUKS device
    if !Path::new(&device).exists() {
        bail!("LUKS device not found: {device}");
    }

    // Check systemd-cryptenroll
    let which = Command::new("which")
        .arg("systemd-cryptenroll")
        .output()
        .context("checking for systemd-cryptenroll")?;
    if !which.status.success() {
        bail!("systemd-cryptenroll not found");
    }

    // Show current enrollment
    println!("Current enrollment status:");
    let _ = Command::new("systemd-cryptenroll")
        .arg(&device)
        .status();
    println!();

    // Prompt for confirmation
    print!("Enroll TPM2 for automatic unlocking? (yes/no): ");
    io::stdout().flush()?;
    let mut response = String::new();
    io::stdin().lock().read_line(&mut response)?;
    if response.trim().to_lowercase() != "yes" {
        println!("Cancelled.");
        return Ok(ExitCode::SUCCESS);
    }

    println!();
    println!("Enrolling TPM2 — you will be prompted for your LUKS passphrase.");
    println!();

    let status = Command::new("systemd-cryptenroll")
        .arg("--tpm2-device=auto")
        .arg(format!("--tpm2-pcrs={}", args.pcrs))
        .arg(&device)
        .status()
        .context("running systemd-cryptenroll")?;

    if status.success() {
        println!();
        println!("TPM2 enrollment successful.");
        println!();
        println!("Notes:");
        println!("  1. Your original passphrase still works — keep it safe");
        println!("  2. UEFI/firmware changes will invalidate TPM2 unlock");
        println!("  3. Re-enroll after firmware updates if needed");
        info!(device = %device, pcrs = %args.pcrs, "TPM2 enrollment complete");
        Ok(ExitCode::SUCCESS)
    } else {
        bail!("systemd-cryptenroll failed (exit {})", status);
    }
}
