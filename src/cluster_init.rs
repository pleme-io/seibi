use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::process::ExitCode;
use tracing::info;

#[derive(ClapArgs)]
pub struct Args {
    /// Cluster name (e.g., ryn-k3s)
    #[arg(long)]
    cluster: String,

    /// Path to SOPS-encrypted secrets file
    #[arg(long, default_value = "secrets.yaml")]
    secrets_file: String,

    /// Path to .sops.yaml config
    #[arg(long, default_value = ".sops.yaml")]
    sops_yaml: String,

    /// Dry-run: generate secrets but don't store them
    #[arg(long)]
    dry_run: bool,
}

pub async fn run(args: Args) -> Result<ExitCode> {
    // Check idempotency: if secrets already exist, show them and exit
    if !args.dry_run && check_existing(&args.cluster, &args.secrets_file).await? {
        return Ok(ExitCode::SUCCESS);
    }

    info!(cluster = %args.cluster, "initializing cluster secrets");

    // 1. Generate age keypair
    let (age_public, age_private) = generate_age_keypair().await?;
    info!(public_key = %age_public, "generated age keypair");

    // 2. Generate k3s server token
    let server_token = generate_random_hex(48).await?;
    info!(token_prefix = %&server_token[..16], "generated server token");

    // 3. Generate admin password for kubeconfig
    let admin_password = generate_random_hex(32).await?;
    info!(pass_prefix = %&admin_password[..16], "generated admin password");

    if args.dry_run {
        println!();
        println!("Cluster: {}", args.cluster);
        println!("  Age public key:   {age_public}");
        println!("  Server token:     {}...", &server_token[..16]);
        println!("  Admin password:   {}...", &admin_password[..16]);
        println!();
        println!("[dry-run] Would store in SOPS and update .sops.yaml");
        return Ok(ExitCode::SUCCESS);
    }

    // 4. Store secrets in SOPS
    sops_set(
        &args.secrets_file,
        &format!("[\"clusters\"][\"{}\"][\"server-token\"]", args.cluster),
        &server_token,
    )
    .await?;
    sops_set(
        &args.secrets_file,
        &format!("[\"clusters\"][\"{}\"][\"age-key\"]", args.cluster),
        &age_private,
    )
    .await?;
    sops_set(
        &args.secrets_file,
        &format!("[\"clusters\"][\"{}\"][\"admin-password\"]", args.cluster),
        &admin_password,
    )
    .await?;

    // 5. Also store as kubeconfig token (referenced by darwin kubeconfig template)
    // Convention: ryn/kubernetes/<cluster>/token
    let host = args.cluster.split('-').next().unwrap_or(&args.cluster);
    sops_set(
        &args.secrets_file,
        &format!("[\"{host}\"][\"kubernetes\"][\"{}\"][\"token\"]", args.cluster),
        &admin_password,
    )
    .await?;
    info!("stored all secrets in SOPS");

    // 6. Update .sops.yaml with VM's age public key
    update_sops_yaml(&args.sops_yaml, &age_public).await?;

    // 7. Re-encrypt with all recipients
    let status = tokio::process::Command::new("sops")
        .args(["updatekeys", "-y", &args.secrets_file])
        .status()
        .await
        .context("running sops updatekeys")?;
    if !status.success() {
        anyhow::bail!("sops updatekeys failed");
    }
    info!("re-encrypted secrets with all recipients");

    println!();
    println!("Cluster '{}' initialized successfully.", args.cluster);
    println!();
    println!("Secrets stored:");
    println!(
        "  clusters/{}/server-token    — k3s server bootstrap token",
        args.cluster
    );
    println!(
        "  clusters/{}/age-key         — VM SOPS age private key",
        args.cluster
    );
    println!(
        "  clusters/{}/admin-password  — k3s admin kubeconfig password",
        args.cluster
    );
    println!(
        "  {host}/kubernetes/{}/token  — kubeconfig user token",
        args.cluster
    );
    println!();
    println!("Next steps:");
    println!("  1. git add .sops.yaml secrets.yaml && git commit");
    println!(
        "  2. nix build .#packages.aarch64-linux.{}-image",
        args.cluster
    );
    println!("  3. nix run .#launch-{}", args.cluster);

    Ok(ExitCode::SUCCESS)
}

async fn check_existing(cluster: &str, secrets_file: &str) -> Result<bool> {
    let output = tokio::process::Command::new("sops")
        .args([
            "-d",
            "--extract",
            &format!("[\"clusters\"][\"{cluster}\"][\"server-token\"]"),
            secrets_file,
        ])
        .output()
        .await
        .context("checking existing secrets")?;

    if output.status.success() {
        let token = String::from_utf8_lossy(&output.stdout);
        let token_preview = if token.len() > 20 {
            &token[..20]
        } else {
            &token
        };
        println!("Cluster '{cluster}' already initialized.");
        println!("  Server token: {token_preview}...");
        println!("  Age key:      present");
        println!();
        println!("To re-initialize, remove the entries first with sops.");
        Ok(true)
    } else {
        Ok(false)
    }
}

async fn generate_age_keypair() -> Result<(String, String)> {
    let output = tokio::process::Command::new("age-keygen")
        .output()
        .await
        .context("running age-keygen")?;

    if !output.status.success() {
        anyhow::bail!(
            "age-keygen failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // age-keygen outputs:
    //   stderr: "Public key: age1xxx..." (capital P)
    //   stdout: "# created: ...\n# public key: age1xxx...\nAGE-SECRET-KEY-xxx..."
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Try stderr first (case-insensitive), then stdout
    let public = stderr
        .lines()
        .chain(stdout.lines())
        .find(|l| l.to_lowercase().contains("public key:"))
        .and_then(|l| {
            // Handle both "Public key: age1..." and "# public key: age1..."
            l.split("key: ").nth(1).or_else(|| l.split("key:").nth(1))
        })
        .ok_or_else(|| anyhow::anyhow!("could not parse age public key"))?
        .trim()
        .to_string();

    let private = stdout
        .lines()
        .find(|l| l.starts_with("AGE-SECRET-KEY-"))
        .ok_or_else(|| anyhow::anyhow!("could not parse age private key"))?
        .trim()
        .to_string();

    Ok((public, private))
}

async fn generate_random_hex(bytes: usize) -> Result<String> {
    let output = tokio::process::Command::new("openssl")
        .args(["rand", "-hex", &bytes.to_string()])
        .output()
        .await
        .context("running openssl rand")?;

    if !output.status.success() {
        anyhow::bail!(
            "openssl rand failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

async fn sops_set(secrets_file: &str, key_path: &str, value: &str) -> Result<()> {
    let status = tokio::process::Command::new("sops")
        .args(["set", secrets_file, key_path, &format!("\"{value}\"")])
        .status()
        .await
        .with_context(|| format!("sops set {key_path}"))?;

    if !status.success() {
        anyhow::bail!("sops set failed for {key_path}");
    }
    Ok(())
}

async fn update_sops_yaml(sops_yaml: &str, age_public: &str) -> Result<()> {
    let output = tokio::process::Command::new("yq")
        .args([".creation_rules[0].age", sops_yaml])
        .output()
        .await
        .context("reading .sops.yaml")?;

    let current = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if current.contains(age_public) {
        info!("age public key already in .sops.yaml");
        return Ok(());
    }

    let new_age = format!("{current},{age_public}");
    let status = tokio::process::Command::new("yq")
        .args([
            "-i",
            &format!(".creation_rules[0].age = \"{new_age}\""),
            sops_yaml,
        ])
        .status()
        .await
        .context("updating .sops.yaml")?;

    if !status.success() {
        anyhow::bail!("yq update failed");
    }
    info!("added VM age public key to .sops.yaml");
    Ok(())
}
