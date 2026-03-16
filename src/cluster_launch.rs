use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use tracing::info;

#[derive(ClapArgs)]
pub struct Args {
    /// Cluster name (e.g., ryn-k3s)
    #[arg(long, default_value = "ryn-k3s")]
    cluster: String,

    /// Number of vCPUs
    #[arg(long, default_value = "4")]
    cpus: u32,

    /// Memory in MiB
    #[arg(long, default_value = "8192")]
    memory: u32,

    /// Data disk size (sparse) e.g. "50G"
    #[arg(long, default_value = "50G")]
    disk_size: String,

    /// Host port for K8s API (forwarded to guest 6443)
    #[arg(long, default_value = "6443")]
    api_port: u16,

    /// Host port for SSH (forwarded to guest 22)
    #[arg(long, default_value = "2222")]
    ssh_port: u16,

    /// Path to SOPS-encrypted secrets file
    #[arg(long, default_value = "secrets.yaml")]
    secrets_file: String,

    /// Skip seed disk provisioning (use existing)
    #[arg(long)]
    no_seed: bool,
}

pub async fn run(args: Args) -> Result<ExitCode> {
    let data_dir = data_dir(&args.cluster)?;
    let data_disk = data_dir.join("data.raw");
    let seed_disk = data_dir.join("seed.img");
    let root_copy = data_dir.join("root.raw");

    // 1. Build/locate root disk image
    info!(cluster = %args.cluster, "locating root disk image");
    let root_disk = locate_root_disk(&args.cluster).await?;

    // 2. Create data directory
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("creating {}", data_dir.display()))?;

    // 3. Create data disk if needed
    if !data_disk.exists() {
        info!(size = %args.disk_size, "creating sparse data disk");
        create_sparse_disk(&data_disk, &args.disk_size).await?;
    }

    // 4. Create seed disk with secrets
    if !args.no_seed {
        info!("provisioning seed disk with cluster secrets");
        create_seed_disk(&seed_disk, &args.cluster, &args.secrets_file).await?;
    }

    // 5. Extract kernel, initrd, init from root image
    info!("extracting kernel and initrd from image");
    let (kernel, initrd, init) = extract_boot_files(&root_disk).await?;

    // 6. Create writable root copy
    info!("creating writable root disk copy");
    std::fs::copy(&root_disk, &root_copy)
        .with_context(|| format!("copying {} to {}", root_disk.display(), root_copy.display()))?;

    // 7. Launch VM
    println!();
    println!("Starting {} VM...", args.cluster);
    println!("  Root: {} (vda)", root_copy.display());
    println!("  Data: {} (vdb)", data_disk.display());
    println!("  Seed: {} (vdc)", seed_disk.display());
    println!("  K8s API: localhost:{}", args.api_port);
    println!("  SSH: localhost:{}", args.ssh_port);
    println!();

    let net_config = format!(
        "nat,localPort={}:guestPort=6443,localPort={}:guestPort=22",
        args.api_port, args.ssh_port
    );

    let status = tokio::process::Command::new("vfkit")
        .args([
            "--cpus",
            &args.cpus.to_string(),
            "--memory",
            &args.memory.to_string(),
            "--bootloader",
            &format!(
                "linux,kernel={},initrd={},cmdline=console=hvc0 root=/dev/vda init={}",
                kernel.display(),
                initrd.display(),
                init
            ),
            "--device",
            &format!("virtio-blk,path={}", root_copy.display()),
            "--device",
            &format!("virtio-blk,path={}", data_disk.display()),
            "--device",
            &format!("virtio-blk,path={}", seed_disk.display()),
            "--device",
            &format!("virtio-net,{net_config}"),
            "--device",
            "virtio-serial,stdio",
        ])
        .status()
        .await
        .context("running vfkit")?;

    if !status.success() {
        anyhow::bail!("vfkit exited with {status}");
    }

    Ok(ExitCode::SUCCESS)
}

fn data_dir(cluster: &str) -> Result<PathBuf> {
    let home = std::env::var("HOME").context("HOME not set")?;
    Ok(PathBuf::from(home)
        .join(".local/share")
        .join(cluster))
}

async fn locate_root_disk(cluster: &str) -> Result<PathBuf> {
    let output = tokio::process::Command::new("nix")
        .args([
            "build",
            &format!(".#packages.aarch64-linux.{cluster}-image"),
            "--no-link",
            "--print-out-paths",
        ])
        .output()
        .await
        .context("building root disk image")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("nix build failed: {stderr}");
    }

    let store_path = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let img_path = PathBuf::from(&store_path).join("nixos.img");
    if !img_path.exists() {
        anyhow::bail!("root disk not found at {}", img_path.display());
    }
    Ok(img_path)
}

async fn create_sparse_disk(path: &Path, size: &str) -> Result<()> {
    let status = tokio::process::Command::new("dd")
        .args([
            "if=/dev/zero",
            &format!("of={}", path.display()),
            "bs=1",
            "count=0",
            &format!("seek={size}"),
        ])
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .context("creating sparse disk")?;

    if !status.success() {
        anyhow::bail!("dd failed creating sparse disk");
    }
    Ok(())
}

async fn create_seed_disk(
    seed_path: &Path,
    cluster: &str,
    secrets_file: &str,
) -> Result<()> {
    // Create 2MB FAT image
    let status = tokio::process::Command::new("dd")
        .args([
            "if=/dev/zero",
            &format!("of={}", seed_path.display()),
            "bs=1M",
            "count=2",
        ])
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .context("creating seed disk")?;
    if !status.success() {
        anyhow::bail!("dd failed creating seed disk");
    }

    // Format as FAT
    let status = tokio::process::Command::new("newfs_msdos")
        .args(["-F", "12", &seed_path.display().to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .context("formatting seed disk as FAT")?;
    if !status.success() {
        anyhow::bail!("newfs_msdos failed");
    }

    // Mount seed disk
    let mount_dir = tempfile::tempdir().context("creating temp mount dir")?;
    let mount_path = mount_dir.path();

    let status = tokio::process::Command::new("hdiutil")
        .args([
            "attach",
            "-mountpoint",
            &mount_path.display().to_string(),
            &seed_path.display().to_string(),
            "-nobrowse",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .context("mounting seed disk")?;
    if !status.success() {
        anyhow::bail!("hdiutil attach failed for seed disk");
    }

    // Write age key
    let age_key = sops_extract(
        secrets_file,
        &format!("[\"clusters\"][\"{cluster}\"][\"age-key\"]"),
    )
    .await?;
    std::fs::write(mount_path.join("age-key.txt"), &age_key).context("writing age key")?;

    // Write k3s admin password (passwd format)
    let admin_pass = sops_extract(
        secrets_file,
        &format!("[\"clusters\"][\"{cluster}\"][\"admin-password\"]"),
    )
    .await?;
    let passwd_line = format!("{admin_pass},admin,admin,system:masters\n");
    std::fs::write(mount_path.join("k3s-passwd"), &passwd_line).context("writing k3s passwd")?;

    // Write server token
    let server_token = sops_extract(
        secrets_file,
        &format!("[\"clusters\"][\"{cluster}\"][\"server-token\"]"),
    )
    .await?;
    std::fs::write(mount_path.join("server-token"), &server_token)
        .context("writing server token")?;

    // Unmount
    let _ = tokio::process::Command::new("hdiutil")
        .args(["detach", &mount_path.display().to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await;

    info!("seed disk provisioned");
    Ok(())
}

async fn sops_extract(secrets_file: &str, key_path: &str) -> Result<String> {
    let output = tokio::process::Command::new("sops")
        .args(["-d", "--extract", key_path, secrets_file])
        .output()
        .await
        .with_context(|| format!("sops extract {key_path}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("sops extract failed for {key_path}: {stderr}");
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

async fn extract_boot_files(root_disk: &Path) -> Result<(PathBuf, PathBuf, String)> {
    let tmp_dir = tempfile::tempdir().context("creating temp dir")?;
    let mount_point = tmp_dir.path().join("mnt");
    std::fs::create_dir_all(&mount_point).context("creating mount point")?;

    // Mount root image
    let status = tokio::process::Command::new("hdiutil")
        .args([
            "attach",
            "-mountpoint",
            &mount_point.display().to_string(),
            "-readonly",
            &root_disk.display().to_string(),
            "-nobrowse",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .context("mounting root disk")?;
    if !status.success() {
        anyhow::bail!("hdiutil attach failed for root disk");
    }

    let nix_store = mount_point.join("nix/store");

    // Find kernel
    let kernel = find_file(&nix_store, &["bzImage", "Image"])?;
    // Find initrd
    let initrd = find_file(&nix_store, &["initrd"])?;
    // Find init
    let init = find_init(&nix_store)?;

    // Copy kernel and initrd to persistent temp files
    let kernel_out = tmp_dir.path().join("kernel");
    let initrd_out = tmp_dir.path().join("initrd");
    std::fs::copy(&kernel, &kernel_out).context("copying kernel")?;
    std::fs::copy(&initrd, &initrd_out).context("copying initrd")?;

    // Unmount
    let _ = tokio::process::Command::new("hdiutil")
        .args(["detach", &mount_point.display().to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await;

    // Leak the temp dir so files persist until process exits
    #[allow(deprecated)]
    let tmp_path = tmp_dir.into_path();

    Ok((
        tmp_path.join("kernel"),
        tmp_path.join("initrd"),
        init,
    ))
}

fn find_file(nix_store: &Path, names: &[&str]) -> Result<PathBuf> {
    for entry in std::fs::read_dir(nix_store).context("reading nix store")? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            for name in names {
                let candidate = entry.path().join(name);
                if candidate.exists() {
                    return Ok(candidate);
                }
            }
        }
    }
    anyhow::bail!(
        "could not find {} in nix store",
        names.join(" or ")
    )
}

fn find_init(nix_store: &Path) -> Result<String> {
    for entry in std::fs::read_dir(nix_store).context("reading nix store")? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.contains("nixos-system-") && entry.file_type()?.is_dir() {
            let init = entry.path().join("init");
            if init.exists() {
                return Ok(init.display().to_string());
            }
        }
    }
    anyhow::bail!("could not find nixos-system init in nix store")
}
