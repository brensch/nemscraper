use anyhow::{Context, Result};
use clap::Parser;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{fs, fs::File};
use tracing::{debug, error, info, instrument};
use tracing_subscriber::{fmt, EnvFilter};

use notify::{Config, Event, RecommendedWatcher, RecursiveMode, Watcher};

use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::objects::get::GetObjectRequest;
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};

/// Command-line args: local path, bucket, optional prefix
#[derive(Parser, Debug)]
struct Args {
    /// Directory to mirror
    #[arg(long)]
    repo_path: PathBuf,

    /// GCS bucket
    #[arg(long)]
    bucket: String,

    /// Optional prefix inside bucket
    #[arg(long)]
    prefix: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing with console fmt and environment filter
    fmt::Subscriber::builder()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .with_target(false)
        .init();

    info!("Starting GCS mirror service");

    let args = Args::parse();
    // Convert repo_path to an absolute, canonical directory
    let repo_root = fs::canonicalize(&args.repo_path).await.with_context(|| {
        format!(
            "failed to canonicalize repo path: {}",
            args.repo_path.display()
        )
    })?;
    info!(repo_root = %repo_root.display(), bucket = %args.bucket, prefix = ?args.prefix, "Configuration");

    // Authenticate with ADC
    let cfg = ClientConfig::default()
        .with_auth()
        .await
        .context("authenticating to GCS")?;
    let client = Arc::new(Client::new(cfg)); // citeturn2view0

    // Initial sync
    let init_span = tracing::info_span!("initial_sync");
    let _init_enter = init_span.enter();
    info!("Performing initial directory scan...");
    walk_and_check(&client, &repo_root, &args).await?;
    info!("Initial directory scan complete");
    drop(_init_enter);

    // Set up watcher on absolute path
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    let mut watcher = RecommendedWatcher::new(
        move |res: notify::Result<Event>| {
            if let Ok(event) = res {
                let _ = tx.blocking_send(event);
            }
        },
        Config::default(),
    )?;
    watcher.watch(&repo_root, RecursiveMode::Recursive)?; // citeturn8search0
    info!("Watching for file changes on {}", repo_root.display());

    // Handle file events
    let mut event_count: usize = 0;
    while let Some(event) = rx.recv().await {
        event_count += 1;
        info!(event_id = event_count, paths = ?event.paths, "Filesystem event");
        for path in event.paths {
            if path.is_file() {
                if let Err(e) = check_and_upload(&client, &repo_root, &args, &path).await {
                    error!(file = %path.display(), error = %e, "Upload error");
                }
            }
        }
    }

    Ok(())
}

/// Iteratively walk the directory tree and invoke `check_and_upload` on every file.
#[instrument(level = "debug", skip(client, root, args), fields(repo=%root.display()))]
async fn walk_and_check(client: &Arc<Client>, root: &Path, args: &Args) -> Result<()> {
    let mut stack = vec![root.to_path_buf()];
    let mut scanned = 0;
    while let Some(dir) = stack.pop() {
        debug!(dir = %dir.display(), "Scanning directory");
        let mut rd = fs::read_dir(&dir).await?;
        while let Some(entry) = rd.next_entry().await? {
            let p = entry.path();
            scanned += 1;
            if p.is_dir() {
                stack.push(p);
            } else {
                info!(scanned, file = %p.display(), "Checking file");
                check_and_upload(client, root, args, &p).await?;
            }
        }
    }
    info!(total_scanned = scanned, "Directory walk complete");
    Ok(())
}

/// If the GCS object is missing, stream-upload the file.
#[instrument(level = "debug", skip(client, root, args), fields(file=%path.display()))]
async fn check_and_upload(
    client: &Arc<Client>,
    root: &Path,
    args: &Args,
    path: &Path,
) -> Result<()> {
    // skip if not a parquet file
    if !path.extension().map_or(false, |ext| ext == "parquet") {
        debug!(file = %path.display(), "Skipping non-parquet file");
        return Ok(());
    }

    // compute GCS object key relative to canonical root
    let rel = path.strip_prefix(root).with_context(|| {
        format!(
            "path {} is not under root {}",
            path.display(),
            root.display()
        )
    })?;
    let object_name = if let Some(pref) = &args.prefix {
        format!("{}/{}", pref.trim_end_matches('/'), rel.display())
    } else {
        rel.display().to_string()
    };

    // existence check via 0-byte download to avoid list/prefix errors
    use google_cloud_storage::http::objects::download::Range;
    let head_req = GetObjectRequest {
        bucket: args.bucket.clone(),
        object: object_name.clone(),
        ..Default::default()
    };
    let exists = client
        .download_object(&head_req, &Range(Some(0), Some(0)))
        .await
        .is_ok();
    if exists {
        debug!(object = %object_name, "Already in GCS, skipping");
        return Ok(());
    }

    // open the file, grab its length
    let file = File::open(path).await?;
    let len = file.metadata().await?.len();

    // set up media with known length
    let mut media = Media::new(object_name.clone());
    media.content_length = Some(len);

    let upload_req = UploadObjectRequest {
        bucket: args.bucket.clone(),
        ..Default::default()
    };

    // streaming upload (no full buffering)
    client
        .upload_object(&upload_req, file, &UploadType::Simple(media))
        .await
        .with_context(|| format!("uploading {}", object_name))?;

    info!(object = %object_name, bytes = len, "Uploaded to GCS");
    Ok(())
}
