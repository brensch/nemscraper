// src/main.rs

use anyhow::Result;
use chrono::{Duration as ChronoDuration, Utc};
use nemscraper::{
    fetch::{urls::spawn_fetch_zip_urls, zips},
    process::{self, split::RowsAndBytes},
};
use reqwest::Client;
use std::{ffi::OsStr, fs, path::PathBuf, sync::Arc, time::Instant};
use tokio::{
    sync::mpsc,
    task::{self, JoinSet},
    time,
};
use tracing::{error, info};
use tracing_subscriber::{fmt, EnvFilter};

// Assuming these modules exist and have the necessary functions.
mod history;
mod utils;

use crate::history::{
    downloaded::DownloadedRow,
    processed::ProcessedRow,
    table_history::{HistoryRow, TableHistory},
};

// A struct to hold the application's shared state, making it easier to pass around.
struct AppState {
    zips_dir: PathBuf,
    parquet_dir: PathBuf,
    downloaded_history: Arc<TableHistory<DownloadedRow>>,
    processed_history: Arc<TableHistory<ProcessedRow>>,
    http_client: Client,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    info!("Starting (pid={})", std::process::id());

    // --- Setup ---
    let (zips_dir, parquet_dir, hist_dir) = setup_directories("assets")?;
    let downloaded_history = setup_history(&hist_dir, TableHistory::new_downloaded)?;
    let processed_history = setup_history(&hist_dir, TableHistory::new_processed)?;

    let state = Arc::new(AppState {
        zips_dir: zips_dir.clone(),
        parquet_dir,
        downloaded_history,
        processed_history,
        http_client: Client::new(),
    });

    // --- Channels ---
    let (url_tx, url_rx) = mpsc::unbounded_channel::<String>();
    let (proc_tx, proc_rx) = mpsc::unbounded_channel::<Result<PathBuf, String>>();

    // --- Pipeline Stages ---
    spawn_fetch_zip_urls(state.http_client.clone(), url_tx);
    spawn_downloader_pool(state.clone(), url_rx, proc_tx.clone());
    spawn_processor_pool(state.clone(), proc_rx);

    // --- Periodic Enqueue Task ---
    spawn_periodic_zip_enqueue(zips_dir, proc_tx);

    // --- Graceful Shutdown ---
    utils::shutdown_signal().await;
    info!("Shutdown complete");
    Ok(())
}

/// Creates directories and returns their paths.
fn setup_directories(root: &str) -> Result<(PathBuf, PathBuf, PathBuf)> {
    let assets = PathBuf::from(root);
    let (zips_dir, parquet_dir, hist_dir) = (
        assets.join("zips"),
        assets.join("parquet"),
        assets.join("history"),
    );
    for d in [&zips_dir, &parquet_dir, &hist_dir] {
        fs::create_dir_all(d)?;
    }
    Ok((zips_dir, parquet_dir, hist_dir))
}

/// Spawns the pool of download workers.
fn spawn_downloader_pool(
    state: Arc<AppState>,
    mut url_rx: mpsc::UnboundedReceiver<String>,
    proc_tx: mpsc::UnboundedSender<Result<PathBuf, String>>,
) {
    const NUM_DOWNLOADERS: usize = 3;
    tokio::spawn(async move {
        let (worker_id_tx, mut worker_id_rx) = mpsc::channel(NUM_DOWNLOADERS);

        // Pre-fill the pool with worker IDs
        for id in 1..=NUM_DOWNLOADERS {
            worker_id_tx.send(id).await.unwrap();
        }

        while let Some(url) = url_rx.recv().await {
            // Wait for a free worker ID
            let worker_id = worker_id_rx.recv().await.unwrap();

            // Clone handles for the new task
            let state_clone = state.clone();
            let proc_tx_clone = proc_tx.clone();
            let worker_id_tx_clone = worker_id_tx.clone();

            task::spawn(async move {
                download_and_log(&url, worker_id, state_clone, proc_tx_clone).await;
                // Return the worker ID to the pool
                worker_id_tx_clone.send(worker_id).await.unwrap();
            });
        }
        info!("URL channel closed. Downloader pool shutting down.");
    });
}

/// Core logic for a single download task.
async fn download_and_log(
    url: &str,
    worker_id: usize,
    state: Arc<AppState>,
    proc_tx: mpsc::UnboundedSender<Result<PathBuf, String>>,
) {
    let name = PathBuf::from(url)
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or("unknown")
        .to_string();

    if state.downloaded_history.get(name.clone()) {
        return;
    }

    info!(file_name = %name, worker_id = worker_id, "Downloading file");
    let start = Instant::now();
    match zips::download_zip(&state.http_client, url, &state.zips_dir).await {
        Ok(result) => {
            let elapsed = start.elapsed();
            let end = Utc::now();
            let start_ts = end - ChronoDuration::microseconds(elapsed.as_micros() as i64);

            let _ = state.downloaded_history.add(&DownloadedRow {
                filename: name.clone(),
                url: url.to_string(),
                size_bytes: result.size,
                download_start: start_ts,
                download_end: end,
                thread: worker_id as u32,
            });

            info!(file_name = %name, size_bytes = result.size, worker_id = worker_id, "Download complete");
            let _ = proc_tx.send(Ok(result.path));
        }
        Err(e) => {
            error!(?url, error = %e, "Download failed");
            let _ = proc_tx.send(Err(url.to_string()));
        }
    }
}

/// Spawns the pool of file processors.
fn spawn_processor_pool(
    state: Arc<AppState>,
    mut proc_rx: mpsc::UnboundedReceiver<Result<PathBuf, String>>,
) {
    let num_workers = std::thread::available_parallelism().map_or(1, |n| n.get());

    tokio::spawn(async move {
        let (worker_id_tx, mut worker_id_rx) = mpsc::channel(num_workers);

        // Pre-fill the pool with worker IDs
        for id in 1..=num_workers {
            worker_id_tx.send(id).await.unwrap();
        }

        let mut tasks = JoinSet::new();

        while let Some(msg) = proc_rx.recv().await {
            // Wait for a free worker ID
            let worker_id = worker_id_rx.recv().await.unwrap();

            // Clone handles for the new task
            let state_clone = state.clone();
            let worker_id_tx_clone = worker_id_tx.clone();

            tasks.spawn(async move {
                process_and_log(msg, worker_id, state_clone).await;
                // Return the worker ID to the pool
                worker_id_tx_clone.send(worker_id).await.unwrap();
            });
        }
        info!("Processing channel closed. Waiting for remaining jobs.");
        while let Some(res) = tasks.join_next().await {
            if let Err(e) = res {
                error!("A processor task failed: {}", e);
            }
        }
        info!("Processor pool shut down.");
    });
}

/// Core logic for a single processing task (runs in a blocking thread).
async fn process_and_log(msg: Result<PathBuf, String>, worker_id: usize, state: Arc<AppState>) {
    let path = match msg {
        Ok(p) => p,
        Err(url) => {
            error!(url, "Skipping processing due to upstream download error");
            return;
        }
    };

    let name = path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "unknown".into());

    info!(file_name = %name, worker_id = worker_id, "Processing file");
    let start = Instant::now();

    // Clone the state for the blocking task. This is the fix.
    let state_for_blocking = state.clone();
    // The actual CPU-bound work is spawned on a blocking thread
    let result = task::spawn_blocking(move || {
        process::split::split_zip_to_parquet(&path, &state_for_blocking.parquet_dir)
    })
    .await;

    match result {
        Ok(Ok(RowsAndBytes { rows, bytes })) => {
            let end = Utc::now();
            let start_ts = end - ChronoDuration::microseconds(start.elapsed().as_micros() as i64);

            // The original `state` is still valid here because we only moved a clone.
            let _ = state.processed_history.add(&ProcessedRow {
                filename: name.clone(),
                total_rows: rows,
                size_bytes: bytes,
                processing_start: start_ts,
                processing_end: end,
                thread: worker_id as u32,
            });

            info!(file_name = %name, total_rows = rows, size_bytes = bytes, worker_id = worker_id, "Processing complete");
        }
        Ok(Err(e)) => {
            error!(file_name = %name, worker_id = worker_id, error = %e, "Processing failed")
        }
        Err(e) => {
            error!(file_name = %name, worker_id = worker_id, error = %e, "Processor task panicked")
        }
    }
}

/// Spawns a task that periodically enqueues existing zip files.
fn spawn_periodic_zip_enqueue(
    zips_dir: PathBuf,
    tx: mpsc::UnboundedSender<Result<PathBuf, String>>,
) {
    tokio::spawn(async move {
        loop {
            info!("Enqueuing existing zip files...");
            if let Err(e) = enqueue_zips(&zips_dir, &tx) {
                error!("Failed to enqueue existing zips: {}", e);
            }
            // Sleep for one day
            time::sleep(time::Duration::from_secs(86_400)).await;
        }
    });
}

// --- Unchanged Helper Functions ---

fn init_logging() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt::Subscriber::builder().with_env_filter(filter).init();
}

fn setup_history<R>(
    dir: &PathBuf,
    builder: impl Fn(PathBuf) -> Result<Arc<TableHistory<R>>>,
) -> Result<Arc<TableHistory<R>>>
where
    R: HistoryRow + Send + Sync + 'static,
{
    let store = builder(dir.clone())?;
    store.vacuum().ok();
    store.start_vacuum_loop();
    Ok(store)
}

fn enqueue_zips(
    zips_dir: &PathBuf,
    tx: &mpsc::UnboundedSender<Result<PathBuf, String>>,
) -> Result<()> {
    for entry in fs::read_dir(zips_dir)? {
        let path = entry?.path();
        if path.extension() == Some(OsStr::new("zip")) {
            let _ = tx.send(Ok(path));
        }
    }
    Ok(())
}
