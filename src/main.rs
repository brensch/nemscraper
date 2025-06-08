use anyhow::Result;
use chrono::Utc;

use nemscraper::{
    fetch::{self, urls::spawn_fetch_zip_urls},
    process::{self, split::RowsAndBytes},
};
use reqwest::Client;
use std::time::Duration;

use std::{ffi::OsStr, fs, path::PathBuf, sync::Arc};
use tokio::{
    sync::{
        mpsc::{self, UnboundedSender},
        Mutex,
    },
    task, time,
};
use tracing::{debug, error, info, warn};
use tracing_subscriber::{fmt, EnvFilter};

use crate::history::{
    downloaded::DownloadedRow, processed::ProcessedRow, table_history::TableHistory,
};

mod history;
mod utils;

#[tokio::main]
async fn main() -> Result<()> {
    // 1) init logging
    init_logging();

    info!(
        "=== Starting application (main) with PID={} ===",
        std::process::id()
    );

    // 2) configure dirs
    let client = Client::new();
    let assets = PathBuf::from("assets");
    let zips_dir = assets.join("zips");
    let parquet_dir = assets.join("parquet");
    let history_dir = assets.join("history");
    create_dirs(&[&zips_dir, &parquet_dir, &history_dir])?;
    info!(
        "Ensured directories exist: zips='{:#?}', parquet='{:#?}', history='{:#?}'",
        zips_dir, parquet_dir, history_dir
    );

    // 3) history & lookup store
    // let history = Arc::new(History::new(history_dir)?);
    let downloaded = TableHistory::<DownloadedRow>::new_downloaded(history_dir.clone())?;
    downloaded.vacuum().unwrap();
    downloaded.start_vacuum_loop();
    let processed = TableHistory::<ProcessedRow>::new_processed(history_dir.clone())?;
    processed.vacuum().unwrap();
    processed.start_vacuum_loop();
    info!("Spawned history vacuum loop");

    // 4) channels
    //
    // - processor_tx/processor_rx: carries either Ok(PathBuf) for a ZIP to process or Err(url, err_msg)
    // - url_tx/url_rx: carries newly discovered URLs (strings) to download
    let (processor_tx, processor_rx) =
        mpsc::unbounded_channel::<Result<PathBuf, (String, String)>>();
    let processor_rx_mu = Arc::new(Mutex::new(processor_rx));
    let (url_tx, url_rx) = mpsc::unbounded_channel::<String>();
    let url_rx_mu = Arc::new(Mutex::new(url_rx));

    info!("Created channels: url_tx/url_rx and processor_tx/processor_rx");

    // 5) spawn download workers
    spawn_download_workers(
        3,
        client.clone(),
        zips_dir.clone(),
        processor_tx.clone(),
        url_rx_mu.clone(),
        downloaded.clone(),
    )
    .await;
    info!("Spawned download workers");

    // 6) enqueue existing ZIPs (one‐time)
    spawn_existing_zip_check(zips_dir.clone(), processor_tx.clone());
    info!(
        "Enqueued existing ZIPs from '{:?}' onto processor queue",
        zips_dir
    );

    // 7) spawn URL fetcher
    spawn_fetch_zip_urls(client.clone(), url_tx.clone());
    info!("Spawned URL fetcher task (spawn_fetch_zip_urls)");

    // 8) spawn processor workers
    let num_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    spawn_processor_workers(
        num_cores,
        processor_rx_mu.clone(),
        processed.clone(),
        parquet_dir.clone(),
    )
    .await;
    info!("Spawned {} processor workers", num_cores);

    // 9) drop the senders so that, once the channels drain, workers will exit
    info!("Dropping main()'s copies of processor_tx and url_tx");
    drop(processor_tx);
    drop(url_tx);

    // 10) wait for a Ctrl+C/shutdown signal
    utils::shutdown_signal().await;
    info!("Application shutting down gracefully");
    Ok(())
}

/// Initialize tracing subscriber with environment filter
fn init_logging() {
    let env = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,fetch_all=info,selectors::matching=info"));
    fmt::Subscriber::builder()
        .with_env_filter(env)
        .with_span_events(fmt::format::FmtSpan::CLOSE)
        .init();
    info!("Logging initialized");
}

/// Create any directories that do not already exist
fn create_dirs(dirs: &[&PathBuf]) -> Result<()> {
    for d in dirs {
        fs::create_dir_all(d)?;
        debug!("Ensured directory exists: {:?}", d);
    }
    Ok(())
}

/// Spawn `n_workers` async tasks that read URLs from `url_rx`, download each ZIP,
/// mark it in history as Downloaded, and send either Ok(path) or Err((url, err_msg)) to `processor_tx`.
async fn spawn_download_workers(
    n_workers: usize,
    client: Client,
    zips_dir: PathBuf,
    processor_tx: UnboundedSender<Result<PathBuf, (String, String)>>,
    url_rx_mu: Arc<Mutex<mpsc::UnboundedReceiver<String>>>,
    downloaded: Arc<TableHistory<DownloadedRow>>,
) {
    for worker_id in 0..n_workers {
        let client = client.clone();
        let zips_dir = zips_dir.clone();
        let processor_tx = processor_tx.clone();
        let url_rx_mu = url_rx_mu.clone();
        let downloaded = downloaded.clone();

        task::spawn(async move {
            info!("Download worker {} started", worker_id);
            loop {
                // 1) grab one URL
                debug!("Download worker {} waiting for URL...", worker_id);
                let maybe_url = {
                    let mut guard = url_rx_mu.lock().await;
                    guard.recv().await
                };
                let url = match maybe_url {
                    Some(u) => u,
                    None => {
                        info!(
                            "Download worker {} shutting down: url channel closed",
                            worker_id
                        );
                        break;
                    }
                };

                // 2) compute filename/key for history
                let name = PathBuf::from(&url)
                    .file_name()
                    .unwrap_or_else(|| OsStr::new("unknown"))
                    .to_string_lossy()
                    .to_string();

                info!(
                    "Download worker {} received URL='{}', derived name='{}'",
                    worker_id, url, name
                );

                // 3) skip if already downloaded or processed
                if downloaded.get(name.clone()) {
                    info!(
                        "Download worker {}: already in history (downloaded), skipping URL='{}'",
                        worker_id, name
                    );
                    continue;
                }

                info!(
                    "Download worker {} downloading '{}' {}",
                    worker_id, url, name
                );
                let start_time = Utc::now();
                let path_result = fetch::zips::download_zip(&client, &url, &zips_dir).await;

                match path_result {
                    Ok(path) => {
                        info!(
                            "Download worker {} successfully downloaded to {:?}",
                            worker_id, path
                        );

                        if let Err(e) = downloaded.add(&DownloadedRow {
                            filename: name.clone(),
                            url: url,
                            size_bytes: 123,
                            download_start: start_time,
                            download_end: Utc::now(),
                        }) {
                            error!(
                                "Download worker {}: history.add(state=Downloaded) failed for '{}': {:#}",
                                worker_id, name, e
                            );
                        } else {
                            info!(
                                "Download worker {}: recorded Downloaded for '{}'",
                                worker_id, name
                            );
                        }
                        // send the newly downloaded ZIP path to processor channel
                        let sent = processor_tx.send(Ok(path.clone()));
                        if sent.is_err() {
                            warn!(
                                "Download worker {}: failed to send Ok({:?}) to processor; channel may be closed",
                                worker_id, path
                            );
                        }
                    }
                    Err(e) => {
                        error!(
                            "Download worker {}: download failed after retries for '{}': {}",
                            worker_id, url, e
                        );
                        // send error to processor channel so it can log upstream
                        let sent = processor_tx.send(Err((url.clone(), e.to_string())));
                        if sent.is_err() {
                            warn!(
                                "Download worker {}: failed to send Err(({},{:?})) to processor; channel may be closed",
                                worker_id, url, e
                            );
                        }
                    }
                }
            }
        });
    }
}

/// Look at `zips_dir` and send any ZIPs not yet marked as Processed into `processor_tx`.
fn enqueue_existing_zips(
    zips_dir: &PathBuf,
    processor_tx: &UnboundedSender<Result<PathBuf, (String, String)>>,
) -> Result<()> {
    for entry in std::fs::read_dir(zips_dir)? {
        let path = entry?.path();
        if path.extension() != Some(std::ffi::OsStr::new("zip")) {
            continue;
        }
        info!("Enqueue existing ZIP for processing: {:?}", path);
        if let Err(err) = processor_tx.send(Ok(path.clone())) {
            warn!(
                "enqueue_existing_zips: failed to send Ok({:?}) to processor: {}",
                path, err
            );
        }
    }
    Ok(())
}

/// Spawn a background task that:
///  1. Calls enqueue_existing_zips() immediately,
///  2. Then sleeps for 24 h,
///  3. And repeats.
fn spawn_existing_zip_check(
    zips_dir: PathBuf,
    processor_tx: UnboundedSender<Result<PathBuf, (String, String)>>,
) {
    task::spawn(async move {
        loop {
            // 1) run it now
            info!("doing zip check");
            if let Err(err) = enqueue_existing_zips(&zips_dir, &processor_tx) {
                error!("Failed to enqueue zips: {:?}", err);
            }

            // 2) wait one day before next run
            time::sleep(Duration::from_secs(24 * 60 * 60)).await;
        }
    });
}

/// Spawn `num_workers` tasks that read from `processor_rx`. Each message is either:
///   Ok(PathBuf) → run the blocking split operation, then (conditionally) record history
///   Err((url, err_str)) → log an upstream download error.
///
/// We “close over” `parquet_dir`, `history`, and the receiver in each worker.
async fn spawn_processor_workers(
    num_workers: usize,
    processor_rx: Arc<Mutex<mpsc::UnboundedReceiver<Result<PathBuf, (String, String)>>>>,
    processed: Arc<TableHistory<ProcessedRow>>,
    parquet_dir: PathBuf,
) {
    info!("Spawning {} processor workers", num_workers);

    for worker_id in 0..num_workers {
        let rx = Arc::clone(&processor_rx);
        let processed = Arc::clone(&processed);
        let parquet_dir = parquet_dir.clone();

        task::spawn(async move {
            info!("Processor worker {} started", worker_id);
            loop {
                // 1) lock & receive exactly one message
                debug!("Processor worker {} waiting for message...", worker_id);
                let msg_opt = {
                    let mut guard = rx.lock().await;
                    guard.recv().await
                };

                let msg = match msg_opt {
                    Some(m) => m,
                    None => {
                        info!(
                            "Processor worker {} shutting down: processor channel closed and drained",
                            worker_id
                        );
                        break;
                    }
                };

                match msg {
                    Ok(zip_path) => {
                        let name = zip_path
                            .file_name()
                            .unwrap_or_else(|| OsStr::new("unknown"))
                            .to_string_lossy()
                            .to_string();

                        // skip if already marked as processed in history
                        if processed.get(name.clone()) {
                            info!(
                                "Processor worker {}: '{}' already processed, skipping",
                                worker_id, name
                            );
                            continue;
                        }

                        info!("Processor worker {}: processing '{}'", worker_id, name);

                        // Run the blocking split on a blocking thread
                        let split_path = zip_path.clone();
                        let out_dir = parquet_dir.clone();
                        let history_clone = Arc::clone(&processed);

                        // Spawn blocking work, but catch panics explicitly
                        let start_processing = Utc::now();
                        let join_handle = task::spawn_blocking(move || {
                            process::split::split_zip_to_parquet(&split_path, &out_dir)
                        });

                        debug!(
                            "Processor worker {}: spawn_blocking for '{}'",
                            worker_id, name
                        );

                        let split_res: Result<RowsAndBytes> = match join_handle.await {
                            Ok(inner_res) => {
                                debug!(
                                    "Processor worker {}: split_block returned for '{}'",
                                    worker_id, name
                                );
                                inner_res
                            }
                            Err(join_err) => {
                                error!(
                                    "Processor worker {}: split thread panicked for '{}': {:?}",
                                    worker_id, name, join_err
                                );
                                // Skip to next message rather than panicking
                                continue;
                            }
                        };
                        let end_processing = Utc::now();

                        match split_res {
                            Ok(rows_and_bytes) => {
                                info!(
                                    "Processor worker {}: split succeeded for '{}' ({} rows)",
                                    worker_id, name, rows_and_bytes.rows
                                );
                                if let Err(e) = history_clone.add(&ProcessedRow {
                                    filename: name.clone(),
                                    total_rows: rows_and_bytes.rows,
                                    size_bytes: rows_and_bytes.bytes,
                                    processing_start: start_processing,
                                    processing_end: end_processing,
                                }) {
                                    error!(
                                        "Processor worker {}: history.add(state=Processed) failed for '{}': {:#}",
                                        worker_id, name, e
                                    );
                                    // Continue rather than return, so this worker stays alive
                                    continue;
                                }
                                info!(
                                    "Processor worker {}: recorded Processed for '{}'",
                                    worker_id, name
                                );
                            }
                            Err(e) => {
                                error!(
                                    "Processor worker {}: split '{}' failed with error: {}",
                                    worker_id, name, e
                                );
                                // do not record history on error, but keep worker alive
                                continue;
                            }
                        }
                    }
                    Err((url, err_str)) => {
                        error!(
                            "Processor worker {}: error processing URL '{}': {}",
                            worker_id, url, err_str
                        );
                        // After logging, continue listening for more messages
                        continue;
                    }
                }
            }
        });
    }
}
