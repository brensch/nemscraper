use anyhow::Result;
use history::state::State;
use history::store::History;
use nemscraper::{
    fetch::{self, urls::spawn_fetch_zip_urls},
    process,
};
use reqwest::Client;
use std::{ffi::OsStr, fs, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    signal,
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
    task,
    time::interval,
};
use tracing::{debug, error, info};
use tracing_subscriber::{fmt, EnvFilter};

mod history;
mod utils;

#[tokio::main]
async fn main() -> Result<()> {
    // 1) init logging
    init_logging();

    info!("Starting application with PID: {}", std::process::id());

    // 2) configure dirs
    let client = Client::new();
    let assets = PathBuf::from("assets");
    let zips_dir = assets.join("zips");
    let parquet_dir = assets.join("parquet");
    let history_dir = assets.join("history");
    create_dirs(&[&zips_dir, &parquet_dir, &history_dir])?;

    // 3) history & lookup store
    let history = Arc::new(History::new(history_dir)?);
    history.clone().spawn_vacuum_loop();

    // 4) channels
    //
    // - processor_tx/processor_rx: carries either Ok(PathBuf) for a ZIP to process or Err(url, err_msg)
    // - url_tx/url_rx: carries newly discovered URLs (strings) to download
    let (processor_tx, processor_rx) =
        mpsc::unbounded_channel::<Result<PathBuf, (String, String)>>();
    let processor_rx_mu = Arc::new(Mutex::new(processor_rx));
    let (url_tx, url_rx) = mpsc::unbounded_channel::<String>();
    let url_rx_mu = Arc::new(Mutex::new(url_rx));

    // 5) spawn download workers
    spawn_download_workers(
        2,
        client.clone(),
        zips_dir.clone(),
        processor_tx.clone(),
        url_rx_mu.clone(),
        history.clone(),
    )
    .await;

    // 6) enqueue existing ZIPs (one‐time)
    enqueue_existing_zips(&zips_dir, processor_tx.clone())?;

    spawn_fetch_zip_urls(client.clone(), url_tx.clone());

    // 8) spawn processor workers
    let num_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    spawn_processor_workers(
        num_cores,
        processor_rx_mu.clone(),
        Arc::clone(&history),
        parquet_dir.clone(),
    )
    .await;

    // 9) drop the senders so that, once the channels drain, workers will exit
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
}

/// Create any directories that do not already exist
fn create_dirs(dirs: &[&PathBuf]) -> Result<()> {
    for d in dirs {
        fs::create_dir_all(d)?;
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
    history: Arc<History>,
) {
    for worker_id in 0..n_workers {
        let client = client.clone();
        let zips_dir = zips_dir.clone();
        let processor_tx = processor_tx.clone();
        let url_rx_mu = url_rx_mu.clone();
        let history = history.clone();

        task::spawn(async move {
            info!("Download worker {} started", worker_id);
            loop {
                // 1) grab one URL
                let maybe_url = {
                    let mut guard = url_rx_mu.lock().await;
                    guard.recv().await
                };
                let url = match maybe_url {
                    Some(u) => u,
                    None => {
                        info!("Download worker {} shutting down", worker_id);
                        break;
                    }
                };

                // 2) compute filename/key for history
                let name = PathBuf::from(&url)
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .to_string();

                // 3) skip if already downloaded or processed
                if history.get(&name, &State::Downloaded) {
                    debug!(%name, "already in history (downloaded), skipping {}", url);
                    continue;
                }

                info!(%name, worker_id = worker_id, "downloading {}", url);
                let path_result = fetch::zips::download_zip(&client, &url, &zips_dir).await;

                match path_result {
                    Ok(path) => {
                        if let Err(e) = history.add(&name, State::Downloaded, 1) {
                            error!(%name, "history.add (Downloaded) failed: {:#}", e);
                        }
                        // send the newly downloaded ZIP path to processor channel
                        let _ = processor_tx.send(Ok(path));
                    }
                    Err(e) => {
                        error!("download failed after retries for {}: {}", url, e);
                        let _ = processor_tx.send(Err((url.clone(), e.to_string())));
                    }
                }
            }
        });
    }
}

/// On startup, look at `zips_dir` and send any ZIPs not yet marked as Processed into `processor_tx`.
fn enqueue_existing_zips(
    zips_dir: &PathBuf,
    processor_tx: UnboundedSender<Result<PathBuf, (String, String)>>,
) -> Result<()> {
    for entry in fs::read_dir(zips_dir)? {
        let path = entry?.path();
        if path.extension() != Some(OsStr::new("zip")) {
            continue;
        }
        // re‐enqueue any ZIP that hasn’t been Processed
        processor_tx.send(Ok(path.clone()))?;
    }
    Ok(())
}

/// Spawn `num_workers` tasks that read from `processor_rx`. Each message is either:
///   Ok(PathBuf) → run the blocking split operation, then (conditionally) record history
///   Err((url, err_str)) → log an upstream download error.
///
/// We “close over” `parquet_dir`, `history`, and the receiver in each worker.
async fn spawn_processor_workers(
    num_workers: usize,
    processor_rx: Arc<Mutex<mpsc::UnboundedReceiver<Result<PathBuf, (String, String)>>>>,
    history: Arc<History>,
    parquet_dir: PathBuf,
) {
    info!("spawning {} processor workers", num_workers);

    for worker_id in 0..num_workers {
        let rx = Arc::clone(&processor_rx);
        let history = Arc::clone(&history);
        let parquet_dir = parquet_dir.clone();

        task::spawn(async move {
            info!("Processor worker {} started", worker_id);
            loop {
                // 1) lock & receive exactly one message
                let msg_opt = {
                    let mut guard = rx.lock().await;
                    guard.recv().await
                };

                let msg = match msg_opt {
                    Some(m) => m,
                    None => {
                        info!("Processor worker {} shutting down", worker_id);
                        break;
                    }
                };

                match msg {
                    Ok(zip_path) => {
                        let name = zip_path.file_name().unwrap().to_string_lossy().to_string();

                        // skip if already marked as processed in history
                        if history.get(&name, &State::Processed) {
                            debug!(%name, worker_id = worker_id, "already processed, skipping");
                            continue;
                        }
                        info!(%name, worker_id = worker_id, "processing");

                        // Run the blocking split on a blocking thread
                        let split_path = zip_path.clone();
                        let out_dir = parquet_dir.clone();
                        let history_clone = Arc::clone(&history);

                        let split_res: Result<i64> = task::spawn_blocking(move || {
                            process::split::split_zip_to_parquet(&split_path, &out_dir)
                        })
                        .await
                        .unwrap(); // unwrap the JoinHandle

                        match split_res {
                            Ok(row_count) => {
                                // Only record history if we got no error
                                if let Err(e) =
                                    history_clone.add(&name, State::Processed, row_count)
                                {
                                    error!("history.add (Processed) failed for {}: {:#}", name, e);
                                    return;
                                }
                                info!(
                                    file_name = %name,
                                    worker_id = worker_id,
                                    "processing completed ({} rows)",
                                    row_count
                                );
                            }

                            Err(e) => {
                                error!("split {} failed: {}", name, e);
                                // do not record history on error
                            }
                        }
                    }
                    Err((url, _err_str)) => {
                        error!("error processing URL: {}", url);
                    }
                }
            }
        });
    }
}
