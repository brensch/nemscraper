use anyhow::Result;
use chrono::Utc;
use reqwest::Client;
use std::{ffi::OsStr, fs, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, Mutex},
    task, time,
};
use tracing::{error, info};
use tracing_subscriber::{fmt, EnvFilter};

use crate::history::{
    downloaded::DownloadedRow,
    processed::ProcessedRow,
    table_history::{HistoryRow, TableHistory},
};
use nemscraper::process::split::RowsAndBytes;
use nemscraper::{fetch::urls::spawn_fetch_zip_urls, process};
mod history;
mod utils;

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    info!("Starting (pid={})", std::process::id());

    // directories
    let assets = PathBuf::from("assets");
    let (zips_dir, parquet_dir, hist_dir) = (
        assets.join("zips"),
        assets.join("parquet"),
        assets.join("history"),
    );
    for d in [&zips_dir, &parquet_dir, &hist_dir] {
        fs::create_dir_all(d)?;
    }
    info!("Directories prepared");

    // history stores
    let downloaded = setup_history(&hist_dir, TableHistory::new_downloaded)?;
    let processed = setup_history(&hist_dir, TableHistory::new_processed)?;
    info!("History stores inited");

    // channels
    let (proc_tx, proc_rx) = mpsc::unbounded_channel::<Result<PathBuf, (String, String)>>();
    let proc_rx = Arc::new(Mutex::new(proc_rx));
    let (url_tx, url_rx) = mpsc::unbounded_channel::<String>();
    let url_rx = Arc::new(Mutex::new(url_rx));

    // workers & tasks
    spawn_downloaders(
        3,
        Client::new(),
        zips_dir.clone(),
        proc_tx.clone(),
        url_rx.clone(),
        downloaded.clone(),
    )
    .await;
    spawn_existing_zip_check(zips_dir.clone(), proc_tx.clone());
    spawn_fetch_zip_urls(Client::new(), url_tx.clone());
    spawn_processors(
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1),
        proc_rx.clone(),
        processed.clone(),
        parquet_dir.clone(),
    )
    .await;

    drop(proc_tx);
    drop(url_tx);

    utils::shutdown_signal().await;
    info!("Shutdown complete");
    Ok(())
}

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

async fn spawn_downloaders(
    n: usize,
    client: Client,
    zips_dir: PathBuf,
    proc_tx: mpsc::UnboundedSender<Result<PathBuf, (String, String)>>,
    url_rx: Arc<Mutex<mpsc::UnboundedReceiver<String>>>,
    history: Arc<TableHistory<DownloadedRow>>,
) {
    for id in 0..n {
        let client = client.clone();
        let zips_dir = zips_dir.clone();
        let proc_tx = proc_tx.clone();
        let url_rx = url_rx.clone();
        let history = history.clone();

        task::spawn(async move {
            while let Some(url) = { url_rx.lock().await.recv().await } {
                let name = PathBuf::from(&url)
                    .file_name()
                    .and_then(OsStr::to_str)
                    .unwrap_or("unknown")
                    .to_string();
                if history.get(name.clone()) {
                    continue;
                }

                match nemscraper::fetch::zips::download_zip(&client, &url, &zips_dir).await {
                    Ok(download_result) => {
                        let now = Utc::now();
                        let _ = history.add(&DownloadedRow {
                            filename: name.clone(),
                            url: url.clone(),
                            size_bytes: download_result.size,
                            download_start: now,
                            download_end: now,
                            thread: id as u32,
                        });
                        let _ = proc_tx.send(Ok(download_result.path));
                        info!(
                            file_name = name.clone(),
                            size_bytes = download_result.size,
                            url = url,
                            downloader_id = id,
                            "Downloaded file"
                        );
                    }
                    Err(e) => {
                        error!("Download failed {}: {}", url, e);
                        let _ = proc_tx.send(Err((url.clone(), e.to_string())));
                    }
                }
            }
            info!("Downloader {} exiting", id);
        });
    }
}

fn spawn_existing_zip_check(
    zips_dir: PathBuf,
    proc_tx: mpsc::UnboundedSender<Result<PathBuf, (String, String)>>,
) {
    task::spawn(async move {
        loop {
            if let Err(e) = enqueue_zips(&zips_dir, &proc_tx) {
                error!("Enqueue existing zips failed: {}", e);
            }
            time::sleep(Duration::from_secs(86_400)).await;
        }
    });
}

fn enqueue_zips(
    zips_dir: &PathBuf,
    tx: &mpsc::UnboundedSender<Result<PathBuf, (String, String)>>,
) -> Result<()> {
    for entry in fs::read_dir(zips_dir)? {
        let path = entry?.path();
        if path.extension() == Some(OsStr::new("zip")) {
            let _ = tx.send(Ok(path.clone()));
        }
    }
    Ok(())
}

async fn spawn_processors(
    n: usize,
    proc_rx: Arc<Mutex<mpsc::UnboundedReceiver<Result<PathBuf, (String, String)>>>>,
    history: Arc<TableHistory<ProcessedRow>>,
    parquet_dir: PathBuf,
) {
    for id in 0..n {
        let rx = proc_rx.clone();
        let history = history.clone();
        let parquet_dir = parquet_dir.clone();

        task::spawn(async move {
            while let Some(msg) = { rx.lock().await.recv().await } {
                match msg {
                    Ok(path) => {
                        let name = path
                            .file_name()
                            .and_then(OsStr::to_str)
                            .unwrap_or("unknown")
                            .to_string();
                        if history.get(name.clone()) {
                            continue;
                        }

                        let start = Utc::now();
                        let handle = task::spawn_blocking({
                            let p = path.clone();
                            let d = parquet_dir.clone();
                            move || process::split::split_zip_to_parquet(&p, &d)
                        });

                        match handle.await {
                            Ok(Ok(RowsAndBytes { rows, bytes })) => {
                                let now = Utc::now();
                                let _ = history.add(&ProcessedRow {
                                    filename: name.clone(),
                                    total_rows: rows,
                                    size_bytes: bytes,
                                    processing_start: start,
                                    processing_end: now,
                                    thread: id as u32, // will never get more than u32 threads. unless...
                                });
                                info!(
                                    file_name = name,
                                    total_rows = rows,
                                    size_bytes = bytes,
                                    processor_id = id,
                                    "Processed file"
                                )
                            }
                            Ok(Err(e)) => error!("Processing {} error: {}", name, e),
                            Err(join_err) => error!("Processor {} panicked: {}", name, join_err),
                        }
                    }
                    Err((url, e)) => {
                        error!("Upstream download error for {}: {}", url, e)
                    }
                }
            }
            info!("Processor {} exiting", id);
        });
    }
}
