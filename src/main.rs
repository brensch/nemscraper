use anyhow::Result;
use nemscraper::{fetch, process};
use reqwest::Client;
use std::{ffi::OsStr, fs, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, Mutex},
    task,
    time::{interval, sleep},
};
use tracing::{debug, error, info};
use tracing_subscriber::{fmt, EnvFilter};

mod history;
use history::History;

#[tokio::main]
async fn main() -> Result<()> {
    // ─── 1) init logging ─────────────────────────────────────────────
    let env = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,fetch_all=info,selectors::matching=info"));
    fmt::Subscriber::builder()
        .with_env_filter(env)
        .with_span_events(fmt::format::FmtSpan::CLOSE)
        .init();
    info!("startup");

    // ─── 2) configure dirs ───────────────────────────────────────────
    let client = Client::new();
    let assets = PathBuf::from("assets");
    let zips_dir = assets.join("zips");
    let parquet_dir = assets.join("parquet");
    let history_dir = assets.join("history");

    for d in [&zips_dir, &parquet_dir, &history_dir] {
        fs::create_dir_all(d)?;
    }

    // ─── 3) history & lookup store ───────────────────────────────────
    let history = Arc::new(History::new(history_dir)?);

    // ─── 4) channels ──────────────────────────────────────────────────
    let (processor_tx, processor_rx) =
        mpsc::unbounded_channel::<Result<PathBuf, (String, String)>>();
    let (url_tx, url_rx) = mpsc::unbounded_channel::<String>();
    let url_rx = Arc::new(Mutex::new(url_rx));

    // ─── 5) spawn download workers ───────────────────────────────────
    for _ in 0..2 {
        let client = client.clone();
        let zips_dir = zips_dir.clone();
        let tx = processor_tx.clone();
        let url_rx = url_rx.clone();
        let history = history.clone();

        task::spawn(async move {
            loop {
                // Lock & recv one URL
                let maybe_url = {
                    let mut guard = url_rx.lock().await;
                    guard.recv().await
                };
                let url = match maybe_url {
                    Some(u) => u,
                    None => break, // channel closed
                };

                let name = PathBuf::from(&url)
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .to_string();

                // Skip if already downloaded/processed
                if history.get(&name, &history::State::Downloaded) {
                    debug!(
                        name = %name,
                        "already in history (downloaded/processed), skipping {}",
                        url
                    );
                    continue;
                }

                info!(name = %name, "downloading {}", url);

                // retry up to 3 times
                let path_result = async {
                    let mut attempt = 0;
                    loop {
                        attempt += 1;
                        match fetch::zips::download_zip(&client, &url, &zips_dir).await {
                            Ok(path) => return Ok(path),
                            Err(e) if attempt < 3 => {
                                error!(
                                    "attempt {}/3 for {} failed: {}, retrying…",
                                    attempt, url, e
                                );
                                sleep(Duration::from_secs((1 << attempt) as u64)).await;
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
                .await;

                match path_result {
                    Ok(path) => {
                        if let Err(e) = history.add(&name, history::State::Downloaded, 1) {
                            error!(name = %name, "history.add failed: {:#}", e);
                        }
                        let _ = tx.send(Ok(path));
                    }
                    Err(e) => {
                        error!("download failed after retries for {}: {}", url, e);
                        let _ = tx.send(Err((url.clone(), e.to_string())));
                    }
                }
            }
        });
    }

    // ─── enqueue existing unprocessed ZIPs one time ───────────────────
    for entry in fs::read_dir(&zips_dir)? {
        let path = entry?.path();
        if path.extension() != Some(OsStr::new("zip")) {
            continue;
        }
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        if history.get(&name, &history::State::Processed) {
            debug!(name = %name, "already processed {}", path.display());
            continue;
        }
        processor_tx.send(Ok(path.clone()))?;
    }

    // ─── 6) scheduler: periodic fetch → queue → process ─────────────
    {
        let url_tx = url_tx.clone();
        let history = history.clone();
        let client = client.clone();

        task::spawn(async move {
            let mut ticker = interval(Duration::from_secs(60));
            loop {
                if let Err(e) = async {
                    info!("vacuuming history");
                    history.vacuum().unwrap();
                    info!("fetching feeds");

                    // a) Fetch new URLs
                    let feeds = fetch::urls::fetch_current_zip_urls(&client).await?;
                    let all_urls_iter = feeds.values().flat_map(|urls| urls.iter().cloned());

                    for url in all_urls_iter {
                        let name = PathBuf::from(&url)
                            .file_name()
                            .unwrap()
                            .to_string_lossy()
                            .to_string();
                        if history.get(&name, &history::State::Downloaded) {
                            debug!(url = %url, "already seen, skipping");
                            continue;
                        }
                        url_tx
                            .send(url.clone())
                            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
                    }
                    Ok::<(), anyhow::Error>(())
                }
                .await
                {
                    error!("scheduler loop failed: {}", e);
                }
                ticker.tick().await;
            }
        });
    }

    // ─── 7) spawn “processor” workers (one per CPU core) ──────────────
    let processor_rx = Arc::new(Mutex::new(processor_rx));
    let num_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    info!("spawning {} processor workers", num_cores);
    for _ in 0..num_cores {
        let rx = processor_rx.clone();
        let history = history.clone();
        let parquet_dir = parquet_dir.clone();

        task::spawn(async move {
            loop {
                // 1) Lock & receive exactly one message
                let msg_opt = {
                    let mut guard = rx.lock().await;
                    guard.recv().await
                };

                let msg = match msg_opt {
                    Some(m) => m,
                    None => break, // channel closed → exit worker
                };

                match msg {
                    Ok(zip_path) => {
                        let name = zip_path.file_name().unwrap().to_string_lossy().to_string();

                        // If already processed, skip
                        if history.get(&name, &history::State::Processed) {
                            debug!(name = %name, "already processed {}, skipping", name);
                            continue;
                        }
                        debug!(file_name = %name, "processing");

                        // Run the blocking split on a dedicated blocking thread
                        let split_path = zip_path.clone();
                        let out_dir = parquet_dir.clone();
                        let history_clone = history.clone();

                        // spawn_blocking returns a JoinHandle<Result<usize, E>>
                        let split_res: Result<i64> = task::spawn_blocking(move || {
                            process::split::split_zip_to_parquet(&split_path, &out_dir)
                        })
                        .await
                        .unwrap(); // unwrap the JoinHandle, giving you the inner Result<usize, _>

                        match split_res {
                            Ok(row_count) => {
                                // Only add to history if the split succeeded
                                if let Err(e) = history_clone.add(
                                    &name,
                                    history::State::Processed,
                                    row_count as i64,
                                ) {
                                    error!("history.add (Processed) failed for {}: {:#}", name, e);
                                } else {
                                    info!(file_name = %name, "processing completed ({} rows)", row_count);
                                }
                            }
                            Err(e) => {
                                error!("split {} failed: {}", name, e);
                                continue;
                            }
                        }
                    }
                    Err((url, _)) => {
                        error!("upstream download error for URL {}", url);
                    }
                }
            }
            // Worker exits once the channel is closed
        });
    }

    // ─── 8) drop the senders so that when queues drain, workers exit ──
    drop(processor_tx);
    drop(url_tx);

    // ─── 9) keep main alive until shutdown (e.g. Ctrl+C). ─────────────
    futures::future::pending::<()>().await;
    Ok(())
}
