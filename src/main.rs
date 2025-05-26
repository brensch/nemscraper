// src/main.rs

use anyhow::Result;
use nemscraper::{
    fetch, process,
    schema::{self, extract_column_types},
};
use reqwest::Client;
use std::{
    collections::{HashMap, HashSet},
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{mpsc, Mutex},
    task,
    time::{interval, sleep, Instant},
};
use tracing::{error, info, level_filters::LevelFilter};
use tracing_subscriber::{fmt, EnvFilter};

mod history;
use history::{Event, History};

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
    let schemas_dir = assets.join("schemas");
    let zips_dir = assets.join("zips");
    let parquet_dir = assets.join("parquet");
    let tmp_dir = assets.join("parquet_tmp");
    let history_dir = assets.join("history");
    let failed_dir = assets.join("failed_zips");

    for d in [
        &schemas_dir,
        &zips_dir,
        &parquet_dir,
        &tmp_dir,
        &history_dir,
        &failed_dir,
    ] {
        fs::create_dir_all(d)?;
    }

    // ─── 3) initial schema fetch & extract ──────────────────────────
    info!("fetch schemas → {}", schemas_dir.display());
    schema::fetch_all(&client, &schemas_dir).await?;
    let init_map: HashMap<String, HashMap<String, HashSet<String>>> =
        extract_column_types(&schemas_dir)?;
    let lookup = Arc::new(Mutex::new(init_map));

    // ─── 4) history manager & load processed ZIPs ────────────────────
    let history = Arc::new(History::new(&history_dir)?);
    let processed = Arc::new(Mutex::new(history.load_event_names(Event::Processed)?));

    // ─── 5) channels ──────────────────────────────────────────────────
    let (tx, mut rx) = mpsc::unbounded_channel::<Result<PathBuf, (String, String)>>();
    let (url_tx, url_rx) = mpsc::unbounded_channel::<String>();
    let url_rx = Arc::new(Mutex::new(url_rx));

    // ─── 6) queue existing ZIPs ───────────────────────────────────────
    {
        let seen = processed.lock().await.clone();
        for entry in fs::read_dir(&zips_dir)? {
            let path = entry?.path();
            if path.extension() == Some(OsStr::new("zip")) {
                let name = path.file_name().unwrap().to_string_lossy().to_string();
                if !seen.contains(&name) {
                    tx.send(Ok(path.clone()))?;
                }
            }
        }
    }

    // ─── 7) spawn download workers ───────────────────────────────────
    for _ in 0..2 {
        let client = client.clone();
        let zips_dir = zips_dir.clone();
        let tx = tx.clone();
        let url_rx = url_rx.clone();
        let history = history.clone();

        task::spawn(async move {
            while let Some(url) = url_rx.lock().await.recv().await {
                let name = PathBuf::from(&url)
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .to_string();
                info!(name=%name, "downloading {}", url);

                // retry up to 3 times with 1s backoff
                let path_result = async {
                    let mut attempt = 0;
                    loop {
                        attempt += 1;
                        match fetch::zips::download_zip(&client, &url, &zips_dir).await {
                            Ok(path) => return Ok(path),
                            Err(e) if attempt < 3 => {
                                error!(
                                    "attempt {}/3 for {} failed: {}, retrying...",
                                    attempt, url, e
                                );
                                sleep(Duration::from_secs(2)).await;
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
                .await;

                match path_result {
                    Ok(path) => {
                        if let Err(e) = history.record_event(&name, Event::Downloaded) {
                            error!("history.record_event failed: {}", e);
                        }
                        let _ = tx.send(Ok(path));
                    }
                    Err(e) => {
                        error!("download failed after retries {}: {}", url, e);
                        let _ = tx.send(Err((url.clone(), e.to_string())));
                    }
                }
            }
        });
    }

    // ─── 8) scheduler: every 60s ──────────────────────────────────────
    {
        let lookup = lookup.clone();
        let url_tx = url_tx.clone();
        let history = history.clone();
        let client = client.clone();
        let schemas_dir = schemas_dir.clone();
        let zips_dir = zips_dir.clone();
        let failed_dir = failed_dir.clone();
        let tx = tx.clone();

        task::spawn(async move {
            let mut ticker = interval(Duration::from_secs(60));
            loop {
                if let Err(e) = async {
                    info!("scheduler tick");
                    // refresh schemas
                    schema::fetch_all(&client, &schemas_dir).await?;
                    let new_map = extract_column_types(&schemas_dir)?;
                    *lookup.lock().await = new_map;

                    // re-enqueue failed splits
                    for entry in fs::read_dir(&failed_dir)? {
                        let path = entry?.path();
                        if path.extension() == Some(OsStr::new("zip")) {
                            let name = path.file_name().unwrap().to_string_lossy().to_string();
                            let dest = zips_dir.join(&name);
                            fs::rename(&path, &dest)?;
                            info!("re-queued failed zip {}", name);
                            tx.send(Ok(dest.clone()))
                                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
                        }
                    }

                    // fetch new URLs and skip already-processed
                    let feeds = fetch::urls::fetch_current_zip_urls(&client).await?;
                    let downloaded = history.load_event_names(Event::Downloaded)?;
                    for url in feeds.values().flatten() {
                        let name = Path::new(url)
                            .file_name()
                            .unwrap()
                            .to_string_lossy()
                            .to_string();
                        if !downloaded.contains(&name) {
                            url_tx
                                .send(url.clone())
                                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
                        }
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

    // ─── 9) single-threaded processor ─────────────────────────────────
    while let Some(msg) = rx.recv().await {
        match msg {
            Ok(zip_path) => {
                let name = zip_path.file_name().unwrap().to_string_lossy().to_string();
                info!("processing {}", name);

                let lookup_map = lookup.lock().await.clone();
                let temp_out = tmp_dir.clone();
                let temp_out_split = temp_out.clone();
                let final_out = parquet_dir.clone();
                let history = history.clone();
                let split_path = zip_path.clone();

                // split in blocking thread
                let split_res = task::spawn_blocking(move || {
                    let arc = Arc::new(lookup_map);
                    process::split::split_zip_to_parquet(&split_path, &temp_out_split, arc)
                })
                .await?;

                if let Err(e) = split_res {
                    error!("split {} failed: {}", name, e);
                    let _ = fs::rename(&zip_path, failed_dir.join(&name));
                    continue;
                }

                // move .parquet files
                if let Err(e) = (|| -> Result<()> {
                    for entry in fs::read_dir(&temp_out)? {
                        let entry = entry?;
                        let path = entry.path();
                        fs::rename(&path, final_out.join(path.file_name().unwrap()))?;
                    }
                    Ok(())
                })() {
                    error!("moving parquet for {} failed: {}", name, e);
                }

                // record processed event
                if let Err(e) = history.record_event(&name, Event::Processed) {
                    error!("record_event processed failed: {}", e);
                }

                //  delete zip file
                if let Err(e) = fs::remove_file(&zip_path) {
                    error!("failed to delete zip {}: {}", name, e);
                } else {
                    info!("deleted zip {}", name);
                }
            }
            Err((url, _)) => {
                error!("upstream download error for URL {}", url);
            }
        }
    }

    Ok(())
}
