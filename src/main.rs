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
    time::{interval, sleep},
};
use tracing::{debug, error, info};
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
    let schema_proposals = assets.join("schema_proposals");
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
        &schema_proposals,
    ] {
        fs::create_dir_all(d)?;
    }

    // ─── 3) history & lookup store ───────────────────────────────────
    let history = Arc::new(History::new(&history_dir)?);
    info!("initial schema fetch → {}", schemas_dir.display());
    schema::fetch_all(&client, &schemas_dir).await?;
    let dirs = vec![&schemas_dir, &schema_proposals];
    let lookup = Arc::new(Mutex::new(extract_column_types(dirs)?));

    // ─── 4) channels ──────────────────────────────────────────────────
    let (processor_tx, mut processor_rx) =
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
            while let Some(url) = url_rx.lock().await.recv().await {
                let name = PathBuf::from(&url)
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .to_string();

                // if the zip was already downloaded, skip
                match history.get_one(&name, Event::Downloaded) {
                    Ok(Some(path)) => {
                        info!(name=%name, "found already downloaded in download loop {:?}, skipping {}", path, url);
                        continue;
                    }
                    Ok(None) => {
                        // not downloaded yet, fall through to download
                    }
                    Err(e) => {
                        error!(name=%name, "history lookup failed, proceeding with download: {}", e);
                    }
                }

                info!(name=%name, "downloading {}", url);

                // retry up to 3 times with 2s backoff
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
                                sleep(Duration::from_secs(1 * attempt)).await;
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

    // ─── enqueue existing unprocessed zips one time. ─────────
    for entry in fs::read_dir(&zips_dir)? {
        let path = entry?.path();
        if path.extension() != Some(OsStr::new("zip")) {
            continue;
        }
        processor_tx.send(Ok(path.clone()))?;
    }

    // ─── 6) scheduler: periodic fetch → queue → process ─────────────
    {
        let lookup = lookup.clone();
        let url_tx = url_tx.clone();
        let history = history.clone();
        let client = client.clone();
        let schemas_dir = schemas_dir.clone();
        let schema_proposals = schema_proposals.clone();
        let zips_dir = zips_dir.clone();
        let failed_dir = failed_dir.clone();
        let processor_tx = processor_tx.clone();

        task::spawn(async move {
            let mut ticker = interval(Duration::from_secs(60));
            // immediate first tick triggers once; no separate init
            loop {
                if let Err(e) = async {
                    info!("scheduler tick");

                    // ─── a) refresh schemas ─────────────────────────
                    info!("fetch schemas → {}", schemas_dir.display());
                    schema::fetch_all(&client, &schemas_dir).await?;
                    let dirs = vec![&schemas_dir, &schema_proposals];
                    let new_map = extract_column_types(dirs)?;
                    *lookup.lock().await = new_map;

                    // ─── b) re-enqueue failed zips ───────────────────
                    for entry in fs::read_dir(&failed_dir)? {
                        let path = entry?.path();
                        if path.extension() != Some(OsStr::new("zip")) {
                            continue;
                        }
                        let name = path.file_name().unwrap().to_string_lossy().to_string();
                        let dest = zips_dir.join(&name);
                        fs::rename(&path, &dest)?;
                        info!("re-queued failed zip {}", name);
                        // record retried event
                        history.record_event(&name, Event::Retried)?;
                        // sending to tx triggers processor workers
                        processor_tx.send(Ok(dest.clone()))?;
                    }

                    // ─── d) fetch new URLs and enqueue ────────────────
                    let feeds = fetch::urls::fetch_current_zip_urls(&client).await?;
                    let already_downloaded = history
                        .get_all(Event::Downloaded)
                        .expect("couldn't get downloaded history");

                    //
                    let unique_urls: HashSet<String> = feeds
                        .values()
                        .flat_map(|urls| urls.iter().cloned())
                        .collect();

                    for url in unique_urls {
                        // check if the csv filename in the url is already downloaded
                        let url_path = PathBuf::from(&url);
                        let name = url_path.file_name().unwrap().to_string_lossy().to_string();
                        if already_downloaded.contains(&name) {
                            debug!(url=%url, "already downloaded, skipping");
                            continue;
                        }
                        // sending to url_tx triggers download workers to download
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

    // ─── 7) single-threaded processor ─────────────────────────────────
    while let Some(msg) = processor_rx.recv().await {
        match msg {
            Ok(zip_path) => {
                let name = zip_path.file_name().unwrap().to_string_lossy().to_string();

                // if the zip was already processed, skip
                match history.get_one(&name, Event::Processed) {
                    Ok(Some(path)) => {
                        debug!(name=%name, "already processed at {:?}, skipping {}", path, &name);
                        continue;
                    }
                    Ok(None) => {
                        // not processed yet, fall through to process
                    }
                    Err(e) => {
                        error!(name=%name, "history lookup failed, proceeding with processing: {}", e);
                    }
                }
                info!("processing {}", name);

                let lookup_map = lookup.lock().await.clone();
                let temp_out = tmp_dir.clone();
                let split_temp_out = tmp_dir.clone();
                let final_out = parquet_dir.clone();
                let history = history.clone();
                let split_path = zip_path.clone();
                let schema_proposals = schema_proposals.clone();

                // split in blocking thread
                let split_res = task::spawn_blocking(move || {
                    let arc = Arc::new(lookup_map);
                    process::split::split_zip_to_parquet(
                        &split_path,
                        &split_temp_out,
                        arc,
                        &schema_proposals,
                    )
                })
                .await?;

                if let Err(e) = split_res {
                    error!("split {} failed: {}", name, e);
                    let _ = fs::rename(&zip_path, failed_dir.join(&name));
                    continue;
                }

                // move .parquet files once write is completed
                if let Err(e) = (|| -> Result<()> {
                    for entry in fs::read_dir(&temp_out)? {
                        let path = entry?.path();
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

                // delete zip file
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
