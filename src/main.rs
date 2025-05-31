use anyhow::Result;
use nemscraper::{fetch, process, schema};
use rand::seq::IteratorRandom;
use rand::thread_rng;
use reqwest::Client;
use std::{collections::HashSet, ffi::OsStr, fs, path::PathBuf, sync::Arc, time::Duration};
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
    let schemas_dir = assets.join("schemas");
    let zips_dir = assets.join("zips");
    let parquet_dir = assets.join("parquet");
    let history_dir = assets.join("history");

    for d in [&schemas_dir, &zips_dir, &parquet_dir, &history_dir] {
        fs::create_dir_all(d)?;
    }

    // ─── 3) history & lookup store ───────────────────────────────────
    // Now History only holds a single in-memory HashSet<String> of “seen” filenames.
    let history = Arc::new(History::new(&history_dir)?);
    info!("initial schema fetch → {}", schemas_dir.display());
    let schema_store = Arc::new(schema::SchemaStore::new(&schemas_dir)?);

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

                // If this ZIP has already been seen (downloaded or processed), skip.
                if history.get(&name, &history::State::Downloaded) {
                    debug!(
                        name = %name,
                        "already in history (downloaded/processed), skipping download of {}",
                        url
                    );
                    continue;
                }

                info!(name = %name, "downloading {}", url);

                // retry up to 3 times with exponential backoff
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
                                sleep(Duration::from_secs((1 << attempt) as u64)).await;
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
                .await;

                match path_result {
                    Ok(path) => {
                        // Mark as “seen”
                        if let Err(e) = history.add(&name, history::State::Downloaded) {
                            error!(name = %name, "history.add failed: {}", e);
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

    // ─── enqueue existing unprocessed zips one time ─────────────────
    for entry in fs::read_dir(&zips_dir)? {
        let path = entry?.path();
        if path.extension() != Some(OsStr::new("zip")) {
            continue;
        }
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        if history.get(&name, &history::State::Processed) {
            // Already processed.
            debug!(name = %name, "already processed {}", path.display());
            continue;
        }
        processor_tx.send(Ok(path.clone()))?;
    }

    // // ─── 6) scheduler: periodic fetch → queue → process ─────────────
    // {
    //     let url_tx = url_tx.clone();
    //     let history = history.clone();
    //     let client = client.clone();

    //     task::spawn(async move {
    //         let mut ticker = interval(Duration::from_secs(60));
    //         // loop {
    //         if let Err(e) = async {
    //             info!("fetching feeds");

    //             // ─── b) fetch new URLs and enqueue ────────────────
    //             let feeds = fetch::urls::fetch_current_zip_urls(&client).await?;
    //             // 1) Flatten all URLs into a single iterator
    //             let all_urls_iter = feeds.values().flat_map(|urls| urls.iter().cloned());

    //             // 2) Use `choose_multiple` to sample up to 1,000 items at random
    //             let mut rng = thread_rng();
    //             let random_sample: Vec<String> = all_urls_iter.choose_multiple(&mut rng, 100);

    //             // 3) Collect into a HashSet<String> (deduplicates any repeats)
    //             let unique_urls: HashSet<String> = random_sample.into_iter().collect();
    //             info!(
    //                 "retrieved feeds, downloading {} unique URLs",
    //                 unique_urls.len()
    //             );

    //             for url in unique_urls {
    //                 let url_path = PathBuf::from(&url);
    //                 let name = url_path.file_name().unwrap().to_string_lossy().to_string();
    //                 if history.get(&name, &history::State::Downloaded) {
    //                     debug!(url = %url, "already seen, skipping");
    //                     continue;
    //                 }
    //                 url_tx
    //                     .send(url.clone())
    //                     .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    //             }

    //             Ok::<(), anyhow::Error>(())
    //         }
    //         .await
    //         {
    //             error!("scheduler loop failed: {}", e);
    //         }

    //         ticker.tick().await;
    //         drop(processor_tx)
    //         // }
    //     });
    // }

    // ─── 7) single‐threaded processor ─────────────────────────────────
    while let Some(msg) = processor_rx.recv().await {
        match msg {
            Ok(zip_path) => {
                let name = zip_path.file_name().unwrap().to_string_lossy().to_string();

                // If this ZIP was already seen (downloaded or processed), skip.
                if history.get(&name, &history::State::Processed) {
                    debug!(name = %name, "already seen, skipping processing of {}", name);
                    continue;
                }
                debug!(file_name = name, "processing");

                // We spawn the blocking split function on a separate thread:
                let out_dir = parquet_dir.clone();
                let history_clone = history.clone();
                let split_path = zip_path.clone();
                let schema_store = Arc::clone(&schema_store);

                let split_res = task::spawn_blocking(move || {
                    process::split::split_zip_to_parquet(&split_path, &out_dir, schema_store)
                })
                .await?;

                if let Err(e) = split_res {
                    error!("split {} failed: {}", name, e);
                    continue;
                }

                // Mark as seen (processed)
                if let Err(e) = history_clone.add(&name, history::State::Processed) {
                    error!("history.add (processed) failed for {}: {}", name, e);
                }

                info!(file_name = name, "processing completed");
            }
            Err((url, _)) => {
                error!("upstream download error for URL {}", url);
            }
        }
    }

    Ok(())
}
