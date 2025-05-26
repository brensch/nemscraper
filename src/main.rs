use anyhow::Result;
use nemscraper::{
    fetch,
    history::{load_processed, record_processed},
    process,
    schema::{self, extract_column_types},
};
use reqwest::Client;
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    sync::{mpsc, Semaphore},
    time::Instant,
};
use tracing::{error, info};
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    // ─── 1) init logging ─────────────────────────────────────────────
    let env = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,data=info,main=info"));
    fmt::Subscriber::builder()
        .with_env_filter(env)
        .with_span_events(fmt::format::FmtSpan::CLOSE)
        .init();
    info!("startup");

    std::panic::set_hook(Box::new(|info| {
        eprintln!("panic: {:?}", info);
    }));

    // ─── 2) configure dirs ───────────────────────────────────────────
    let client = Client::new();
    let schemas_dir = Path::new("schemas");
    let zips_dir = PathBuf::from("zips");
    let out_parquet_dir = PathBuf::from("parquet");
    let history_dir = PathBuf::from("history");
    let failed_zips_dir = PathBuf::from("failed_zips");

    for d in &[
        schemas_dir,
        &zips_dir,
        &out_parquet_dir,
        &history_dir,
        &failed_zips_dir,
    ] {
        fs::create_dir_all(d)?;
    }

    // ─── 3) fetch & extract column types ──────────────────────────────
    info!("fetch schemas → {}", schemas_dir.display());
    schema::fetch_all(&client, schemas_dir).await?;
    let column_lookup: HashMap<String, HashMap<String, HashSet<String>>> =
        extract_column_types(schemas_dir)?;
    info!("extracted column types for {} tables", column_lookup.len());
    let lookup = Arc::new(column_lookup);

    // ─── 4) load history to skip processed ZIPs ──────────────────────
    let processed: HashSet<String> = load_processed(&history_dir)?;
    info!("{} ZIPs already done", processed.len());

    // ─── 5) discover new ZIP URLs ────────────────────────────────────
    let feeds = fetch::urls::fetch_current_zip_urls(&client).await?;
    let to_process: Vec<String> = feeds
        .values()
        .flatten()
        .filter_map(|u| {
            let name = Path::new(u)
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string();
            if processed.contains(&name) {
                None
            } else {
                Some(u.clone())
            }
        })
        .collect();

    if to_process.is_empty() {
        info!("no new ZIPs; exit");
        return Ok(());
    }
    info!("{} ZIPs to download + split", to_process.len());

    // ─── 6) spawn downloader tasks ──────────────────────────────────
    let (tx, mut rx) = mpsc::channel::<Result<PathBuf, (String, String)>>(100);
    let dl_sem = Arc::new(Semaphore::new(3));
    let mut dl_handles = Vec::with_capacity(to_process.len());

    for url in to_process {
        let client = client.clone();
        let zips_dir = zips_dir.clone();
        let tx = tx.clone();
        let sem = dl_sem.clone();

        dl_handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let name = Path::new(&url)
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string();
            info!(name=%name, "downloading");
            let start = Instant::now();
            match fetch::zips::download_zip(&client, &url, &zips_dir).await {
                Ok(path) => {
                    info!(name=%name, elapsed=?start.elapsed(), "downloaded");
                    let _ = tx.send(Ok(path)).await;
                }
                Err(err) => {
                    error!("{} download failed: {}", url, err);
                    let _ = tx.send(Err((url.clone(), err.to_string()))).await;
                }
            }
        }));
    }
    drop(tx);

    // ─── 7) process downloaded ZIPs one at a time ────────────────────
    while let Some(msg) = rx.recv().await {
        match msg {
            Ok(zip_path) => {
                let name = zip_path.file_name().unwrap().to_string_lossy().to_string();
                info!("processing {}", name);

                // offload the heavy split to the blocking pool
                let split_result = tokio::task::spawn_blocking({
                    let lookup = Arc::clone(&lookup);
                    let out_parquet_dir = out_parquet_dir.clone();
                    let zip_clone = zip_path.clone();
                    move || {
                        process::split::split_zip_to_parquet(&zip_clone, &out_parquet_dir, lookup)
                    }
                })
                .await?;

                if let Err(e) = split_result {
                    error!("split {} failed: {}", name, e);
                    // record failure
                    let failed_marker = failed_zips_dir.join(&name);
                    fs::File::create(&failed_marker)?;
                    continue;
                }

                // write history record
                record_processed(&history_dir, &name)?;
                info!("wrote history for {}", name);

                // NOTE: we no longer delete the ZIP here, so you can inspect
            }

            Err((url, err)) => {
                error!("{} download error: {}", url, err);
                // optionally record download failures too:
                if let Some(name) = Path::new(&url)
                    .file_name()
                    .map(|s| s.to_string_lossy().to_string())
                {
                    let failed_marker = failed_zips_dir.join(&name);
                    fs::File::create(&failed_marker)?;
                }
            }
        }
    }

    // ─── 8) await all downloader tasks ───────────────────────────────
    for h in dl_handles {
        let _ = h.await;
    }

    // ─── 9) report total ZIP disk usage ─────────────────────────────
    let total_size: u64 = fs::read_dir(&zips_dir)?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.metadata().ok())
        .map(|meta| meta.len())
        .sum();
    info!("total size of remaining ZIPs: {} bytes", total_size);

    info!("all done");
    Ok(())
}
