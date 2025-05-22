// data/src/main.rs

use nemscraper::{
    fetch, process,
    schema::{self, SchemaEvolution},
};
use reqwest::Client;

use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    sync::{mpsc, Mutex, Semaphore},
    time::Instant,
};
use tracing_subscriber::{fmt, EnvFilter};

// for Parquet‐based history
use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tracing::Instrument;
use tracing::{error, info, info_span};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ─── 1) init logging ─────────────────────────────────────────────
    let env = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,data=info,main=info"));
    fmt::Subscriber::builder()
        .with_env_filter(env)
        .with_span_events(fmt::format::FmtSpan::CLOSE)
        .init();
    tracing::info!("startup");

    std::panic::set_hook(Box::new(|info| {
        eprintln!("panic: {:?}", info);
    }));

    // ─── 2) configure dirs ───────────────────────────────────────────
    let client = Client::new();
    let schemas_dir = Path::new("schemas");
    let zips_dir = PathBuf::from("zips");
    let out_parquet_dir = PathBuf::from("parquet");
    let history_dir = PathBuf::from("history");

    for d in &[schemas_dir, &zips_dir, &out_parquet_dir, &history_dir] {
        fs::create_dir_all(d)?;
    }

    // ─── 3) fetch & evolve CTL schemas ───────────────────────────────
    tracing::info!("fetch schemas → {}", schemas_dir.display());
    schema::fetch_all(&client, schemas_dir).await?;
    let evolutions = schema::extract_schema_evolutions(schemas_dir)?;
    // build lookup once
    tracing::info!("{} evolutions", evolutions.len());
    let lookup: Arc<HashMap<String, Vec<Arc<SchemaEvolution>>>> =
        Arc::new(SchemaEvolution::build_lookup(evolutions));

    // ─── 4) load history to skip processed ZIPs ──────────────────────
    let processed: HashSet<String> = fs::read_dir(&history_dir)?
        .filter_map(|e| e.ok())
        .filter_map(|ent| {
            ent.path()
                .file_stem()
                .and_then(|s| s.to_str())
                .map(|s| s.to_string())
        })
        .collect();
    tracing::info!("{} ZIPs already done", processed.len());

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
                tracing::debug!("skip {}", name);
                None
            } else {
                Some(u.clone())
            }
        })
        .collect();

    if to_process.is_empty() {
        tracing::info!("no new ZIPs; exit");
        return Ok(());
    }
    tracing::info!("{} ZIPs to download + split", to_process.len());

    // ─── 6) spawn downloader tasks ──────────────────────────────────
    let (tx, rx) = mpsc::channel::<Result<PathBuf, (String, String)>>(100);
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
            tracing::info!(name=%name, "downloading");
            let start = Instant::now();
            match fetch::zips::download_zip(&client, &url, &zips_dir).await {
                Ok(path) => {
                    tracing::info!(name=%name, elapsed=?start.elapsed(), "downloaded");
                    let _ = tx.send(Ok(path)).await;
                }
                Err(err) => {
                    tracing::error!("{} failed: {}", url, err);
                    let _ = tx.send(Err((url, err.to_string()))).await;
                }
            }
        }));
    }
    drop(tx);

    // ─── 7) spawn processor tasks ────────────────────────────────────
    let rx = Arc::new(Mutex::new(rx));
    let mut proc_handles = Vec::new();

    let num_workers = num_cpus::get().max(1);
    for worker_id in 0..num_workers {
        // clone all shared handles *before* entering the async block
        let lookup = Arc::clone(&lookup);
        let rx = Arc::clone(&rx);
        let out_parquet_dir = out_parquet_dir.clone();
        let history_dir = history_dir.clone();
        let worker_span = info_span!("worker", id = worker_id);

        let handle = tokio::spawn(
            async move {
                loop {
                    // 1) Grab one message, then immediately drop the lock
                    let msg_opt = {
                        let mut guard = rx.lock().await;
                        guard.recv().await
                    };
                    let msg = match msg_opt {
                        Some(m) => m,
                        None => break, // channel closed -> exit loop
                    };

                    match msg {
                        Ok(zip_path) => {
                            let name = zip_path.file_name().unwrap().to_string_lossy().to_string();

                            info!(
                                worker = worker_id,
                                thread = ?std::thread::current().id(),
                                "processing {}",
                                name
                            );

                            // 2) Offload the heavy split to the blocking pool,
                            //    passing in our cloned lookup Arc
                            let lookup_clone = Arc::clone(&lookup);
                            let out_clone = out_parquet_dir.clone();
                            let zip_clone = zip_path.clone();

                            let split_result = tokio::task::spawn_blocking(move || {
                                process::split::split_zip_to_parquet(
                                    &zip_clone,
                                    &out_clone,
                                    lookup_clone,
                                )
                            })
                            .await; // `Result<Result<(), E>, JoinError>`

                            match split_result {
                                Ok(Ok(())) => { /* success */ }
                                Ok(Err(e)) => {
                                    error!(worker = worker_id, "split {} failed: {}", name, e);
                                    continue;
                                }
                                Err(join_err) => {
                                    error!(
                                        worker = worker_id,
                                        "blocking task panicked: {:?}", join_err
                                    );
                                    continue;
                                }
                            }

                            // 3) write history/<name>.parquet
                            let hist_file = history_dir.join(format!("{}.parquet", name));
                            {
                                let schema = Schema::new(vec![Field::new(
                                    "zip_name",
                                    DataType::Utf8,
                                    false,
                                )]);
                                let arr = StringArray::from(vec![name.clone()]);
                                let batch = RecordBatch::try_new(
                                    Arc::new(schema.clone()),
                                    vec![Arc::new(arr)],
                                )
                                .expect("make batch");

                                let file = File::create(&hist_file).expect("create history file");
                                let props = WriterProperties::builder()
                                    .set_compression(Compression::SNAPPY)
                                    .build();
                                let mut writer =
                                    ArrowWriter::try_new(file, Arc::new(schema), Some(props))
                                        .expect("create writer");
                                writer.write(&batch).expect("write batch");
                                writer.close().expect("close writer");
                            }
                            info!(worker = worker_id, "wrote history for {}", name);

                            // 4) delete the ZIP
                            if let Err(e) = std::fs::remove_file(&zip_path) {
                                error!(worker = worker_id, "failed to delete {}: {}", name, e);
                            } else {
                                info!(
                                    worker   = worker_id,
                                    zip_path = %zip_path.display(),
                                    "deleted zip"
                                );
                            }
                        }

                        Err((url, err)) => {
                            error!(worker = worker_id, "download error {}: {}", url, err);
                        }
                    }
                }
            }
            .instrument(worker_span),
        );

        proc_handles.push(handle);
    }

    // ─── 8) await all tasks ─────────────────────────────────────────
    for h in dl_handles {
        let _ = h.await;
    }
    drop(rx);
    for h in proc_handles {
        let _ = h.await;
    }

    tracing::info!("all done");
    Ok(())
}
