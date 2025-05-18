use nemscraper::{duck, fetch, history, process, schema};
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
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1) initialize logging
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,nemscraper=info,main=info"));
    fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_span_events(fmt::format::FmtSpan::CLOSE)
        .init();
    tracing::info!("Starting up…");

    // 2) shared config
    let client = Client::new();
    let schemas_dir = Path::new("schemas");
    let evolutions_file = Path::new("schema_evolutions.json");
    let zips_dir = PathBuf::from("zips");
    let duckdb_path = "nem_data.duckdb";

    fs::create_dir_all(schemas_dir)?;
    fs::create_dir_all(&zips_dir)?;

    // 3) fetch & write schemas
    tracing::info!("Fetching schemas into {}", schemas_dir.display());
    schema::fetch_all(&client, schemas_dir).await?;
    tracing::info!("Schemas fetched");

    // 4) extract evolutions & write JSON
    tracing::info!("Extracting schema evolutions…");
    let evolutions = schema::extract_schema_evolutions(schemas_dir)?;
    tracing::info!("{} evolutions extracted", evolutions.len());
    fs::write(evolutions_file, serde_json::to_string_pretty(&evolutions)?)
        .map(|_| tracing::info!("Wrote evolutions to {}", evolutions_file.display()))
        .unwrap_or_else(|e| tracing::warn!("Failed to write evolutions.json: {}", e));

    // 5) open DuckDB & create tables
    let conn = duck::open_file_db(duckdb_path)?;
    let mut created = 0;
    let mut failed = 0;
    for evo in &evolutions {
        let tbl = format!("{}_{}", evo.table_name, evo.fields_hash);
        tracing::info!(
            "Creating DuckDB table {} ({} cols)…",
            tbl,
            evo.columns.len()
        );
        match duck::create_table_from_schema(&conn, &tbl, &evo.columns) {
            Ok(_) => created += 1,
            Err(e) => {
                failed += 1;
                tracing::error!("  → failed to create {}: {}", tbl, e);
            }
        }
    }
    tracing::info!("Tables created: {}, failed: {}", created, failed);
    if created == 0 {
        return Err(anyhow::anyhow!("No schema tables created – aborting"));
    }

    // 6) build lookup map
    let mut lookup: HashMap<String, Vec<Arc<schema::SchemaEvolution>>> = HashMap::new();
    for evo in &evolutions {
        lookup
            .entry(evo.table_name.clone())
            .or_default()
            .push(Arc::new(evo.clone()));
    }
    for versions in lookup.values_mut() {
        versions.sort_by_key(|e| e.start_month.clone());
    }

    // 7) ensure history table & load processed set
    history::ensure_history_table_exists(&conn)?;
    let processed: HashSet<String> = history::get_processed_zipfiles(&conn)?
        .into_iter()
        .collect();
    tracing::info!("{} ZIPs already processed", processed.len());

    // 8) retrieve current ZIP URLs
    tracing::info!("Retrieving current ZIP URLs…");
    let all_feeds = fetch::urls::fetch_current_zip_urls(&client).await?;
    let urls = all_feeds
        .values()
        .flatten()
        .cloned()
        .filter(|url| {
            let name = Path::new(url)
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string();
            if processed.contains(&name) {
                tracing::info!("Skipping already‐processed {}", name);
                false
            } else {
                true
            }
        })
        .collect::<Vec<_>>();

    if urls.is_empty() {
        tracing::info!("No new ZIPs to process, exiting.");
        return Ok(());
    }
    tracing::info!("{} ZIPs to download + process", urls.len());

    // 9) spawn concurrent download tasks
    let (tx, mut rx) = mpsc::channel::<Result<PathBuf, (String, String)>>(100);
    let download_semaphore = Arc::new(Semaphore::new(3)); // 4 concurrent downloads
    let mut handles = Vec::with_capacity(urls.len());

    for url in urls {
        let client = client.clone();
        let zips_dir = zips_dir.clone();
        let tx = tx.clone();
        let sem = download_semaphore.clone();
        let url_clone = url.clone();

        handles.push(tokio::spawn(async move {
            let permit = sem.acquire().await.unwrap();
            let name = Path::new(&url_clone)
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string();

            tracing::info!(name = %name, "Started download");

            // start timer
            let start = Instant::now();
            let result = fetch::zips::download_zip(&client, &url_clone, &zips_dir).await;
            // drop permit immediately after download finishes
            drop(permit);

            match result {
                Ok(path) => {
                    // compute elapsed time
                    let elapsed = start.elapsed();
                    tracing::info!(
                        name    = %name,
                        elapsed = ?elapsed,
                        "Finished download"
                    );
                    let _ = tx.send(Ok(path)).await;
                }
                Err(e) => {
                    tracing::error!("Failed to download {}: {}", url_clone, e);
                    let _ = tx.send(Err((url_clone, e.to_string()))).await;
                }
            }
        }));
    }
    drop(tx); // close once all clones are dropped

    // 10) single processor loop
    while let Some(msg) = rx.recv().await {
        match msg {
            Ok(zip_path) => {
                let filename = zip_path.file_name().unwrap().to_string_lossy().to_string();
                tracing::info!("Processing {}", filename);

                let tables = match process::load_aemo_zip(&zip_path) {
                    Ok(t) => t,
                    Err(e) => {
                        tracing::error!("Failed to unpack {}: {}", filename, e);
                        continue;
                    }
                };

                let mut success = true;
                for (table, raw) in &tables {
                    if let Some(evo) =
                        schema::find_schema_evolution(&lookup, table, &raw.effective_month)
                    {
                        let tbl = format!("{}_{}", evo.table_name, evo.fields_hash);
                        tracing::debug!("Inserting {} rows into {}", raw.rows.len(), tbl);
                        if let Err(e) = duck::insert_raw_table_data(&conn, &tbl, raw, &evo) {
                            tracing::error!("Insert failed for {}: {}", tbl, e);
                            success = false;
                        }
                    } else {
                        tracing::warn!("No schema for {} @ {}", table, raw.effective_month);
                        success = false;
                    }
                }

                if success {
                    tracing::info!("Recording history for {}", filename);
                    if let Err(e) = history::record_processed_csv_data(&conn, &zip_path, &tables) {
                        tracing::error!("History record failed for {}: {}", filename, e);
                    } else {
                        let _ = fs::remove_file(&zip_path);
                    }
                } else {
                    tracing::warn!("Skipping history for {} due to errors", filename);
                }
            }
            Err((url, err)) => {
                tracing::error!("Download error for {}: {}", url, err);
            }
        }
    }

    // 11) wait for all download tasks to finish
    for h in handles {
        if let Err(e) = h.await {
            tracing::error!("A download task panicked: {}", e);
        }
    }

    // 12) final checkpoint
    tracing::info!("Checkpointing database…");
    conn.execute("CHECKPOINT;", [])?;
    tracing::info!("All done.");

    Ok(())
}
