use nemscraper::{duck, fetch, history, process, schema};
use reqwest::Client;
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    sync::{mpsc, Mutex, Semaphore},
    time::Instant,
};
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ─── 1) init logging ─────────────────────────────────────────────
    let env = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,nemscraper=info,main=info"));
    fmt::Subscriber::builder()
        .with_env_filter(env)
        .with_span_events(fmt::format::FmtSpan::CLOSE)
        .init();
    tracing::info!("Starting up…");

    // ─── 2) config paths ────────────────────────────────────────────
    let client = Client::new();
    let schemas_dir = Path::new("schemas");
    let evolutions_file = Path::new("schema_evolutions.json");
    let zips_dir = PathBuf::from("zips");
    let base_db = "nem_data.duckdb";
    fs::create_dir_all(schemas_dir)?;
    fs::create_dir_all(&zips_dir)?;

    // ─── 3) fetch & extract schemas ─────────────────────────────────
    tracing::info!("Fetching schemas to {}", schemas_dir.display());
    schema::fetch_all(&client, schemas_dir).await?;
    let evolutions = schema::extract_schema_evolutions(schemas_dir)?;
    tracing::info!("{} schema evolutions", evolutions.len());
    fs::write(evolutions_file, serde_json::to_string_pretty(&evolutions)?)?;
    tracing::info!("Wrote evolutions to {}", evolutions_file.display());

    // ─── 4) create base DB with all tables + history ────────────────
    {
        let conn = duck::open_file_db(base_db)?;
        // create each versioned table
        let mut created = 0;
        for evo in &evolutions {
            let tbl = format!("{}_{}", evo.table_name, evo.fields_hash);
            duck::create_table_from_schema(&conn, &tbl, &evo.columns)
                .map(|_| created += 1)
                .map_err(|e| tracing::error!("create {} failed: {}", tbl, e))
                .ok();
        }
        tracing::info!("Created {} schema tables", created);

        // history table
        history::ensure_history_table_exists(&conn)?;
        tracing::info!("History table initialized");

        conn.execute("CHECKPOINT;", [])?;
        // drop to flush everything
    }

    // ─── 5) shard the base DB ────────────────────────────────────────
    let num_shards = num_cpus::get().max(1);
    let mut shard_paths = Vec::with_capacity(num_shards);
    for id in 0..num_shards {
        let shard = format!("nem_data_shard_{}.duckdb", id);
        fs::copy(base_db, &shard)?;
        shard_paths.push(shard);
    }
    tracing::info!("Made {} shards", num_shards);

    // ─── 6) build lookup for later inserts ───────────────────────────
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

    // ─── 7) figure out which ZIPs are new ────────────────────────────
    let mut base_conn = duck::open_file_db(base_db)?;
    history::ensure_history_table_exists(&base_conn)?;
    let processed: HashSet<String> = history::get_processed_zipfiles(&base_conn)?
        .into_iter()
        .collect();
    tracing::info!("{} ZIPs already done", processed.len());

    let feeds = fetch::urls::fetch_current_zip_urls(&client).await?;
    let urls: Vec<_> = feeds
        .values()
        .flatten()
        .cloned()
        .filter(|u| {
            let name = Path::new(u)
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string();
            if processed.contains(&name) {
                tracing::debug!("Skipping {}", name);
                false
            } else {
                true
            }
        })
        .collect();

    if urls.is_empty() {
        tracing::info!("No new ZIPs—exiting");
        return Ok(());
    }
    tracing::info!("{} ZIPs to fetch + process", urls.len());

    // ─── 8) start download tasks ────────────────────────────────────
    let (tx, rx) = mpsc::channel::<Result<PathBuf, (String, String)>>(100);
    let download_sem = Arc::new(Semaphore::new(3));
    let mut download_handles = Vec::with_capacity(urls.len());

    for url in urls {
        let client = client.clone();
        let zips_dir = zips_dir.clone();
        let tx = tx.clone();
        let sem = download_sem.clone();
        download_handles.push(tokio::spawn(async move {
            let _p = sem.acquire().await.unwrap();
            let name = Path::new(&url)
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string();
            tracing::info!(name = %name, "Downloading…");
            let start = Instant::now();
            match fetch::zips::download_zip(&client, &url, &zips_dir).await {
                Ok(path) => {
                    tracing::info!(name = %name, elapsed = ?start.elapsed(), "Downloaded");
                    let _ = tx.send(Ok(path)).await;
                }
                Err(e) => {
                    tracing::error!("Download failed for {}: {}", url, e);
                    let _ = tx.send(Err((url, e.to_string()))).await;
                }
            }
        }));
    }
    drop(tx);

    // ─── 9) spawn N writer tasks, each with its own shard DB ─────────
    let rx = Arc::new(Mutex::new(rx));
    let mut writer_handles = Vec::with_capacity(num_shards);

    for (shard_id, shard_path) in shard_paths.iter().enumerate() {
        let rx = rx.clone();
        let lookup = lookup.clone();
        let shard_db = shard_path.clone();
        writer_handles.push(tokio::spawn(async move {
            let conn = duck::open_file_db(&shard_db).expect("open shard");
            while let Some(msg) = { rx.lock().await.recv().await } {
                match msg {
                    Ok(zip_path) => {
                        let filename = zip_path.file_name().unwrap().to_string_lossy().to_string();
                        tracing::info!("Shard#{} processing {}", shard_id, filename);

                        let tables = match process::load_aemo_zip(&zip_path) {
                            Ok(t) => t,
                            Err(e) => {
                                tracing::error!("Unpack {} err: {}", filename, e);
                                continue;
                            }
                        };
                        let mut ok = true;
                        for (tbl, raw) in &tables {
                            if let Some(evo) =
                                schema::find_schema_evolution(&lookup, tbl, &raw.effective_month)
                            {
                                let target = format!("{}_{}", evo.table_name, evo.fields_hash);
                                if let Err(e) =
                                    duck::insert_raw_table_data(&conn, &target, raw, &evo)
                                {
                                    tracing::error!("Insert {} failed: {}", target, e);
                                    ok = false;
                                }
                            } else {
                                tracing::warn!("No schema for {}@{}", tbl, raw.effective_month);
                                ok = false;
                            }
                        }
                        if ok {
                            // history::record_processed_csv_data(&conn, &zip_path, &tables)
                            //     .unwrap_or_else(|e| {
                            //         tracing::error!("History rec err {}: {}", filename, e)
                            //     });
                            // let _ = fs::remove_file(&zip_path);
                        }
                    }
                    Err((url, err)) => {
                        tracing::error!("Download error {}: {}", url, err);
                    }
                }
            }
            tracing::info!("Shard#{} writer done", shard_id);
        }));
    }

    // ─── 10) wait for all downloads to finish ────────────────────────
    for h in download_handles {
        let _ = h.await;
    }

    // ─── 11) wait for all writers to drain ──────────────────────────
    drop(rx); // unlock channel
    for w in writer_handles {
        let _ = w.await;
    }

    // ─── 12) merge all shards back into base DB ──────────────────────
    let conn = duck::open_file_db(base_db)?;
    for (shard_id, shard_path) in shard_paths.iter().enumerate() {
        let alias = format!("sh{}", shard_id);
        conn.execute(&format!("ATTACH '{}' AS {};", shard_path, alias), [])?;

        // merge each schema table
        for evo in &evolutions {
            let tbl = format!("{}_{}", evo.table_name, evo.fields_hash);
            conn.execute(
                &format!(
                    "INSERT INTO main.{tbl} SELECT * FROM {alias}.{tbl};",
                    tbl = tbl,
                    alias = alias
                ),
                [],
            )?;
        }
        // merge history
        conn.execute(
            &format!(
                "INSERT INTO main.history SELECT * FROM {alias}.history;",
                alias = alias
            ),
            [],
        )?;
        conn.execute(&format!("DETACH {};", alias), [])?;
    }

    conn.execute("CHECKPOINT;", [])?;
    tracing::info!("All shards merged – done.");

    Ok(())
}
