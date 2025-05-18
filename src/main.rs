use nemscraper::{duck, fetch, history, process, schema};
use reqwest::Client;
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // -- initialize logging
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,nemscraper=info,main=info"));
    fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_span_events(fmt::format::FmtSpan::CLOSE)
        .init();
    tracing::info!("Starting up…");

    // -- shared config
    let client = Client::new();
    let schemas_dir = Path::new("schemas");
    let evolutions_file = Path::new("schema_evolutions.json");
    let zips_dir = PathBuf::from("zips");
    let duckdb_path = "nem_data.duckdb";

    fs::create_dir_all(schemas_dir)?;
    fs::create_dir_all(&zips_dir)?;

    // 1) fetch all schemas
    tracing::info!("Fetching schemas into {}", schemas_dir.display());
    let _ = schema::fetch_all(&client, schemas_dir).await?;
    tracing::info!("Schemas fetched");

    // 2) extract evolutions and write to JSON
    tracing::info!("Extracting schema evolutions…");
    let evolutions = schema::extract_schema_evolutions(schemas_dir)?;
    tracing::info!("{} evolutions extracted", evolutions.len());
    fs::write(evolutions_file, serde_json::to_string_pretty(&evolutions)?)
        .map(|_| tracing::info!("Wrote evolutions to {}", evolutions_file.display()))
        .unwrap_or_else(|e| tracing::warn!("Failed to write evolutions.json: {}", e));

    // 3) open DuckDB and create a table for each schema evolution
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
    tracing::info!("DuckDB tables created: {}, failed: {}", created, failed);
    if created == 0 {
        return Err(anyhow::anyhow!(
            "No schema tables created in DuckDB – aborting"
        ));
    }

    // ── build lookup map for later inserts ──
    let mut lookup: HashMap<String, Vec<Arc<schema::SchemaEvolution>>> = HashMap::new();
    for evo in evolutions.iter() {
        lookup
            .entry(evo.table_name.clone())
            .or_default()
            .push(Arc::new(evo.clone()));
    }
    for versions in lookup.values_mut() {
        versions.sort_by_key(|e| e.start_month.clone());
    }

    // 4) ensure history table
    history::ensure_history_table_exists(&conn)?;
    tracing::info!("History table ready");

    // 5) get already‐processed filenames
    let processed: HashSet<String> = history::get_processed_zipfiles(&conn)?
        .into_iter()
        .collect();
    tracing::info!("{} ZIPs already processed", processed.len());

    // 6) fetch current ZIP URLs
    tracing::info!("Retrieving current ZIP URLs…");
    let all_feeds = fetch::urls::fetch_current_zip_urls(&client).await?;
    let urls = all_feeds
        .get("https://nemweb.com.au/Reports/Current/FPP/")
        .cloned()
        .unwrap_or_default()
        .into_iter()
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

    // 7) sequentially download & process
    for zip_url in urls {
        let filename = Path::new(&zip_url)
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();
        tracing::info!("Downloading {}", filename);
        let zip_path = fetch::zips::download_zip(&client, &zip_url, &zips_dir).await?;

        tracing::info!("Processing {}", filename);
        let tables = process::load_aemo_zip(&zip_path)?;
        let mut success = true;

        for (table, raw) in &tables {
            if let Some(evo) = schema::find_schema_evolution(&lookup, table, &raw.effective_month) {
                let target_tbl = format!("{}_{}", evo.table_name, evo.fields_hash);
                tracing::debug!("Inserting {} rows into {}", raw.rows.len(), target_tbl);
                if let Err(e) = duck::insert_raw_table_data(&conn, &target_tbl, raw, &evo) {
                    tracing::error!("Insert failed: {}", e);
                    success = false;
                }
            } else {
                tracing::warn!("No schema found for {} @ {}", table, raw.effective_month);
                success = false;
            }
        }

        if success {
            tracing::info!("Recording history for {}", filename);
            history::record_processed_csv_data(&conn, &zip_path, &tables)?;
            fs::remove_file(&zip_path)?;
        } else {
            tracing::warn!("Skipping history record for {} due to errors", filename);
        }
    }

    // 8) final checkpoint
    tracing::info!("Checkpointing database…");
    conn.execute("CHECKPOINT;", [])?;
    tracing::info!("Done. All ZIPs processed.");

    Ok(())
}
