use nemscraper::{duck, fetch, history, process, schema};
use reqwest::Client;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing subscriber for logging
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("info,nemscraper=info,main=info")
    });
    fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_span_events(fmt::format::FmtSpan::CLOSE)
        .init();

    tracing::info!("Application starting up. Logger initialized.");

    // Configuration and shared resources
    let client = Arc::new(Client::new());
    let schemas_output_dir = Path::new("schemas");
    let schema_evolutions_output_file = Path::new("schema_evolutions.json");
    let zips_output_dir = Arc::new(PathBuf::from("zips"));
    let duckdb_file_path = "nem_data.duckdb";

    // Ensure output directories exist
    fs::create_dir_all(schemas_output_dir)?;
    fs::create_dir_all(zips_output_dir.as_path())?;

    // 1. Fetch and process schemas
    tracing::info!(output_dir = %schemas_output_dir.display(), "Starting schema fetch process...");
    match schema::fetch_all(&client, schemas_output_dir).await {
        Ok(fetched_month_schemas) => {
            tracing::info!(
                count = fetched_month_schemas.len(),
                "Schema fetch process completed."
            );
        }
        Err(e) => {
            tracing::error!(error = %e, "Schema fetch process failed. Critical error, stopping.");
            return Err(e.into());
        }
    }

    tracing::info!(input_dir = %schemas_output_dir.display(), "Starting schema evolution extraction...");
    let processed_evolutions: Vec<schema::SchemaEvolution> = match schema::extract_schema_evolutions(
        schemas_output_dir,
    ) {
        Ok(evolutions) => {
            tracing::info!(
                count = evolutions.len(),
                output_file = %schema_evolutions_output_file.display(),
                "Schema evolution extraction completed successfully."
            );
            // Write to disk, log warning on failure
            if let Ok(json_data) = serde_json::to_string_pretty(&evolutions) {
                if let Err(e) = fs::write(schema_evolutions_output_file, json_data) {
                    tracing::warn!(error = %e, file_path = %schema_evolutions_output_file.display(), 
                        "Failed to write schema evolutions to disk.");
                } else {
                    tracing::info!(file_path = %schema_evolutions_output_file.display(), 
                        "Schema evolutions saved to disk.");
                }
            } else {
                tracing::warn!("Failed to serialize schema evolutions to JSON.");
            }
            evolutions
        }
        Err(e) => {
            tracing::error!(error = %e, "Schema evolution extraction failed. Critical error, stopping.");
            return Err(e.into());
        }
    };

    // 2. Create DuckDB tables for each schema version
    if processed_evolutions.is_empty() {
        tracing::info!("No schema evolutions processed. Cannot create DuckDB tables. Critical error, stopping.");
        return Err(anyhow::anyhow!("No schema evolutions found."));
    }
    
    tracing::info!(duckdb_file = %duckdb_file_path, "Creating tables in DuckDB for each schema version.");
    match duck::open_file_db(duckdb_file_path) {
        Ok(conn) => {
            let mut tables_created_count = 0;
            let mut tables_failed_count = 0;
            for evolution in &processed_evolutions {
                tracing::info!(
                    table_name = %evolution.table_name,
                    fields_hash = %evolution.fields_hash,
                    columns_count = evolution.columns.len(),
                    "Creating schema version table in DuckDB."
                );
                let duckdb_table_name = format!("{}_{}", evolution.table_name, evolution.fields_hash);
                match duck::create_table_from_schema(&conn, &duckdb_table_name, &evolution.columns) {
                    Ok(_) => tables_created_count += 1,
                    Err(e) => {
                        tables_failed_count += 1;
                        tracing::error!(duckdb_table = %duckdb_table_name, error = %e, 
                            "Failed to create schema version table in DuckDB.");
                    }
                }
            }
            tracing::info!(
                created_count = tables_created_count,
                failed_count = tables_failed_count,
                "DuckDB schema version table creation process finished."
            );
            if tables_failed_count > 0 && tables_failed_count == processed_evolutions.len() {
                tracing::error!("All schema table creations failed in DuckDB. Stopping.");
                return Err(anyhow::anyhow!("Failed to create any schema tables in DuckDB"));
            }
        }
        Err(e) => {
            tracing::error!(error = %e, duckdb_file = %duckdb_file_path, 
                "Failed to open DuckDB database for table creation. Critical error, stopping.");
            return Err(e.into());
        }
    }

    // Prepare schema lookup map
    let mut schema_lookup_map: HashMap<String, Vec<Arc<schema::SchemaEvolution>>> = HashMap::new();
    for evo in processed_evolutions {
        schema_lookup_map
            .entry(evo.table_name.clone())
            .or_default()
            .push(Arc::new(evo));
    }
    for evolutions_for_table in schema_lookup_map.values_mut() {
        evolutions_for_table.sort_by_key(|e| e.start_month.clone());
    }
    let schema_lookup_arc = Arc::new(schema_lookup_map);

    // 3. Ensure history table exists
    let conn = duck::open_file_db(duckdb_file_path)?;
    history::ensure_history_table_exists(&conn)?;
    // Let's drop this connection to avoid keeping it open
    drop(conn);

    // 4. Fetch URLs to download
    tracing::info!("Fetching current ZIP URLs...");
    let urls_to_download: Vec<String> = match fetch::urls::fetch_current_zip_urls(&client).await {
        Ok(feeds) => {
            const FPP_KEY: &str = "https://nemweb.com.au/Reports/Current/FPP/";
            feeds.get(FPP_KEY).cloned().unwrap_or_else(|| {
                tracing::warn!("FPP URL key ('{}') not found in feeds or no files listed.", FPP_KEY);
                Vec::new()
            })
        }
        Err(e) => {
            tracing::error!(error = %e, "Failed to fetch current ZIP URLs. No files to download.");
            Vec::new()
        }
    };

    if urls_to_download.is_empty() {
        tracing::info!("No URLs to download. Application will now exit.");
        return Ok(());
    }
    tracing::info!("Found {} URLs to potentially download.", urls_to_download.len());

    // 5. Concurrent Downloading with Sequential Processing
    let (tx, mut rx) = mpsc::channel::<Result<PathBuf, (String, String)>>(128);
    let download_semaphore = Arc::new(Semaphore::new(3)); // Max 2 concurrent downloads

    // Start downloads
    for zip_url in urls_to_download {
        let client_clone = Arc::clone(&client);
        let zips_output_dir_clone = Arc::clone(&zips_output_dir);
        let tx_clone = tx.clone();
        let semaphore_clone = Arc::clone(&download_semaphore);

        tokio::spawn(async move {
            let zip_filename_from_url = Path::new(&zip_url)
                .file_name()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_else(|| zip_url.clone());

            let permit = match semaphore_clone.acquire().await {
                Ok(p) => p,
                Err(e) => {
                    let err_msg = format!("Semaphore acquisition failed: {}", e);
                    tracing::error!(url = %zip_url, zip_filename = %zip_filename_from_url, 
                        error = %err_msg, "Semaphore acquisition failed for download task.");
                    let _ = tx_clone.send(Err((zip_url, err_msg))).await;
                    return;
                }
            };

            tracing::info!(url = %zip_url, zip_filename = %zip_filename_from_url, "Starting download...");
            match fetch::zips::download_zip(&client_clone, &zip_url, &*zips_output_dir_clone).await {
                Ok(path) => {
                    let downloaded_zip_filename = path
                        .file_name()
                        .map(|s| s.to_string_lossy().into_owned())
                        .unwrap_or_else(|| path.to_string_lossy().into_owned());
                    tracing::info!(url = %zip_url, zip_filename = %downloaded_zip_filename, 
                        file_path = %path.display(), "Download successful.");

                    if tx_clone.send(Ok(path)).await.is_err() {
                        tracing::error!(url = %zip_url, original_zip_filename = %zip_filename_from_url, 
                            attempted_file_sent = %downloaded_zip_filename, 
                            "Failed to send downloaded path to channel (receiver dropped).");
                    }
                }
                Err(e) => {
                    let download_failure_message = e.to_string();
                    tracing::error!(url = %zip_url, zip_filename = %zip_filename_from_url, 
                        error = %download_failure_message, "Failed to download ZIP.");

                    if tx_clone.send(Err((zip_url.clone(), download_failure_message))).await.is_err() {
                        tracing::error!(url = %zip_url, zip_filename = %zip_filename_from_url, 
                            "Failed to send download error to channel (receiver dropped).");
                    }
                }
            }
            drop(permit); // Release semaphore permit
        });
    }
    drop(tx); // Drop the original sender

    // 6. Sequential Processing of Downloaded Files
    tracing::info!("Starting sequential processing of downloaded files.");
    while let Some(received_item_result) = rx.recv().await {
        match received_item_result {
            Ok(zip_path) => {
                let current_zip_filename = zip_path
                    .file_name()
                    .map(|s| s.to_string_lossy().into_owned())
                    .unwrap_or_else(|| zip_path.to_string_lossy().into_owned());

                tracing::info!(zip_filename = %current_zip_filename, file_path = %zip_path.display(), 
                    "Processing downloaded file.");
                
                // Open a fresh connection for each file
                match duck::open_file_db(duckdb_file_path) {
                    Ok(conn) => {
                        match process::load_aemo_zip(&zip_path) {
                            Ok(loaded_data_map) if !loaded_data_map.is_empty() => {
                                let mut all_tables_processed = true;
                                
                                for (aemo_table_name, raw_table_data) in &loaded_data_map {
                                    if let Some(evolution) = schema::find_schema_evolution(
                                        &schema_lookup_arc, 
                                        aemo_table_name,
                                        &raw_table_data.effective_month,
                                    ) {
                                        let duckdb_table_name = format!(
                                            "{}_{}",
                                            evolution.table_name, 
                                            evolution.fields_hash
                                        );

                                        tracing::debug!(zip_filename = %current_zip_filename, 
                                            source_csv = %aemo_table_name, 
                                            target_duckdb = %duckdb_table_name, 
                                            rows = raw_table_data.rows.len(), 
                                            "Inserting data.");
                                            
                                        if let Err(e) = duck::insert_raw_table_data(
                                            &conn,
                                            &duckdb_table_name,
                                            raw_table_data,
                                            &evolution,
                                        ) {
                                            tracing::error!(zip_filename = %current_zip_filename, 
                                                error = %e, 
                                                duckdb_table = %duckdb_table_name, 
                                                source_zip_path = %zip_path.display(), 
                                                "Failed to insert data.");
                                            all_tables_processed = false;
                                        }
                                    } else {
                                        tracing::warn!(zip_filename = %current_zip_filename, 
                                            aemo_table = %aemo_table_name, 
                                            month = %raw_table_data.effective_month, 
                                            source_zip_path = %zip_path.display(), 
                                            "No matching schema evolution found. Skipping table insertion.");
                                        all_tables_processed = false;
                                    }
                                }

                                if all_tables_processed {
                                    tracing::info!(zip_filename = %current_zip_filename, 
                                        file_path = %zip_path.display(), 
                                        "All tables from ZIP processed, recording history.");
                                    
                                    // Uncomment if you want to record history and delete processed files
                                    // if let Err(e) = history::record_processed_csv_data(
                                    //     &conn,
                                    //     &zip_path,
                                    //     &loaded_data_map,
                                    // ) {
                                    //     tracing::error!(zip_filename = %current_zip_filename, 
                                    //         error = %e, 
                                    //         zip_file_path = %zip_path.display(), 
                                    //         "Failed to record processed CSV data. File will not be deleted.");
                                    // } else {
                                    //     tracing::info!(zip_filename = %current_zip_filename, 
                                    //         file_path = %zip_path.display(), 
                                    //         "History recorded. Attempting to delete ZIP file.");
                                    //     if let Err(e) = fs::remove_file(&zip_path) {
                                    //         tracing::error!(zip_filename = %current_zip_filename, 
                                    //             error = %e, 
                                    //             file_path = %zip_path.display(), 
                                    //             "Failed to delete processed ZIP file.");
                                    //     } else {
                                    //         tracing::info!(zip_filename = %current_zip_filename, 
                                    //             file_path = %zip_path.display(), 
                                    //             "Successfully deleted processed ZIP file.");
                                    //     }
                                    // }
                                } else {
                                    tracing::warn!(zip_filename = %current_zip_filename, 
                                        file_path = %zip_path.display(), 
                                        "Skipping history record and deletion due to processing errors or missing schemas.");
                                }
                            }
                            Ok(_) => {
                                // ZIP is empty
                                tracing::info!(zip_filename = %current_zip_filename, 
                                    file_path = %zip_path.display(), 
                                    "ZIP contained no data tables to process. Deleting file.");
                                if let Err(e) = fs::remove_file(&zip_path) {
                                    tracing::error!(zip_filename = %current_zip_filename, 
                                        error = %e, 
                                        file_path = %zip_path.display(), 
                                        "Failed to delete empty ZIP file.");
                                }
                            }
                            Err(e) => {
                                tracing::error!(zip_filename = %current_zip_filename, 
                                    error = %e, 
                                    file_path = %zip_path.display(), 
                                    "Failed to load/process AEMO ZIP. File may remain.");
                            }
                        }
                        // Explicitly drop the connection after processing each file
                        drop(conn);
                    }
                    Err(e) => {
                        tracing::error!(zip_filename = %current_zip_filename, 
                            file_path = %zip_path.display(), 
                            error = %e, 
                            "Failed to open DuckDB connection. Skipping file.");
                    }
                }
            }
            Err((url, download_err_msg)) => {
                let failed_zip_filename = Path::new(&url)
                    .file_name()
                    .map(|s| s.to_string_lossy().into_owned())
                    .unwrap_or_else(|| url.clone());
                
                tracing::error!(zip_filename = %failed_zip_filename, 
                    url = %url, 
                    error = %download_err_msg, 
                    "Download failed. File will not be processed.");
            }
        }
    }

    tracing::info!("All files have been processed. Application finished successfully.");
    Ok(())
}