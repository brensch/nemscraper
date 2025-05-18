use futures::future::join_all;
use nemscraper::{duck, fetch, history, process, schema};
use reqwest::Client;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tracing_subscriber::{fmt, EnvFilter}; // For waiting on spawned tasks if needed for the processor pool

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing subscriber for logging
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("info,nemscraper=info,main=info") // Adjusted default log levels
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
    let zips_output_dir = Arc::new(PathBuf::from("zips")); // Arc for sharing with download tasks
    let duckdb_file_path = Arc::new(String::from("nem_data.duckdb")); // Arc for sharing with processor tasks

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
            // Attempt to write to disk, log warning on failure
            match serde_json::to_string_pretty(&evolutions) {
                Ok(json_data) => {
                    if let Err(e) = fs::write(schema_evolutions_output_file, json_data) {
                        tracing::warn!(error = %e, file_path = %schema_evolutions_output_file.display(), "Failed to write schema evolutions to disk.");
                    } else {
                        tracing::info!(file_path = %schema_evolutions_output_file.display(), "Schema evolutions saved to disk.");
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to serialize schema evolutions to JSON.")
                }
            }
            evolutions
        }
        Err(e) => {
            tracing::error!(error = %e, "Schema evolution extraction failed. Critical error, stopping.");
            return Err(e.into());
        }
    };

    // 2. Create DuckDB tables for each schema version
    if !processed_evolutions.is_empty() {
        tracing::info!(duckdb_file = %duckdb_file_path.as_str(), "Attempting to create tables in DuckDB for each schema version.");
        match duck::open_file_db(duckdb_file_path.as_str()) {
            Ok(conn) => {
                let mut tables_created_count = 0;
                let mut tables_failed_count = 0;
                for evolution in &processed_evolutions {
                    let duckdb_table_name =
                        format!("{}_{}", evolution.table_name, evolution.fields_hash);
                    match duck::create_table_from_schema(
                        &conn,
                        &duckdb_table_name,
                        &evolution.columns,
                    ) {
                        Ok(_) => tables_created_count += 1,
                        Err(e) => {
                            tables_failed_count += 1;
                            tracing::error!(duckdb_table = %duckdb_table_name, error = %e, "Failed to create schema version table in DuckDB.");
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
                    return Err(anyhow::anyhow!(
                        "Failed to create any schema tables in DuckDB"
                    ));
                }
            }
            Err(e) => {
                tracing::error!(error = %e, duckdb_file = %duckdb_file_path.as_str(), "Failed to open DuckDB database for table creation. Critical error, stopping.");
                return Err(e.into());
            }
        }
    } else {
        tracing::info!("No schema evolutions processed. Cannot create DuckDB tables. Critical error, stopping.");
        return Err(anyhow::anyhow!("No schema evolutions found."));
    }

    // Prepare schema lookup map for efficient access in processor tasks
    let mut schema_lookup_map: HashMap<String, Vec<Arc<schema::SchemaEvolution>>> = HashMap::new();
    for evo in processed_evolutions {
        // Consumes processed_evolutions
        schema_lookup_map
            .entry(evo.table_name.clone())
            .or_default()
            .push(Arc::new(evo));
    }
    for evolutions_for_table in schema_lookup_map.values_mut() {
        evolutions_for_table.sort_by_key(|e| e.start_month.clone()); // Sort for predictable lookup if needed
    }
    let schema_lookup_arc = Arc::new(schema_lookup_map);

    // 3. Ensure history table exists
    {
        // Scoped connection for initial history table check
        let conn = duck::open_file_db(duckdb_file_path.as_str())?;
        history::ensure_history_table_exists(&conn)?;
        // Note: To avoid re-downloading/processing, implement and use a function like:
        // let _previously_processed_zips: HashSet<String> = history::get_processed_zip_filenames(&conn).unwrap_or_default();
        // This set would then be used to filter `urls_to_download`.
    }

    // 4. Fetch URLs to download
    tracing::info!("Fetching current ZIP URLs...");
    let urls_to_download: Vec<String> = match fetch::urls::fetch_current_zip_urls(&client).await {
        Ok(feeds) => {
            // Example: Process all files from FPP feed. This might need to be generalized.
            const FPP_KEY: &str = "https://nemweb.com.au/Reports/Current/FPP/";
            feeds.get(FPP_KEY).cloned().unwrap_or_else(|| {
                tracing::warn!(
                    "FPP URL key ('{}') not found in feeds or no files listed.",
                    FPP_KEY
                );
                Vec::new()
            })
            // To process all URLs from all feeds:
            // feeds.values().flatten().cloned().collect()
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
    tracing::info!(
        "Found {} URLs to potentially download.",
        urls_to_download.len()
    );

    // 5. Concurrent Downloading
    // Channel for paths of downloaded files or download errors: Ok(PathBuf) or Err((url, error_string))
    let (tx, rx) = mpsc::channel::<Result<PathBuf, (String, String)>>(128); // Buffer size for the channel
    let download_semaphore = Arc::new(Semaphore::new(4)); // Max 4 concurrent downloads
    let mut download_handles = Vec::new();

    for zip_url in urls_to_download {
        // TODO: Filter zip_url here against `previously_processed_zips` (see step 3 note)
        let client_clone = Arc::clone(&client);
        let zips_output_dir_clone = Arc::clone(&zips_output_dir);
        let tx_clone = tx.clone();
        let semaphore_clone = Arc::clone(&download_semaphore);

        let handle = tokio::spawn(async move {
            let zip_filename_from_url = Path::new(&zip_url)
                .file_name()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_else(|| zip_url.clone());

            let permit = match semaphore_clone.acquire().await {
                Ok(p) => p,
                Err(e) => {
                    let err_msg = format!("Semaphore acquisition failed: {}", e);
                    tracing::error!(url = %zip_url, zip_filename = %zip_filename_from_url, error = %err_msg, "Semaphore acquisition failed for download task.");
                    let _ = tx_clone.send(Err((zip_url, err_msg))).await; // Report error
                    return;
                }
            };
            tracing::debug!(url = %zip_url, zip_filename = %zip_filename_from_url, "Acquired permit, starting download...");
            // CORRECTED LINE: Dereference Arc to get &PathBuf for AsRef<Path>
            match fetch::zips::download_zip(&client_clone, &zip_url, &*zips_output_dir_clone).await
            {
                Ok(path) => {
                    let downloaded_zip_filename = path
                        .file_name()
                        .map(|s| s.to_string_lossy().into_owned())
                        .unwrap_or_else(|| path.to_string_lossy().into_owned());
                    tracing::info!(url = %zip_url, zip_filename = %downloaded_zip_filename, file_path = %path.display(), "Download successful.");

                    if tx_clone.send(Ok(path)).await.is_err() {
                        // zip_url is still valid here as it wasn't consumed by Ok(path)
                        // 'path' is moved, use 'downloaded_zip_filename' which was captured before move.
                        tracing::error!(url = %zip_url, original_zip_filename = %zip_filename_from_url, attempted_file_sent = %downloaded_zip_filename, "Failed to send downloaded path to channel (receiver dropped).");
                    }
                }
                Err(e) => {
                    // Error during fetch::zips::download_zip
                    let download_failure_message = e.to_string();
                    // Log the primary download failure. zip_url is borrowed here and is still valid.
                    tracing::error!(url = %zip_url, zip_filename = %zip_filename_from_url, error = %download_failure_message, "Failed to download ZIP.");

                    // Clone zip_url for the tuple that will be sent through the channel.
                    // The original zip_url (in this task's scope) remains valid for logging the send failure.
                    let zip_url_for_channel_send = zip_url.clone();

                    if tx_clone
                        .send(Err((zip_url_for_channel_send, download_failure_message)))
                        .await
                        .is_err()
                    {
                        // If sending the error fails, log this failure using the original zip_url.
                        tracing::error!(url = %zip_url, zip_filename = %zip_filename_from_url, "Failed to send details of download error to channel (receiver likely dropped).");
                    }
                }
            }
            drop(permit); // Release semaphore permit
        });
        download_handles.push(handle);
    }
    drop(tx); // Drop the original sender; `rx` will know when all download tasks are done.

    // 6. Concurrent Processing
    let num_processor_workers = 16;
    tracing::info!("Spawning {} processor workers.", num_processor_workers);
    let mut processor_handles = Vec::new();
    let processing_semaphore = Arc::new(Semaphore::new(num_processor_workers)); // Limit concurrent active processors

    use tokio_stream::wrappers::ReceiverStream;
    use tokio_stream::StreamExt;

    let mut receiver_stream = ReceiverStream::new(rx);

    while let Some(received_item_result) = receiver_stream.next().await {
        let permit = Arc::clone(&processing_semaphore).acquire_owned().await?; // Or handle error
        let schema_lookup_clone = Arc::clone(&schema_lookup_arc);
        let duckdb_file_path_clone = Arc::clone(&duckdb_file_path);

        let processor_task_handle = tokio::spawn(async move {
            match received_item_result {
                Ok(zip_path) => {
                    let current_zip_filename = zip_path
                        .file_name()
                        .map(|s| s.to_string_lossy().into_owned())
                        .unwrap_or_else(|| zip_path.to_string_lossy().into_owned());

                    tracing::info!(zip_filename = %current_zip_filename, file_path = %zip_path.display(), "Processor picked up downloaded file.");
                    let conn = match duck::open_file_db(duckdb_file_path_clone.as_str()) {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::error!(zip_filename = %current_zip_filename, file_path = %zip_path.display(), error = %e, "Processor: Failed to open DuckDB connection. Skipping file.");
                            drop(permit); // Release permit early
                            return;
                        }
                    };

                    match process::load_aemo_zip(&zip_path) {
                        Ok(loaded_data_map) if !loaded_data_map.is_empty() => {
                            let mut all_tables_in_zip_processed_successfully = true;
                            for (aemo_table_name, raw_table_data) in &loaded_data_map {
                                if let Some(evolution) = schema::find_schema_evolution(
                                    &schema_lookup_clone,
                                    aemo_table_name,
                                    &raw_table_data.effective_month,
                                ) {
                                    let duckdb_target_table_name = format!(
                                        "{}_{}",
                                        evolution.table_name, evolution.fields_hash
                                    );
                                    tracing::debug!(zip_filename = %current_zip_filename, source_csv = %aemo_table_name, target_duckdb = %duckdb_target_table_name, rows = raw_table_data.rows.len(), "Inserting data.");
                                    if let Err(e) = duck::insert_raw_table_data(
                                        &conn,
                                        &duckdb_target_table_name,
                                        raw_table_data,
                                        &evolution,
                                    ) {
                                        tracing::error!(zip_filename = %current_zip_filename, error = %e, duckdb_table = %duckdb_target_table_name, source_zip_path = %zip_path.display(), "Failed to insert data.");
                                        all_tables_in_zip_processed_successfully = false;
                                    }
                                } else {
                                    tracing::warn!(zip_filename = %current_zip_filename, aemo_table = %aemo_table_name, month = %raw_table_data.effective_month, source_zip_path = %zip_path.display(), "No matching schema evolution found. Skipping table insertion.");
                                    all_tables_in_zip_processed_successfully = false;
                                }
                            }

                            if all_tables_in_zip_processed_successfully {
                                tracing::info!(zip_filename = %current_zip_filename, file_path = %zip_path.display(), "All tables from ZIP processed, recording history.");
                                if let Err(e) = history::record_processed_csv_data(
                                    &conn,
                                    &zip_path,
                                    &loaded_data_map,
                                ) {
                                    tracing::error!(zip_filename = %current_zip_filename, error = %e, zip_file_path = %zip_path.display(), "Failed to record processed CSV data. File will not be deleted.");
                                } else {
                                    tracing::info!(zip_filename = %current_zip_filename, file_path = %zip_path.display(), "History recorded. Attempting to delete ZIP file.");
                                    if let Err(e) = fs::remove_file(&zip_path) {
                                        tracing::error!(zip_filename = %current_zip_filename, error = %e, file_path = %zip_path.display(), "Failed to delete processed ZIP file.");
                                    } else {
                                        tracing::info!(zip_filename = %current_zip_filename, file_path = %zip_path.display(), "Successfully deleted processed ZIP file.");
                                    }
                                }
                            } else {
                                tracing::warn!(zip_filename = %current_zip_filename, file_path = %zip_path.display(), "Skipping history record and deletion for ZIP due to internal processing errors or missing schemas.");
                            }
                        }
                        Ok(_) => {
                            // loaded_data_map is empty
                            tracing::info!(zip_filename = %current_zip_filename, file_path = %zip_path.display(), "ZIP contained no data tables to process. Deleting file.");
                            if let Err(e) = fs::remove_file(&zip_path) {
                                tracing::error!(zip_filename = %current_zip_filename, error = %e, file_path = %zip_path.display(), "Failed to delete empty ZIP file.");
                            }
                        }
                        Err(e) => {
                            // Error from process::load_aemo_zip
                            tracing::error!(zip_filename = %current_zip_filename, error = %e, file_path = %zip_path.display(), "Failed to load/process AEMO ZIP. File may remain.");
                        }
                    }
                }
                Err((url, download_err_msg)) => {
                    let failed_zip_filename_from_url = Path::new(&url)
                        .file_name()
                        .map(|s| s.to_string_lossy().into_owned())
                        .unwrap_or_else(|| url.clone());
                    // Error during download stage
                    tracing::error!(zip_filename = %failed_zip_filename_from_url, url = %url, error = %download_err_msg, "Download for this URL failed. It will not be processed.");
                }
            }
            drop(permit); // Release processing permit
        });
        processor_handles.push(processor_task_handle);
    }

    // 7. Wait for completion
    tracing::info!(
        "Waiting for all {} download tasks to complete their dispatches...",
        download_handles.len()
    );
    for handle in download_handles {
        // These tasks finish once they've sent to the channel
        if let Err(e) = handle.await {
            tracing::error!("A download task panicked: {:?}", e); // Note: zip_filename not easily available here without more context on panic.
        }
    }
    tracing::info!("All download tasks have completed their dispatches.");

    tracing::info!(
        "Waiting for all {} processing tasks to complete...",
        processor_handles.len()
    );
    join_all(processor_handles).await; // Wait for all spawned processor tasks
    tracing::info!("All processing tasks have completed.");

    tracing::info!("Application finished successfully.");
    Ok(())
}
