use nemscraper::{duck, fetch, history, process, schema};
use reqwest::Client;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tokio::task;
use num_cpus;
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

    // 3. Ensure history table exists and configure database settings
    let conn = duck::open_file_db(duckdb_file_path)?;
    
    // No need for special configuration - DuckDB handles WAL automatically
    // We just need to make sure our workers use the mutex for serialized access
    
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

    // 5. Concurrent Downloading with Parallel Processing
    // Use a larger buffer to prevent blocking downloads
    let (tx, mut rx) = mpsc::channel::<Result<PathBuf, (String, String)>>(1024);
    let download_semaphore = Arc::new(Semaphore::new(3)); // Max 3 concurrent downloads

    // Start downloads
    let mut download_handles = Vec::with_capacity(urls_to_download.len());
    for zip_url in urls_to_download {
        let client_clone = Arc::clone(&client);
        let zips_output_dir_clone = Arc::clone(&zips_output_dir);
        let tx_clone = tx.clone();
        let semaphore_clone = Arc::clone(&download_semaphore);

        let download_handle = tokio::spawn(async move {
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
                    
                    // Use a separate task to send to channel to prevent blocking this task
                    let tx_for_err = tx_clone.clone();
                    let zip_url_for_err = zip_url.clone();
                    tokio::spawn(async move {
                        if tx_for_err.send(Err((zip_url_for_err, err_msg))).await.is_err() {
                            // Just log and continue if sending fails
                        }
                    });
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

                    // Use a separate task to send to channel to prevent blocking this task
                    let tx_for_ok = tx_clone.clone();
                    let path_clone = path.clone();
                    tokio::spawn(async move {
                        if tx_for_ok.send(Ok(path_clone)).await.is_err() {
                            // Just log and continue if sending fails
                        }
                    });
                }
                Err(e) => {
                    let download_failure_message = e.to_string();
                    tracing::error!(url = %zip_url, zip_filename = %zip_filename_from_url, 
                        error = %download_failure_message, "Failed to download ZIP.");

                    // Use a separate task to send to channel to prevent blocking this task
                    let tx_for_err = tx_clone.clone();
                    let zip_url_for_err = zip_url.clone();
                    let download_failure_msg_clone = download_failure_message.clone();
                    tokio::spawn(async move {
                        if tx_for_err.send(Err((zip_url_for_err, download_failure_msg_clone))).await.is_err() {
                            // Just log and continue if sending fails
                        }
                    });
                }
            }
            drop(permit); // Release semaphore permit
        });
        download_handles.push(download_handle);
    }
    drop(tx); // Drop the original sender
    
    // Create a task to wait for all downloads to complete
    let downloads_complete_handle = tokio::spawn(async move {
        for (i, handle) in download_handles.into_iter().enumerate() {
            if let Err(e) = handle.await {
                tracing::error!(download_index = i, error = %e, "Download task panicked");
            }
        }
        tracing::info!("All download tasks have completed");
    });

    // 6. Parallel Processing of Downloaded Files
    tracing::info!("Starting parallel processing of downloaded files using thread pool.");

    // Create a synchronized way to protect database access
    let db_mutex = Arc::new(tokio::sync::Mutex::new(()));
    
    // Create a synchronized completion mechanism
    let (completion_tx, mut completion_rx) = mpsc::channel::<()>(1);
    let completion_tx = Arc::new(completion_tx);

    // Create multiple channels for processing tasks (one per worker)
    let num_workers = num_cpus::get();
    tracing::info!("Creating {} worker threads for parallel processing", num_workers);

    // Create worker channels and store their senders
    let mut worker_senders: Vec<mpsc::Sender<Result<PathBuf, (String, String)>>> = Vec::with_capacity(num_workers);
    let db_file_path = duckdb_file_path.to_string();
    let schema_lookup_for_workers = Arc::clone(&schema_lookup_arc);
    
    // Create a Vec to store all worker task handles
    let mut worker_handles = Vec::with_capacity(num_workers);
    
    // Create worker threads
    for worker_id in 0..num_workers {
        // Create a channel for this worker
        let (worker_tx, mut worker_rx) = mpsc::channel(32);
        worker_senders.push(worker_tx);
        
        let worker_schema_lookup = Arc::clone(&schema_lookup_for_workers);
        let worker_db_path = db_file_path.clone();
        let worker_completion_tx = Arc::clone(&completion_tx);
        let worker_db_mutex = Arc::clone(&db_mutex);
        
        let worker_handle = tokio::spawn(async move {
            tracing::info!(worker_id = worker_id, "Processing worker started");
            
            // Each worker gets its own DuckDB connection
            let worker_conn = match duck::open_file_db(&worker_db_path) {
                Ok(conn) => conn,
                Err(e) => {
                    tracing::error!(worker_id = worker_id, error = %e, 
                        "Worker failed to open DuckDB connection. Worker stopping.");
                    return;
                }
            };

            while let Some(received_item_result) = worker_rx.recv().await {
                match received_item_result {
                    Ok(zip_path) => {
                        let current_zip_filename = zip_path
                            .file_name()
                            .map(|s| s.to_string_lossy().into_owned())
                            .unwrap_or_else(|| zip_path.to_string_lossy().into_owned());

                        tracing::info!(worker_id = worker_id, zip_filename = %current_zip_filename, 
                            file_path = %zip_path.display(), "Worker processing downloaded file.");
                        
                        match process::load_aemo_zip(&zip_path) {
                            Ok(loaded_data_map) if !loaded_data_map.is_empty() => {
                                let mut all_tables_processed = true;
                                
                                for (aemo_table_name, raw_table_data) in &loaded_data_map {
                                    if let Some(evolution) = schema::find_schema_evolution(
                                        &worker_schema_lookup, 
                                        aemo_table_name,
                                        &raw_table_data.effective_month,
                                    ) {
                                        let duckdb_table_name = format!(
                                            "{}_{}",
                                            evolution.table_name, 
                                            evolution.fields_hash
                                        );

                                        tracing::debug!(worker_id = worker_id, zip_filename = %current_zip_filename, 
                                            source_csv = %aemo_table_name, 
                                            target_duckdb = %duckdb_table_name, 
                                            rows = raw_table_data.rows.len(), 
                                            "Worker inserting data.");
                                            
                                        // Acquire mutex before database operations
                                        let _db_lock = worker_db_mutex.lock().await;
                                            
                                        if let Err(e) = duck::insert_raw_table_data(
                                            &worker_conn,
                                            &duckdb_table_name,
                                            raw_table_data,
                                            &evolution,
                                        ) {
                                            tracing::error!(worker_id = worker_id, zip_filename = %current_zip_filename, 
                                                error = %e, 
                                                duckdb_table = %duckdb_table_name, 
                                                source_zip_path = %zip_path.display(), 
                                                "Worker failed to insert data.");
                                            all_tables_processed = false;
                                        }
                                    } else {
                                        tracing::warn!(worker_id = worker_id, zip_filename = %current_zip_filename, 
                                            aemo_table = %aemo_table_name, 
                                            month = %raw_table_data.effective_month, 
                                            source_zip_path = %zip_path.display(), 
                                            "Worker found no matching schema evolution. Skipping table insertion.");
                                        all_tables_processed = false;
                                    }
                                }

                                if all_tables_processed {
                                    tracing::info!(worker_id = worker_id, zip_filename = %current_zip_filename, 
                                        file_path = %zip_path.display(), 
                                        "Worker completed processing all tables from ZIP, recording history.");
                                    
                                    // Uncomment if you want to record history and delete processed files
                                    // if let Err(e) = history::record_processed_csv_data(
                                    //     &worker_conn,
                                    //     &zip_path,
                                    //     &loaded_data_map,
                                    // ) {
                                    //     tracing::error!(worker_id = worker_id, zip_filename = %current_zip_filename, 
                                    //         error = %e, 
                                    //         zip_file_path = %zip_path.display(), 
                                    //         "Worker failed to record processed CSV data. File will not be deleted.");
                                    // } else {
                                    //     tracing::info!(worker_id = worker_id, zip_filename = %current_zip_filename, 
                                    //         file_path = %zip_path.display(), 
                                    //         "Worker recorded history. Attempting to delete ZIP file.");
                                    //     if let Err(e) = fs::remove_file(&zip_path) {
                                    //         tracing::error!(worker_id = worker_id, zip_filename = %current_zip_filename, 
                                    //             error = %e, 
                                    //             file_path = %zip_path.display(), 
                                    //             "Worker failed to delete processed ZIP file.");
                                    //     } else {
                                    //         tracing::info!(worker_id = worker_id, zip_filename = %current_zip_filename, 
                                    //             file_path = %zip_path.display(), 
                                    //             "Worker successfully deleted processed ZIP file.");
                                    //     }
                                    // }
                                } else {
                                    tracing::warn!(worker_id = worker_id, zip_filename = %current_zip_filename, 
                                        file_path = %zip_path.display(), 
                                        "Worker skipping history record and deletion due to processing errors or missing schemas.");
                                }
                            }
                            Ok(_) => {
                                // ZIP is empty
                                tracing::info!(worker_id = worker_id, zip_filename = %current_zip_filename, 
                                    file_path = %zip_path.display(), 
                                    "Worker found ZIP contained no data tables to process. Deleting file.");
                                if let Err(e) = fs::remove_file(&zip_path) {
                                    tracing::error!(worker_id = worker_id, zip_filename = %current_zip_filename, 
                                        error = %e, 
                                        file_path = %zip_path.display(), 
                                        "Worker failed to delete empty ZIP file.");
                                }
                            }
                            Err(e) => {
                                tracing::error!(worker_id = worker_id, zip_filename = %current_zip_filename, 
                                    error = %e, 
                                    file_path = %zip_path.display(), 
                                    "Worker failed to load/process AEMO ZIP. File may remain.");
                            }
                        }
                    }
                    Err((url, download_err_msg)) => {
                        let failed_zip_filename = Path::new(&url)
                            .file_name()
                            .map(|s| s.to_string_lossy().into_owned())
                            .unwrap_or_else(|| url.clone());
                        
                        tracing::error!(worker_id = worker_id, zip_filename = %failed_zip_filename, 
                            url = %url, 
                            error = %download_err_msg, 
                            "Worker received download failure. File will not be processed.");
                    }
                }
            }
            
            tracing::info!(worker_id = worker_id, "Processing worker shutting down");
            
            // Signal that this worker has completed
            if let Err(e) = worker_completion_tx.send(()).await {
                tracing::warn!(worker_id = worker_id, error = %e, "Failed to send completion signal");
            }
        });
        
        worker_handles.push(worker_handle);
    }

    // Forward downloads to the processing workers using round-robin distribution
    let distribution_handle = task::spawn(async move {
        let mut current_worker = 0;
        
        while let Some(item) = rx.recv().await {
            // Round-robin assignment to workers
            if let Err(e) = worker_senders[current_worker].send(item).await {
                tracing::error!(worker_id = current_worker, error = %e, 
                    "Failed to send item to worker");
            }
            
            // Move to next worker
            current_worker = (current_worker + 1) % worker_senders.len();
        }
        
        // When all downloads are done, drop all senders to signal workers to shut down
        tracing::info!("All downloads processed, shutting down worker channels");
        drop(worker_senders);
    });
    
    // Wait for the distribution task to complete
    if let Err(e) = distribution_handle.await {
        tracing::error!(error = %e, "Error occurred while waiting for distribution task to complete");
    }
    
    // Wait for downloads to complete
    if let Err(e) = downloads_complete_handle.await {
        tracing::error!(error = %e, "Error occurred while waiting for downloads to complete");
    }
    
    // Wait for all workers to signal completion
    for _ in 0..num_workers {
        if completion_rx.recv().await.is_none() {
            tracing::warn!("Completion channel closed before receiving all worker completions");
            break;
        }
    }
    
    // Optionally wait for all worker tasks to complete
    for (i, handle) in worker_handles.into_iter().enumerate() {
        if let Err(e) = handle.await {
            tracing::error!(worker_id = i, error = %e, "Worker task panicked");
        }
    }

    tracing::info!("All files have been processed. Application finished successfully.");
    Ok(())
}