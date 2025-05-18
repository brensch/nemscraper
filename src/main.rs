use nemscraper::{duck, fetch, history, schema};
use reqwest::Client; // Use crate name to access library modules
                     // HashSet is no longer needed here as each evolution gets its own table
use std::fs;
use std::path::Path;
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize the tracing subscriber
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("info,nemscraper::schema=debug,nemscraper::duck=debug,main=info")
    }); // Adjusted log levels

    fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_span_events(fmt::format::FmtSpan::CLOSE)
        .init();

    tracing::info!("Application starting up. Logger initialized.");

    let client = Client::new();
    let schemas_output_dir = Path::new("schemas");
    let schema_evolutions_output_file = Path::new("schema_evolutions.json");
    let zips_output_dir = Path::new("zips");
    let duckdb_file_path = "nem_data.duckdb";

    // Ensure output directories exist
    fs::create_dir_all(schemas_output_dir)?;
    fs::create_dir_all(zips_output_dir)?;

    tracing::info!(output_dir = %schemas_output_dir.display(), "Starting schema fetch process...");
    match schema::fetch_all(&client, schemas_output_dir).await {
        Ok(fetched_month_schemas) => {
            tracing::info!(
                count = fetched_month_schemas.len(),
                "Schema fetch process completed."
            );
        }
        Err(e) => {
            tracing::error!(error = %e, "Schema fetch process failed.");
        }
    }

    tracing::info!(input_dir = %schemas_output_dir.display(), "Starting schema evolution extraction...");
    let mut processed_evolutions: Vec<schema::SchemaEvolution> = Vec::new(); // Store evolutions for DuckDB step

    match schema::extract_schema_evolutions(schemas_output_dir) {
        Ok(evolutions) => {
            tracing::info!(
                count = evolutions.len(),
                output_file = %schema_evolutions_output_file.display(),
                "Schema evolution extraction completed successfully."
            );
            processed_evolutions = evolutions.clone(); // Clone for later use

            match serde_json::to_string_pretty(&evolutions) {
                Ok(json_data) => {
                    if let Err(e) = fs::write(schema_evolutions_output_file, json_data) {
                        tracing::warn!(error = %e, file_path = %schema_evolutions_output_file.display(), "Failed to write schema evolutions to disk.");
                    } else {
                        tracing::info!(file_path = %schema_evolutions_output_file.display(), "Schema evolutions saved to disk.");
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to serialize schema evolutions to JSON.");
                }
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "Schema evolution extraction failed. Skipping DuckDB table creation.");
        }
    }

    // --- Create DuckDB tables for each distinct schema version ---
    if !processed_evolutions.is_empty() {
        tracing::info!(duckdb_file = %duckdb_file_path, "Attempting to create tables in DuckDB for each schema version.");
        match duck::open_file_db(duckdb_file_path) {
            Ok(conn) => {
                let mut tables_created_count = 0;
                let mut tables_failed_count = 0;

                // Each evolution represents a distinct schema version for a table over a period.
                // We will create a unique DuckDB table for each evolution.
                for evolution in &processed_evolutions {
                    // Construct a unique table name for this specific schema version.
                    // Using a simple concatenation. Ensure this naming convention is valid for DuckDB.
                    // Hashes can be long; consider a prefix or a shorter unique ID if necessary,
                    // but for now, hash ensures uniqueness.
                    // DuckDB table names are case-insensitive by default unless quoted.
                    // Let's keep it simple and rely on the schema module to quote if needed,
                    // or ensure names are valid. The create_table_from_schema function already quotes.
                    let duckdb_table_name =
                        format!("{}_{}", evolution.table_name, evolution.fields_hash);

                    tracing::debug!(duckdb_table = %duckdb_table_name, original_table = %evolution.table_name, hash = %evolution.fields_hash, "Defining schema version table in DuckDB.");

                    match duck::create_table_from_schema(
                        &conn,
                        &duckdb_table_name, // Use the version-specific name
                        &evolution.columns,
                    ) {
                        Ok(_) => {
                            tables_created_count += 1;
                            // Table for this specific schema version created (or already existed)
                        }
                        Err(e) => {
                            tables_failed_count += 1;
                            tracing::error!(duckdb_table = %duckdb_table_name, error = %e, "Failed to create schema version table in DuckDB.");
                        }
                    }
                }
                tracing::info!(
                    created_count = tables_created_count,
                    failed_count = tables_failed_count,
                    total_evolutions_processed = processed_evolutions.len(),
                    "DuckDB schema version table creation process finished."
                );
            }
            Err(e) => {
                tracing::error!(error = %e, duckdb_file = %duckdb_file_path, "Failed to open DuckDB database. Skipping table creation.");
            }
        }
    } else {
        tracing::info!("No schema evolutions processed, skipping DuckDB table creation.");
    }

    // open a DuckDB connection
    let conn = duck::open_file_db(duckdb_file_path).expect("Failed to open DuckDB database");
    history::ensure_history_table_exists(&conn).expect("failed to ensure history table exists");

    let already_processed = history::get_distinct_processed_csv_identifiers(&conn)
        .expect("failed to get already processed identifiers");

    tracing::debug!(
        count = already_processed.len(),
        "Already processed identifiers fetched."
    );
    // --- Existing ZIP fetching logic ---
    tracing::info!("Fetching current ZIP URLs...");
    match fetch::urls::fetch_current_zip_urls(&client).await {
        // Corrected path to urls module
        Ok(feeds) => {
            tracing::info!(feed_count = feeds.len(), "Current ZIP URLs fetched.");
            let fpp_key = "https://nemweb.com.au/Reports/Current/FPP/";
            if let Some(zip_files) = feeds.get(fpp_key) {
                if let Some(some_zip_url) = zip_files.get(0) {
                    tracing::info!(zip_url = %some_zip_url, output_dir = %zips_output_dir.display(), "Downloading specific ZIP...");
                    match fetch::zips::download_zip(&client, some_zip_url, zips_output_dir).await {
                        // Corrected path to zips module
                        Ok(_) => tracing::info!("Specific ZIP downloaded successfully."),
                        Err(e) => tracing::error!(error = %e, "Failed to download specific ZIP."),
                    }
                } else {
                    tracing::warn!(
                        "No ZIP files found for the FPP URL ('{}') in the feeds.",
                        fpp_key
                    );
                }
            } else {
                tracing::warn!("FPP URL key ('{}') not found in feeds.", fpp_key);
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "Failed to fetch current ZIP URLs.");
        }
    }

    tracing::info!("Application finished successfully.");
    Ok(())
}
