// These are your existing module declarations
mod duck;
mod fetch;
mod process;
mod schema; // This is the module we've been working on

use fetch::urls;
use fetch::zips;
use reqwest::Client;
use std::fs; // For creating the output directory for schema evolutions if needed
use std::path::Path; // For path manipulation

// Add necessary imports for tracing and tracing-subscriber
// This `fmt` here refers to `tracing_subscriber::fmt`
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize the tracing subscriber
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,schema=debug,main=info")); // Added main=info

    // This line requires the "fmt" feature of tracing-subscriber
    fmt::Subscriber::builder()
        .with_env_filter(env_filter) // Requires "env-filter" feature
        .with_span_events(fmt::format::FmtSpan::CLOSE) // Optional: Uncomment to log span open/close times more verbosely
        .init(); // Initialize the global logger

    tracing::info!("Application starting up. Logger initialized.");

    let client = Client::new();
    let schemas_output_dir = Path::new("schemas");
    let schema_evolutions_output_file = Path::new("schema_evolutions.json");
    let zips_output_dir = Path::new("zips");

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
            // Depending on requirements, you might want to exit here or attempt to continue
            // For now, we'll let it proceed to schema evolution if fetch_all failed partially or if old files exist
        }
    }

    tracing::info!(input_dir = %schemas_output_dir.display(), "Starting schema evolution extraction...");
    match schema::extract_schema_evolutions(schemas_output_dir) {
        Ok(evolutions) => {
            tracing::info!(
                count = evolutions.len(),
                output_file = %schema_evolutions_output_file.display(),
                "Schema evolution extraction completed successfully."
            );
            // Optionally, save the evolutions to a file
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
            // For verbose logging, you could log the evolutions themselves:
            // for (i, evolution) in evolutions.iter().take(5).enumerate() { // Log first 5 as an example
            //     tracing::debug!(index = i, evolution = ?evolution, "Schema Evolution Detail");
            // }
        }
        Err(e) => {
            tracing::error!(error = %e, "Schema evolution extraction failed.");
        }
    }

    tracing::info!("Fetching current ZIP URLs...");
    match urls::fetch_current_zip_urls(&client).await {
        Ok(feeds) => {
            tracing::info!(feed_count = feeds.len(), "Current ZIP URLs fetched.");

            let fpp_key = "https://nemweb.com.au/Reports/Current/FPP/";
            if let Some(zip_files) = feeds.get(fpp_key) {
                if let Some(some_zip_url) = zip_files.get(0) {
                    // Assuming zip_files is Vec<Url> or Vec<String>
                    tracing::info!(zip_url = %some_zip_url, output_dir = %zips_output_dir.display(), "Downloading specific ZIP...");
                    // Assuming zips::download_zip expects a Url or something convertible
                    // If some_zip_url is already a reqwest::Url, great. If it's a string, parse it.
                    // For simplicity, let's assume it can be passed directly or your `download_zip` handles it.
                    // If `zips::download_zip` takes a &str for the URL:
                    // let url_to_download = some_zip_url.as_str(); // If it's a String or Url
                    // Or if it's a Url from your `Workspace_current_zip_urls`
                    match zips::download_zip(&client, some_zip_url, zips_output_dir).await {
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
