// These are your existing module declarations
mod duck;
mod fetch;
mod process;
mod schema; // This is the module we've been working on

use fetch::urls;
use fetch::zips;
use reqwest::Client;

// Add necessary imports for tracing and tracing-subscriber
// This `fmt` here refers to `tracing_subscriber::fmt`
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize the tracing subscriber
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,schema=debug")); // Default filter

    // This line requires the "fmt" feature of tracing-subscriber
    fmt::Subscriber::builder()
        .with_env_filter(env_filter) // Requires "env-filter" feature
        // .with_span_events(fmt::format::FmtSpan::CLOSE) // Optional: Uncomment to log span open/close times
        .init(); // Initialize the global logger

    tracing::info!("Application starting up. Logger initialized.");

    // Your existing application logic
    let client = Client::new();

    tracing::info!("Starting schema fetch process...");
    schema::fetch_all(&client, "schemas").await?;
    tracing::info!("Schema fetch process completed.");

    tracing::info!("Fetching current ZIP URLs...");
    let feeds = urls::fetch_current_zip_urls(&client).await?;
    tracing::info!("Current ZIP URLs fetched.");

    let fpp_key = "https://nemweb.com.au/Reports/Current/FPP/";
    if let Some(zip_files) = feeds.get(fpp_key) {
        if let Some(some_zip) = zip_files.get(0) {
            // Assuming `some_zip` is a struct with a field that can be logged (e.g., name or url)
            // If `some_zip` is just a URL string, you can log it directly.
            // For this example, let's assume it's a URL string or has a relevant Display/Debug impl.
            tracing::info!("Downloading specific ZIP: {:?}", some_zip);
            zips::download_zip(&client, some_zip, "zips").await?;
            tracing::info!("Specific ZIP downloaded.");
        } else {
            tracing::warn!(
                "No ZIP files found for the FPP URL ('{}') in the feeds.",
                fpp_key
            );
        }
    } else {
        tracing::warn!("FPP URL key ('{}') not found in feeds.", fpp_key);
    }

    tracing::info!("Application finished successfully.");
    Ok(())
}
