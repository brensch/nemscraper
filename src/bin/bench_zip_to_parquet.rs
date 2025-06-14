use std::env;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::Result;
use nemscraper::process3::split::stream_zip_to_parquet;
use tracing::Level;
use tracing_subscriber::{self, EnvFilter};

fn print_usage_and_exit(program: &str) -> ! {
    eprintln!("Usage: {} <input-zip-url-or-path> <output-dir>", program);
    eprintln!("Examples:");
    eprintln!("  {} https://example.com/data.zip ./output", program);
    eprintln!("  {} /path/to/local/file.zip ./output", program);
    std::process::exit(1);
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize a basic tracing subscriber so that the `#[instrument]` on split_zip_to_parquet logs work.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(Level::DEBUG.into()))
        .init();

    let mut args = env::args();
    let program = args.next().unwrap_or_else(|| "splitter".into());

    // Expect exactly two positional arguments: input path/URL and output directory.
    let (input_path_or_url, out_dir) = match (args.next(), args.next()) {
        (Some(i), Some(o)) => (i, PathBuf::from(o)),
        _ => print_usage_and_exit(&program),
    };

    // Time the call to stream_zip_to_parquet
    let start = Instant::now();

    let result =
        if input_path_or_url.starts_with("http://") || input_path_or_url.starts_with("https://") {
            // It's a URL - use streaming download
            stream_zip_to_parquet(&input_path_or_url, &out_dir).await
        } else {
            // It's a local file path - you might want to add a local file version
            // For now, just error out or you could add a local file handler
            return Err(anyhow::anyhow!(
                "Local file processing not implemented yet. Please provide a URL."
            ));
        };

    let elapsed = start.elapsed();

    match result {
        Ok(rows_and_bytes) => {
            println!(
                "✅ Successfully processed `{}` to `{}` in {:.3}s",
                input_path_or_url,
                out_dir.display(),
                elapsed.as_secs_f64()
            );
            println!(
                "📊 Processed {} rows, {} bytes of Parquet output",
                rows_and_bytes.rows, rows_and_bytes.bytes
            );
        }
        Err(e) => {
            eprintln!("❌ Error processing `{}`: {}", input_path_or_url, e);
            std::process::exit(1);
        }
    }

    Ok(())
}
