use anyhow::{Context, Result};
use futures_util::StreamExt;
use reqwest;
use std::io::{BufRead, BufReader, Cursor};
use std::path::Path;
use tracing::{debug, instrument, warn};
use zip::ZipArchive;

// Import the unified processor
use super::csv_processor::process_csv_entry_unified;

/// A simple accumulator for rows and bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowsAndBytes {
    pub rows: u64,
    pub bytes: u64,
}

impl RowsAndBytes {
    pub const ZERO: Self = RowsAndBytes { rows: 0, bytes: 0 };

    /// Add another RowsAndBytes into `self`, saturating on overflow.
    pub fn add(&mut self, other: RowsAndBytes) {
        self.rows = self.rows.saturating_add(other.rows);
        self.bytes = self.bytes.saturating_add(other.bytes);
    }
}

/// Streams a ZIP file from URL, processes each CSV into chunks by D-row count.
/// Uses minimal memory by streaming download and processing entries sequentially.
#[instrument(level = "debug", skip(url, out_dir), fields(url = %url))]
pub async fn stream_zip_to_parquet<P: AsRef<Path>>(url: &str, out_dir: P) -> Result<RowsAndBytes> {
    let max_data_rows_per_chunk = 1_000_000;
    debug!("Starting download from: {}", url);

    // Stream the ZIP file from URL
    let client = reqwest::Client::new();
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("Failed to send request to {}", url))?;

    if !response.status().is_success() {
        return Err(anyhow::anyhow!("HTTP error: {}", response.status()));
    }

    // Stream the response body into memory with progress tracking
    let mut zip_data = Vec::new();
    let mut stream = response.bytes_stream();
    let mut total_downloaded = 0u64;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("Failed to read chunk from response")?;
        zip_data.extend_from_slice(&chunk);
        total_downloaded += chunk.len() as u64;

        // Log progress every 10MB
        if total_downloaded % (10 * 1024 * 1024) == 0 {
            debug!("Downloaded {} MB", total_downloaded / (1024 * 1024));
        }
    }

    debug!("Download complete: {} bytes", zip_data.len());

    // Process the ZIP from memory
    let cursor = Cursor::new(zip_data);
    let mut archive =
        ZipArchive::new(cursor).context("Failed to create ZipArchive from downloaded data")?;

    let mut totals = RowsAndBytes::ZERO;

    for idx in 0..archive.len() {
        let entry = archive.by_index(idx)?;
        let name = entry.name().to_string();

        if !name.to_lowercase().ends_with(".csv") {
            continue;
        }

        debug!("Processing CSV entry: {}", name);
        let reader = BufReader::new(entry);

        let (rows, bytes) =
            process_csv_entry_unified(reader, &name, out_dir.as_ref(), max_data_rows_per_chunk)
                .with_context(|| format!("processing CSV entry {}", name))?;

        let entry_counts = RowsAndBytes { rows, bytes };
        totals.add(entry_counts);

        debug!(
            "Entry {} complete: {} rows, {} bytes",
            name, entry_counts.rows, entry_counts.bytes
        );
    }

    debug!(
        "Completed ZIP: total rows = {}, total bytes = {}",
        totals.rows, totals.bytes
    );
    Ok(totals)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use glob::glob;
    use tempfile::tempdir;
    use tokio;
    use tracing_subscriber::{fmt, EnvFilter};

    fn init_logging() {
        let _ = fmt()
            .with_env_filter(EnvFilter::new("debug"))
            .with_target(false)
            .try_init();
    }

    #[tokio::test]
    async fn integration_stream_zip_to_parquet() -> Result<()> {
        init_logging();

        // Test with a public ZIP file URL
        let url = "https://example.com/test.zip"; // Replace with actual test URL
        let output_dir = tempdir()?;
        let max_rows_per_chunk = 10_000; // 10k D-rows per chunk

        let RowsAndBytes { rows, bytes } = stream_zip_to_parquet(url, output_dir.path()).await?;

        assert!(rows > 0, "Expected at least one row, got {}", rows);
        assert!(bytes > 0, "Expected some bytes, got {}", bytes);

        // Check for generated parquet files
        let pattern = format!("{}/**/*.parquet", output_dir.path().display());
        let parquet_files: Vec<_> = glob(&pattern)?.filter_map(Result::ok).collect();
        assert!(!parquet_files.is_empty(), "No .parquet files found");

        Ok(())
    }
}
