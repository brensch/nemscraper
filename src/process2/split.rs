use anyhow::{Context, Result};
use futures_util::StreamExt;
use reqwest;
use std::io::{BufRead, BufReader, Cursor};
use std::path::Path;
use tracing::{debug, instrument, warn};
use zip::ZipArchive;

use super::csv_batch_processor::OptimizedCsvBatchProcessor;

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

/// Streams a ZIP file from URL, processes each CSV into ~100 MiB Parquet chunks.
/// Uses optimized processing with direct Arrow array construction.
#[instrument(level = "debug", skip(url, out_dir), fields(url = %url))]
pub async fn stream_zip_to_parquet<P: AsRef<Path>>(url: &str, out_dir: P) -> Result<RowsAndBytes> {
    const MAX_BATCH_BYTES: usize = 100 * 1024 * 1024; // 100 MiB

    debug!("Starting optimized download from: {}", url);

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

        debug!("Processing CSV entry with optimized processor: {}", name);
        let reader = BufReader::new(entry);
        let entry_counts =
            process_csv_entry_optimized(reader, &name, out_dir.as_ref(), MAX_BATCH_BYTES)
                .with_context(|| format!("processing CSV entry {}", name))?;

        totals.add(entry_counts);
        debug!(
            "Entry {} complete: {} rows, {} bytes",
            name, entry_counts.rows, entry_counts.bytes
        );
    }

    debug!(
        "Completed optimized ZIP processing: total rows = {}, total bytes = {}",
        totals.rows, totals.bytes
    );
    Ok(totals)
}

/// Process a single CSV entry using the optimized processor
/// Handles I-lines (schema changes) and D-lines (data) efficiently
fn process_csv_entry_optimized<R: BufRead>(
    mut reader: R,
    file_name: &str,
    out_dir: &Path,
    max_batch_bytes: usize,
) -> Result<RowsAndBytes> {
    // 1) Skip the very first "C" line (metadata header). If missing, warn and proceed.
    skip_top_c_header(&mut reader, file_name);

    // 2) Create an optimized batch processor that builds Arrow arrays directly
    let mut batcher = OptimizedCsvBatchProcessor::new(max_batch_bytes);

    // 3) Loop through every subsequent line until EOF or a "C," footer
    let mut buf = String::new();
    let mut lines_processed = 0u64;

    loop {
        buf.clear();
        let bytes_read = reader.read_line(&mut buf)?;
        if bytes_read == 0 {
            // EOF
            break;
        }

        let trimmed = buf.trim_start();
        if trimmed.starts_with("C,") {
            // footer reached → stop reading further
            debug!("Found footer C-line after {} lines", lines_processed);
            break;
        }

        // Feed this line into the optimized processor
        batcher.feed_line(&buf, file_name, out_dir)?;
        lines_processed += 1;

        // Log progress for large files
        if lines_processed % 100_000 == 0 {
            debug!("Processed {} lines in {}", lines_processed, file_name);
        }
    }

    // 4) Flush any remaining batch at EOF.
    batcher.flush_final(file_name, out_dir)?;

    let rows = batcher.row_count();
    let bytes = batcher.parquet_bytes();

    debug!(
        "Optimized processing complete for {}: {} rows, {} bytes",
        file_name, rows, bytes
    );
    Ok(RowsAndBytes { rows, bytes })
}

/// Try to read exactly one line and check if it starts with a "C". If not, warn and continue.
fn skip_top_c_header<R: BufRead>(reader: &mut R, file_name: &str) {
    let mut peek = String::new();
    match reader.read_line(&mut peek) {
        Ok(n) => {
            if n == 0 || !peek.trim_start().starts_with('C') {
                warn!(
                    "Expected a top C-line in {} but got {:?}. Continuing anyway.",
                    file_name, peek
                );
            } else {
                debug!("Found expected C-line header in {}", file_name);
            }
        }
        Err(e) => {
            warn!(
                "Unable to read top C-line in {}: {:?}. Continuing anyway.",
                file_name, e
            );
        }
    }
}

/// Legacy compatibility function - delegates to optimized version
pub async fn stream_zip_to_parquet_legacy<P: AsRef<Path>>(
    url: &str,
    out_dir: P,
) -> Result<RowsAndBytes> {
    stream_zip_to_parquet(url, out_dir).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use glob::glob;
    use std::path::PathBuf;
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
    async fn integration_optimized_stream_zip_to_parquet() -> Result<()> {
        init_logging();

        // Test with a public ZIP file URL
        let url = "https://example.com/test.zip"; // Replace with actual test URL
        let output_dir = tempdir()?;

        let RowsAndBytes { rows, bytes } = stream_zip_to_parquet(url, output_dir.path()).await?;

        assert!(rows > 0, "Expected at least one row, got {}", rows);
        assert!(bytes > 0, "Expected some bytes, got {}", bytes);

        // Check for generated parquet files
        let pattern = format!("{}/**/*.parquet", output_dir.path().display());
        let parquet_files: Vec<_> = glob(&pattern)?.filter_map(Result::ok).collect();
        assert!(!parquet_files.is_empty(), "No .parquet files found");

        debug!("Generated {} parquet files", parquet_files.len());
        for file in parquet_files.iter().take(5) {
            debug!("Generated file: {}", file.display());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_schema_change_handling() -> Result<()> {
        init_logging();

        // This test would verify that I-lines are handled correctly
        // In a real test, you'd create a test CSV with multiple I-lines
        // and verify that separate parquet files are created for each schema

        debug!("Schema change handling test - implement with real test data");
        Ok(())
    }
}

// Export for benchmarking
// pub use OptimizedCsvBatchProcessor as BenchmarkProcessor;
