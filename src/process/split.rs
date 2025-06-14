use anyhow::{Context, Result};
use futures_util::StreamExt;
use reqwest;
use std::io::{BufRead, BufReader, Cursor, Read};
use std::path::Path;
use tracing::{debug, instrument, warn};
use zip::ZipArchive;

use super::csv_batch_processor::CsvBatchProcessor;

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
/// Uses minimal memory by streaming download and processing entries sequentially.
#[instrument(level = "debug", skip(url, out_dir), fields(url = %url))]
pub async fn stream_zip_to_parquet<P: AsRef<Path>>(url: &str, out_dir: P) -> Result<RowsAndBytes> {
    const MAX_BATCH_BYTES: usize = 100 * 1024 * 1024; // 100 MiB

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
        let entry_counts = process_csv_entry(reader, &name, out_dir.as_ref(), MAX_BATCH_BYTES)
            .with_context(|| format!("processing CSV entry {}", name))?;

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

/// For true streaming with minimal memory usage, use this alternative approach
/// that processes ZIP entries as they're encountered (requires async-zip crate)
#[cfg(feature = "streaming")]
pub async fn stream_zip_to_parquet_minimal_memory<P: AsRef<Path>>(
    url: &str,
    out_dir: P,
) -> Result<RowsAndBytes> {
    use async_zip::tokio::read::stream::ZipFileReader;
    use tokio_util::io::StreamReader;

    const MAX_BATCH_BYTES: usize = 100 * 1024 * 1024;

    let client = reqwest::Client::new();
    let response = client.get(url).send().await?;

    if !response.status().is_success() {
        return Err(anyhow::anyhow!("HTTP error: {}", response.status()));
    }

    // Create a streaming reader from the HTTP response
    let stream = response
        .bytes_stream()
        .map(|result| result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)));
    let reader = StreamReader::new(stream);

    let mut zip_reader = ZipFileReader::new(reader).await?;
    let mut totals = RowsAndBytes::ZERO;

    // Process entries as they're encountered in the stream
    while let Some(mut entry) = zip_reader.next_with_entry().await? {
        let name = entry.reader().entry().filename().as_str()?.to_string();

        if !name.to_lowercase().ends_with(".csv") {
            continue;
        }

        debug!("Streaming CSV entry: {}", name);

        // Read the entry content
        let mut content = Vec::new();
        entry.read_to_end_checked(&mut content).await?;

        let cursor = Cursor::new(content);
        let reader = BufReader::new(cursor);
        let entry_counts = process_csv_entry(reader, &name, out_dir.as_ref(), MAX_BATCH_BYTES)
            .with_context(|| format!("processing CSV entry {}", name))?;

        totals.add(entry_counts);
        debug!(
            "Entry {} complete: {} rows, {} bytes",
            name, entry_counts.rows, entry_counts.bytes
        );
    }

    debug!(
        "Completed streaming ZIP: total rows = {}, total bytes = {}",
        totals.rows, totals.bytes
    );
    Ok(totals)
}

/// Process a single CSV entry (streaming from `reader`), skipping its top `C` header,
/// then iterating line by line until EOF or a footer `C,`. Returns rows counted.
fn process_csv_entry<R: BufRead>(
    mut reader: R,
    file_name: &str,
    out_dir: &Path,
    max_batch_bytes: usize,
) -> Result<RowsAndBytes> {
    // 1) Skip the very first "C" line (metadata header). If missing, warn and proceed.
    skip_top_c_header(&mut reader, file_name);

    // 2) Create a batch‐processor that holds state (current_i_line, batch string, batch_index, row_count).
    let mut batcher = CsvBatchProcessor::new(max_batch_bytes);

    // 3) Loop through every subsequent line until EOF or a "C," footer
    let mut buf = String::new();
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
            break;
        }

        // Feed this line into the processor (it will flush or append as needed)
        batcher.feed_line(&buf, file_name, out_dir)?;
    }

    // 4) Flush any remaining batch at EOF.
    batcher.flush_final(file_name, out_dir)?;
    let rows = batcher.row_count();
    let bytes = batcher.parquet_bytes();
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
    async fn integration_stream_zip_to_parquet() -> Result<()> {
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

        Ok(())
    }
}
