use anyhow::{Context, Result};
use futures_util::StreamExt;
use reqwest;
use std::io::{BufRead, BufReader, Cursor, Read};
use std::path::Path;
use tracing::{debug, instrument, warn};
use zip::ZipArchive;

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

/// Fast ZIP processing using chunk-based CSV parsing with Arrow's vectorized operations
#[instrument(level = "debug", skip(url, out_dir), fields(url = %url))]
pub async fn stream_zip_to_parquet<P: AsRef<Path>>(url: &str, out_dir: P) -> Result<RowsAndBytes> {
    const MAX_BATCH_BYTES: usize = 100 * 1024 * 1024; // 100 MiB

    debug!("Starting fast download from: {}", url);

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

        debug!("Processing CSV entry with fast chunk processor: {}", name);
        let entry_counts = process_csv_entry_fast(entry, &name, out_dir.as_ref(), MAX_BATCH_BYTES)
            .with_context(|| format!("processing CSV entry {}", name))?;

        totals.add(entry_counts);
        debug!(
            "Entry {} complete: {} rows, {} bytes",
            name, entry_counts.rows, entry_counts.bytes
        );
    }

    debug!(
        "Completed fast ZIP processing: total rows = {}, total bytes = {}",
        totals.rows, totals.bytes
    );
    Ok(totals)
}

/// Process a single CSV entry using the fast chunk-based approach
/// This reads the entire CSV content and processes it as chunks for maximum speed
fn process_csv_entry_fast<R: Read>(
    mut reader: R,
    file_name: &str,
    out_dir: &Path,
    _max_batch_bytes: usize, // Not used in the fast approach
) -> Result<RowsAndBytes> {
    // Read the entire CSV content into memory
    let mut content = String::new();
    reader
        .read_to_string(&mut content)
        .context("reading CSV entry content")?;

    debug!("Read {} characters from {}", content.len(), file_name);

    // Find and skip the top C-line
    let processed_content = skip_c_header_from_content(&content, file_name);

    // Use the fast chunk processor to handle I-line boundaries and bulk process
    let mut processor = super::chunk::FastChunkProcessor::new();
    processor.process_csv_content(&processed_content, file_name, out_dir)?;

    let rows = processor.total_rows();
    let bytes = processor.total_bytes();

    debug!(
        "Fast processing complete for {}: {} rows, {} bytes",
        file_name, rows, bytes
    );
    Ok(RowsAndBytes { rows, bytes })
}

/// Remove the C-header from the beginning of the content and any C-footer at the end
fn skip_c_header_from_content(content: &str, file_name: &str) -> String {
    let lines: Vec<&str> = content.lines().collect();
    if lines.is_empty() {
        return String::new();
    }

    let mut start_idx = 0;
    let mut end_idx = lines.len();

    // Skip C-header at the beginning
    if let Some(first_line) = lines.first() {
        if first_line.trim_start().starts_with('C') {
            start_idx = 1;
            debug!("Skipped C-header in {}", file_name);
        } else {
            warn!(
                "Expected C-header in {} but found: {:?}",
                file_name, first_line
            );
        }
    }

    // Skip C-footer at the end
    for i in (start_idx..lines.len()).rev() {
        if lines[i].trim_start().starts_with("C,") {
            end_idx = i;
            debug!("Found C-footer at line {} in {}", i, file_name);
            break;
        }
    }

    if start_idx >= end_idx {
        warn!("No content between C-header and C-footer in {}", file_name);
        return String::new();
    }

    // Join the remaining lines
    lines[start_idx..end_idx].join("\n") + "\n"
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use tempfile::tempdir;
    use tokio;
    use tracing_subscriber::{fmt, EnvFilter};

    fn init_logging() {
        let _ = fmt()
            .with_env_filter(EnvFilter::new("debug"))
            .with_target(false)
            .try_init();
    }

    #[test]
    fn test_c_header_removal() {
        init_logging();

        let test_content = r#"C,Some metadata
I,DATE,RUN,REGION,DEMAND
D,2025/06/14 12:00:00,1,NSW1,8500.5
D,2025/06/14 12:05:00,1,NSW1,8520.3
I,DATE,RUN,REGION,DEMAND,PRICE
D,2025/06/14 12:10:00,1,NSW1,8490.8,45.67
C,Footer metadata"#;

        let processed = skip_c_header_from_content(test_content, "test");

        // Should remove C-lines at start and end
        assert!(!processed.contains("Some metadata"));
        assert!(!processed.contains("Footer metadata"));
        assert!(processed.contains("I,DATE,RUN,REGION,DEMAND"));
        assert!(processed.contains("D,2025/06/14 12:00:00,1,NSW1,8500.5"));

        println!("Processed content:\n{}", processed);
    }

    #[tokio::test]
    async fn test_fast_processing_integration() -> Result<()> {
        init_logging();

        // This would be a test with actual ZIP file processing
        // For now, just verify the functions exist and can be called

        debug!("Fast processing integration test placeholder");
        Ok(())
    }
}
