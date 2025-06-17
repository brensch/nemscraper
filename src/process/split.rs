use super::gcs_processor::process_csv_entry_gcs;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use reqwest;
use std::io::Read;
use std::io::{BufReader, Cursor};
use tokio::io::BufReader as AsyncBufReader;
use tracing::{debug, instrument};
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
/// Streams a ZIP file from URL, processes each CSV into chunks and uploads to GCS.
/// Uses minimal memory by streaming download and processing entries sequentially.
#[instrument(level = "debug", skip(url, gcs_bucket), fields(url = %url, bucket = %gcs_bucket))]
pub async fn stream_zip_to_parquet_gcs(
    url: &str,
    gcs_bucket: &str,
    gcs_prefix: Option<&str>,
) -> Result<(RowsAndBytes, Vec<String>)> {
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
    let mut all_gcs_objects = Vec::new();

    for idx in 0..archive.len() {
        let entry = archive.by_index(idx)?;
        let name = entry.name().to_string();

        if !name.to_lowercase().ends_with(".csv") {
            continue;
        }

        debug!("Processing CSV entry: {}", name);

        // Convert sync reader to async reader
        let data = {
            let mut buf = Vec::new();
            let mut reader = BufReader::new(entry);
            reader.read_to_end(&mut buf)?;
            buf
        };

        let cursor = Cursor::new(data);
        let async_reader = AsyncBufReader::new(cursor);

        let (metrics, gcs_objects) = process_csv_entry_gcs(
            async_reader,
            &name,
            gcs_bucket,
            gcs_prefix,
            max_data_rows_per_chunk,
        )
        .await
        .with_context(|| format!("processing CSV entry {}", name))?;

        totals.add(metrics);
        all_gcs_objects.extend(gcs_objects);

        debug!(
            "Entry {} complete: {} rows, {} bytes, {} GCS objects created",
            name,
            metrics.rows,
            metrics.bytes,
            all_gcs_objects.len()
        );
    }

    debug!(
        "Completed ZIP: total rows = {}, total bytes = {}, total GCS objects = {}",
        totals.rows,
        totals.bytes,
        all_gcs_objects.len()
    );

    Ok((totals, all_gcs_objects))
}
