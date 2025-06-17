use super::gcs_processor::process_csv_entry_gcs;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use reqwest;
use std::io::{BufRead, BufReader, Cursor};
use tokio::sync::mpsc;
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

/// Custom async reader that receives data from a channel
struct ChannelReader {
    receiver: mpsc::Receiver<Result<Vec<u8>>>,
    buffer: Vec<u8>,
    position: usize,
}

impl ChannelReader {
    fn new(receiver: mpsc::Receiver<Result<Vec<u8>>>) -> Self {
        Self {
            receiver,
            buffer: Vec::new(),
            position: 0,
        }
    }
}

impl tokio::io::AsyncRead for ChannelReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::task::Poll;

        // If we have data in the buffer, return it
        if self.position < self.buffer.len() {
            let remaining = self.buffer.len() - self.position;
            let to_copy = remaining.min(buf.remaining());
            buf.put_slice(&self.buffer[self.position..self.position + to_copy]);
            self.position += to_copy;

            // If we've consumed the buffer, reset it
            if self.position >= self.buffer.len() {
                self.buffer.clear();
                self.position = 0;
            }

            return Poll::Ready(Ok(()));
        }

        // Try to receive more data
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(Ok(data))) => {
                self.buffer = data;
                self.position = 0;
                // Recurse to handle the new data
                self.poll_read(cx, buf)
            }
            Poll::Ready(Some(Err(e))) => {
                Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, e)))
            }
            Poll::Ready(None) => Poll::Ready(Ok(())), // EOF
            Poll::Pending => Poll::Pending,
        }
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
    let max_data_rows_per_chunk = 256_000;
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

    let mut totals = RowsAndBytes::ZERO;
    let mut all_gcs_objects: Vec<String> = Vec::new();

    // Process ZIP in a separate task to handle streaming
    let gcs_bucket_clone = gcs_bucket.to_string();
    let gcs_prefix_clone = gcs_prefix.map(|s| s.to_string());

    let (totals_result, gcs_objects_result) =
        tokio::task::spawn_blocking(move || -> Result<(RowsAndBytes, Vec<String>)> {
            let cursor = Cursor::new(zip_data);
            let mut archive = ZipArchive::new(cursor)?;
            let mut totals = RowsAndBytes::ZERO;
            let mut all_gcs_objects = Vec::new();

            // Use tokio runtime handle to spawn async tasks from blocking context
            let handle = tokio::runtime::Handle::current();

            for idx in 0..archive.len() {
                let entry = archive.by_index(idx)?;
                let name = entry.name().to_string();

                if !name.to_lowercase().ends_with(".csv") {
                    continue;
                }

                debug!("Processing CSV entry: {}", name);

                // Create a channel for streaming lines
                let (tx, rx) = mpsc::channel::<Result<Vec<u8>>>(100);

                // Spawn async task to process the stream
                let gcs_bucket_task = gcs_bucket_clone.clone();
                let gcs_prefix_task = gcs_prefix_clone.clone();
                let name_clone = name.clone();

                let process_handle = handle.spawn(async move {
                    let reader = ChannelReader::new(rx);
                    let buf_reader = tokio::io::BufReader::new(reader);

                    process_csv_entry_gcs(
                        buf_reader,
                        &name_clone,
                        &gcs_bucket_task,
                        gcs_prefix_task.as_deref(),
                        max_data_rows_per_chunk,
                    )
                    .await
                });

                // Stream the CSV entry line by line
                let reader = BufReader::new(entry);
                for line_result in reader.lines() {
                    match line_result {
                        Ok(line) => {
                            let mut line_bytes = line.into_bytes();
                            line_bytes.push(b'\n'); // Add newline back
                            if tx.blocking_send(Ok(line_bytes)).is_err() {
                                break; // Receiver dropped
                            }
                        }
                        Err(e) => {
                            let _ = tx.blocking_send(Err(e.into()));
                            break;
                        }
                    }
                }

                // Close the channel
                drop(tx);

                // Wait for processing to complete
                let (metrics, gcs_objects) = handle
                    .block_on(process_handle)
                    .context("task panicked")?
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

            Ok((totals, all_gcs_objects))
        })
        .await
        .context("blocking task panicked")??;

    debug!(
        "Completed ZIP: total rows = {}, total bytes = {}, total GCS objects = {}",
        totals_result.rows,
        totals_result.bytes,
        gcs_objects_result.len()
    );

    Ok((totals_result, gcs_objects_result))
}
