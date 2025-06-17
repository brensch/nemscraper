use super::gcs_processor::process_csv_entry_gcs;
use anyhow::{Context, Result};
use bytes::Bytes;
use reqwest;
use std::io::{Cursor, Read};
use tokio::io::BufReader;
use tokio::sync::mpsc;
use tokio_util::io::SyncIoBridge;
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

/// Async reader that receives chunks from a channel
struct ChunkReader {
    receiver: mpsc::Receiver<Vec<u8>>,
    current_chunk: Vec<u8>,
    position: usize,
}

impl ChunkReader {
    fn new(receiver: mpsc::Receiver<Vec<u8>>) -> Self {
        Self {
            receiver,
            current_chunk: Vec::new(),
            position: 0,
        }
    }
}

impl tokio::io::AsyncRead for ChunkReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::task::Poll;

        // If we have data in current chunk, copy it
        if self.position < self.current_chunk.len() {
            let available = self.current_chunk.len() - self.position;
            let to_copy = available.min(buf.remaining());
            buf.put_slice(&self.current_chunk[self.position..self.position + to_copy]);
            self.position += to_copy;

            if self.position >= self.current_chunk.len() {
                self.current_chunk.clear();
                self.position = 0;
            }

            return Poll::Ready(Ok(()));
        }

        // Try to get next chunk
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(chunk)) => {
                self.current_chunk = chunk;
                self.position = 0;
                self.poll_read(cx, buf)
            }
            Poll::Ready(None) => Poll::Ready(Ok(())), // EOF
            Poll::Pending => Poll::Pending,
        }
    }
}

#[instrument(level = "debug", skip_all, fields(url = %url, bucket = %gcs_bucket))]
pub async fn stream_zip_to_parquet_gcs(
    url: &str,
    gcs_bucket: &str,
    gcs_prefix: Option<&str>,
) -> Result<(RowsAndBytes, Vec<String>)> {
    debug!("Starting download from {}", url);
    let resp = reqwest::get(url)
        .await
        .with_context(|| format!("GET {}", url))?
        .error_for_status()?;
    let zip_bytes: Bytes = resp.bytes().await.context("reading body")?;
    debug!("Downloaded ZIP ({} bytes)", zip_bytes.len());

    // Do all the ZipArchive work in one blocking task:
    let (totals, all_objects) = tokio::task::spawn_blocking({
        let bucket = gcs_bucket.to_string();
        let prefix = gcs_prefix.map(|s| s.to_string());
        move || -> Result<_> {
            let cursor = Cursor::new(zip_bytes);
            let mut archive = ZipArchive::new(cursor).context("opening ZIP")?;
            let mut totals = RowsAndBytes::ZERO;
            let mut objects = Vec::new();
            let handle = tokio::runtime::Handle::current();
            let max_rows = 512_000;

            for idx in 0..archive.len() {
                let mut entry = archive
                    .by_index(idx)
                    .with_context(|| format!("entry {}", idx))?;
                let name = entry.name().to_string();
                if !name.to_lowercase().ends_with(".csv") {
                    continue;
                }
                debug!("ZIP[{}] → {}", idx, name);

                // channel for streaming decompressed bytes
                let (tx, rx) = mpsc::channel::<Vec<u8>>(2);
                let name2 = name.clone();
                let bucket2 = bucket.clone();
                let prefix2 = prefix.clone();

                // spawn the async processor
                let proc = handle.spawn(async move {
                    let reader = BufReader::new(ChunkReader::new(rx));
                    process_csv_entry_gcs(reader, &name2, &bucket2, prefix2.as_deref(), max_rows)
                        .await
                });

                // read the ZIP entry in this blocking thread, send to tx
                let mut buf = vec![0u8; 256 * 1024];
                loop {
                    let n = entry.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    if tx.blocking_send(buf[..n].to_vec()).is_err() {
                        break;
                    }
                }
                drop(tx);

                // wait for async work to finish
                let (metrics, objs) = handle.block_on(proc)??;
                totals.add(metrics);
                objects.extend(objs);
                debug!("Finished {} → {} rows", name, metrics.rows);
            }

            Ok((totals, objects))
        }
    })
    .await?
    .context("ZIP task panicked")?;

    debug!(
        "Done: {} rows, {} bytes, {} objects",
        totals.rows,
        totals.bytes,
        all_objects.len()
    );
    Ok((totals, all_objects))
}
