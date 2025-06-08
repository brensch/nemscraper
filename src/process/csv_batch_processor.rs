use anyhow::{anyhow, Result};
use std::path::Path;

use super::chunk_polars::csv_to_parquet;

/// Holds the mutable state needed to group lines into batches
/// (always beginning with the latest “I,” header), flush when size or new schema arrives.
pub struct CsvBatchProcessor {
    max_bytes: usize,
    current_i_line: Option<String>,
    batch: String,
    batch_index: usize,
    row_count: u64,
    parquet_bytes: u64,
}

impl CsvBatchProcessor {
    /// Create a new processor with a given max‐size threshold.
    pub fn new(max_bytes: usize) -> Self {
        CsvBatchProcessor {
            max_bytes,
            current_i_line: None,
            batch: String::new(),
            batch_index: 0,
            row_count: 0,
            parquet_bytes: 0,
        }
    }

    /// Returns how many rows have been counted so far.
    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    pub fn parquet_bytes(&self) -> u64 {
        self.parquet_bytes
    }

    /// Feed in one line of CSV (excluding top‐header and excluding footer “C,”).
    /// If this line starts with “I,”, treat as new schema: flush old batch, start a new one.
    /// Otherwise, append to current batch (if any), and flush if size ≥ max_bytes.
    pub fn feed_line(&mut self, buf: &str, file_name: &str, out_dir: &Path) -> Result<()> {
        let trimmed = buf.trim_start();

        if trimmed.starts_with("I,") {
            // Count the I-line itself
            self.row_count += 1;

            // 1) If there’s an existing batch, flush it now
            if !self.batch.is_empty() {
                self.flush_batch(file_name, out_dir)?;
            }

            // 2) Start a brand-new batch with this I-line
            self.current_i_line = Some(buf.to_string());
            self.batch.push_str(buf);
            return Ok(());
        }

        // Any non-I line:
        if let Some(i_line_ref) = &self.current_i_line {
            // We are inside a schema group
            self.batch.push_str(buf);
            self.row_count += 1;

            // If the batch is too large, flush and start a fresh batch re-using the same I-line
            if self.batch.len() >= self.max_bytes {
                let i_clone = i_line_ref.clone();

                self.flush_batch(file_name, out_dir)?;
                // start new batch: only the I-line
                self.batch.clear();
                self.batch.push_str(&i_clone);
            }
        }
        // If `current_i_line` is None, we haven’t seen our first “I,” yet → drop this line.
        Ok(())
    }

    /// Flush whatever is in `self.batch` (if nonempty) to Parquet, increment batch_index, clear batch.
    pub fn flush_batch(&mut self, file_name: &str, out_dir: &Path) -> Result<()> {
        if self.batch.is_empty() {
            return Err(anyhow!("Batch is empty"));
        }
        let batch_name = format!("{}-batch-{}", file_name, self.batch_index);
        let parquet_bytes = csv_to_parquet(&batch_name, self.batch.as_str(), out_dir)?;
        self.parquet_bytes += parquet_bytes;
        self.batch_index += 1;
        self.batch.clear();

        Ok(())
    }

    /// Call this once EOF or footer “C,” is reached, to flush any leftover data.
    pub fn flush_final(&mut self, file_name: &str, out_dir: &Path) -> Result<()> {
        self.flush_batch(file_name, out_dir)
    }
}
