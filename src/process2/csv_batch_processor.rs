use anyhow::{anyhow, Result};
use std::path::Path;

use super::chunk::{csv_to_parquet_optimized, OptimizedCsvProcessor};

/// Optimized batch processor that handles schema changes (I-lines) efficiently
/// Uses direct Arrow array construction instead of string reconstruction
pub struct OptimizedCsvBatchProcessor {
    max_bytes: usize,
    current_processor: Option<OptimizedCsvProcessor>,
    current_i_line: Option<String>,
    batch_index: usize,
    row_count: u64,
    parquet_bytes: u64,
    current_batch_size: usize,
}

impl OptimizedCsvBatchProcessor {
    /// Create a new optimized processor with a given max-size threshold.
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            current_processor: None,
            current_i_line: None,
            batch_index: 0,
            row_count: 0,
            parquet_bytes: 0,
            current_batch_size: 0,
        }
    }

    /// Returns how many rows have been counted so far.
    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    pub fn parquet_bytes(&self) -> u64 {
        self.parquet_bytes
    }

    /// Feed in one line of CSV (excluding top‐header and excluding footer "C,").
    /// If this line starts with "I,", treat as new schema: flush old batch, start a new one.
    /// Otherwise, append to current batch (if any), and flush if size ≥ max_bytes.
    pub fn feed_line(&mut self, buf: &str, file_name: &str, out_dir: &Path) -> Result<()> {
        let trimmed = buf.trim_start();

        if trimmed.starts_with("I,") {
            // Schema change detected - flush current processor if exists
            if let Some(mut processor) = self.current_processor.take() {
                let bytes_written = processor.finalize()?;
                self.parquet_bytes += bytes_written;
                self.batch_index += 1;
            }

            // Start new processor with new schema
            let batch_name = format!("{}-batch-{}", file_name, self.batch_index);
            let mut new_processor =
                OptimizedCsvProcessor::new(calculate_optimal_batch_size(self.max_bytes));
            new_processor.initialize(&batch_name, buf, out_dir)?;

            self.current_processor = Some(new_processor);
            self.current_i_line = Some(buf.to_string());
            self.current_batch_size = buf.len();
            self.row_count += 1;

            return Ok(());
        }

        // Handle data lines (D-lines or other non-I lines)
        if let Some(ref mut processor) = self.current_processor {
            // Estimate if adding this line would exceed our memory limit
            let estimated_new_size = self.current_batch_size + buf.len();

            if estimated_new_size >= self.max_bytes {
                // Flush current processor and start a new one with same schema
                let bytes_written = processor.finalize()?;
                self.parquet_bytes += bytes_written;
                self.batch_index += 1;

                // Start new processor with same schema (reuse I-line)
                if let Some(ref i_line) = self.current_i_line {
                    let batch_name = format!("{}-batch-{}", file_name, self.batch_index);
                    let mut new_processor =
                        OptimizedCsvProcessor::new(calculate_optimal_batch_size(self.max_bytes));
                    new_processor.initialize(&batch_name, i_line, out_dir)?;
                    self.current_processor = Some(new_processor);
                    self.current_batch_size = i_line.len();
                }
            }

            // Feed the data line to current processor
            if let Some(ref mut processor) = self.current_processor {
                processor.feed_row(buf)?;
                self.current_batch_size += buf.len();
                self.row_count += 1;
            }
        }
        // If no current processor exists, we're dropping lines until we see an I-line

        Ok(())
    }

    /// Flush whatever is in the current processor to Parquet
    pub fn flush_batch(&mut self, _file_name: &str, _out_dir: &Path) -> Result<()> {
        if let Some(mut processor) = self.current_processor.take() {
            let bytes_written = processor.finalize()?;
            self.parquet_bytes += bytes_written;
            self.batch_index += 1;
            self.current_batch_size = 0;
        }
        Ok(())
    }

    /// Call this once EOF or footer "C," is reached, to flush any leftover data.
    pub fn flush_final(&mut self, file_name: &str, out_dir: &Path) -> Result<()> {
        self.flush_batch(file_name, out_dir)
    }
}

/// Calculate optimal batch size in rows based on memory limit
fn calculate_optimal_batch_size(max_bytes: usize) -> usize {
    // Estimate average row size and calculate optimal batch size
    // For most CSV data, 200 bytes per row is a reasonable estimate
    const ESTIMATED_ROW_SIZE: usize = 200;
    let max_rows = max_bytes / ESTIMATED_ROW_SIZE;

    // Clamp to reasonable bounds (minimum 1k rows, maximum 256k rows)
    max_rows.max(1_024).min(256_1024)
}

/// Legacy compatibility - maintains the old string-based interface but uses optimized processing
pub struct CsvBatchProcessor {
    optimized: OptimizedCsvBatchProcessor,
    batch: String,
}

impl CsvBatchProcessor {
    /// Create a new processor with a given max‐size threshold.
    pub fn new(max_bytes: usize) -> Self {
        Self {
            optimized: OptimizedCsvBatchProcessor::new(max_bytes),
            batch: String::new(),
        }
    }

    /// Returns how many rows have been counted so far.
    pub fn row_count(&self) -> u64 {
        self.optimized.row_count()
    }

    pub fn parquet_bytes(&self) -> u64 {
        self.optimized.parquet_bytes()
    }

    /// Feed in one line of CSV - delegates to optimized processor
    pub fn feed_line(&mut self, buf: &str, file_name: &str, out_dir: &Path) -> Result<()> {
        self.optimized.feed_line(buf, file_name, out_dir)
    }

    /// Flush whatever is in the current batch to Parquet
    pub fn flush_batch(&mut self, file_name: &str, out_dir: &Path) -> Result<()> {
        self.optimized.flush_batch(file_name, out_dir)
    }

    /// Call this once EOF or footer "C," is reached, to flush any leftover data.
    pub fn flush_final(&mut self, file_name: &str, out_dir: &Path) -> Result<()> {
        self.optimized.flush_final(file_name, out_dir)
    }
}
