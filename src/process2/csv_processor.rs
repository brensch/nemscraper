use anyhow::{anyhow, Context, Result};
use arrow::{
    csv::ReaderBuilder,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::ArrowWriter,
    basic::{BrotliLevel, Compression},
    file::properties::WriterProperties,
};
use std::{
    fs::{self, File},
    io::Cursor,
    path::Path,
    sync::Arc,
};
use tracing::{debug, info, warn};

use crate::process::{
    convert::convert_to_final_types,
    schema::{analyze_batch_for_schema, SchemaInfo},
    trimming::apply_trimming,
    utils::{clean_str, extract_date_from_filename},
};

/// Unified processor that chunks by D-row count and handles I-line schema changes
pub struct UnifiedCsvProcessor {
    /// Maximum number of D-rows per chunk
    max_data_rows: usize,

    /// Current schema line (I-line)
    current_schema_line: Option<String>,

    /// Accumulated data lines for current chunk
    data_lines: Vec<String>,

    /// Schema information derived from current I-line
    schema_info: Option<SchemaInfo>,

    /// Output directory for this file
    output_dir: std::path::PathBuf,

    /// Base filename for output
    file_name: String,

    /// Chunk counter for unique filenames
    chunk_index: usize,

    /// Total rows processed
    total_data_rows: u64,

    /// Total parquet bytes written
    total_parquet_bytes: u64,
}

impl UnifiedCsvProcessor {
    pub fn new(max_data_rows: usize, file_name: String, output_dir: &Path) -> Result<Self> {
        fs::create_dir_all(output_dir).context("creating output directory")?;

        Ok(Self {
            max_data_rows,
            current_schema_line: None,
            data_lines: Vec::with_capacity(max_data_rows),
            schema_info: None,
            output_dir: output_dir.to_path_buf(),
            file_name,
            chunk_index: 0,
            total_data_rows: 0,
            total_parquet_bytes: 0,
        })
    }

    /// Process a single line from the CSV
    pub fn process_line(&mut self, line: &str) -> Result<()> {
        if line.starts_with("I,") {
            // New schema line - flush current chunk if we have data
            if !self.data_lines.is_empty() {
                self.flush_current_chunk()?;
            }

            // Start new schema context
            self.current_schema_line = Some(line.to_string());
            self.schema_info = None; // Will be derived on first data row
        } else if line.starts_with("D,") {
            // Data line - add to current chunk
            if self.current_schema_line.is_none() {
                warn!("Found data line before schema line, skipping");
                return Ok(());
            }

            self.data_lines.push(line.to_string());

            // Flush if chunk is full
            if self.data_lines.len() >= self.max_data_rows {
                self.flush_current_chunk()?;
            }
        }
        // Ignore other lines (C, etc.)

        Ok(())
    }

    /// Flush current accumulated data lines to a parquet file
    fn flush_current_chunk(&mut self) -> Result<()> {
        if self.data_lines.is_empty() {
            return Ok(());
        }

        let schema_line = self
            .current_schema_line
            .as_ref()
            .ok_or_else(|| anyhow!("No schema line available"))?;

        // Build complete CSV content with schema + data (efficient string building)
        let csv_content = self.build_csv_content(schema_line)?;

        // Convert to parquet in one go
        let parquet_bytes = self.csv_to_parquet(&csv_content)?;

        self.total_data_rows += self.data_lines.len() as u64;
        self.total_parquet_bytes += parquet_bytes;
        self.chunk_index += 1;

        // Clear for next chunk (but keep schema_line for reuse)
        self.data_lines.clear();

        debug!(
            "Flushed chunk {} with {} rows, {} bytes",
            self.chunk_index - 1,
            self.data_lines.len(),
            parquet_bytes
        );

        Ok(())
    }

    /// Build CSV content efficiently without unnecessary copying
    fn build_csv_content(&self, schema_line: &str) -> Result<String> {
        // Pre-calculate size to avoid reallocations
        let schema_len = schema_line.len();
        let data_len: usize = self.data_lines.iter().map(|line| line.len()).sum();
        let total_size = schema_len + data_len + self.data_lines.len(); // +newlines

        let mut csv = String::with_capacity(total_size);
        csv.push_str(schema_line);
        if !schema_line.ends_with('\n') {
            csv.push('\n');
        }

        for line in &self.data_lines {
            csv.push_str(line);
            if !line.ends_with('\n') {
                csv.push('\n');
            }
        }

        Ok(csv)
    }

    /// Drop first 4 columns from RecordBatch
    fn drop_first_4_columns(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        if batch.num_columns() < 4 {
            return Err(anyhow!(
                "Batch has fewer than 4 columns, cannot drop first 4"
            ));
        }

        // Take columns 4 and onwards
        let remaining_columns: Vec<Arc<dyn arrow::array::Array>> =
            batch.columns().iter().skip(4).cloned().collect();

        // Create new schema with remaining fields
        let remaining_fields: Vec<Arc<Field>> =
            batch.schema().fields().iter().skip(4).cloned().collect();
        let new_schema = Arc::new(Schema::new(remaining_fields));

        RecordBatch::try_new(new_schema, remaining_columns)
            .context("creating batch with dropped columns")
    }

    /// Convert CSV content to parquet file
    fn csv_to_parquet(&mut self, csv_content: &str) -> Result<u64> {
        // Parse headers from schema line
        let schema_line = self.current_schema_line.as_ref().unwrap();
        let headers: Vec<String> = schema_line.trim().split(',').map(clean_str).collect();

        // Convert CSV to Arrow batch (with all columns first)
        let full_batch = self.csv_to_arrow_batch(csv_content, &headers)?;

        // Drop first 4 columns
        let batch = self.drop_first_4_columns(&full_batch)?;

        // Get headers for remaining columns (for schema inference)
        let remaining_headers: Vec<String> = headers.iter().skip(4).cloned().collect();

        // Derive schema info if not already done
        if self.schema_info.is_none() {
            self.schema_info = Some(
                self.infer_schema_from_batch(&batch, &remaining_headers, &headers)
                    .context("failed to infer schema")?,
            );
        }
        let schema_info = self.schema_info.as_ref().unwrap();

        info!("got schema info: {:?}", schema_info.trim_columns);

        // Determine output path
        let output_path = self
            .determine_output_path(&schema_info.table_name)
            .context("failed to determine output path")?;

        // Apply transformations (trimming and date parsing)
        let trimmed = apply_trimming(&batch, &schema_info.trim_columns)
            .context("failed to apply trimming")?;
        let final_batch = convert_to_final_types(&trimmed, schema_info)
            .context("failed to convert to final types")?;

        // Write to parquet
        self.write_parquet_file(&final_batch, &schema_info.schema, &output_path)
            .context("failed to write parquet file")
    }

    fn infer_schema_from_batch(
        &self,
        batch: &RecordBatch,
        remaining_headers: &[String],
        original_headers: &[String],
    ) -> Result<SchemaInfo> {
        // Analyze batch for remaining columns
        let mut schema_info = analyze_batch_for_schema(batch, remaining_headers)?;

        // Override table name using original headers (before dropping columns)
        schema_info.table_name = if original_headers.len() >= 4 {
            format!(
                "{}---{}---{}",
                original_headers[1], original_headers[2], original_headers[3]
            )
        } else {
            "default_table".into()
        };

        Ok(schema_info)
    }

    fn determine_output_path(&self, table_name: &str) -> Result<std::path::PathBuf> {
        // Extract partition date from filename
        let partition_date = extract_date_from_filename(&self.file_name).unwrap_or_else(|| {
            warn!(
                "No date found in filename '{}', using default",
                self.file_name
            );
            "unknown-date".to_string()
        });

        // Create table/partition directory structure
        let table_dir = self.output_dir.join(table_name);
        fs::create_dir_all(&table_dir).context("creating table directory")?;

        let partition_dir = table_dir.join(format!("date={}", partition_date));
        fs::create_dir_all(&partition_dir).context("creating partition directory")?;

        // Generate unique filename for this chunk
        let filename = format!("{}-chunk-{}.parquet", self.file_name, self.chunk_index);
        Ok(partition_dir.join(filename))
    }

    fn csv_to_arrow_batch(&self, csv_content: &str, headers: &[String]) -> Result<RecordBatch> {
        // Create string schema for parsing
        let fields: Vec<Field> = headers
            .iter()
            .map(|n| Field::new(n, DataType::Utf8, true))
            .collect();
        let schema = Schema::new(fields);

        let cursor = Cursor::new(csv_content.as_bytes());
        let mut reader = ReaderBuilder::new(Arc::new(schema))
            .with_header(true)
            .with_batch_size(self.max_data_rows)
            .with_quote(b'"')
            .with_escape(b'"')
            .with_delimiter(b',')
            .build(cursor)
            .context("creating CSV reader")?;

        match reader.next() {
            Some(Ok(batch)) => Ok(batch),
            Some(Err(e)) => {
                // Log the problematic CSV content for debugging
                let lines: Vec<&str> = csv_content.lines().take(3).collect();
                warn!("CSV parsing failed. First few lines: {:?}", lines);
                warn!("Expected {} fields, error: {}", headers.len(), e);
                Err(e).context("reading CSV batch")
            }
            None => Err(anyhow!("No data in CSV batch")),
        }
    }

    fn write_parquet_file(
        &self,
        batch: &RecordBatch,
        schema: &Schema,
        output_path: &Path,
    ) -> Result<u64> {
        let file = File::create(output_path)
            .with_context(|| format!("creating file {}", output_path.display()))?;

        let props = WriterProperties::builder()
            .set_compression(Compression::BROTLI(BrotliLevel::try_new(5)?))
            .build();

        let mut writer = ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(props))
            .context("creating parquet writer")?;

        writer.write(batch).context("writing batch to parquet")?;
        writer.close().context("closing parquet writer")?;

        let metadata = fs::metadata(output_path).context("getting file metadata")?;

        Ok(metadata.len())
    }

    /// Finalize processing - flush any remaining data
    pub fn finalize(mut self) -> Result<(u64, u64)> {
        if !self.data_lines.is_empty() {
            self.flush_current_chunk()?;
        }
        Ok((self.total_data_rows, self.total_parquet_bytes))
    }
}

/// Simplified entry point for processing CSV entries
pub fn process_csv_entry_unified<R: std::io::BufRead>(
    mut reader: R,
    file_name: &str,
    out_dir: &Path,
    max_data_rows: usize,
) -> Result<(u64, u64)> {
    // Skip top C-header if present
    let mut first_line = String::new();
    reader.read_line(&mut first_line)?;

    let mut processor = UnifiedCsvProcessor::new(max_data_rows, file_name.to_string(), out_dir)?;

    // If first line wasn't a C-header, process it
    if !first_line.trim_start().starts_with('C') {
        processor.process_line(&first_line)?;
    }

    // Process remaining lines
    let mut line = String::new();
    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line)?;
        if bytes_read == 0 {
            break; // EOF
        }

        let trimmed = line.trim_start();
        if trimmed.starts_with("C,") {
            break; // Footer reached
        }

        processor.process_line(&line)?;
    }

    processor.finalize()
}
