use anyhow::{anyhow, Context, Result};
use arrow::{
    csv::ReaderBuilder,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use google_cloud_storage::{
    client::{Client, ClientConfig},
    http::objects::upload::{Media, UploadObjectRequest, UploadType},
};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use std::{io::Cursor, sync::Arc};
use tracing::{debug, info, warn};

use crate::process::{
    convert::convert_to_final_types,
    schema::{analyze_batch_for_schema, SchemaInfo},
    split::RowsAndBytes, // Import from split module
    trimming::apply_trimming,
    utils::{clean_str, extract_date_from_filename},
};

/// GCS-enabled processor that chunks by D-row count and uploads to Google Cloud Storage
pub struct GcsUnifiedCsvProcessor {
    /// Maximum number of D-rows per chunk
    max_data_rows: usize,

    /// Current schema line (I-line)
    current_schema_line: Option<String>,

    /// Accumulated data lines for current chunk
    data_lines: Vec<String>,

    /// Schema information derived from current I-line
    schema_info: Option<SchemaInfo>,

    /// GCS bucket name
    gcs_bucket: String,

    /// GCS prefix for organized storage
    gcs_prefix: String,

    /// Base filename for output
    file_name: String,

    /// Chunk counter for unique filenames
    chunk_index: usize,

    /// Total rows processed
    total_data_rows: u64,

    /// Total parquet bytes written
    total_parquet_bytes: u64,

    /// GCS client
    gcs_client: Client,

    /// List of created GCS objects
    gcs_objects: Vec<String>,
}

impl GcsUnifiedCsvProcessor {
    pub async fn new(
        max_data_rows: usize,
        file_name: String,
        gcs_bucket: String,
        gcs_prefix: Option<&str>,
    ) -> Result<Self> {
        // Initialize GCS client
        let config = ClientConfig::default().with_auth().await?;
        let gcs_client = Client::new(config);

        let prefix = gcs_prefix.unwrap_or("").to_string();
        let formatted_prefix = if prefix.is_empty() || prefix.ends_with('/') {
            prefix
        } else {
            format!("{}/", prefix)
        };

        Ok(Self {
            max_data_rows,
            current_schema_line: None,
            data_lines: Vec::with_capacity(max_data_rows),
            schema_info: None,
            gcs_bucket,
            gcs_prefix: formatted_prefix,
            file_name,
            chunk_index: 0,
            total_data_rows: 0,
            total_parquet_bytes: 0,
            gcs_client,
            gcs_objects: Vec::new(),
        })
    }

    /// Process a single line from the CSV
    pub async fn process_line(&mut self, line: &str) -> Result<()> {
        let trimmed = line.trim_start();

        if trimmed.starts_with("I,") {
            // New schema line - flush current chunk if we have data
            if !self.data_lines.is_empty() {
                self.flush_current_chunk().await?;
            }

            // Start new schema context
            self.current_schema_line = Some(line.to_string());
            self.schema_info = None; // Will be derived on first data row
        } else if trimmed.starts_with("D,") {
            // Data line - add to current chunk
            if self.current_schema_line.is_none() {
                warn!("Found data line before schema line, skipping");
                return Ok(());
            }

            self.data_lines.push(line.to_string());

            // Flush if chunk is full
            if self.data_lines.len() >= self.max_data_rows {
                self.flush_current_chunk().await?;
            }
        }
        // Ignore other lines (C, etc.)

        Ok(())
    }

    /// Flush current accumulated data lines to a parquet file in GCS
    async fn flush_current_chunk(&mut self) -> Result<()> {
        if self.data_lines.is_empty() {
            return Ok(());
        }

        let schema_line = self
            .current_schema_line
            .as_ref()
            .ok_or_else(|| anyhow!("No schema line available"))?;

        // Build complete CSV content with schema + data (efficient string building)
        let csv_content = self.build_csv_content(schema_line)?;

        // Convert to parquet and upload to GCS
        let (parquet_bytes, gcs_object_name) = self.csv_to_parquet_gcs(&csv_content).await?;

        self.total_data_rows += self.data_lines.len() as u64;
        self.total_parquet_bytes += parquet_bytes;
        self.gcs_objects.push(gcs_object_name.clone());
        self.chunk_index += 1;

        // Clear for next chunk (but keep schema_line for reuse)
        self.data_lines.clear();

        debug!(
            "Flushed chunk {} with {} rows, {} bytes to GCS object: {}",
            self.chunk_index - 1,
            self.data_lines.len(),
            parquet_bytes,
            gcs_object_name
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

    /// Convert CSV content to parquet and upload to GCS
    async fn csv_to_parquet_gcs(&mut self, csv_content: &str) -> Result<(u64, String)> {
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

        // Apply transformations (trimming and date parsing)
        let trimmed = apply_trimming(&batch, &schema_info.trim_columns)
            .context("failed to apply trimming")?;
        let final_batch = convert_to_final_types(&trimmed, schema_info)
            .context("failed to convert to final types")?;

        // Write to parquet in memory
        let parquet_data = self.write_parquet_to_memory(&final_batch, &schema_info.schema)?;
        let parquet_data_size = parquet_data.len() as u64;

        // Determine GCS object name
        let gcs_object_name = self.determine_gcs_object_name(&schema_info.table_name)?;

        // Upload to GCS
        self.upload_to_gcs(parquet_data, &gcs_object_name).await?;

        Ok((parquet_data_size, gcs_object_name))
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

    fn determine_gcs_object_name(&self, table_name: &str) -> Result<String> {
        // Extract partition date from filename
        let partition_date = extract_date_from_filename(&self.file_name).unwrap_or_else(|| {
            warn!(
                "No date found in filename '{}', using default",
                self.file_name
            );
            "unknown-date".to_string()
        });

        // Build GCS object path: prefix/table_name/date=YYYY-MM-DD/filename-chunk-N.parquet
        let object_name = format!(
            "{}{}/date={}/{}-chunk-{}.parquet",
            self.gcs_prefix, table_name, partition_date, self.file_name, self.chunk_index
        );

        Ok(object_name)
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
            .build(cursor)
            .context("creating CSV reader")?;

        match reader.next() {
            Some(Ok(batch)) => Ok(batch),
            Some(Err(e)) => {
                warn!("CSV parsing failed, error: {}", e);
                Err(e).context("reading CSV batch")
            }
            None => Err(anyhow!("No data in CSV batch")),
        }
    }

    fn write_parquet_to_memory(&self, batch: &RecordBatch, schema: &Schema) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        let cursor = Cursor::new(&mut buffer);

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY) // Fast compression for GCS
            .build();

        let mut writer = ArrowWriter::try_new(cursor, Arc::new(schema.clone()), Some(props))
            .context("creating parquet writer")?;

        writer.write(batch).context("writing batch to parquet")?;
        writer.close().context("closing parquet writer")?;

        Ok(buffer)
    }

    async fn upload_to_gcs(&self, data: Vec<u8>, object_name: &str) -> Result<()> {
        let upload_type = UploadType::Simple(Media::new(object_name.to_string()));
        let request = UploadObjectRequest {
            bucket: self.gcs_bucket.clone(),
            ..Default::default()
        };

        self.gcs_client
            .upload_object(&request, data, &upload_type)
            .await
            .with_context(|| {
                format!(
                    "Failed to upload {} to GCS bucket {}",
                    object_name, self.gcs_bucket
                )
            })?;

        info!(
            "Successfully uploaded {} to gs://{}/{}",
            object_name, self.gcs_bucket, object_name
        );

        Ok(())
    }

    /// Finalize processing - flush any remaining data
    pub async fn finalize(mut self) -> Result<(RowsAndBytes, Vec<String>)> {
        if !self.data_lines.is_empty() {
            self.flush_current_chunk().await?;
        }

        let metrics = RowsAndBytes {
            rows: self.total_data_rows,
            bytes: self.total_parquet_bytes,
        };

        Ok((metrics, self.gcs_objects))
    }
}

/// GCS-enabled entry point for processing CSV entries
pub async fn process_csv_entry_gcs<R: tokio::io::AsyncBufReadExt + Unpin>(
    mut reader: R,
    file_name: &str,
    gcs_bucket: &str,
    gcs_prefix: Option<&str>,
    max_data_rows: usize,
) -> Result<(RowsAndBytes, Vec<String>)> {
    // Skip top C-header if present
    let mut first_line = String::new();
    reader.read_line(&mut first_line).await?;

    let mut processor = GcsUnifiedCsvProcessor::new(
        max_data_rows,
        file_name.to_string(),
        gcs_bucket.to_string(),
        gcs_prefix,
    )
    .await?;

    // If first line wasn't a C-header, process it
    if !first_line.trim_start().starts_with('C') {
        processor.process_line(&first_line).await?;
    }

    // Process remaining lines
    let mut line = String::new();
    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;
        if bytes_read == 0 {
            break; // EOF
        }

        let trimmed = line.trim_start();
        if trimmed.starts_with("C,") {
            break; // Footer reached
        }

        processor.process_line(&line).await?;
    }

    processor.finalize().await
}
