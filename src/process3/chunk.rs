use anyhow::{anyhow, Context, Result};
use arrow::array::*;
use arrow::csv::ReaderBuilder;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use chrono::{NaiveDateTime, TimeZone};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs;
use std::fs::File;
use std::io::{Cursor, Read};
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, warn};

/// Fast processor that scans for I-line boundaries then bulk processes each chunk
pub struct FastChunkProcessor {
    chunk_index: usize,
    total_rows: u64,
    total_bytes: u64,
}

/// A chunk of CSV data with a single schema (from I-line to next I-line)
#[derive(Debug)]
struct SchemaChunk<'a> {
    /// Slice of the original CSV content
    content: &'a str,
    /// Line index where the I-line occurs
    start_line: usize,
    /// Number of D-lines in this chunk
    line_count: usize,
}

impl FastChunkProcessor {
    pub fn new() -> Self {
        Self {
            chunk_index: 0,
            total_rows: 0,
            total_bytes: 0,
        }
    }

    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Process entire CSV content by scanning for I-line boundaries first
    pub fn process_csv_content(
        &mut self,
        content: &str,
        file_name: &str,
        out_dir: &Path,
    ) -> Result<()> {
        debug!("Fast processing CSV content: {} characters", content.len());

        // Step 1: Scan for I-line boundaries (zero-copy)
        let chunks = self.scan_for_schema_chunks(content)?;
        debug!("Found {} schema chunks", chunks.len());

        // Step 2: Process each chunk with Arrow's fast CSV reader
        for chunk in chunks {
            let chunk_name = format!("{}-chunk-{}", file_name, self.chunk_index);
            let bytes_written = self.process_single_chunk(&chunk, &chunk_name, out_dir)?;

            self.total_rows += chunk.line_count as u64;
            self.total_bytes += bytes_written;
            self.chunk_index += 1;

            debug!(
                "Processed chunk {}: {} lines, {} bytes",
                self.chunk_index - 1,
                chunk.line_count,
                bytes_written
            );
        }

        Ok(())
    }

    /// Scan the entire content and identify chunks separated by I-lines
    fn scan_for_schema_chunks<'a>(&self, content: &'a str) -> Result<Vec<SchemaChunk<'a>>> {
        let mut chunks = Vec::new();
        let mut current_start_byte: Option<usize> = None;
        let mut current_start_line: Option<usize> = None;
        let mut current_line_count = 0;
        let mut in_chunk = false;
        let mut byte_offset = 0;

        for (line_idx, line) in content.lines().enumerate() {
            let trimmed = line.trim_start();

            // Check for I-line marker
            if trimmed.starts_with("I,") {
                // Finalize previous chunk if present
                if let (Some(start_byte), Some(start_line)) =
                    (current_start_byte, current_start_line)
                {
                    if current_line_count > 0 {
                        let chunk_str = &content[start_byte..byte_offset];
                        chunks.push(SchemaChunk {
                            content: chunk_str,
                            start_line,
                            line_count: current_line_count,
                        });
                    }
                }

                // Initialize new chunk
                current_start_byte = Some(byte_offset);
                current_start_line = Some(line_idx);
                current_line_count = 0;
                in_chunk = true;
            } else if in_chunk
                && (trimmed.starts_with("D,")
                    || (!trimmed.starts_with("C,") && !trimmed.is_empty()))
            {
                // Count D-lines or data-like lines
                current_line_count += 1;
            }

            // Advance byte offset (+1 for the '\n')
            byte_offset += line.len() + 1;
        }

        // Finalize the last chunk
        if let (Some(start_byte), Some(start_line)) = (current_start_byte, current_start_line) {
            if current_line_count > 0 {
                let chunk_str = &content[start_byte..];
                chunks.push(SchemaChunk {
                    content: chunk_str,
                    start_line,
                    line_count: current_line_count,
                });
            }
        }

        if chunks.is_empty() {
            return Err(anyhow!("No valid I-line chunks found in CSV content"));
        }

        Ok(chunks)
    }

    /// Process a single schema chunk using Arrow's fast CSV reader
    fn process_single_chunk(
        &self,
        chunk: &SchemaChunk,
        chunk_name: &str,
        out_dir: &Path,
    ) -> Result<u64> {
        debug!("Processing chunk with {} lines", chunk.line_count);

        // Parse the header (I-line) for field names
        let header_line = chunk
            .content
            .lines()
            .next()
            .ok_or_else(|| anyhow!("Empty chunk content"))?;
        let headers: Vec<String> = header_line
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        // Create initial string schema
        let string_fields: Vec<Field> = headers
            .iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect();
        let string_schema = Arc::new(Schema::new(string_fields));

        // CSV reader over the slice (zero-copy reader still uses IO under the hood)
        let cursor = Cursor::new(chunk.content.as_bytes());
        let mut csv_reader = ReaderBuilder::new(string_schema.clone())
            .with_header(true)
            .with_batch_size(8_192)
            .build(cursor)
            .context("Failed to create Arrow CSV reader")?;

        // Read first batch and infer final schema
        let first_batch = csv_reader
            .next()
            .ok_or_else(|| anyhow!("No data in CSV chunk"))?
            .context("Failed to read first batch")?;
        let enhanced_schema = analyze_batch_for_schema(&first_batch, &headers)?;

        // Set up output paths
        let table_name = enhanced_schema.table_name.clone();
        let partition_date = extract_date_from_filename(chunk_name).unwrap_or_else(|| {
            warn!(
                "No date found in chunk name '{}'; using default partition",
                chunk_name
            );
            "unknown-date".to_string()
        });
        let table_dir = out_dir.join(&table_name);
        fs::create_dir_all(&table_dir).context("creating table directory")?;
        let partition_dir = table_dir.join(format!("date={}", partition_date));
        fs::create_dir_all(&partition_dir).context("creating partition directory")?;

        let final_name = format!("{}.parquet", chunk_name);
        let tmp_name = format!("{}.tmp", final_name);
        let final_path = partition_dir.join(&final_name);
        let tmp_path = partition_dir.join(&tmp_name);

        // Parquet writer setup
        let tmp_file = File::create(&tmp_path).context("creating temporary Parquet file")?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(
            tmp_file,
            Arc::new(enhanced_schema.schema.clone()),
            Some(props),
        )
        .context("initializing Parquet writer")?;

        // Write first batch
        let trimmed_batch = apply_trimming(&first_batch, &enhanced_schema.trim_columns)?;
        let final_batch = convert_to_final_types(&trimmed_batch, &enhanced_schema)?;
        writer
            .write(&final_batch)
            .context("writing first batch to Parquet")?;
        let mut total_rows = final_batch.num_rows();

        // Write remaining batches
        for batch_result in csv_reader {
            let batch = batch_result.context("reading CSV batch")?;
            let trimmed = apply_trimming(&batch, &enhanced_schema.trim_columns)?;
            let fb = convert_to_final_types(&trimmed, &enhanced_schema)?;
            writer.write(&fb).context("writing batch to Parquet")?;
            total_rows += fb.num_rows();
        }

        writer.close().context("closing Parquet writer")?;

        // Finalize file
        let file_size = fs::metadata(&tmp_path)
            .context("getting file metadata")?
            .len();
        fs::rename(&tmp_path, &final_path).context("renaming Parquet file")?;

        debug!(
            "Chunk complete: {} rows, {} bytes -> {}",
            total_rows,
            file_size,
            final_path.display()
        );

        Ok(file_size)
    }
}

/// Extract date from filename for partitioning
fn extract_date_from_filename(filename: &str) -> Option<String> {
    let chars: Vec<char> = filename.chars().collect();
    for i in 0..chars.len().saturating_sub(7) {
        if chars[i..i + 8].iter().all(|c| c.is_ascii_digit()) {
            let date_str: String = chars[i..i + 8].iter().collect();
            if let (Ok(year), Ok(month), Ok(day)) = (
                date_str[0..4].parse::<u32>(),
                date_str[4..6].parse::<u32>(),
                date_str[6..8].parse::<u32>(),
            ) {
                if (2000..=2030).contains(&year)
                    && (1..=12).contains(&month)
                    && (1..=31).contains(&day)
                {
                    return Some(format!(
                        "{}-{}-{}",
                        &date_str[0..4],
                        &date_str[4..6],
                        &date_str[6..8]
                    ));
                }
            }
        }
    }
    None
}

/// Fast CSV processing entrypoint
pub fn csv_to_parquet_fast<R: Read>(mut reader: R, file_name: &str, out_dir: &Path) -> Result<u64> {
    let mut content = String::new();
    reader.read_to_string(&mut content)?;

    let mut processor = FastChunkProcessor::new();
    processor.process_csv_content(&content, file_name, out_dir)?;

    Ok(processor.total_bytes())
}

/// Backward compatibility
pub fn csv_to_parquet(file_name: &str, data: &str, out_dir: &Path) -> Result<u64> {
    let cursor = Cursor::new(data.as_bytes());
    csv_to_parquet_fast(cursor, file_name, out_dir)
}

// ... (the remainder of trimming, schema analysis, and conversion functions remain unchanged)

/// Schema inference and trimming detection
struct SchemaInfo {
    schema: Schema,
    date_columns: Vec<String>,
    trim_columns: Vec<String>,
    table_name: String,
}

/// 1) Trim whitespace + strip outer quotes if present.
fn clean_str(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
        trimmed[1..trimmed.len() - 1].to_string()
    } else {
        trimmed.to_string()
    }
}

/// 2) Infer Arrow dtype from a cleaned string, treating ANY parseable number as Float64.
fn infer_arrow_dtype_from_str(s: &str) -> DataType {
    if s.parse::<f64>().is_ok() {
        DataType::Float64
    } else {
        DataType::Utf8
    }
}

fn analyze_batch_for_schema(
    batch: &RecordBatch,
    headers: &[String],
) -> Result<SchemaInfo, anyhow::Error> {
    let mut final_fields = Vec::new();
    let mut date_columns = Vec::new();
    let mut trim_columns = Vec::new();

    // Build table name from headers 1,2,3
    let table_name = if headers.len() >= 4 {
        format!("{}---{}---{}", headers[1], headers[2], headers[3])
    } else {
        "default_table".to_string()
    };

    // Analyze each column
    for (col_idx, header) in headers.iter().enumerate() {
        let array = batch.column(col_idx);

        if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
            let mut needs_trimming = false;
            let mut is_date_column = false;
            let mut inferred_type = DataType::Utf8;

            // Check first non-null value
            if let Some(first_val) = string_array.iter().find_map(|v| v) {
                let cleaned = clean_str(first_val);

                // Check if trimming changed the value
                if cleaned != first_val {
                    needs_trimming = true;
                }

                // Check if it's a date column
                if NaiveDateTime::parse_from_str(&cleaned, "%Y/%m/%d %H:%M:%S").is_ok() {
                    is_date_column = true;
                } else {
                    // Try to infer numeric type
                    inferred_type = infer_arrow_dtype_from_str(&cleaned);
                }
            }

            if needs_trimming {
                trim_columns.push(header.clone());
            }

            if is_date_column {
                date_columns.push(header.clone());
                // Date columns get timestamp type in final schema
                final_fields.push(Field::new(
                    header,
                    DataType::Timestamp(TimeUnit::Millisecond, Some("+10:00".into())),
                    true,
                ));
            } else {
                final_fields.push(Field::new(header, inferred_type, true));
            }
        } else {
            // Shouldn't happen since we read everything as strings initially
            final_fields.push(Field::new(header, DataType::Utf8, true));
        }
    }

    debug!("Date columns detected: {:?}", date_columns);
    debug!("Trim columns detected: {:?}", trim_columns);

    Ok(SchemaInfo {
        schema: Schema::new(final_fields),
        date_columns,
        trim_columns,
        table_name,
    })
}

/// 6) Apply string trimming to specified columns in a RecordBatch.
fn apply_trimming(
    batch: &RecordBatch,
    trim_columns: &[String],
) -> Result<RecordBatch, anyhow::Error> {
    if trim_columns.is_empty() {
        return Ok(batch.clone());
    }

    let mut new_columns = Vec::with_capacity(batch.num_columns());

    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
        let array = batch.column(col_idx);

        if trim_columns.contains(field.name()) {
            // Apply trimming to this column
            if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
                let trimmed: StringArray = string_array
                    .iter()
                    .map(|opt_str| opt_str.map(clean_str))
                    .collect();
                new_columns.push(Arc::new(trimmed) as ArrayRef);
            } else {
                new_columns.push(array.clone());
            }
        } else {
            new_columns.push(array.clone());
        }
    }

    Ok(RecordBatch::try_new(batch.schema(), new_columns)?)
}

fn convert_to_final_types(
    batch: &RecordBatch,
    schema_info: &SchemaInfo,
) -> Result<RecordBatch, anyhow::Error> {
    let mut new_fields = Vec::new();
    let mut new_columns = Vec::new();

    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
        let array = batch.column(col_idx);
        let target_field = &schema_info.schema.fields()[col_idx];

        if schema_info.date_columns.contains(field.name()) {
            // Convert date column
            if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
                // Parse naive datetimes first
                let naive_timestamps: Vec<Option<i64>> = string_array
                    .iter()
                    .map(|opt_str| {
                        opt_str.and_then(|s| {
                            let cleaned = clean_str(s);
                            NaiveDateTime::parse_from_str(&cleaned, "%Y/%m/%d %H:%M:%S")
                                .ok()
                                .map(|dt| {
                                    // Treat the naive datetime as being in +10:00 timezone
                                    // and convert to UTC timestamp
                                    let offset = chrono::FixedOffset::east_opt(10 * 3600).unwrap();
                                    offset
                                        .from_local_datetime(&dt)
                                        .single()
                                        .map(|dt_tz| dt_tz.timestamp_millis())
                                        .unwrap_or(0)
                                })
                        })
                    })
                    .collect();

                // Create timezone-aware timestamp array
                let timestamps =
                    TimestampMillisecondArray::from(naive_timestamps).with_timezone("+10:00");

                new_fields.push((**target_field).clone()); // Use target field with correct timestamp type
                new_columns.push(Arc::new(timestamps) as ArrayRef);
            } else {
                new_fields.push((**target_field).clone());
                new_columns.push(array.clone());
            }
        } else if target_field.data_type() == &DataType::Float64 {
            // Convert numeric column
            if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
                let floats: Float64Array = string_array
                    .iter()
                    .map(|opt_str| {
                        opt_str.and_then(|s| {
                            let cleaned = clean_str(s);
                            cleaned.parse::<f64>().ok()
                        })
                    })
                    .collect();

                new_fields.push((**target_field).clone()); // Use target field with Float64 type
                new_columns.push(Arc::new(floats) as ArrayRef);
            } else {
                new_fields.push((**target_field).clone());
                new_columns.push(array.clone());
            }
        } else {
            // Keep as string
            new_fields.push((**target_field).clone());
            new_columns.push(array.clone());
        }
    }

    let new_schema = Arc::new(Schema::new(new_fields));
    Ok(RecordBatch::try_new(new_schema, new_columns)?)
}
