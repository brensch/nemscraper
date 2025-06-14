use anyhow::{anyhow, Context, Result};
use arrow::array::*;
use arrow::csv::ReaderBuilder;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use chrono::{NaiveDateTime, TimeZone};
use parquet::arrow::ArrowWriter;
use parquet::basic::BrotliLevel;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs;
use std::fs::File;
use std::io::Seek;
use std::io::{BufRead, BufReader, Cursor};
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, warn};

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

/// Schema inference and trimming detection
struct SchemaInfo {
    schema: Schema,
    date_columns: Vec<String>,
    trim_columns: Vec<String>,
    table_name: String,
}

/// Streaming CSV batch accumulator
pub struct StreamingCsvProcessor {
    max_batch_rows: usize,
    headers: Option<Vec<String>>,
    schema_info: Option<SchemaInfo>,
    current_batch_lines: Vec<String>,
    batch_index: usize,
    row_count: u64,
    parquet_bytes: u64,
    writer: Option<ArrowWriter<File>>,
    output_path: Option<std::path::PathBuf>,
}

impl StreamingCsvProcessor {
    pub fn new(max_batch_rows: usize) -> Self {
        Self {
            max_batch_rows,
            headers: None,
            schema_info: None,
            current_batch_lines: Vec::new(),
            batch_index: 0,
            row_count: 0,
            parquet_bytes: 0,
            writer: None,
            output_path: None,
        }
    }

    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    pub fn parquet_bytes(&self) -> u64 {
        self.parquet_bytes
    }

    /// Initialize the processor with header and schema info
    pub fn initialize(&mut self, file_name: &str, header_line: &str, out_dir: &Path) -> Result<()> {
        // Parse headers
        let headers: Vec<String> = header_line.trim().split(',').map(clean_str).collect();

        // Build table name from headers 1,2,3
        let table_name = if headers.len() >= 4 {
            format!("{}---{}---{}", headers[1], headers[2], headers[3])
        } else {
            "default_table".to_string()
        };

        // Setup output directory and partition
        let partition_date = extract_date_from_filename(file_name).unwrap_or_else(|| {
            warn!(
                "No date found in filename '{}'; using default partition",
                file_name
            );
            "unknown-date".to_string()
        });

        let table_dir = out_dir.join(&table_name);
        fs::create_dir_all(&table_dir).context("creating table directory")?;
        let partition_dir = table_dir.join(format!("date={}", partition_date));
        fs::create_dir_all(&partition_dir).context("creating partition directory")?;

        let final_name = format!("{}.parquet", file_name);
        let tmp_name = format!("{}.tmp", final_name);
        let final_path = partition_dir.join(&final_name);
        let tmp_path = partition_dir.join(&tmp_name);

        self.headers = Some(headers);
        self.output_path = Some(tmp_path);

        debug!("Initialized streaming processor for table: {}", table_name);
        Ok(())
    }

    /// Feed a data row (not header) to the processor
    pub fn feed_row(&mut self, line: &str) -> Result<()> {
        self.current_batch_lines.push(line.to_string());
        self.row_count += 1;

        // If we've accumulated enough rows, process the batch
        if self.current_batch_lines.len() >= self.max_batch_rows {
            self.flush_current_batch()?;
        }

        Ok(())
    }

    /// Process the current batch of lines
    fn flush_current_batch(&mut self) -> Result<()> {
        if self.current_batch_lines.is_empty() {
            return Ok(());
        }

        let headers = self
            .headers
            .as_ref()
            .ok_or_else(|| anyhow!("Headers not initialized"))?;

        // Create CSV content with header + current batch
        let mut csv_content = headers.join(",") + "\n";
        for line in &self.current_batch_lines {
            csv_content.push_str(line);
            if !line.ends_with('\n') {
                csv_content.push('\n');
            }
        }

        // If this is the first batch, infer schema
        if self.schema_info.is_none() {
            let schema_info = self.infer_schema_from_batch(&csv_content, headers)?;
            self.schema_info = Some(schema_info);
            self.setup_parquet_writer()?;
        }

        // Convert to Arrow batch and write
        let batch = self.convert_csv_to_arrow_batch(&csv_content)?;

        if let Some(ref mut writer) = self.writer {
            writer.write(&batch).context("writing batch to Parquet")?;
        }

        debug!(
            "Processed batch {} with {} rows",
            self.batch_index,
            self.current_batch_lines.len()
        );

        self.batch_index += 1;
        self.current_batch_lines.clear();
        Ok(())
    }

    /// Infer schema from the first batch
    fn infer_schema_from_batch(&self, csv_content: &str, headers: &[String]) -> Result<SchemaInfo> {
        // Create initial string schema
        let string_fields: Vec<Field> = headers
            .iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect();
        let string_schema = Schema::new(string_fields);

        // Read first batch to analyze types
        let cursor = Cursor::new(csv_content.as_bytes());
        let mut schema_reader = ReaderBuilder::new(Arc::new(string_schema))
            .with_header(true)
            .with_batch_size(1_000)
            .build(cursor)
            .context("creating CSV reader for schema inference")?;

        let first_batch = schema_reader
            .next()
            .ok_or_else(|| anyhow!("No data found in CSV"))??;

        analyze_batch_for_schema(&first_batch, headers)
    }

    /// Setup the Parquet writer once we have schema info
    fn setup_parquet_writer(&mut self) -> Result<()> {
        let schema_info = self
            .schema_info
            .as_ref()
            .ok_or_else(|| anyhow!("Schema info not available"))?;
        let output_path = self
            .output_path
            .as_ref()
            .ok_or_else(|| anyhow!("Output path not set"))?;

        let tmp_file = File::create(output_path).context("creating temporary Parquet file")?;
        let props = WriterProperties::builder()
            .set_compression(Compression::BROTLI(BrotliLevel::try_new(5)?))
            .build();

        let writer =
            ArrowWriter::try_new(tmp_file, Arc::new(schema_info.schema.clone()), Some(props))
                .context("initializing Parquet writer")?;

        self.writer = Some(writer);
        Ok(())
    }

    /// Convert CSV content to Arrow RecordBatch
    fn convert_csv_to_arrow_batch(&self, csv_content: &str) -> Result<RecordBatch> {
        let headers = self.headers.as_ref().unwrap();
        let schema_info = self.schema_info.as_ref().unwrap();

        // Read as strings first
        let string_fields: Vec<Field> = headers
            .iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect();
        let string_schema = Schema::new(string_fields);

        let cursor = Cursor::new(csv_content.as_bytes());
        let mut csv_reader = ReaderBuilder::new(Arc::new(string_schema))
            .with_header(true)
            .with_batch_size(self.max_batch_rows)
            .build(cursor)
            .context("creating CSV reader for batch conversion")?;

        let batch = csv_reader
            .next()
            .ok_or_else(|| anyhow!("No data in batch"))??;

        // Apply trimming and type conversion
        let trimmed = apply_trimming(&batch, &schema_info.trim_columns)?;
        convert_to_final_types(&trimmed, schema_info)
    }

    /// Finalize processing - flush remaining batch and close writer
    pub fn finalize(&mut self) -> Result<u64> {
        // Flush any remaining rows
        if !self.current_batch_lines.is_empty() {
            self.flush_current_batch()?;
        }

        // Close the writer and get file size
        if let Some(writer) = self.writer.take() {
            writer.close().context("closing Parquet writer")?;
        }

        let output_path = self
            .output_path
            .as_ref()
            .ok_or_else(|| anyhow!("Output path not set"))?;

        // Get file size
        let metadata = fs::metadata(output_path).context("getting file metadata")?;
        let parquet_bytes = metadata.len();
        self.parquet_bytes = parquet_bytes;

        // Rename to final location
        let final_path = output_path.with_extension("parquet");
        fs::rename(output_path, &final_path).context("renaming Parquet file into place")?;

        debug!(
            "Finalized Parquet file: {} bytes written to {}",
            parquet_bytes,
            final_path.display()
        );
        Ok(parquet_bytes)
    }
}

/// Stream-processing version of csv_to_parquet that reads line by line
pub fn csv_to_parquet_streaming<R: BufRead>(
    mut reader: R,
    file_name: &str,
    out_dir: &Path,
) -> Result<u64> {
    const MAX_BATCH_ROWS: usize = 8_192;

    let mut processor = StreamingCsvProcessor::new(MAX_BATCH_ROWS);

    // Read header line
    let mut header_line = String::new();
    reader.read_line(&mut header_line)?;

    processor.initialize(file_name, &header_line, out_dir)?;

    // Process data lines
    let mut line = String::new();
    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line)?;
        if bytes_read == 0 {
            break; // EOF
        }

        // Skip empty lines
        if line.trim().is_empty() {
            continue;
        }

        processor.feed_row(&line)?;
    }

    processor.finalize()
}

/// Original function signature maintained for compatibility, but now streams the data
pub fn csv_to_parquet(file_name: &str, data: &str, out_dir: &Path) -> Result<u64> {
    let cursor = Cursor::new(data.as_bytes());
    let reader = BufReader::new(cursor);
    csv_to_parquet_streaming(reader, file_name, out_dir)
}

/// 4) Parse CSV header and create initial string schema
fn parse_header_and_create_string_schema(
    data: &str,
) -> Result<(Vec<String>, Schema), anyhow::Error> {
    let cursor = Cursor::new(data.as_bytes());
    let mut reader = BufReader::new(cursor);
    let mut header_line = String::new();
    reader.read_line(&mut header_line)?;

    // Parse CSV header (simple comma splitting for now)
    let headers: Vec<String> = header_line.trim().split(',').map(clean_str).collect();

    // Create schema with all string fields
    let fields: Vec<Field> = headers
        .iter()
        .map(|name| Field::new(name, DataType::Utf8, true))
        .collect();

    Ok((headers, Schema::new(fields)))
}

/// 5) Analyze first batch to detect schema, dates, and trimming needs
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

/// 7) Convert columns to their final types (dates, numbers, etc.)
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

/// 8) Extract date from filename for partitioning.
fn extract_date_from_filename(filename: &str) -> Option<String> {
    // Look for YYYYMMDD pattern (8 consecutive digits)
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

    // Look for YYYY-MM-DD or YYYY_MM_DD pattern
    for i in 0..chars.len().saturating_sub(9) {
        if chars.len() >= i + 10 {
            let potential_date: String = chars[i..i + 10].iter().collect();
            if potential_date.contains('-') || potential_date.contains('_') {
                let parts: Vec<&str> = potential_date.split(&['-', '_'][..]).collect();
                if parts.len() == 3 {
                    if let (Ok(year), Ok(month), Ok(day)) = (
                        parts[0].parse::<u32>(),
                        parts[1].parse::<u32>(),
                        parts[2].parse::<u32>(),
                    ) {
                        if (2000..=2030).contains(&year)
                            && (1..=12).contains(&month)
                            && (1..=31).contains(&day)
                        {
                            return Some(format!("{:04}-{:02}-{:02}", year, month, day));
                        }
                    }
                }
            }
        }
    }
    None
}
