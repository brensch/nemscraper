use anyhow::{anyhow, Context, Result};
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use chrono::{NaiveDateTime, TimeZone};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs;
use std::fs::File;
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
#[derive(Debug, Clone)]
pub struct SchemaInfo {
    pub schema: Schema,
    pub date_columns: Vec<String>,
    pub trim_columns: Vec<String>,
    pub table_name: String,
}

/// Parsed CSV row - avoids string reconstruction
#[derive(Debug, Clone)]
pub struct ParsedRow {
    pub values: Vec<String>,
}

impl ParsedRow {
    pub fn from_csv_line(line: &str) -> Self {
        let values: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
        Self { values }
    }
}

/// High-performance streaming CSV processor that builds Arrow arrays directly
pub struct OptimizedCsvProcessor {
    max_batch_rows: usize,
    headers: Option<Vec<String>>,
    schema_info: Option<SchemaInfo>,
    current_batch: Vec<ParsedRow>,
    batch_index: usize,
    row_count: u64,
    parquet_bytes: u64,
    writer: Option<ArrowWriter<File>>,
    output_path: Option<std::path::PathBuf>,
    final_path: Option<std::path::PathBuf>,
}

impl OptimizedCsvProcessor {
    pub fn new(max_batch_rows: usize) -> Self {
        Self {
            max_batch_rows,
            headers: None,
            schema_info: None,
            current_batch: Vec::with_capacity(max_batch_rows),
            batch_index: 0,
            row_count: 0,
            parquet_bytes: 0,
            writer: None,
            output_path: None,
            final_path: None,
        }
    }

    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    pub fn parquet_bytes(&self) -> u64 {
        self.parquet_bytes
    }

    /// Initialize the processor with header (I-line)
    pub fn initialize(&mut self, file_name: &str, header_line: &str, out_dir: &Path) -> Result<()> {
        // Parse headers from I-line
        let parsed_header = ParsedRow::from_csv_line(header_line);
        let headers = parsed_header.values;

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
        self.final_path = Some(final_path);

        debug!("Initialized optimized processor for table: {}", table_name);
        Ok(())
    }

    /// Feed a data row (D-line)
    pub fn feed_row(&mut self, line: &str) -> Result<()> {
        let parsed_row = ParsedRow::from_csv_line(line);
        self.current_batch.push(parsed_row);
        self.row_count += 1;

        // If we've accumulated enough rows, process the batch
        if self.current_batch.len() >= self.max_batch_rows {
            self.flush_current_batch()?;
        }

        Ok(())
    }

    /// Process the current batch of rows directly to Arrow
    fn flush_current_batch(&mut self) -> Result<()> {
        if self.current_batch.is_empty() {
            return Ok(());
        }

        let headers = self
            .headers
            .as_ref()
            .ok_or_else(|| anyhow!("Headers not initialized"))?;

        // If this is the first batch, infer schema
        if self.schema_info.is_none() {
            let schema_info = self.infer_schema_from_rows(&self.current_batch, headers)?;
            self.schema_info = Some(schema_info);
            self.setup_parquet_writer()?;
        }

        // Convert directly to Arrow batch
        let batch = self.convert_rows_to_arrow_batch(&self.current_batch)?;

        if let Some(ref mut writer) = self.writer {
            writer.write(&batch).context("writing batch to Parquet")?;
        }

        debug!(
            "Processed batch {} with {} rows",
            self.batch_index,
            self.current_batch.len()
        );

        self.batch_index += 1;
        self.current_batch.clear();
        Ok(())
    }

    /// Infer schema from the first batch of rows (much faster than CSV reconstruction)
    fn infer_schema_from_rows(&self, rows: &[ParsedRow], headers: &[String]) -> Result<SchemaInfo> {
        let mut final_fields = Vec::new();
        let mut date_columns = Vec::new();
        let mut trim_columns = Vec::new();

        // Build table name from headers 1,2,3
        let table_name = if headers.len() >= 4 {
            format!("{}---{}---{}", headers[1], headers[2], headers[3])
        } else {
            "default_table".to_string()
        };

        // Analyze first few rows to infer types (limit to 100 for speed)
        let sample_size = rows.len().min(100);

        for (col_idx, header) in headers.iter().enumerate() {
            let mut needs_trimming = false;
            let mut is_date_column = false;
            let mut inferred_type = DataType::Utf8;

            // Check first few non-empty values for this column
            for row in rows.iter().take(sample_size) {
                if col_idx < row.values.len() {
                    let raw_val = &row.values[col_idx];
                    if raw_val.trim().is_empty() {
                        continue;
                    }

                    let cleaned = clean_str(raw_val);

                    // Check if trimming changed the value
                    if cleaned != *raw_val {
                        needs_trimming = true;
                    }

                    // Check if it's a date column
                    if NaiveDateTime::parse_from_str(&cleaned, "%Y/%m/%d %H:%M:%S").is_ok() {
                        is_date_column = true;
                        break; // Found a date, no need to check more
                    } else {
                        // Try to infer numeric type
                        inferred_type = infer_arrow_dtype_from_str(&cleaned);
                        if inferred_type != DataType::Utf8 {
                            break; // Found a number, assume rest are numbers
                        }
                    }
                }
            }

            if needs_trimming {
                trim_columns.push(header.clone());
            }

            if is_date_column {
                date_columns.push(header.clone());
                final_fields.push(Field::new(
                    header,
                    DataType::Timestamp(TimeUnit::Millisecond, Some("+10:00".into())),
                    true,
                ));
            } else {
                final_fields.push(Field::new(header, inferred_type, true));
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
            .set_compression(Compression::SNAPPY)
            .build();

        let writer =
            ArrowWriter::try_new(tmp_file, Arc::new(schema_info.schema.clone()), Some(props))
                .context("initializing Parquet writer")?;

        self.writer = Some(writer);
        Ok(())
    }

    /// Convert parsed rows directly to Arrow RecordBatch (eliminates CSV reconstruction!)
    fn convert_rows_to_arrow_batch(&self, rows: &[ParsedRow]) -> Result<RecordBatch> {
        let headers = self.headers.as_ref().unwrap();
        let schema_info = self.schema_info.as_ref().unwrap();

        let mut arrays: Vec<ArrayRef> = Vec::new();

        // Build each column
        for (col_idx, field) in schema_info.schema.fields().iter().enumerate() {
            let column_name = field.name();

            // Extract values for this column
            let mut column_values: Vec<Option<String>> = Vec::with_capacity(rows.len());
            for row in rows {
                if col_idx < row.values.len() {
                    let raw_val = &row.values[col_idx];
                    let cleaned = if schema_info.trim_columns.contains(column_name) {
                        clean_str(raw_val)
                    } else {
                        raw_val.clone()
                    };

                    if cleaned.trim().is_empty() {
                        column_values.push(None);
                    } else {
                        column_values.push(Some(cleaned));
                    }
                } else {
                    column_values.push(None);
                }
            }

            // Build the appropriate Arrow array based on the field type
            let array: ArrayRef = match field.data_type() {
                DataType::Float64 => {
                    let float_values: Vec<Option<f64>> = column_values
                        .iter()
                        .map(|opt_str| opt_str.as_ref().and_then(|s| s.parse::<f64>().ok()))
                        .collect();
                    Arc::new(Float64Array::from(float_values))
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    let timestamp_values: Vec<Option<i64>> = column_values
                        .iter()
                        .map(|opt_str| {
                            opt_str.as_ref().and_then(|s| {
                                NaiveDateTime::parse_from_str(s, "%Y/%m/%d %H:%M:%S")
                                    .ok()
                                    .map(|dt| {
                                        let offset =
                                            chrono::FixedOffset::east_opt(10 * 3600).unwrap();
                                        offset
                                            .from_local_datetime(&dt)
                                            .single()
                                            .map(|dt_tz| dt_tz.timestamp_millis())
                                            .unwrap_or(0)
                                    })
                            })
                        })
                        .collect();

                    let timestamps =
                        TimestampMillisecondArray::from(timestamp_values).with_timezone("+10:00");
                    Arc::new(timestamps)
                }
                _ => {
                    // String column
                    let string_values: Vec<Option<&str>> = column_values
                        .iter()
                        .map(|opt_str| opt_str.as_ref().map(|s| s.as_str()))
                        .collect();
                    Arc::new(StringArray::from(string_values))
                }
            };

            arrays.push(array);
        }

        RecordBatch::try_new(Arc::new(schema_info.schema.clone()), arrays)
            .context("creating RecordBatch from arrays")
    }

    /// Finalize processing - flush remaining batch and close writer
    pub fn finalize(&mut self) -> Result<u64> {
        // Flush any remaining rows
        if !self.current_batch.is_empty() {
            self.flush_current_batch()?;
        }

        // Close the writer
        if let Some(writer) = self.writer.take() {
            writer.close().context("closing Parquet writer")?;
        }

        let output_path = self
            .output_path
            .as_ref()
            .ok_or_else(|| anyhow!("Output path not set"))?;
        let final_path = self
            .final_path
            .as_ref()
            .ok_or_else(|| anyhow!("Final path not set"))?;

        // Get file size
        let metadata = fs::metadata(output_path).context("getting file metadata")?;
        let parquet_bytes = metadata.len();
        self.parquet_bytes = parquet_bytes;

        // Rename to final location
        fs::rename(output_path, final_path).context("renaming Parquet file into place")?;

        debug!(
            "Finalized Parquet file: {} bytes written to {}",
            parquet_bytes,
            final_path.display()
        );
        Ok(parquet_bytes)
    }
}

/// Optimized streaming version - processes rows directly without CSV reconstruction
pub fn csv_to_parquet_optimized<R: BufRead>(
    mut reader: R,
    file_name: &str,
    out_dir: &Path,
) -> Result<u64> {
    const MAX_BATCH_ROWS: usize = 65_536; // 64k rows for better Arrow performance

    let mut processor = OptimizedCsvProcessor::new(MAX_BATCH_ROWS);

    // Read header line (I-line)
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

/// Backward compatibility function
pub fn csv_to_parquet(file_name: &str, data: &str, out_dir: &Path) -> Result<u64> {
    let cursor = Cursor::new(data.as_bytes());
    let reader = BufReader::new(cursor);
    csv_to_parquet_optimized(reader, file_name, out_dir)
}

/// Extract date from filename for partitioning.
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
