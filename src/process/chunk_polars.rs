use arrow::array::*;
use arrow::compute;
use arrow::csv::{Reader, ReaderBuilder};
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use chrono::{NaiveDateTime, TimeZone, Utc};
use parquet::arrow::ArrowWriter;
use parquet::basic::BrotliLevel;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::error::Error;
use std::fs::File;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info, warn};

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

/// 3) Promote integer types to Float64 in Arrow schema.
fn promote_to_float64(dt: &DataType) -> DataType {
    match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32 => DataType::Float64,
        DataType::Float64 => DataType::Float64,
        other => other.clone(),
    }
}

/// Schema inference and trimming detection from first batch
struct SchemaInfo {
    schema: Schema,
    date_columns: Vec<String>,
    trim_columns: Vec<String>,
    table_name: String,
}

/// 4) Sample the first batch to infer schema, detect date columns and trimming needs.
fn sample_and_infer_schema(data: &str) -> Result<SchemaInfo, Box<dyn Error>> {
    let mut cursor = Cursor::new(data.as_bytes());

    // Read first 1000 rows only for sampling
    let reader = ReaderBuilder::new(Arc::new(Schema::empty()))
        .with_header(true)
        .with_batch_size(1000) // Only read first 1000 rows
        .build(&mut cursor)?;

    let first_batch = reader
        .into_iter()
        .next()
        .transpose()?
        .ok_or("No data found in CSV")?;

    let mut final_fields = Vec::new();
    let mut date_columns = Vec::new();
    let mut trim_columns = Vec::new();

    // Extract header names for table naming (columns 1,2,3)
    let header_names: Vec<String> = first_batch
        .schema()
        .fields()
        .iter()
        .map(|f| clean_str(f.name()))
        .collect();

    let table_name = if header_names.len() >= 4 {
        format!(
            "{}---{}---{}",
            header_names[1], header_names[2], header_names[3]
        )
    } else {
        "default_table".to_string()
    };

    // Analyze each column
    for (col_idx, field) in first_batch.schema().fields().iter().enumerate() {
        let col_name = field.name();
        let array = first_batch.column(col_idx);

        // Check if this is a string column that needs analysis
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
                    // Date columns stay as Utf8 initially, we'll convert later
                } else {
                    // Try to infer numeric type
                    inferred_type = infer_arrow_dtype_from_str(&cleaned);
                }
            }

            if needs_trimming {
                trim_columns.push(col_name.clone());
            }

            if is_date_column {
                date_columns.push(col_name.clone());
                // Keep as Utf8 for now, will convert to timestamp later
                final_fields.push(Field::new(col_name, DataType::Utf8, true));
            } else {
                final_fields.push(Field::new(col_name, inferred_type, true));
            }
        } else {
            // Non-string column, promote integers to Float64
            let promoted_type = promote_to_float64(field.data_type());
            final_fields.push(Field::new(col_name, promoted_type, field.is_nullable()));
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

/// 5) Apply string trimming to specified columns in a RecordBatch.
fn apply_trimming(
    batch: &RecordBatch,
    trim_columns: &[String],
) -> Result<RecordBatch, Box<dyn Error>> {
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

/// 6) Convert date columns from Utf8 to Timestamp with timezone.
fn convert_date_columns(
    batch: &RecordBatch,
    date_columns: &[String],
) -> Result<RecordBatch, Box<dyn Error>> {
    if date_columns.is_empty() {
        return Ok(batch.clone());
    }

    let mut new_fields = Vec::new();
    let mut new_columns = Vec::new();

    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
        let array = batch.column(col_idx);

        if date_columns.contains(field.name()) {
            // Convert this date column
            if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
                // Parse dates and convert to timestamps (milliseconds since epoch)
                let timestamps: TimestampMillisecondArray = string_array
                    .iter()
                    .map(|opt_str| {
                        opt_str.and_then(|s| {
                            let cleaned = clean_str(s);
                            NaiveDateTime::parse_from_str(&cleaned, "%Y/%m/%d %H:%M:%S")
                                .ok()
                                .map(|dt| {
                                    // Convert to GMT+10 timezone
                                    let offset = chrono::FixedOffset::east_opt(10 * 3600).unwrap(); // +10:00
                                    offset
                                        .from_local_datetime(&dt)
                                        .single()
                                        .map(|dt_tz| dt_tz.timestamp_millis())
                                        .unwrap_or(0)
                                })
                        })
                    })
                    .collect();

                new_fields.push(Field::new(
                    field.name(),
                    DataType::Timestamp(TimeUnit::Millisecond, Some("+10:00".into())),
                    field.is_nullable(),
                ));
                new_columns.push(Arc::new(timestamps) as ArrayRef);
            } else {
                new_fields.push((**field).clone());
                new_columns.push(array.clone());
            }
        } else {
            new_fields.push((**field).clone());
            new_columns.push(array.clone());
        }
    }

    let new_schema = Arc::new(Schema::new(new_fields));
    Ok(RecordBatch::try_new(new_schema, new_columns)?)
}

/// 7) Extract date from filename for partitioning.
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
                if year >= 2000
                    && year <= 2030
                    && month >= 1
                    && month <= 12
                    && day >= 1
                    && day <= 31
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
                        if year >= 2000
                            && year <= 2030
                            && month >= 1
                            && month <= 12
                            && day >= 1
                            && day <= 31
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

/// 8) Main CSV→Parquet conversion using Arrow (memory efficient, single pass).
pub fn csv_to_parquet(file_name: &str, data: &str, out_dir: &Path) -> Result<(), Box<dyn Error>> {
    debug!("Starting Arrow-based CSV to Parquet conversion");

    // Step A: Sample and infer schema (reads only first 1000 rows)
    let schema_info = sample_and_infer_schema(data)?;

    debug!("Inferred schema: {:?}", schema_info.schema);
    debug!("Table name: {}", schema_info.table_name);

    // Step B: Setup output directory structure
    let partition_date = extract_date_from_filename(file_name).unwrap_or_else(|| {
        warn!(
            "No date found in filename '{}', using default partition",
            file_name
        );
        "unknown-date".to_string()
    });

    let table_dir = out_dir.join(&schema_info.table_name);
    std::fs::create_dir_all(&table_dir)?;
    let partition_dir = table_dir.join(format!("date={}", partition_date));
    std::fs::create_dir_all(&partition_dir)?;

    // Step C: Setup Parquet writer
    let final_name = format!("{}.parquet", file_name);
    let tmp_name = format!("{}.tmp", final_name);
    let final_path = partition_dir.join(final_name);
    let tmp_path = partition_dir.join(tmp_name);

    let tmp_file = File::create(&tmp_path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::BROTLI(BrotliLevel::try_new(5)?))
        .build();

    // Step D: Single-pass CSV read with transformations
    let mut cursor = Cursor::new(data.as_bytes());
    let reader = ReaderBuilder::new(Arc::new(schema_info.schema.clone()))
        .with_header(true)
        .with_batch_size(8192) // Process in 8K row batches
        .build(&mut cursor)?;

    let mut writer =
        ArrowWriter::try_new(tmp_file, Arc::new(schema_info.schema.clone()), Some(props))?;

    // Process each batch
    for batch_result in reader {
        let batch = batch_result?;

        // Apply trimming if needed
        let trimmed_batch = apply_trimming(&batch, &schema_info.trim_columns)?;

        // Convert date columns if needed
        let final_batch = convert_date_columns(&trimmed_batch, &schema_info.date_columns)?;

        // Write batch to Parquet
        writer.write(&final_batch)?;
    }

    writer.close()?;

    // Atomic rename
    std::fs::rename(&tmp_path, &final_path)?;

    info!(
        "Wrote Parquet file to table '{}' with date partition '{}'",
        schema_info.table_name, partition_date
    );

    debug!("Completed Arrow-based conversion: {}", final_path.display());
    Ok(())
}
