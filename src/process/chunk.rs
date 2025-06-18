use anyhow::{anyhow, Context, Result};
use arrow::array::*;
use arrow::csv::ReaderBuilder;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use chrono::{FixedOffset, NaiveDate};
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

/// Convert a string array to a timezone-aware timestamp array
fn convert_date_column(array: &ArrayRef, target_field: &Field) -> (Field, ArrayRef) {
    if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
        let timestamps: Vec<Option<i64>> = string_array
            .iter()
            .map(|opt_str| {
                opt_str.and_then(|s| {
                    let cleaned = clean_str(s);
                    parse_timestamp_millis(&cleaned)
                })
            })
            .collect();

        let timestamp_array = TimestampMillisecondArray::from(timestamps).with_timezone("+10:00");
        (target_field.clone(), Arc::new(timestamp_array) as ArrayRef)
    } else {
        (target_field.clone(), array.clone())
    }
}

/// Convert a string array to a float64 array
fn convert_numeric_column(array: &ArrayRef, target_field: &Field) -> (Field, ArrayRef) {
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

        (target_field.clone(), Arc::new(floats) as ArrayRef)
    } else {
        (target_field.clone(), array.clone())
    }
}

/// Convert columns to their final types (dates, numbers, etc.)
fn convert_to_final_types(
    batch: &RecordBatch,
    schema_info: &SchemaInfo,
) -> Result<RecordBatch, anyhow::Error> {
    let mut new_fields = Vec::new();
    let mut new_columns = Vec::new();

    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
        let array = batch.column(col_idx);
        let target_field = &schema_info.schema.fields()[col_idx];

        let (converted_field, converted_column) = if schema_info.date_columns.contains(field.name())
        {
            convert_date_column(array, target_field)
        } else if target_field.data_type() == &DataType::Float64 {
            convert_numeric_column(array, target_field)
        } else {
            ((**target_field).clone(), array.clone())
        };

        new_fields.push(converted_field);
        new_columns.push(converted_column);
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

/// 9) Main CSV→Parquet conversion using Arrow (memory efficient, single pass).
/// Returns the exact number of bytes written into the Parquet file.
pub fn csv_to_parquet(file_name: &str, data: &str, out_dir: &Path) -> Result<u64> {
    debug!("Starting Arrow-based CSV to Parquet conversion");

    // Step A: Parse header and create initial string schema
    let (headers, string_schema) =
        parse_header_and_create_string_schema(data).context("parsing CSV header")?;
    debug!("Headers: {:?}", headers);

    // Step B: Read first batch with string schema to infer types
    let mut cursor = Cursor::new(data.as_bytes());
    let mut schema_reader = ReaderBuilder::new(Arc::new(string_schema.clone()))
        .with_header(true)
        .with_batch_size(1_000)
        .build(&mut cursor)
        .context("creating CSV reader for schema inference")?;
    let first_batch = schema_reader
        .next()
        .ok_or_else(|| anyhow!("No data found in CSV"))?
        .context("reading first CSV batch")?;
    let schema_info =
        analyze_batch_for_schema(&first_batch, &headers).context("analyzing schema")?;
    debug!("Final schema: {:?}", schema_info.schema);
    debug!("Table name: {}", schema_info.table_name);

    // ─── Build a schema that skips the first 4 columns ────────────────────────────────
    let num_skip = 4;
    let output_fields = schema_info
        .schema
        .fields()
        .iter()
        .skip(num_skip)
        .map(|f| f.as_ref().clone())
        .collect::<Vec<Field>>();
    let output_schema = Arc::new(Schema::new(output_fields));

    // Step C: Setup output directory and partition
    let partition_date = extract_date_from_filename(file_name).unwrap_or_else(|| {
        warn!("No date found in '{}'; using 'unknown-date'", file_name);
        "unknown-date".to_string()
    });
    let table_dir = out_dir.join(&schema_info.table_name);
    fs::create_dir_all(&table_dir).context("creating table directory")?;
    let partition_dir = table_dir.join(format!("date={}", partition_date));
    fs::create_dir_all(&partition_dir).context("creating partition directory")?;

    // Step D: Setup Parquet writer with trimmed schema
    let final_name = format!("{}.parquet", file_name);
    let tmp_name = format!("{}.tmp", final_name);
    let final_path = partition_dir.join(&final_name);
    let tmp_path = partition_dir.join(&tmp_name);

    let mut tmp_file = File::create(&tmp_path).context("creating temporary Parquet file")?;
    let writer_file = tmp_file
        .try_clone()
        .context("cloning file handle for Parquet writer")?;
    let props = WriterProperties::builder()
        .set_compression(Compression::BROTLI(BrotliLevel::try_new(5)?))
        .build();
    let mut writer = ArrowWriter::try_new(writer_file, output_schema.clone(), Some(props))
        .context("initializing Parquet writer")?;

    // Step E: Re-read full CSV and write each batch, projecting out first 4 cols
    let mut cursor = Cursor::new(data.as_bytes());
    let string_schema_for_reading = {
        let fields: Vec<Field> = headers
            .iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect();
        Schema::new(fields)
    };
    let csv_reader = ReaderBuilder::new(Arc::new(string_schema_for_reading))
        .with_header(true)
        .with_batch_size(8_192)
        .build(&mut cursor)
        .context("creating CSV reader for data conversion")?;

    for batch_result in csv_reader {
        let batch = batch_result.context("reading CSV batch")?;
        let trimmed =
            apply_trimming(&batch, &schema_info.trim_columns).context("applying trimming")?;
        let final_batch =
            convert_to_final_types(&trimmed, &schema_info).context("converting types")?;

        // Drop the first 4 columns before writing
        let projected_cols = final_batch.columns()[num_skip..].to_vec();
        let projected_batch = RecordBatch::try_new(output_schema.clone(), projected_cols)
            .context("projecting columns")?;

        writer
            .write(&projected_batch)
            .context("writing batch to Parquet")?;
    }

    // Finish writing (flush metadata etc.)
    writer.close().context("closing Parquet writer")?;

    // Capture byte‐count
    let parquet_bytes = tmp_file
        .stream_position()
        .context("getting Parquet file size")?;

    // Atomically move into place
    fs::rename(&tmp_path, &final_path).context("renaming Parquet file")?;

    debug!(
        "Wrote {} bytes of Parquet to {}",
        parquet_bytes,
        final_path.display()
    );
    Ok(parquet_bytes)
}

/// Fast parse of `"YYYY/MM/DD HH:MM:SS"` → millis UTC
pub fn parse_timestamp_millis(s: &str) -> Option<i64> {
    let s = s.trim();
    // minimal length + separators check
    if s.len() < 19 || &s[4..5] != "/" || &s[7..8] != "/" || &s[10..11] != " " {
        return None;
    }
    let year: i32 = s[0..4].parse().ok()?;
    let month: u32 = s[5..7].parse().ok()?;
    let day: u32 = s[8..10].parse().ok()?;
    let hour: u32 = s[11..13].parse().ok()?;
    let min: u32 = s[14..16].parse().ok()?;
    let sec: u32 = s[17..19].parse().ok()?;

    let naive = NaiveDate::from_ymd_opt(year, month, day)?.and_hms_opt(hour, min, sec)?;
    let offset = FixedOffset::east_opt(10 * 3600).unwrap();
    offset
        .from_local_datetime(&naive)
        .single()
        .map(|dt| dt.timestamp_millis())
}
