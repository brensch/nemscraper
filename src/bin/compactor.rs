use anyhow::{Context, Result};
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_array::RecordBatchReader;
use chrono::TimeZone;
use chrono::{FixedOffset, NaiveDate, Utc};
use nemscraper::history::compacted::InputCompactionRow;
use nemscraper::history::table_history::TableHistory;
use once_cell::sync::Lazy;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::basic::{BrotliLevel, Compression};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{
    collections::HashMap,
    env,
    fs::{self, File},
    io::BufWriter,
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex},
    thread,
    time::Duration,
};
use tracing::{debug, error, info, instrument, warn};
use walkdir::WalkDir;

// Per-partition locks to prevent concurrent compactions
static PARTITION_LOCKS: Lazy<StdMutex<HashMap<String, Arc<StdMutex<()>>>>> =
    Lazy::new(|| StdMutex::new(HashMap::new()));

fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let mut args = env::args().skip(1);
    let input_root = PathBuf::from(args.next().expect("Missing <INPUT_DIR>"));
    let output_root = PathBuf::from(args.next().expect("Missing <OUTPUT_DIR>"));
    let meta_root = PathBuf::from(args.next().expect("Missing <META_DIR>"));

    info!(
        input = %input_root.display(),
        output = %output_root.display(),
        meta = %meta_root.display(),
        "Starting compactor loop"
    );

    let hist = TableHistory::new_compacted(&meta_root)
        .with_context(|| format!("Failed to create TableHistory at {}", meta_root.display()))?;

    hist.vacuum()
        .with_context(|| "Failed to perform initial history vacuum")?;

    // determine worker count automatically
    let num_workers = std::thread::available_parallelism().map_or(1, |n| n.get());
    info!(workers = num_workers, "Using parallelism");

    rayon::ThreadPoolBuilder::new()
        .num_threads(num_workers)
        .build_global()
        .with_context(|| "Failed to build global thread pool")?;

    loop {
        let cycle_start = Utc::now();
        info!(timestamp = %cycle_start.to_rfc3339(), "Beginning compaction cycle");

        // discover partitions
        let mut partitions = discover_partitions(&input_root).with_context(|| {
            format!("Failed to discover partitions in {}", input_root.display())
        })?;
        partitions.sort();
        partitions.dedup();

        info!(partition_count = partitions.len(), "Discovered partitions");

        rayon::scope(|s| {
            for partition in partitions {
                let input_root = input_root.clone();
                let output_root = output_root.clone();
                let hist = Arc::clone(&hist);

                s.spawn(move |_| {
                    if let Err(e) = compact_partition(&input_root, &output_root, &partition, &hist)
                    {
                        error!(
                            partition = %partition.display(),
                            error = %e,
                            error_chain = ?e.chain().collect::<Vec<_>>(),
                            "Partition compaction failed"
                        );
                    }
                });
            }
        });

        if let Err(e) = hist.vacuum() {
            error!(
                error = %e,
                error_chain = ?e.chain().collect::<Vec<_>>(),
                "History vacuum failed"
            );
        }

        let cycle_end = Utc::now();
        let elapsed = cycle_end - cycle_start;
        info!(duration_ms = elapsed.num_milliseconds(), "Cycle complete");

        info!("Sleeping 5 minutes before next cycle");
        thread::sleep(Duration::from_secs(300));
    }
}

#[instrument(skip(input_root))]
fn discover_partitions(input_root: &Path) -> Result<Vec<PathBuf>> {
    let mut parts = Vec::new();

    if !input_root.exists() {
        warn!(path = %input_root.display(), "Input root directory does not exist");
        return Ok(parts);
    }

    for entry in WalkDir::new(input_root)
        .follow_links(false)
        .into_iter()
        .filter_map(Result::ok)
    {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
            if let Some(parent) = path.parent() {
                let rel = parent
                    .strip_prefix(input_root)
                    .with_context(|| {
                        format!(
                            "Failed to strip prefix {} from path {}",
                            input_root.display(),
                            parent.display()
                        )
                    })?
                    .to_path_buf();
                parts.push(rel);
            }
        }
    }

    debug!(
        discovered_partitions = parts.len(),
        "Partition discovery complete"
    );
    Ok(parts)
}

#[instrument(skip(hist))]
fn compact_partition(
    input_root: &Path,
    output_root: &Path,
    partition: &Path,
    hist: &Arc<TableHistory<InputCompactionRow>>,
) -> Result<()> {
    let partition_key = partition.to_string_lossy().to_string();

    // Get the lock for this partition - this will block until available
    let lock_arc = {
        let mut map = PARTITION_LOCKS
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire partition locks map: {}", e))?;
        map.entry(partition_key.clone())
            .or_insert_with(|| Arc::new(StdMutex::new(())))
            .clone()
    };

    info!(partition = %partition.display(), "Waiting for partition lock");
    let _guard = lock_arc.lock().map_err(|e| {
        anyhow::anyhow!(
            "Failed to acquire lock for partition {}: {}",
            partition_key,
            e
        )
    })?;

    info!(partition = %partition.display(), "Acquired partition lock, starting compaction");

    let result = compact_partition_inner(input_root, output_root, partition, hist);

    match &result {
        Ok(_) => {
            info!(partition = %partition.display(), "Partition compaction completed successfully")
        }
        Err(e) => error!(
            partition = %partition.display(),
            error = %e,
            error_chain = ?e.chain().collect::<Vec<_>>(),
            "Partition compaction failed"
        ),
    }

    result
}

/// Helper function to read schema directly from parquet metadata
fn read_schema_from_parquet_metadata(file_path: &Path) -> Result<Arc<Schema>> {
    debug!(file = %file_path.display(), "Reading schema from parquet metadata");

    let file = File::open(file_path)
        .with_context(|| format!("Failed to open parquet file {}", file_path.display()))?;

    let reader = SerializedFileReader::new(file).with_context(|| {
        format!(
            "Failed to create parquet reader for {}",
            file_path.display()
        )
    })?;

    let metadata = reader.metadata();
    let file_metadata = metadata.file_metadata();
    let parquet_schema = file_metadata.schema_descr();
    let key_value_metadata = file_metadata.key_value_metadata();

    let arrow_schema =
        parquet_to_arrow_schema(parquet_schema, key_value_metadata).with_context(|| {
            format!(
                "Failed to convert parquet schema to arrow schema for {}",
                file_path.display()
            )
        })?;

    Ok(Arc::new(arrow_schema))
}

/// Determines the "most evolved" type between two data types
fn get_most_evolved_type(type1: &DataType, type2: &DataType) -> DataType {
    match (type1, type2) {
        // Float64 is more evolved than Utf8
        (DataType::Utf8, DataType::Float64) => DataType::Float64,
        (DataType::Float64, DataType::Utf8) => DataType::Float64,

        // Timestamp is more evolved than Utf8 - always use +10:00 timezone
        (DataType::Utf8, DataType::Timestamp(TimeUnit::Millisecond, _)) => {
            DataType::Timestamp(TimeUnit::Millisecond, Some("+10:00".into()))
        }
        (DataType::Timestamp(TimeUnit::Millisecond, _), DataType::Utf8) => {
            DataType::Timestamp(TimeUnit::Millisecond, Some("+10:00".into()))
        }

        // Handle timestamp timezone normalization - always prefer +10:00
        (
            DataType::Timestamp(TimeUnit::Millisecond, _),
            DataType::Timestamp(TimeUnit::Millisecond, _),
        ) => DataType::Timestamp(TimeUnit::Millisecond, Some("+10:00".into())),

        // If types are the same, return either one
        (a, b) if a == b => a.clone(),

        // Default: return the first type (shouldn't happen in our case)
        (a, _) => a.clone(),
    }
}

/// Determines if a conversion from one type to another is supported
fn can_convert_type(from: &DataType, to: &DataType) -> bool {
    match (from, to) {
        // Utf8 can be converted to Float64 or Timestamp
        (DataType::Utf8, DataType::Float64) => true,
        (DataType::Utf8, DataType::Timestamp(TimeUnit::Millisecond, _)) => true,

        // Float64 stays as Float64
        (DataType::Float64, DataType::Float64) => true,

        // Timestamp conversions (including timezone changes)
        (
            DataType::Timestamp(TimeUnit::Millisecond, _),
            DataType::Timestamp(TimeUnit::Millisecond, _),
        ) => true,

        // Same types are always convertible
        (a, b) if a == b => true,

        _ => false,
    }
}

/// Creates a null array of the specified type and length
fn create_null_array(data_type: &DataType, length: usize) -> Result<ArrayRef> {
    match data_type {
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for _ in 0..length {
                builder.append_null();
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::new();
            for _ in 0..length {
                builder.append_null();
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            let mut builder = TimestampMillisecondBuilder::new();
            for _ in 0..length {
                builder.append_null();
            }
            let array = builder.finish().with_timezone_opt(tz.clone());
            Ok(Arc::new(array))
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::new();
            for _ in 0..length {
                builder.append_null();
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::new();
            for _ in 0..length {
                builder.append_null();
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for _ in 0..length {
                builder.append_null();
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(anyhow::anyhow!(
            "Unsupported data type for null array creation: {:?}",
            data_type
        )),
    }
}

/// Evolves a schema by taking the union of all fields and the most evolved type for each field
fn evolve_schema(base_schema: &Schema, new_schema: &Schema) -> Result<Arc<Schema>> {
    let mut field_map: HashMap<String, Field> = HashMap::new();
    let mut changes_made = false;

    // First, add all fields from the base schema
    for field in base_schema.fields() {
        field_map.insert(field.name().clone(), field.as_ref().clone());
    }

    // Then, process fields from the new schema
    for new_field in new_schema.fields() {
        if let Some(existing_field) = field_map.get(new_field.name()) {
            // Field exists in both schemas - evolve the type
            let evolved_type =
                get_most_evolved_type(existing_field.data_type(), new_field.data_type());

            // Check that both types can be converted to the evolved type
            if !can_convert_type(existing_field.data_type(), &evolved_type) {
                return Err(anyhow::anyhow!(
                    "Cannot convert field '{}' from {:?} to evolved type {:?}",
                    existing_field.name(),
                    existing_field.data_type(),
                    evolved_type
                ));
            }

            if !can_convert_type(new_field.data_type(), &evolved_type) {
                return Err(anyhow::anyhow!(
                    "Cannot convert field '{}' from {:?} to evolved type {:?}",
                    new_field.name(),
                    new_field.data_type(),
                    evolved_type
                ));
            }

            if &evolved_type != existing_field.data_type() {
                info!(
                    field = %existing_field.name(),
                    base_type = ?existing_field.data_type(),
                    new_type = ?new_field.data_type(),
                    evolved_type = ?evolved_type,
                    "Evolving field type"
                );
                changes_made = true;
            }

            // Create field with evolved type, preserving nullability (most permissive)
            let nullable = existing_field.is_nullable() || new_field.is_nullable();
            let evolved_field = Field::new(existing_field.name(), evolved_type, nullable);
            field_map.insert(existing_field.name().clone(), evolved_field);
        } else {
            // New field doesn't exist in base schema - add it
            info!(
                field = %new_field.name(),
                field_type = ?new_field.data_type(),
                "Adding new field to schema"
            );
            field_map.insert(new_field.name().clone(), new_field.as_ref().clone());
            changes_made = true;
        }
    }

    if changes_made {
        info!("Schema evolution completed with type upgrades and/or new fields");
    }

    // Create evolved schema with fields in a consistent order (alphabetical)
    let mut evolved_fields: Vec<Field> = field_map.into_values().collect();
    evolved_fields.sort_by(|a, b| a.name().cmp(b.name()));

    Ok(Arc::new(Schema::new(evolved_fields)))
}

/// Parse timestamp from string in the format "YYYY/MM/DD HH:MM:SS"
fn parse_timestamp_millis(s: &str) -> Option<i64> {
    let s = s.trim();
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

/// Converts a column from one type to another
fn convert_column(array: &dyn Array, target_field: &Field) -> Result<ArrayRef> {
    match (array.data_type(), target_field.data_type()) {
        // Utf8 to Float64
        (DataType::Utf8, DataType::Float64) => {
            if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
                let float_values: Float64Array = string_array
                    .iter()
                    .map(|opt_str| {
                        opt_str.and_then(|s| {
                            let cleaned = s.trim();
                            if cleaned.is_empty() {
                                None
                            } else {
                                cleaned.parse::<f64>().ok()
                            }
                        })
                    })
                    .collect();
                Ok(Arc::new(float_values))
            } else {
                Err(anyhow::anyhow!(
                    "Expected StringArray for Utf8 to Float64 conversion"
                ))
            }
        }
        // Utf8 to Timestamp
        (DataType::Utf8, DataType::Timestamp(TimeUnit::Millisecond, tz)) => {
            if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
                let timestamp_values: Vec<Option<i64>> = string_array
                    .iter()
                    .map(|opt_str| {
                        opt_str.and_then(|s| {
                            let cleaned = s.trim();
                            if cleaned.is_empty() {
                                None
                            } else {
                                parse_timestamp_millis(cleaned)
                            }
                        })
                    })
                    .collect();
                let timestamp_array =
                    TimestampMillisecondArray::from(timestamp_values).with_timezone_opt(tz.clone());
                Ok(Arc::new(timestamp_array))
            } else {
                Err(anyhow::anyhow!(
                    "Expected StringArray for Utf8 to Timestamp conversion"
                ))
            }
        }
        // Timestamp timezone conversion
        (
            DataType::Timestamp(TimeUnit::Millisecond, from_tz),
            DataType::Timestamp(TimeUnit::Millisecond, to_tz),
        ) if from_tz != to_tz => {
            if let Some(ts_array) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
                let new_array = ts_array.clone().with_timezone_opt(to_tz.clone());
                Ok(Arc::new(new_array))
            } else {
                Err(anyhow::anyhow!(
                    "Expected TimestampMillisecondArray for timezone conversion"
                ))
            }
        }
        // No conversion needed
        _ => Ok(array.slice(0, array.len())),
    }
}

/// Converts a record batch to match a target schema, handling missing and extra columns
fn convert_batch_to_schema(batch: RecordBatch, target_schema: &Schema) -> Result<RecordBatch> {
    let source_schema = batch.schema();
    let mut new_columns = Vec::new();

    // Create a mapping from field name to column index in the source batch
    let source_field_map: HashMap<String, usize> = source_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| (field.name().clone(), i))
        .collect();

    // Process each field in the target schema
    for target_field in target_schema.fields() {
        if let Some(&source_column_idx) = source_field_map.get(target_field.name()) {
            // Field exists in source - convert it
            let source_array = batch.column(source_column_idx);
            let converted_array = convert_column(source_array, target_field)
                .with_context(|| format!("Failed to convert column '{}'", target_field.name()))?;
            new_columns.push(converted_array);
        } else {
            // Field doesn't exist in source - create null array
            let null_array = create_null_array(target_field.data_type(), batch.num_rows())
                .with_context(|| {
                    format!(
                        "Failed to create null array for missing column '{}'",
                        target_field.name()
                    )
                })?;
            new_columns.push(null_array);

            debug!(
                field = %target_field.name(),
                field_type = ?target_field.data_type(),
                rows = batch.num_rows(),
                "Created null array for missing column"
            );
        }
    }

    RecordBatch::try_new(Arc::new(target_schema.clone()), new_columns)
        .with_context(|| "Failed to create converted record batch")
}

/// Streams data from a parquet file, converting each batch to target schema and writing immediately
fn stream_and_convert_parquet_file(
    file_path: &Path,
    target_schema: &Schema,
    writer: &mut ArrowWriter<BufWriter<File>>,
) -> Result<usize> {
    let file = File::open(file_path)
        .with_context(|| format!("Failed to open parquet file {}", file_path.display()))?;

    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("Failed to create reader for {}", file_path.display()))?
        .with_batch_size(1024)
        .build()
        .with_context(|| format!("Failed to build reader for {}", file_path.display()))?;

    let mut batch_count = 0;
    while let Some(batch) = reader.next().transpose()? {
        let converted_batch = convert_batch_to_schema(batch, target_schema).with_context(|| {
            format!(
                "Failed to convert batch {} from {}",
                batch_count,
                file_path.display()
            )
        })?;

        writer.write(&converted_batch).with_context(|| {
            format!(
                "Failed to write converted batch {} from {}",
                batch_count,
                file_path.display()
            )
        })?;

        batch_count += 1;
    }

    Ok(batch_count)
}

/// Performs the core compaction logic with streaming schema evolution
fn compact_partition_inner(
    input_root: &Path,
    output_root: &Path,
    partition: &Path,
    hist: &Arc<TableHistory<InputCompactionRow>>,
) -> Result<()> {
    info!(partition = %partition.display(), "Scanning inputs");
    let in_dir = input_root.join(partition);
    let out_dir = output_root.join(partition);

    fs::create_dir_all(&out_dir)
        .with_context(|| format!("Failed to create output directory {}", out_dir.display()))?;

    let mut new_inputs = Vec::new();
    if in_dir.exists() {
        for entry in fs::read_dir(&in_dir)
            .with_context(|| format!("Failed to read input directory {}", in_dir.display()))?
        {
            let entry = entry.with_context(|| {
                format!("Failed to read directory entry in {}", in_dir.display())
            })?;
            let p = entry.path();

            if p.extension().and_then(|s| s.to_str()) == Some("parquet") {
                let rel = p.strip_prefix(input_root).with_context(|| {
                    format!(
                        "Failed to strip prefix {} from input file {}",
                        input_root.display(),
                        p.display()
                    )
                })?;
                let key = rel.to_string_lossy().to_string();
                let sanitized = key.replace(['/', '\\'], "_");

                let already_processed = hist.get(sanitized.clone());
                debug!(
                    file = %p.display(),
                    sanitized_key = %sanitized,
                    already_processed = already_processed,
                    "Checking input file"
                );

                if !already_processed {
                    new_inputs.push((p.clone(), sanitized));
                }
            }
        }
    }

    if new_inputs.is_empty() {
        debug!(partition = %partition.display(), "No new inputs to process");
        return Ok(());
    }

    info!(
        partition = %partition.display(),
        new_input_count = new_inputs.len(),
        "Found new inputs to compact"
    );

    let compacted = out_dir.join("compacted.parquet");
    let tmp = out_dir.join("compacted.parquet.tmp");

    // Clean up any existing temp file
    if tmp.exists() {
        fs::remove_file(&tmp)
            .with_context(|| format!("Failed to remove existing temp file {}", tmp.display()))?;
    }

    // Step 1: Determine the evolved schema by examining all schemas (metadata only)
    info!("Determining evolved schema from all input files");
    let mut evolved_schema = if compacted.exists() {
        debug!("Reading schema from existing compacted file");
        read_schema_from_parquet_metadata(&compacted)?
    } else {
        debug!("Reading schema from first input file");
        read_schema_from_parquet_metadata(&new_inputs[0].0)?
    };

    // Evolve schema by examining all input files (just metadata, not data)
    for (input_path, _) in &new_inputs {
        let input_schema = read_schema_from_parquet_metadata(input_path)?;
        evolved_schema = evolve_schema(&evolved_schema, &input_schema)
            .with_context(|| format!("Failed to evolve schema with {}", input_path.display()))?;
    }

    info!(
        final_schema_fields = evolved_schema.fields().len(),
        field_types = ?evolved_schema.fields().iter().map(|f| format!("{}: {:?}", f.name(), f.data_type())).collect::<Vec<_>>(),
        "Computed evolved schema"
    );

    // Step 2: Create streaming writer with evolved schema
    let writer_file = File::create(&tmp)
        .with_context(|| format!("Failed to create temp file {}", tmp.display()))?;

    let props = WriterProperties::builder()
        .set_compression(Compression::BROTLI(
            BrotliLevel::try_new(5).with_context(|| "Failed to create Brotli compression level")?,
        ))
        .build();

    let mut writer = ArrowWriter::try_new(
        BufWriter::new(writer_file),
        evolved_schema.clone(),
        Some(props),
    )
    .with_context(|| {
        format!(
            "Failed to create Arrow writer for temp file {}",
            tmp.display()
        )
    })?;

    // Step 3: Stream existing compacted file if it exists (convert and write immediately)
    if compacted.exists() {
        info!("Streaming and converting existing compacted data");
        let existing_batch_count =
            stream_and_convert_parquet_file(&compacted, &evolved_schema, &mut writer)
                .with_context(|| "Failed to stream and convert existing compacted file")?;
        info!(
            "Streamed {} batches from existing compacted file",
            existing_batch_count
        );
    }

    // Step 4: Stream all new input files (one at a time, convert and write immediately)
    let start = Utc::now();
    let mut successful_inputs = Vec::new();

    for (i, (path, key)) in new_inputs.iter().enumerate() {
        info!(
            file = %path.display(),
            progress = format!("{}/{}", i + 1, new_inputs.len()),
            "Streaming input file"
        );

        let batch_count = stream_and_convert_parquet_file(path, &evolved_schema, &mut writer)
            .with_context(|| {
                format!("Failed to stream and convert input file {}", path.display())
            })?;

        debug!(
            file = %path.display(),
            batches_processed = batch_count,
            "Successfully streamed input file"
        );

        successful_inputs.push((path.clone(), key.clone()));
    }

    // Step 5: Finalize (same as before)
    writer.close().with_context(|| {
        format!(
            "Failed to close Arrow writer for temp file {}",
            tmp.display()
        )
    })?;

    fs::rename(&tmp, &compacted).with_context(|| {
        format!(
            "Failed to rename temp file {} to final file {}",
            tmp.display(),
            compacted.display()
        )
    })?;

    info!(
        outfile = %compacted.display(),
        input_files_processed = successful_inputs.len(),
        "Successfully wrote compacted file with evolved schema"
    );

    // Record successful compactions in history
    let end = Utc::now();
    let part_date = extract_partition_date(partition).with_context(|| {
        format!(
            "Failed to extract partition date from {}",
            partition.display()
        )
    })?;

    for (_path, key) in successful_inputs {
        let row = InputCompactionRow {
            input_file: key.clone(),
            partition: part_date,
            compaction_start: start,
            compaction_end: end,
            thread: 0,
        };

        hist.add(&row).with_context(|| {
            format!(
                "Failed to record compaction in history for input file {}",
                key
            )
        })?;

        debug!(input_file = %key, "Recorded successful compaction in history");
    }

    info!(
        partition = %partition.display(),
        duration_ms = (end - start).num_milliseconds(),
        "Partition compaction completed successfully"
    );

    Ok(())
}

fn extract_partition_date(part: &Path) -> Result<NaiveDate> {
    for comp in part.iter().map(|c| c.to_string_lossy()) {
        if let Some(d) = comp.strip_prefix("date=") {
            return NaiveDate::parse_from_str(d, "%Y-%m-%d").with_context(|| {
                format!(
                    "Invalid date format '{}' in partition path {}",
                    d,
                    part.display()
                )
            });
        }
    }
    Err(anyhow::anyhow!(
        "No date=YYYY-MM-DD component found in partition path {:?}",
        part
    ))
}
