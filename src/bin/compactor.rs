use anyhow::{Context, Result};
use arrow_array::RecordBatchReader;
use chrono::{DateTime, NaiveDate, Utc};
use nemscraper::history::compacted::InputCompactionRow;
use nemscraper::history::table_history::{HistoryRow, TableHistory};
use once_cell::sync::Lazy;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{BrotliLevel, Compression};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use rayon;
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
use tracing_subscriber;
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
                let sanitized = key.replace('/', "_").replace('\\', "_");

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

    // Create the temp file for writing
    let writer_file = File::create(&tmp)
        .with_context(|| format!("Failed to create temp file {}", tmp.display()))?;

    // infer schema from existing compacted file or first new input
    let schema = if compacted.exists() {
        info!(compacted_file = %compacted.display(), "Reading schema from existing compacted file");
        let f = File::open(&compacted).with_context(|| {
            format!(
                "Failed to open existing compacted file {}",
                compacted.display()
            )
        })?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(f)
            .with_context(|| {
                format!(
                    "Failed to create reader for existing compacted file {}",
                    compacted.display()
                )
            })?
            .with_batch_size(1024)
            .build()
            .with_context(|| {
                format!(
                    "Failed to build reader for existing compacted file {}",
                    compacted.display()
                )
            })?;
        reader.schema()
    } else {
        let first = &new_inputs[0].0;
        info!(first_input = %first.display(), "Reading schema from first input file");
        let f = File::open(first)
            .with_context(|| format!("Failed to open first input file {}", first.display()))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(f)
            .with_context(|| {
                format!(
                    "Failed to create reader for first input file {}",
                    first.display()
                )
            })?
            .with_batch_size(1024)
            .build()
            .with_context(|| {
                format!(
                    "Failed to build reader for first input file {}",
                    first.display()
                )
            })?;
        reader.schema()
    };

    let props = WriterProperties::builder()
        .set_compression(Compression::BROTLI(
            BrotliLevel::try_new(5).with_context(|| "Failed to create Brotli compression level")?,
        ))
        .build();

    let mut writer = ArrowWriter::try_new(BufWriter::new(writer_file), schema, Some(props))
        .with_context(|| {
            format!(
                "Failed to create Arrow writer for temp file {}",
                tmp.display()
            )
        })?;

    // Copy existing data from compacted file if it exists
    if compacted.exists() {
        info!(compacted_file = %compacted.display(), "Copying existing data from compacted file");
        let f = File::open(&compacted).with_context(|| {
            format!(
                "Failed to open existing compacted file for reading {}",
                compacted.display()
            )
        })?;
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(f)
            .with_context(|| {
                format!(
                    "Failed to create reader for existing compacted file {}",
                    compacted.display()
                )
            })?
            .with_batch_size(1024)
            .build()
            .with_context(|| {
                format!(
                    "Failed to build reader for existing compacted file {}",
                    compacted.display()
                )
            })?;

        let mut batch_count = 0;
        while let Some(batch) = reader.next().transpose().with_context(|| {
            format!(
                "Failed to read batch from existing compacted file {}",
                compacted.display()
            )
        })? {
            writer.write(&batch).with_context(|| {
                format!(
                    "Failed to write existing batch {} to temp file {}",
                    batch_count,
                    tmp.display()
                )
            })?;
            batch_count += 1;
        }
        info!(batches_copied = batch_count, "Copied existing batches");
    }

    // Process new input files
    let start = Utc::now();
    let mut successful_inputs = Vec::new();

    for (i, (path, key)) in new_inputs.iter().enumerate() {
        info!(
            file = %path.display(),
            progress = format!("{}/{}", i + 1, new_inputs.len()),
            "Processing input file"
        );

        let f = File::open(path)
            .with_context(|| format!("Failed to open input file {}", path.display()))?;
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(f)
            .with_context(|| format!("Failed to create reader for input file {}", path.display()))?
            .with_batch_size(1024)
            .build()
            .with_context(|| format!("Failed to build reader for input file {}", path.display()))?;

        let mut batch_count = 0;
        while let Some(batch) = reader.next().transpose().with_context(|| {
            format!(
                "Failed to read batch {} from input file {}",
                batch_count,
                path.display()
            )
        })? {
            writer.write(&batch).with_context(|| {
                format!(
                    "Failed to write batch {} from input file {} to temp file {}",
                    batch_count,
                    path.display(),
                    tmp.display()
                )
            })?;
            batch_count += 1;
        }

        debug!(
            file = %path.display(),
            batches_written = batch_count,
            "Successfully processed input file"
        );

        successful_inputs.push((path.clone(), key.clone()));
    }

    // Close the writer and ensure all data is flushed
    writer.close().with_context(|| {
        format!(
            "Failed to close Arrow writer for temp file {}",
            tmp.display()
        )
    })?;

    // Atomically replace the compacted file
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
        "Successfully wrote compacted file"
    );

    // Only record successful compactions in history AFTER everything succeeds
    let end = Utc::now();
    let part_date = extract_partition_date(partition).with_context(|| {
        format!(
            "Failed to extract partition date from {}",
            partition.display()
        )
    })?;

    for (path, key) in successful_inputs {
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
            return NaiveDate::parse_from_str(&d, "%Y-%m-%d").with_context(|| {
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
