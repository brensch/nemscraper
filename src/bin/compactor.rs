use anyhow::{Context, Result};
use arrow_array::RecordBatchReader;
use chrono::{DateTime, NaiveDate, Utc};
use nemscraper::history::compacted::InputCompactionRow;
use nemscraper::history::table_history::{HistoryRow, TableHistory};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{
    env,
    fs::{self, File},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};
use tracing::{debug, error, info, instrument, warn};
use tracing_subscriber;
use walkdir::WalkDir;

static COMPACTION_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

#[instrument]
fn main() -> Result<()> {
    // initialize tracing subscriber
    tracing_subscriber::fmt().with_env_filter("info").init();

    info!("Starting continuous compactor (5-minute intervals)");

    // usage: compactor <INPUT_DIR> <OUTPUT_DIR> <METADATA_DIR>
    let mut args = env::args().skip(1);
    let input_root = PathBuf::from(
        args.next()
            .expect("Usage: compactor <INPUT_DIR> <OUTPUT_DIR> <METADATA_DIR>"),
    );
    let output_root = PathBuf::from(
        args.next()
            .expect("Usage: compactor <INPUT_DIR> <OUTPUT_DIR> <METADATA_DIR>"),
    );
    let meta_root = PathBuf::from(
        args.next()
            .expect("Usage: compactor <INPUT_DIR> <OUTPUT_DIR> <METADATA_DIR>"),
    );

    // ensure roots exist
    info!(
        input = input_root.to_str(),
        output = output_root.to_str(),
        meta = meta_root.to_str(),
        "Paths set"
    );
    if !input_root.exists() {
        error!(root = input_root.to_str(), "Input directory does not exist");
        anyhow::bail!("Input directory not found: {}", input_root.display());
    }

    // initialize history tracker once
    let hist = TableHistory::new_compacted(meta_root.clone())?;
    hist.vacuum()?;
    info!(metadata_dir=%meta_root.display(), "Initialized history tracker");

    // main loop - run every 5 minutes
    loop {
        let start_time = Utc::now();

        // check if previous compaction is still running
        if COMPACTION_IN_PROGRESS.load(Ordering::Acquire) {
            warn!("Previous compaction still in progress, skipping this cycle");
        } else {
            // set flag to indicate compaction started
            COMPACTION_IN_PROGRESS.store(true, Ordering::Release);

            info!("Starting compaction cycle");

            // run compaction
            match run_compaction_cycle(&input_root, &output_root, &hist) {
                Ok(()) => {
                    let duration = Utc::now() - start_time;
                    info!(
                        duration_ms = duration.num_milliseconds(),
                        "Compaction cycle completed successfully"
                    );
                }
                Err(e) => {
                    let duration = Utc::now() - start_time;
                    error!(
                        duration_ms = duration.num_milliseconds(),
                        error = ?e,
                        "Compaction cycle failed"
                    );
                }
            }

            // clear flag to indicate compaction finished
            COMPACTION_IN_PROGRESS.store(false, Ordering::Release);
        }

        info!("Sleeping for 5 minutes until next cycle...");
        thread::sleep(Duration::from_secs(300)); // 5 minutes
    }
}

#[instrument(skip(hist))]
fn run_compaction_cycle(
    input_root: &Path,
    output_root: &Path,
    hist: &TableHistory<InputCompactionRow>,
) -> Result<()> {
    // discover partitions
    let mut parts = discover_partitions(input_root)?;
    parts.sort();
    parts.dedup();
    info!(count = parts.len(), "Discovered partitions");

    if parts.is_empty() {
        info!("No partitions found, nothing to compact");
        return Ok(());
    }

    let mut success_count = 0;
    let mut error_count = 0;

    for partition in parts {
        match compact_partition(input_root, output_root, &partition, hist) {
            Ok(()) => {
                success_count += 1;
                debug!(?partition, "Partition compacted successfully");
            }
            Err(e) => {
                error_count += 1;
                error!(partition=?partition, error=?e, "Partition compaction failed");
            }
        }
    }

    info!(success_count, error_count, "Compaction cycle finished");

    // run vacuum after compaction cycle
    if let Err(e) = hist.vacuum() {
        warn!(error=?e, "History vacuum failed");
    }

    Ok(())
}

#[instrument(skip(root))]
fn discover_partitions(root: &Path) -> Result<Vec<PathBuf>> {
    let mut parts = Vec::new();
    for entry in WalkDir::new(root).into_iter().filter_map(Result::ok) {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
            if let Some(parent) = path.parent() {
                let rel = parent
                    .strip_prefix(root)
                    .context("strip_prefix failed")?
                    .to_path_buf();
                parts.push(rel.clone());
                debug!(relative_path = rel.to_str(), "Found partition");
            }
        }
    }
    Ok(parts)
}

#[instrument(skip(hist))]
fn compact_partition(
    input_root: &Path,
    output_root: &Path,
    partition: &Path,
    hist: &TableHistory<InputCompactionRow>,
) -> Result<()> {
    info!(?partition, "Compacting partition");
    let in_dir = input_root.join(partition);
    debug!(input_dir=%in_dir.display(), "Input partition directory");

    let out_dir = output_root.join(partition);
    debug!(output_dir=%out_dir.display(), "Output partition directory");

    // gather new input files
    let mut to_compact = Vec::new();
    if in_dir.exists() {
        debug!("Listing directory {:?}", in_dir.display());
        for entry in fs::read_dir(&in_dir)
            .with_context(|| format!("Reading directory {}", in_dir.display()))?
        {
            let p = entry?.path();
            if p.extension().and_then(|s| s.to_str()) == Some("parquet") {
                let rel = p
                    .strip_prefix(input_root)
                    .with_context(|| format!("Strip prefix for input file {}", p.display()))?;
                let key = rel.to_string_lossy().to_string();
                let sanitized_key = key.replace('/', "_").replace('\\', "_"); // Replace path separators
                if !hist.get(sanitized_key.clone()) {
                    debug!(file=%p.display(), key, "Queueing for compaction");
                    to_compact.push((p.clone(), key));
                }
            }
        }
    } else {
        info!(
            "Input directory {} does not exist, skipping",
            in_dir.display()
        );
    }

    if to_compact.is_empty() {
        info!(?partition, "No new files to compact");
        return Ok(());
    }

    let compaction_count = to_compact.len();
    let start_time = Utc::now();
    info!(count = compaction_count, "Starting compaction run");

    // prepare writer with schema from first file
    fs::create_dir_all(&out_dir)
        .with_context(|| format!("Creating output directory {}", out_dir.display()))?;
    let out_file = out_dir.join("compacted.parquet");
    debug!(outfile=%out_file.display(), "Creating writer");
    let writer_file = File::create(&out_file)
        .with_context(|| format!("Creating output file {}", out_file.display()))?;

    let props = WriterProperties::builder().build();
    let first_path = &to_compact[0].0;
    debug!(first_input=%first_path.display(), "Opening first input to infer schema");
    let mut reader0 = ParquetRecordBatchReaderBuilder::try_new(
        File::open(&first_path)
            .with_context(|| format!("Opening first input file {}", first_path.display()))?,
    )?
    .with_batch_size(1024)
    .build()?;
    let schema = reader0.schema();
    let mut writer = ArrowWriter::try_new(writer_file, schema, Some(props))?;

    // stream inputs
    for (path, _) in &to_compact {
        debug!("Opening input file {}", path.display());
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(
            File::open(path).with_context(|| format!("Opening input file {}", path.display()))?,
        )?
        .with_batch_size(1024)
        .build()?;
        while let Some(batch) = reader.next().transpose()? {
            writer
                .write(&batch)
                .with_context(|| format!("Writing batch from {}", path.display()))?;
        }
    }

    // stream existing outputs (except compacted)
    if out_dir.exists() {
        for entry in fs::read_dir(&out_dir)
            .with_context(|| format!("Reading output directory {}", out_dir.display()))?
        {
            let p = entry?.path();
            if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
                if name != "compacted.parquet"
                    && p.extension().and_then(|s| s.to_str()) == Some("parquet")
                {
                    debug!("Opening existing output file {}", p.display());
                    let mut reader = ParquetRecordBatchReaderBuilder::try_new(
                        File::open(&p)
                            .with_context(|| format!("Opening output file {}", p.display()))?,
                    )?
                    .with_batch_size(1024)
                    .build()?;
                    while let Some(batch) = reader.next().transpose()? {
                        writer
                            .write(&batch)
                            .with_context(|| format!("Writing batch from {}", p.display()))?;
                    }
                }
            }
        }
    }

    writer.close().context("Closing Parquet writer")?;
    info!(outfile=%out_file.display(), "Finished writing compacted.parquet");

    // record history events
    let end_time = Utc::now();
    let partition_date = extract_partition_date(partition)?; // <-- Parse from path

    for (_, key) in to_compact {
        let sanitized_key = key.replace('/', "_").replace('\\', "_"); // Replace path separators
        let row = InputCompactionRow {
            input_file: sanitized_key.clone(),
            partition: partition_date,
            compaction_start: start_time,
            compaction_end: end_time,
            thread: 0,
        };
        hist.add(&row)
            .with_context(|| format!("Recording history for {}", key))?;
        debug!(key, "Recorded history event");
    }

    info!(count = compaction_count, ?partition, "Compaction complete");
    Ok(())
}

// Extract partition date from the path like "FPP---CONTRIBUTION_FACTOR---1/date=2025-04-28"
fn extract_partition_date(partition_path: &Path) -> Result<NaiveDate> {
    let path_str = partition_path.to_string_lossy();

    // Look for "date=YYYY-MM-DD" pattern
    if let Some(date_part) = path_str.split('/').find(|part| part.starts_with("date=")) {
        let date_str = date_part.strip_prefix("date=").unwrap();
        NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
            .with_context(|| format!("Failed to parse date from {}", date_str))
    } else {
        anyhow::bail!("No date found in partition path: {}", path_str);
    }
}
