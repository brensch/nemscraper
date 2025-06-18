use anyhow::{Context, Result};
use arrow_array::RecordBatchReader;
use chrono::{DateTime, NaiveDate, Utc};
use nemscraper::history::compacted::InputCompactionRow;
use nemscraper::history::table_history::{HistoryRow, TableHistory};
use once_cell::sync::Lazy;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
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
use tracing::{debug, error, info, instrument};
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

    let hist = TableHistory::new_compacted(&meta_root)?;
    hist.vacuum()?;

    // determine worker count automatically
    let num_workers = std::thread::available_parallelism().map_or(1, |n| n.get());
    info!(workers = num_workers, "Using parallelism");

    rayon::ThreadPoolBuilder::new()
        .num_threads(num_workers)
        .build_global()?;

    loop {
        let cycle_start = Utc::now();
        info!(timestamp = %cycle_start.to_rfc3339(), "Beginning compaction cycle");

        // discover partitions
        let mut partitions = discover_partitions(&input_root)?;
        partitions.sort();
        partitions.dedup();

        rayon::scope(|s| {
            for partition in partitions {
                let input_root = input_root.clone();
                let output_root = output_root.clone();
                let hist = Arc::clone(&hist);

                s.spawn(move |_| {
                    if let Err(e) = compact_partition(&input_root, &output_root, &partition, &hist)
                    {
                        error!(?partition, error=?e, "Partition compaction failed");
                    }
                });
            }
        });

        if let Err(e) = hist.vacuum() {
            error!(error=?e, "History vacuum failed");
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
                    .context("strip_prefix failed")?
                    .to_path_buf();
                parts.push(rel);
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
    hist: &Arc<TableHistory<InputCompactionRow>>,
) -> Result<()> {
    let partition_key = partition.to_string_lossy().to_string();
    let lock_arc = {
        let mut map = PARTITION_LOCKS.lock().unwrap();
        map.entry(partition_key.clone())
            .or_insert_with(|| Arc::new(StdMutex::new(())))
            .clone()
    };

    let guard = match lock_arc.try_lock() {
        Ok(g) => g,
        Err(_) => {
            info!(?partition, "Skip; compaction already in progress");
            return Ok(());
        }
    };

    info!(?partition, "Scanning inputs");
    let in_dir = input_root.join(partition);
    let out_dir = output_root.join(partition);
    fs::create_dir_all(&out_dir)?;

    let mut new_inputs = Vec::new();
    if in_dir.exists() {
        for entry in fs::read_dir(&in_dir)? {
            let p = entry?.path();
            if p.extension().and_then(|s| s.to_str()) == Some("parquet") {
                let rel = p.strip_prefix(input_root)?;
                let key = rel.to_string_lossy().to_string();
                let sanitized = key.replace('/', "_").replace('\\', "_");
                if !hist.get(sanitized.clone()) {
                    new_inputs.push((p.clone(), sanitized));
                }
            }
        }
    }

    if new_inputs.is_empty() {
        drop(guard);
        return Ok(());
    }

    let compacted = out_dir.join("compacted.parquet");
    let tmp = out_dir.join("compacted.parquet.tmp");
    let writer_file = File::create(&tmp)?;
    let props = WriterProperties::builder().build();

    // infer schema
    let schema = if compacted.exists() {
        let f = File::open(&compacted)?;
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(f)?
            .with_batch_size(1024)
            .build()?;
        reader.schema()
    } else {
        let first = &new_inputs[0].0;
        let f = File::open(first)?;
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(f)?
            .with_batch_size(1024)
            .build()?;
        reader.schema()
    };

    let mut writer = ArrowWriter::try_new(BufWriter::new(writer_file), schema, Some(props))?;

    if compacted.exists() {
        let f = File::open(&compacted)?;
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(f)?
            .with_batch_size(1024)
            .build()?;
        while let Some(batch) = reader.next().transpose()? {
            writer.write(&batch)?;
        }
    }

    let start = Utc::now();
    for (path, key) in &new_inputs {
        debug!(file=%path.display(), "Adding input");
        let f = File::open(path)?;
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(f)?
            .with_batch_size(1024)
            .build()?;
        while let Some(batch) = reader.next().transpose()? {
            writer.write(&batch)?;
        }
        let end = Utc::now();
        let part_date = extract_partition_date(partition)?;
        let row = InputCompactionRow {
            input_file: key.clone(),
            partition: part_date,
            compaction_start: start,
            compaction_end: end,
            thread: 0,
        };
        hist.add(&row)?;
    }

    writer.close()?;
    fs::rename(&tmp, &compacted)?;
    info!(outfile=%compacted.display(), "Wrote compacted");

    drop(guard);
    Ok(())
}

fn extract_partition_date(part: &Path) -> Result<NaiveDate> {
    for comp in part.iter().map(|c| c.to_string_lossy()) {
        if let Some(d) = comp.strip_prefix("date=") {
            return NaiveDate::parse_from_str(&d, "%Y-%m-%d").context("Invalid date");
        }
    }
    Err(anyhow::anyhow!("No date=YYYY-MM-DD in {:?}", part))
}
