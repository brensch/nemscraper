use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use nemscraper::history::compacted::InputCompactionRow;
use nemscraper::history::table_history::TableHistory;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::sync::Arc;
use std::{
    env,
    fs::{self, File},
    path::{Path, PathBuf},
};
use walkdir::WalkDir;

fn main() -> Result<()> {
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

    // Initialize history tracking for input file compactions
    let hist: Arc<TableHistory<InputCompactionRow>> =
        TableHistory::new(meta_root, "compacted_inputs")?;

    // discover partitions under input
    let mut parts = discover_partitions(&input_root)?;
    parts.sort();
    parts.dedup();

    for partition in parts {
        compact_partition(&input_root, &output_root, &partition, &hist)?;
    }

    Ok(())
}

fn discover_partitions(root: &Path) -> Result<Vec<PathBuf>> {
    let mut parts = Vec::new();
    for entry in WalkDir::new(root).into_iter().filter_map(Result::ok) {
        let p = entry.path();
        if p.extension().and_then(|s| s.to_str()) == Some("parquet") {
            if let Some(parent) = p.parent() {
                let rel = parent
                    .strip_prefix(root)
                    .context("strip_prefix failed")?
                    .to_path_buf();
                parts.push(rel);
            }
        }
    }
    Ok(parts)
}

fn compact_partition(
    input_root: &Path,
    output_root: &Path,
    partition: &Path,
    hist: &TableHistory<InputCompactionRow>,
) -> Result<()> {
    let in_dir = input_root.join(partition);
    let out_dir = output_root.join(partition);

    // Collect files that haven't yet been compacted
    let mut to_compact: Vec<(PathBuf, String)> = Vec::new();
    if in_dir.exists() {
        for entry in fs::read_dir(&in_dir)? {
            let p = entry?.path();
            if p.extension().and_then(|s| s.to_str()) == Some("parquet") {
                let rel = p.strip_prefix(input_root)?;
                let key = rel.to_string_lossy().to_string();
                if !hist.get(key.clone()) {
                    to_compact.push((p, key));
                }
            }
        }
    }

    if to_compact.is_empty() {
        println!("→ nothing to compact in {:?}", partition);
        return Ok(());
    }

    // snapshot count and start time
    let compaction_count = to_compact.len();
    let start_time = Utc::now();

    // Also include any existing output to avoid data loss
    let mut files = Vec::new();
    for (ref p, _) in &to_compact {
        files.push(p.clone());
    }
    if out_dir.exists() {
        for entry in fs::read_dir(&out_dir)? {
            let p = entry?.path();
            if p.extension().and_then(|s| s.to_str()) == Some("parquet") {
                files.push(p);
            }
        }
    }

    // Read and merge
    let batches = read_parquet_files(&files)?;
    if batches.is_empty() {
        println!("→ no data for {:?}", partition);
        return Ok(());
    }

    // Write consolidated output
    fs::create_dir_all(&out_dir)?;
    let out_file = out_dir.join("compacted.parquet");
    let file = File::create(&out_file)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, batches[0].schema(), Some(props))?;
    for batch in &batches {
        writer.write(batch)?;
    }
    writer.close()?;

    // Record each input file compaction with start/end
    let end_time = Utc::now();
    let thread_id = 0;
    for (_path, key) in to_compact {
        let row = InputCompactionRow {
            input_file: key,
            partition: end_time.date_naive(),
            compaction_start: start_time,
            compaction_end: end_time,
            thread: thread_id,
        };
        hist.add(&row)?;
    }

    println!("✔ compacted {:?} ({} files)", partition, compaction_count);
    Ok(())
}

fn read_parquet_files(paths: &[PathBuf]) -> Result<Vec<RecordBatch>> {
    let mut all = Vec::new();
    for path in paths {
        let file = File::open(path)?;
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
        while let Some(batch) = reader.next().transpose()? {
            all.push(batch);
        }
    }
    Ok(all)
}
