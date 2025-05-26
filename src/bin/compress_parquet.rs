use anyhow::Result;
use arrow::record_batch::RecordBatch;
use parquet::basic::BrotliLevel;
use parquet::file::reader::FileReader;
use parquet::{
    // <-- here’s the change:
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    basic::Compression,
    file::{properties::WriterProperties, serialized_reader::SerializedFileReader},
};
use regex::Regex;
use std::{
    collections::HashMap,
    fs::{self, File},
    path::{Path, PathBuf},
};
use tracing::{debug, error, info};
use tracing_subscriber::EnvFilter;

// <-- pull in rayon traits -->
use rayon::prelude::*;

fn main() -> Result<()> {
    // ─── init tracing ─────────────────────────────────────────
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("debug"))
        .init();
    info!("nemscraper_munge starting up");

    let input_dir = Path::new("./parquet");
    let output_dir = Path::new("./munged");
    fs::create_dir_all(output_dir)?;
    debug!(dir = %output_dir.display(), "Ensured output dir exists");

    // ─── group files by (table, YYYYMMDD) ────────────────────
    let mut groups: HashMap<String, HashMap<String, Vec<PathBuf>>> = HashMap::new();
    let date_re = Regex::new(r"(\d{8})")?;

    for entry in fs::read_dir(input_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("parquet") {
            debug!(path = %path.display(), "Skipping non-parquet");
            continue;
        }

        let fname = path.file_name().unwrap().to_string_lossy();
        let parts: Vec<&str> = fname.split('\u{2014}').collect(); // em-dash
        if parts.len() < 3 {
            error!(file = %fname, "Bad filename, skipping");
            continue;
        }
        let table = parts[1].to_string();
        let prefix = parts[0];
        let ymd = match date_re.captures(prefix).and_then(|c| c.get(1)) {
            Some(m) => m.as_str().to_string(),
            None => {
                error!(file = %fname, "No full YYYYMMDD, skipping");
                continue;
            }
        };

        groups
            .entry(table.clone())
            .or_default()
            .entry(ymd.clone())
            .or_default()
            .push(path.clone());
        debug!(file = %fname, table, ymd, "Grouped file");
    }

    // ─── flatten into a Vec of tasks ─────────────────────────
    let tasks: Vec<(String, String, Vec<PathBuf>)> = groups
        .into_iter()
        .flat_map(|(table, by_day)| {
            by_day
                .into_iter()
                .map(move |(ymd, files)| (table.clone(), ymd, files))
        })
        .collect();

    let n_threads = 8;
    info!("Processing {} groups on {} threads", tasks.len(), n_threads);

    // ─── run tasks in parallel with a fixed thread-pool ───────
    rayon::ThreadPoolBuilder::new()
        .num_threads(n_threads)
        .build()?
        .install(|| {
            tasks.into_par_iter().for_each(|(table, ymd, files)| {
                info!(table, day = %ymd, count = files.len(), "Processing group");

                // sum input sizes
                let total_input: u64 = files
                    .iter()
                    .map(|p| fs::metadata(p).map(|m| m.len()).unwrap_or(0))
                    .sum();
                debug!(bytes = total_input, "Total input bytes");

                // infer schema
                let first = &files[0];
                let first_reader = ParquetRecordBatchReaderBuilder::try_new(
                    File::open(first).expect("open first parquet"),
                )
                .expect("build reader")
                .with_batch_size(2048);
                let schema_ref = first_reader.schema().clone();

                // writer props
                let props = WriterProperties::builder()
                    .set_compression(Compression::BROTLI(BrotliLevel::try_new(5).unwrap()))
                    .build();

                let out_path = output_dir.join(format!("{}—{}.parquet", ymd, table));
                let out_file = File::create(&out_path).expect("create output file");
                let mut writer =
                    ArrowWriter::try_new(out_file, schema_ref, Some(props)).expect("create writer");

                // concat all batches
                for path in &files {
                    let mut batch_reader = ParquetRecordBatchReaderBuilder::try_new(
                        File::open(path).expect("open part file"),
                    )
                    .expect("build batch reader")
                    .with_batch_size(2048)
                    .build()
                    .expect("finalize reader");

                    while let Some(batch) = batch_reader.next() {
                        let batch: RecordBatch = batch.expect("read batch");
                        writer.write(&batch).expect("write batch");
                    }
                }

                writer.close().expect("close writer");
                info!(output = %out_path.display(), "Wrote compacted file");

                // log row-group stats
                let f = File::open(&out_path).expect("open final parquet");
                let reader = SerializedFileReader::new(f).expect("new serialized reader");
                let meta = reader.metadata();
                let mut stats: HashMap<String, (i64, i64)> = HashMap::new();
                for i in 0..meta.num_row_groups() {
                    let rg = meta.row_group(i);
                    for j in 0..rg.num_columns() {
                        let col_md = rg.column(j);
                        let codec = format!("{:?}", col_md.compression());
                        let entry = stats.entry(codec).or_insert((0, 0));
                        entry.0 += col_md.compressed_size() as i64;
                        entry.1 += col_md.uncompressed_size() as i64;
                    }
                }
                for (codec, (comp, uncomp)) in stats {
                    info!(
                        codec,
                        comp_bytes = comp,
                        uncomp_bytes = uncomp,
                        "Parquet metadata"
                    );
                }
            });
        });

    info!("nemscraper_munge complete");
    Ok(())
}
