use std::{
    collections::HashMap,
    fs::{self, File},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use num_cpus;
use parquet::{
    arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::{
        properties::WriterProperties, reader::FileReader, serialized_reader::SerializedFileReader,
    },
};
use regex::Regex;
use tracing::{debug, error, info};
use tracing_subscriber::{fmt, EnvFilter};

fn main() -> Result<()> {
    // ─── initialize tracing at DEBUG ─────────────────────────────
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("debug"))
        .init();
    info!("nemscraper_munge starting up");

    let input_dir = Path::new("./parquet_small");
    let output_dir = Path::new("./munged");
    fs::create_dir_all(output_dir)?;
    debug!(dir = %output_dir.display(), "Ensured output dir exists");

    // ─── group files by (table, YYYYMMDD) ───────────────────────────
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

    // ─── for each (table,day), concatenate into one big Parquet ─────
    for (table, by_day) in groups {
        for (ymd, files) in by_day {
            info!(table, day = %ymd, count = files.len(), "Processing group");

            // sum input sizes
            let total_input: u64 = files
                .iter()
                .map(|p| fs::metadata(p).map(|m| m.len()).unwrap_or(0))
                .sum();
            debug!(bytes = total_input, "Total input bytes");

            // infer schema from the first file
            let first = &files[0];
            let mut first_reader =
                ParquetRecordBatchReaderBuilder::try_new(File::open(first)?)?.with_batch_size(2048);
            let schema_ref = first_reader.schema().clone();

            // set up writer props
            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(22)?))
                .build();

            let out_path = output_dir.join(format!("{}—{}.parquet", ymd, table));
            let out_file = File::create(&out_path)?;
            let mut writer = ArrowWriter::try_new(out_file, schema_ref, Some(props))?;

            // read each small file and append its batches
            for path in &files {
                let mut batch_reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?
                    .with_batch_size(2048)
                    .build()?;
                while let Some(batch) = batch_reader.next() {
                    let batch: RecordBatch = batch?;
                    writer.write(&batch)?;
                }
            }

            // finalize write
            writer.close()?;
            info!(output = %out_path.display(), "Wrote compacted file");

            // inspect output metadata
            let f = File::open(&out_path)?;
            let reader = SerializedFileReader::new(f)?;
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
        }
    }

    info!("nemscraper_munge complete");
    Ok(())
}
