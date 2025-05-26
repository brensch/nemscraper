use anyhow::Result;
use arrow::record_batch::RecordBatch;
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel},
    file::{properties::WriterProperties, reader::SerializedFileReader},
};
use regex::Regex;
use std::{
    collections::HashMap,
    fs::{self, File},
    path::{Path, PathBuf},
    time::Instant,
};
use tracing::{debug, info};
use tracing_subscriber::{fmt, EnvFilter};

struct Report {
    group: String,
    algorithm: String,
    level: String,
    compressed_size: u64,
    time_ms: u128,
}

fn main() -> Result<()> {
    // ─── init tracing ─────────────────────────────────────────
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("debug"))
        .init();
    info!("nemscraper_munge benchmarking start");

    let input_dir = Path::new("./parquet");
    let output_dir = Path::new("./munged_bench");
    fs::create_dir_all(output_dir)?;

    // ─── group files by (table, YYYYMMDD) ────────────────────
    let mut temp_map: HashMap<String, HashMap<String, Vec<PathBuf>>> = HashMap::new();
    let date_re = Regex::new(r"(\d{8})")?;

    for entry in fs::read_dir(input_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("parquet") {
            continue;
        }
        let fname = path.file_name().unwrap().to_string_lossy();
        let parts: Vec<&str> = fname.split('\u{2014}').collect();
        if parts.len() < 3 {
            continue;
        }
        let table = parts[1].to_string();
        let prefix = parts[0];
        let ymd = match date_re.captures(prefix).and_then(|c| c.get(1)) {
            Some(m) => m.as_str().to_string(),
            None => continue,
        };
        temp_map
            .entry(table.clone())
            .or_default()
            .entry(ymd.clone())
            .or_default()
            .push(path.clone());
    }

    // ─── flatten and compute total input sizes ────────────────
    let mut groups: Vec<(String, String, Vec<PathBuf>, u64)> = Vec::new();
    for (table, by_day) in temp_map {
        for (ymd, files) in by_day {
            let total_input: u64 = files
                .iter()
                .map(|p| fs::metadata(p).map(|m| m.len()).unwrap_or(0))
                .sum();
            groups.push((table.clone(), ymd.clone(), files, total_input));
        }
    }

    // ─── find smallest and largest ───────────────────────────
    if groups.is_empty() {
        info!("No parquet groups found, exiting");
        return Ok(());
    }
    groups.sort_by_key(|(_, _, _, size)| *size);
    let smallest = &groups[0];
    let largest = &groups[groups.len() - 1];

    let tasks = vec![("smallest", smallest), ("largest", largest)];

    // ─── prepare compressors ─────────────────────────────────
    let compressors = vec![
        ("UNCOMPRESSED", Compression::UNCOMPRESSED, "0"),
        ("SNAPPY", Compression::SNAPPY, "-"),
        (
            "GZIP",
            Compression::GZIP(GzipLevel::try_new(9).unwrap()),
            "9",
        ),
        (
            "GZIP",
            Compression::GZIP(GzipLevel::try_new(1).unwrap()),
            "1",
        ),
        (
            "BROTLI",
            Compression::BROTLI(BrotliLevel::try_new(5).unwrap()),
            "5",
        ),
        (
            "BROTLI",
            Compression::BROTLI(BrotliLevel::try_new(5).unwrap()),
            "11",
        ),
        ("LZ4", Compression::LZ4, "-"),
        (
            "ZSTD",
            Compression::ZSTD(ZstdLevel::try_new(1).unwrap()),
            "1",
        ),
        (
            "ZSTD",
            Compression::ZSTD(ZstdLevel::try_new(10).unwrap()),
            "10",
        ),
        (
            "ZSTD",
            Compression::ZSTD(ZstdLevel::try_new(22).unwrap()),
            "15",
        ),
    ];

    let mut reports: Vec<Report> = Vec::new();

    for (label, (table, ymd, files, _)) in tasks {
        info!(group = label, table = %table, day = %ymd, "Benchmarking group");

        // infer schema once
        let first = &files[0];
        let mut first_reader =
            ParquetRecordBatchReaderBuilder::try_new(File::open(first)?)?.with_batch_size(1024);
        let schema = first_reader.schema().clone();

        for (algo_name, compression, level) in &compressors {
            info!(algorithm = algo_name, level = level, "Starting compression");
            let start = Instant::now();

            // writer props
            let props = WriterProperties::builder()
                .set_compression(*compression)
                .build();

            let out_fname = format!("{}_{}—{}—lvl{}.parquet", ymd, table, algo_name, level);
            let out_path = output_dir.join(out_fname);
            let out_file = File::create(&out_path)?;
            let mut writer = ArrowWriter::try_new(out_file, schema.clone(), Some(props))?;

            // concat all batches
            for p in files {
                let mut batch_reader = ParquetRecordBatchReaderBuilder::try_new(File::open(p)?)?
                    .with_batch_size(1024)
                    .build()?;
                while let Some(batch) = batch_reader.next() {
                    let batch: RecordBatch = batch?;
                    writer.write(&batch)?;
                }
            }
            writer.close()?;

            let duration = start.elapsed();
            let size = fs::metadata(&out_path)?.len();
            reports.push(Report {
                group: label.to_string(),
                algorithm: algo_name.to_string(),
                level: level.to_string(),
                compressed_size: size,
                time_ms: duration.as_millis(),
            });

            info!(
                algorithm = algo_name,
                time_ms = duration.as_millis(),
                size,
                "Completed compression"
            );
        }
    }

    // ─── print results ────────────────────────────────────────
    println!(
        "{:<10} | {:<12} | {:<5} | {:<15} | {:<10}",
        "Group", "Algorithm", "Level", "CompressedSize", "TimeMs"
    );
    println!("{:-<65}", "");
    for r in &reports {
        println!(
            "{:<10} | {:<12} | {:<5} | {:<15} | {:<10}",
            r.group, r.algorithm, r.level, r.compressed_size, r.time_ms
        );
    }

    info!("nemscraper_munge benchmarking complete");
    Ok(())
}
