mod raw_table;
pub use raw_table::RawTable;

use anyhow::{Context, Result};
use std::{
    fs::{self, File},
    io::Cursor,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};
use tracing::{info, instrument};
use zip::ZipArchive;

use arrow::csv::ReaderBuilder;
use arrow::{
    array::ArrayRef,
    datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use num_cpus;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use rayon::prelude::*;

/// Split a single “I…D…D…” segment into row‐chunks and write each as its own Parquet file.
/// - `name`: original CSV file name
/// - `schema_id`: the segment’s schema identifier
/// - `header_slice`: the raw “I,…\n” header line
/// - `data_slice`: everything between that header and the next I- or C-line
/// - `out_dir`: where to write `name—schema_id—chunkN.parquet`
fn chunk_and_write_segment(
    name: &str,
    schema_id: &str,
    header_slice: &str,
    data_slice: &str,
    out_dir: &Path,
) {
    // 1) parse column names once
    let cols: Vec<String> = header_slice
        .lines()
        .next()
        .unwrap()
        .split(',')
        .map(str::to_string)
        .collect();

    // 2) build the Arrow schema
    let arrow_schema = Arc::new(ArrowSchema::new(
        cols.iter()
            .map(|c| ArrowField::new(c, ArrowDataType::Utf8, true))
            .collect::<Vec<ArrowField>>(),
    ));

    // 3) choose your chunk size
    let chunk_size = 1_000_000;

    // 4) view each row as a &str
    let lines: Vec<&str> = data_slice.lines().collect();
    let total_rows = lines.len();
    let n_chunks = (total_rows + chunk_size - 1) / chunk_size;

    info!(
        segment = %schema_id,
        rows = total_rows,
        chunks = n_chunks,
        "splitting segment into {}-row chunks",
        chunk_size
    );

    // 5) parallelize per-chunk
    lines
        .chunks(chunk_size)
        .into_iter()
        .enumerate()
        .for_each(|(chunk_idx, chunk_lines)| {
            // reassemble a mini-CSV
            let chunk_body = chunk_lines.join("\n");
            let cursor = Cursor::new(chunk_body.as_bytes());

            // fresh CSV reader for this chunk
            let mut csv_reader = ReaderBuilder::new(arrow_schema.clone())
                .with_header(false)
                .build(cursor)
                .unwrap();

            // open per-chunk Parquet file
            let out_file =
                out_dir.join(format!("{}—{}—chunk{}.parquet", name, schema_id, chunk_idx));
            let file = File::create(&out_file).unwrap();
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();
            let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();

            // write all record batches
            while let Some(batch) = csv_reader.next().transpose().unwrap() {
                writer.write(&batch).unwrap();
            }
            writer.close().unwrap();

            info!(
                segment = %schema_id,
                chunk = chunk_idx,
                "wrote chunk {} ({} rows)",
                chunk_idx,
                chunk_lines.len()
            );
        });
}

#[instrument(level = "info", skip(zip_path, out_dir), fields(zip = %zip_path.as_ref().display()))]
pub fn split_zip_to_parquet<P: AsRef<Path>, Q: AsRef<Path>>(zip_path: P, out_dir: Q) -> Result<()> {
    // Build a global Rayon pool on first call (ignore error if already built)
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_cpus::get())
        .build_global()
        .ok();

    let start_total = Instant::now();
    info!("starting parallel split_zip_to_parquet");

    let zip_path_buf: PathBuf = zip_path.as_ref().to_path_buf();
    let out_dir_buf: PathBuf = out_dir.as_ref().to_path_buf();

    fs::create_dir_all(&out_dir_buf)?;

    let file =
        File::open(&zip_path_buf).with_context(|| format!("opening ZIP {:?}", zip_path_buf))?;
    let mut archive =
        ZipArchive::new(file).with_context(|| format!("reading ZIP {:?}", zip_path_buf))?;

    // Iterate each CSV in the ZIP
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        let name = entry.name().to_string();
        if !name.to_lowercase().ends_with(".csv") {
            continue;
        }

        // Read entire CSV into memory
        let mut buf = Vec::with_capacity(entry.size() as usize);
        entry.read_to_end(&mut buf)?;
        let text = Arc::new(String::from_utf8_lossy(&buf).to_string());

        // Identify I…D… segments
        let segments: Vec<(String, usize, usize, usize)> = {
            let mut segs = Vec::new();
            let mut pos = 0;
            let mut header_start = 0;
            let mut data_start = 0;
            let mut current_schema: Option<String> = None;
            let mut seen_footer = false;

            for chunk in text.split_inclusive('\n') {
                if chunk.starts_with("C,") {
                    if seen_footer {
                        break;
                    }
                    seen_footer = true;
                    pos += chunk.len();
                    continue;
                }
                if chunk.starts_with("I,") {
                    if let Some(schema_id) = current_schema.take() {
                        segs.push((schema_id, header_start, data_start, pos));
                    }
                    header_start = pos;
                    let parts: Vec<&str> = chunk.trim_end_matches('\n').split(',').collect();
                    current_schema = Some(format!("{}_{}", parts[1], parts[2]));
                    data_start = pos + chunk.len();
                }
                pos += chunk.len();
            }
            if let Some(schema_id) = current_schema {
                segs.push((schema_id, header_start, data_start, pos));
            }
            segs
        };

        // Process each segment in parallel, chunking rows internally
        segments
            .into_par_iter()
            .for_each(|(schema_id, hs, ds, es)| {
                let t0 = Instant::now();
                info!(segment = %schema_id, "processing segment");
                let header_slice = &text[hs..ds];
                let data_slice = &text[ds..es];
                chunk_and_write_segment(&name, &schema_id, header_slice, data_slice, &out_dir_buf);
                info!(
                    segment = %schema_id,
                    "segment total time {:?}",
                    t0.elapsed()
                );
            });
    }

    info!(
        "parallel split_zip_to_parquet total time {:?}",
        start_total.elapsed()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::{env, fs, io::Write};
    use tempfile::NamedTempFile;
    use tracing_subscriber::{EnvFilter, FmtSubscriber};
    use zip::write::{ExtendedFileOptions, FileOptions};
    use zip::CompressionMethod;

    #[test]
    fn test_split_zip_to_parquet() -> Result<()> {
        let subscriber = FmtSubscriber::builder()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| EnvFilter::new("info,nemscraper::history=trace")),
            )
            .with_test_writer()
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);

        // prepare sample ZIP...
        let zip_path = if let Ok(path) = env::var("ZIP_PATH") {
            PathBuf::from(path)
        } else {
            let sample = r#"C,SETP.WORLD,FPP_DCF,AEMO,PUBLIC,\"2024/12/14\",...\nI,FPP,FORECAST_DEFAULT_CF,1,UID,CON,ST_DT,EN_DT,VER,BID,REG,DCF,FLAG,TOTAL,UID\nD,FPP,FORECAST_DEFAULT_CF,1,X,Y,2024/12/22 00:00,2024/12/23 00:00,1,A,B,0.1,0,5,X\nD,FPP,FORECAST_DEFAULT_CF,1,X,Y,2024/12/22 01:00,2024/12/23 01:00,1,A,B,0.2,0,6,X\nI,FPP,FORECAST_RESIDUAL_DCF,1,CON,ST_DT,EN_DT,VER,BID,RES,FLAG,TOTAL\nD,FPP,FORECAST_RESIDUAL_DCF,1,Y,2024/12/22 00:00,2024/12/23 00:00,1,A,0.3,0,7\nD,FPP,FORECAST_RESIDUAL_DCF,1,Y,2024/12/22 01:00,2024/12/23 01:00,1,A,0.4,0,8"#;
            let mut buf = Vec::new();
            {
                let mut zip = zip::ZipWriter::new(Cursor::new(&mut buf));
                let options = FileOptions::<ExtendedFileOptions>::default()
                    .compression_method(CompressionMethod::Stored);
                zip.start_file("aemo.csv", options)?;
                zip.write_all(sample.as_bytes())?;
                zip.finish()?;
            }
            let tmp = NamedTempFile::new()?;
            tmp.reopen()?.write_all(&buf)?;
            tmp.path().to_path_buf()
        };

        let out_dir = PathBuf::from("tests/output");
        if !out_dir.exists() {
            fs::create_dir_all(&out_dir)?;
        }

        split_zip_to_parquet(&zip_path, &out_dir)?;

        let files: Vec<_> = fs::read_dir(&out_dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("parquet"))
            .collect();
        assert!(!files.is_empty(), "no parquet files produced");
        Ok(())
    }
}
