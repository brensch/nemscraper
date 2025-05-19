// Declare submodules and re-export RawTable
mod raw_table;
pub use raw_table::RawTable;

use anyhow::{Context, Result};
use futures::stream::{FuturesUnordered, StreamExt};
use std::{
    fs::{self, File},
    io::{Cursor, Read},
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};
use tokio::task;
use tracing::{info, instrument};
use zip::ZipArchive;

use arrow::csv::ReaderBuilder;
use arrow::{
    array::ArrayRef,
    datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

#[instrument(level = "info", skip(zip_path, out_dir), fields(zip = %zip_path.as_ref().display()))]
pub async fn split_zip_to_parquet_async<P: AsRef<Path>, Q: AsRef<Path>>(
    zip_path: P,
    out_dir: Q,
) -> Result<()> {
    let start_total = Instant::now();
    info!("starting async split_zip_to_parquet");

    let zip_path_buf: PathBuf = zip_path.as_ref().to_path_buf();
    let out_dir_buf: PathBuf = out_dir.as_ref().to_path_buf();

    // Ensure output directory exists
    let t0 = Instant::now();
    fs::create_dir_all(&out_dir_buf)?;
    info!("created output directory in {:?}", t0.elapsed());

    // Open ZIP archive
    let t1 = Instant::now();
    let file =
        File::open(&zip_path_buf).with_context(|| format!("opening ZIP {:?}", zip_path_buf))?;
    let mut archive =
        ZipArchive::new(file).with_context(|| format!("reading ZIP {:?}", zip_path_buf))?;
    info!("opened and read ZIP archive in {:?}", t1.elapsed());

    // Iterate entries
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        let name = entry.name().to_string();
        if !name.to_lowercase().ends_with(".csv") {
            continue;
        }

        // Read entire CSV entry
        let t_read = Instant::now();
        let mut buf = Vec::with_capacity(entry.size() as usize);
        entry.read_to_end(&mut buf)?;
        let text = String::from_utf8_lossy(&buf).to_string();
        info!(
            index = i,
            bytes = entry.size(),
            "read entry in {:?}",
            t_read.elapsed()
        );

        // Identify segments at I-row boundaries, capturing header_start and data_start
        let t_seg = Instant::now();
        let mut segments: Vec<(String, usize, usize, usize)> = Vec::new();
        let mut pos: usize = 0;
        let mut current_schema: Option<String> = None;
        let mut header_start: usize = 0;
        let mut data_start: usize = 0;

        for line in text.lines() {
            let len = line.len() + 1;

            // only break on the final C-row (END OF REPORT), not the initial metadata C-row
            if line.starts_with("C,\"END") {
                break;
            }

            if line.starts_with("I,") {
                // close out previous segment
                if let Some(schema_id) = current_schema.take() {
                    segments.push((schema_id, header_start, data_start, pos));
                }
                // mark new segment start
                header_start = pos;
                let parts: Vec<&str> = line.split(',').collect();
                let schema_id = format!("{}_{}", parts[1], parts[2]);
                current_schema = Some(schema_id);
                data_start = pos + len;
            }

            pos = pos.saturating_add(len);
        }

        // close out the last I-segment up to just before the final C-row
        if let Some(schema_id) = current_schema {
            segments.push((schema_id, header_start, data_start, pos));
        }

        info!(
            "segmented into {} segments in {:?}",
            segments.len(),
            t_seg.elapsed()
        );

        // Process each segment in parallel via Tokio tasks
        let mut futures = FuturesUnordered::new();
        for (schema_id, header_start, data_start, segment_end) in segments {
            let out_dir_clone = out_dir_buf.clone();
            let name_clone = name.clone();
            let text_clone = text.clone();

            futures.push(task::spawn_blocking(move || -> Result<()> {
                let t_segment = Instant::now();
                info!(segment = %schema_id, "processing segment");

                // Extract data slice (skip I-line)
                let data_slice = &text_clone[data_start..segment_end];

                // Log each CSV line for debugging
                for (row_idx, line) in data_slice.lines().enumerate() {
                    info!(segment = %schema_id, row = row_idx, line = %line, "csv line");
                }

                // Parse columns from the I-line header slice
                let header_line = &text_clone[header_start..data_start];
                let cols: Vec<String> = header_line
                    .lines()
                    .next()
                    .unwrap()
                    .split(',')
                    .map(str::to_string)
                    .collect();
                let num_cols = cols.len();
                info!(segment = %schema_id, num_cols, "parsed columns");

                // Build Arrow schema
                let arrow_schema = Arc::new(ArrowSchema::new(
                    cols.iter()
                        .map(|c| ArrowField::new(c, ArrowDataType::Utf8, true))
                        .collect::<Vec<ArrowField>>(),
                ));

                // CSV -> RecordBatch, projecting only data columns
                let cursor = Cursor::new(data_slice.as_bytes());
                let mut csv_reader = ReaderBuilder::new(arrow_schema.clone())
                    .with_header(false)
                    // .with_projection(projection)
                    .build(cursor)?;
                let batch = csv_reader.next().transpose()?.unwrap();
                info!(
                    segment = %schema_id,
                    "deserialized into RecordBatch in {:?}",
                    t_segment.elapsed()
                );

                // Write Parquet in row-group chunks
                let t_write = Instant::now();
                let out_file = out_dir_clone.join(format!("{}—{}.parquet", name_clone, schema_id));
                let file = File::create(&out_file)?;
                let props = WriterProperties::builder()
                    .set_compression(Compression::SNAPPY)
                    .build();
                let mut writer = ArrowWriter::try_new(file, arrow_schema, Some(props))?;

                let total_rows = batch.num_rows();
                let chunk_size = 1_000_000;
                for offset in (0..total_rows).step_by(chunk_size) {
                    let len = (chunk_size).min(total_rows - offset);
                    let slice_batch = batch.slice(offset, len);
                    writer.write(&slice_batch)?;
                }
                writer.close()?;
                info!(
                    segment = %schema_id,
                    "wrote parquet segment in {:?}",
                    t_write.elapsed()
                );
                info!(
                    segment = %schema_id,
                    "segment total time {:?}",
                    t_segment.elapsed()
                );

                Ok(())
            }));
        }

        // Await all tasks
        while let Some(res) = futures.next().await {
            res??;
        }
    }

    info!(
        "async split_zip_to_parquet total time {:?}",
        start_total.elapsed()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::{
        env,
        fs::File,
        io::{Cursor, Read, Write},
    };
    use tempfile::{tempdir, NamedTempFile};
    use tracing_subscriber::{EnvFilter, FmtSubscriber};
    use zip::write::{ExtendedFileOptions, FileOptions};
    use zip::CompressionMethod;

    #[tokio::test]
    async fn test_split_zip_to_parquet_async() -> Result<()> {
        let subscriber = FmtSubscriber::builder()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| EnvFilter::new("info,nemscraper::history=trace")), // Adjust crate name if necessary
            )
            .with_test_writer()
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
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

        let out_dir = tempdir()?;
        split_zip_to_parquet_async(&zip_path, out_dir.path()).await?;

        let files: Vec<_> = std::fs::read_dir(out_dir.path())?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("parquet"))
            .collect();
        assert!(!files.is_empty(), "no parquet files produced");
        Ok(())
    }
}
