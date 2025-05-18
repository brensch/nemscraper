use anyhow::{Context, Result};
use futures::stream::{FuturesUnordered, StreamExt};
use std::{
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};
use tokio::task;
use tracing::{info, instrument};
use zip::ZipArchive;

use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

#[derive(Debug)]
pub struct RawTable {
    /// Column names, from the “I” row of the specific CSV file.
    /// While SchemaEvolution provides canonical names, these are what the file claims.
    pub headers: Vec<String>,
    /// Each “D” row, as a Vec of Strings (one per field).
    pub rows: Vec<Vec<String>>,
    /// The effective month (YYYYMM) for the data in this table, derived from the 'C' row of the CSV.
    pub effective_month: String,
}

/// Async version using Tokio tasks instead of Rayon
#[instrument(level = "info", skip(zip_path, out_dir), fields(zip = %zip_path.as_ref().display()))]
pub async fn split_zip_to_parquet_async<P: AsRef<Path>, Q: AsRef<Path>>(
    zip_path: P,
    out_dir: Q,
) -> Result<()> {
    let start_total = Instant::now();
    info!("starting async split_zip_to_parquet");

    // Normalize to owned paths for moving into tasks
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

    // Iterate over entries
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

        // Find segments
        let t_seg = Instant::now();
        let mut segments = Vec::new();
        let mut pos: usize = 0;
        let mut current = None;
        for line in text.lines() {
            let len = line.len() + 1;
            if line.starts_with("I,") {
                if let Some((schema, start)) = current.take() {
                    segments.push((schema, start, pos));
                }
                let parts: Vec<&str> = line.split(',').collect();
                let schema = format!("{}_{}", parts[1], parts[2]);
                current = Some((schema, pos));
            }
            pos = pos.saturating_add(len);
        }
        if let Some((schema, start)) = current {
            segments.push((schema, start, text.len()));
        }
        info!(
            "segmented into {} segments in {:?}",
            segments.len(),
            t_seg.elapsed()
        );

        // Spawn a blocking task for each segment
        let mut futures = FuturesUnordered::new();
        for (schema_id, start, end) in segments {
            let out_dir_clone = out_dir_buf.clone();
            let name_clone = name.clone();
            let text_clone = text.clone();

            futures.push(task::spawn_blocking(move || -> Result<()> {
                let t_segment = Instant::now();
                info!(segment = %schema_id, "processing segment");

                let slice = &text_clone[start..end];
                // Parse header
                let cols: Vec<String> = slice
                    .lines()
                    .find(|l| l.starts_with("I,"))
                    .unwrap()
                    .split(',')
                    .skip(4)
                    .map(str::to_string)
                    .collect();
                let num_cols = cols.len();

                // Build arrays per column
                let t_arrays = Instant::now();
                let arrays: Vec<ArrayRef> = (0..num_cols)
                    .map(|col_idx| {
                        let cells: Vec<&str> = slice
                            .lines()
                            .filter(|l| l.starts_with("D,"))
                            .map(|row| row.split(',').skip(4 + col_idx).next().unwrap())
                            .collect();
                        Arc::new(StringArray::from(cells)) as ArrayRef
                    })
                    .collect();
                info!(segment = %schema_id, "built {} arrays in {:?}", num_cols, t_arrays.elapsed());

                // Create schema & RecordBatch
                let fields: Vec<ArrowField> = cols
                    .iter()
                    .map(|name| ArrowField::new(name, ArrowDataType::Utf8, true))
                    .collect();
                let schema = Arc::new(ArrowSchema::new(fields));
                let batch = RecordBatch::try_new(schema.clone(), arrays)?;
                info!(segment = %schema_id, "created RecordBatch in {:?}", t_segment.elapsed());

                // Write Parquet
                let t_write = Instant::now();
                let out_file = out_dir_clone.join(format!("{}—{}.parquet", name_clone, schema_id));
                let file = File::create(&out_file)?;
                let props = WriterProperties::builder().set_compression(Compression::UNCOMPRESSED).build();
                let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
                let total_rows = batch.num_rows();
                let chunk_size = 1_000_000;
                for offset in (0..total_rows).step_by(chunk_size) {
                    let len = (chunk_size).min(total_rows - offset);
                    let slice = batch.slice(offset, len);
                    writer.write(&slice)?;
                }
                writer.close()?;
                info!(segment = %schema_id, "wrote parquet segment in {:?}", t_write.elapsed());
                info!(segment = %schema_id, "segment total time {:?}", t_segment.elapsed());
                Ok(())
            }));
        }

        // Await all segment tasks
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
    use parquet::file::reader::SerializedFileReader;
    use std::io::{Cursor, Read, Write};
    use std::{env, fs::File};
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
        // Pick up external ZIP or build sample
        let zip_path = if let Ok(path) = env::var("ZIP_PATH") {
            PathBuf::from(path)
        } else {
            let sample = r##"C,SETP.WORLD,FPP_DCF,AEMO,PUBLIC,\"2024/12/14\",...\n\  I,FPP,FORECAST_DEFAULT_CF,1,UID,CON,ST_DT,EN_DT,VER,BID,REG,DCF,FLAG,TOTAL,UID\n\  D,FPP,FORECAST_DEFAULT_CF,1,X,Y,2024/12/22 00:00,2024/12/23 00:00,1,A,B,0.1,0,5,X\n\  D,FPP,FORECAST_DEFAULT_CF,1,X,Y,2024/12/22 01:00,2024/12/23 01:00,1,A,B,0.2,0,6,X\n\  I,FPP,FORECAST_RESIDUAL_DCF,1,CON,ST_DT,EN_DT,VER,BID,RES,FLAG,TOTAL\n\  D,FPP,FORECAST_RESIDUAL_DCF,1,Y,2024/12/22 00:00,2024/12/23 00:00,1,A,0.3,0,7\n\  D,FPP,FORECAST_RESIDUAL_DCF,1,Y,2024/12/22 01:00,2024/12/23 01:00,1,A,0.4,0,8"##;
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

        // Verify at least one parquet file created
        let files: Vec<_> = std::fs::read_dir(out_dir.path())?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("parquet"))
            .collect();
        assert!(!files.is_empty(), "no parquet files produced");
        Ok(())
    }
}
