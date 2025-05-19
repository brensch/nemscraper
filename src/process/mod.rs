// Declare submodules and re-export RawTable
mod raw_table;
pub use raw_table::RawTable;

use anyhow::{Context, Result};
use std::{
    fs::{self, File},
    io::{Cursor, Read},
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
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

#[instrument(level = "info", skip(zip_path, out_dir), fields(zip = %zip_path.as_ref().display()))]
pub fn split_zip_to_parquet<P: AsRef<Path>, Q: AsRef<Path>>(zip_path: P, out_dir: Q) -> Result<()> {
    let start_total = Instant::now();
    info!("starting serial split_zip_to_parquet");

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

        // Identify segments by I-row boundaries
        let mut segments: Vec<(String, usize, usize, usize)> = Vec::new();
        let mut pos = 0; // byte offset into `text`
        let mut header_start = 0; // start of current I-header
        let mut data_start = 0; // start of first D-row after that header
        let mut current_schema: Option<String> = None;
        let mut encountered_header = false;

        for chunk in text.split_inclusive('\n') {
            // stop at the "C,…" footer
            if chunk.starts_with("C,") {
                if !encountered_header {
                    pos += chunk.len();
                    encountered_header = true;
                }
                continue;
            }

            // whenever we hit an I-row, close out the previous segment
            if chunk.starts_with("I,") {
                if let Some(schema_id) = current_schema.take() {
                    // segment_end = pos is the exact byte index where this I-line began
                    segments.push((schema_id, header_start, data_start, pos));
                }

                // start a new segment at this I-row
                header_start = pos;
                // pull out your 2nd and 3rd fields to build the schema_id
                let parts: Vec<&str> = chunk.trim_end_matches('\n').split(',').collect();
                current_schema = Some(format!("{}_{}", parts[1], parts[2]));

                // data starts immediately *after* this header chunk
                data_start = pos + chunk.len();
            }

            // advance our offset by the full length of this chunk (including newline)
            pos += chunk.len();
        }

        // push the very last segment (up to the footer or EOF)
        if let Some(schema_id) = current_schema {
            segments.push((schema_id, header_start, data_start, pos));
        }

        // Process each segment serially
        for (schema_id, header_start, data_start, segment_end) in segments {
            let t_segment = Instant::now();
            info!(segment = %schema_id, "processing segment");

            // Extract data slice (skip I-line)
            let data_slice = &text[data_start..segment_end];

            // Parse columns from the I-line header slice
            let header_line = &text[header_start..data_start];
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

            // Parse CSV in streaming batches
            let cursor = Cursor::new(data_slice.as_bytes());
            let mut csv_reader = ReaderBuilder::new(arrow_schema.clone())
                .with_header(false)
                // // if you *really* want to read more than 1024 at once:
                // .with_batch_size(10_000_000)
                .build(cursor)?;

            // Prepare Parquet writer
            let out_file = out_dir_buf.join(format!("{}—{}.parquet", name, schema_id));
            let file = File::create(&out_file)?;
            let props = WriterProperties::builder()
                .set_compression(Compression::UNCOMPRESSED)
                .build();
            let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props))?;

            // Stream *all* batches through to Parquet
            let t_write = Instant::now();

            while let Some(batch) = csv_reader.next().transpose()? {
                writer.write(&batch)?;
            }

            // Finalize
            writer.close()?;
            info!(segment = %schema_id, "wrote full parquet in {:?}", t_write.elapsed());

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
        }
    }

    info!(
        "serial split_zip_to_parquet total time {:?}",
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
        io::{Cursor, Write},
    };
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

        // prepare sample ZIP as before...
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

        // use a non-temp, project-relative test output dir
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
