// data/src/process/chunk.rs
use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tracing::info;

/// Split a single segment into row-chunks and write each as its own Parquet file.
pub fn chunk_and_write_segment(
    name: &str,
    schema_id: &str,
    header_slice: &str,
    data_slice: &str,
    out_dir: &Path,
) {
    // 1) parse column names
    let cols: Vec<String> = header_slice
        .lines()
        .next()
        .unwrap()
        .split(',')
        .map(ToString::to_string)
        .collect();

    // 2) build Arrow schema (note the explicit Vec<ArrowField> here)
    let arrow_schema = Arc::new(ArrowSchema::new(
        cols.iter()
            .map(|c| ArrowField::new(c, ArrowDataType::Utf8, true))
            .collect::<Vec<ArrowField>>(),
    ));

    // 3) choose your chunk size
    let chunk_size = 500_000;
    let lines: Vec<&str> = data_slice.lines().collect();
    let total_rows = lines.len();

    info!(segment = %schema_id, rows = total_rows, "splitting into chunks");

    // 4) parallelize per-chunk
    lines
        .par_chunks(chunk_size)
        .enumerate()
        .for_each(|(chunk_idx, chunk_lines)| {
            // reassemble a mini-CSV
            let chunk_body = chunk_lines.join("\n");
            let cursor = std::io::Cursor::new(chunk_body.as_bytes());

            // fresh CSV reader for this chunk
            let mut reader = ReaderBuilder::new(arrow_schema.clone())
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
            while let Some(batch) = reader.next().transpose().unwrap() {
                writer.write(&batch).unwrap();
            }
            writer.close().unwrap();

            info!(segment = %schema_id, chunk = chunk_idx, "wrote {} rows", chunk_lines.len());
        });
}
