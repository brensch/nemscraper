// data/src/process/chunk.rs
use arrow::csv::ReaderBuilder;
use arrow::datatypes::Schema as ArrowSchema;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tracing::info;

/// Split a single CSV segment into row-chunks and write each as its own Parquet file.
///
/// # Arguments
/// * `table_name`   – the original CSV file name (e.g. `"aemo.csv"`)
/// * `schema_id`    – identifier for the schema (e.g. the `fields_hash`)
/// * `header`       – the CSV bytes from the `I,` line (metadata + trailing `\n`)
/// * `data`         – the CSV bytes for all rows belonging to this segment
/// * `arrow_schema` – the pre-built Arrow schema from SchemaEvolution
/// * `out_dir`      – directory to write parquet files into
pub fn chunk_and_write_segment(
    table_name: &str,
    schema_id: &str,
    header: &str,
    data: &str,
    arrow_schema: Arc<ArrowSchema>,
    out_dir: &Path,
) {
    // max rows per Parquet file
    let chunk_size = 500_000;

    // split data rows (preserves original line order)
    let data_lines: Vec<&str> = data.lines().collect();
    let total_rows = data_lines.len();

    info!(
        table = %table_name,
        schema = %schema_id,
        rows = total_rows,
        "splitting into chunks"
    );

    data_lines
        .par_chunks(chunk_size)
        .enumerate()
        .for_each(|(chunk_idx, chunk)| {
            // build a CSV record: header + chunk rows
            let mut buf = String::with_capacity(header.len() + chunk.len() * 30);
            buf.push_str(header);
            if !header.ends_with('\n') {
                buf.push('\n');
            }
            for line in chunk {
                buf.push_str(line);
                buf.push('\n');
            }

            // CSV reader with no header row (we treat the `I,` line as data)
            let cursor = std::io::Cursor::new(buf.as_bytes());
            let mut reader = ReaderBuilder::new(arrow_schema.clone())
                .with_header(false)
                .build(cursor)
                .expect("CSV reader failed");

            // open per-chunk Parquet
            let out_path = out_dir.join(format!(
                "{}—{}—chunk{}.parquet",
                table_name, schema_id, chunk_idx
            ));
            let file = File::create(&out_path).expect("failed to create parquet file");
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();
            let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props))
                .expect("failed to create parquet writer");

            // write all batches
            while let Some(batch) = reader.next().transpose().unwrap() {
                writer.write(&batch).unwrap();
            }
            writer.close().unwrap();

            info!(
                table = %table_name,
                schema = %schema_id,
                chunk = chunk_idx,
                "wrote {} rows",
                chunk.len()
            );
        });
}
