// data/src/process/chunk.rs
use arrow::array::{Array, ArrayRef, PrimitiveBuilder, StringArray, TimestampMicrosecondArray};
use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimestampMicrosecondType};
use arrow::record_batch::RecordBatch;
use chrono::{NaiveDate, NaiveDateTime};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tracing::info;

use crate::schema::Column;

/// Split a single CSV segment into row-chunks and write each as its own Parquet file,
/// converting CTL-style DATE columns ("yyyy/mm/dd hh24:mi:ss") into timestamps.
pub fn chunk_and_write_segment(
    table_name: &str,
    schema_id: &str,
    header: &str,
    data: &str,
    headers: Vec<Column>, // now takes ownership of headers
    arrow_schema: Arc<ArrowSchema>,
    out_dir: &Path,
) {
    let chunk_size = 500_000;

    // Pre-build full Arrow schema: 4 metadata cols + data cols
    let full_schema: Arc<ArrowSchema> = {
        let mut fields = Vec::with_capacity(arrow_schema.fields().len() + 4);
        fields.push(Field::new("rec_type", DataType::Utf8, false));
        fields.push(Field::new("domain", DataType::Utf8, false));
        fields.push(Field::new("measure", DataType::Utf8, false));
        fields.push(Field::new("seq", DataType::Utf8, false));
        for f in arrow_schema.fields() {
            fields.push((**f).clone());
        }
        Arc::new(ArrowSchema::new(fields))
    };

    let data_lines: Vec<&str> = data.lines().collect();
    let total_rows = data_lines.len();
    info!(table=%table_name, schema=%schema_id, rows=total_rows, "splitting into chunks");

    data_lines
        .par_chunks(chunk_size)
        .enumerate()
        .for_each(|(chunk_idx, chunk)| {


            let cursor = std::io::Cursor::new(data.as_bytes());
            let mut reader = ReaderBuilder::new(full_schema.clone())
                .with_header(false)
                .build(cursor)
                .expect("CSV reader failed");

            // prepare Parquet writer
            let out_path = out_dir.join(format!("{}—{}—chunk{}.parquet", table_name, schema_id, chunk_idx));
            let file = File::create(&out_path).expect("failed to create parquet file");
            let props = WriterProperties::builder().set_compression(Compression::SNAPPY).build();
            let mut writer = ArrowWriter::try_new(file, full_schema.clone(), Some(props))
                .expect("failed to create parquet writer");

            // process each RecordBatch
            while let Some(batch) = reader.next().transpose().unwrap() {
                let converted = convert_date_columns(batch, &headers, &full_schema);
                writer.write(&converted).unwrap();
            }
            writer.close().unwrap();
            info!(table=%table_name, schema=%schema_id, chunk=chunk_idx, "wrote {} rows", chunk.len());
        });
}

/// Convert CTL DATE columns (utf8) to TimestampMicrosecondArray in the RecordBatch,
/// parsing digits directly for performance.
fn convert_date_columns(
    batch: RecordBatch,
    headers: &Vec<Column>, // borrow Vec for metadata
    full_schema: &Arc<ArrowSchema>,
) -> RecordBatch {
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();

    // data columns start at index 4 in full_schema
    for (i, col_meta) in headers.iter().enumerate() {
        if col_meta.ty.eq_ignore_ascii_case("DATE") {
            let idx = 4 + i;
            let str_arr = columns[idx]
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("DATE column not StringArray");

            // build timestamp (microsecond) array
            let mut builder =
                PrimitiveBuilder::<TimestampMicrosecondType>::with_capacity(str_arr.len());
            for row in 0..str_arr.len() {
                if str_arr.is_null(row) {
                    builder.append_null();
                } else {
                    let s = str_arr.value(row);
                    let b = s.as_bytes();
                    // handle possible surrounding quotes
                    let start = if b.first() == Some(&b'"') { 1 } else { 0 };
                    // parse YYYY/MM/DD HH:MM:SS
                    let y = (b[start + 0] - b'0') as i32 * 1000
                        + (b[start + 1] - b'0') as i32 * 100
                        + (b[start + 2] - b'0') as i32 * 10
                        + (b[start + 3] - b'0') as i32;
                    let m = ((b[start + 5] - b'0') as u32) * 10 + (b[start + 6] - b'0') as u32;
                    let d = ((b[start + 8] - b'0') as u32) * 10 + (b[start + 9] - b'0') as u32;
                    let hh = ((b[start + 11] - b'0') as u32) * 10 + (b[start + 12] - b'0') as u32;
                    let mm = ((b[start + 14] - b'0') as u32) * 10 + (b[start + 15] - b'0') as u32;
                    let ss = ((b[start + 17] - b'0') as u32) * 10 + (b[start + 18] - b'0') as u32;

                    // build NaiveDateTime and convert to micros
                    let date = NaiveDate::from_ymd_opt(y, m, d).unwrap();
                    let dt = NaiveDateTime::new(
                        date,
                        chrono::NaiveTime::from_hms_opt(hh, mm, ss).unwrap(),
                    );
                    builder.append_value(dt.and_utc().timestamp_micros());
                }
            }
            let ts_arr = TimestampMicrosecondArray::from(builder.finish());
            columns[idx] = Arc::new(ts_arr);
        }
    }

    // build new batch
    RecordBatch::try_new(full_schema.clone(), columns).expect("failed to build converted batch")
}
