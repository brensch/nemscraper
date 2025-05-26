// data/src/process/chunk.rs

use crate::schema::Column;
use arrow::array::Array;
use arrow::array::{ArrayRef, PrimitiveBuilder, StringArray, TimestampMicrosecondArray};
use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimestampMicrosecondType};
use arrow::record_batch::RecordBatch;
use arrow_schema::TimeUnit;
use chrono::TimeZone;
use chrono::{FixedOffset, NaiveDate, NaiveDateTime};
use parquet::arrow::ArrowWriter;
use parquet::basic::{BrotliLevel, Compression};
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use std::{fs::File, path::Path, sync::Arc};
use tracing::{debug, info};

/// Split a single CSV segment into row-chunks and write each as its own Parquet file,
/// converting CTL-style DATE columns ("yyyy/mm/dd hh24:mi:ss") (in UTC+10) into true UTC timestamps.
pub fn chunk_and_write_segment(
    table_name: &str,
    schema_id: &str,
    header: &str,
    data: &str,
    headers: Vec<Column>,           // takes ownership of headers metadata
    arrow_schema: Arc<ArrowSchema>, // schema of *data* columns only
    out_dir: &Path,
) {
    let chunk_size = 1_000_000;

    // 1) Build read_schema: DATE columns forced to Utf8 so CSV reader doesn't parse them.
    let read_schema: Arc<ArrowSchema> = {
        let mut fields = Vec::with_capacity(arrow_schema.fields().len() + 4);
        // metadata fields
        fields.push(Field::new("rec_type", DataType::Utf8, false));
        fields.push(Field::new("domain", DataType::Utf8, false));
        fields.push(Field::new("measure", DataType::Utf8, false));
        fields.push(Field::new("seq", DataType::Utf8, false));
        // data fields
        for (i, f) in arrow_schema.fields().iter().enumerate() {
            if headers[i].ty.eq_ignore_ascii_case("DATE") {
                fields.push(Field::new(f.name(), DataType::Utf8, f.is_nullable()));
            } else {
                fields.push((**f).clone());
            }
        }
        Arc::new(ArrowSchema::new(fields))
    };

    // 2) Build write_schema for data columns only: DATE → Timestamp(Microsecond)
    let write_schema: Arc<ArrowSchema> = {
        let mut fields = Vec::with_capacity(arrow_schema.fields().len());
        for (i, f) in arrow_schema.fields().iter().enumerate() {
            if headers[i].ty.eq_ignore_ascii_case("DATE") {
                fields.push(Field::new(
                    f.name(),
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    f.is_nullable(),
                ));
            } else {
                fields.push((**f).clone());
            }
        }
        Arc::new(ArrowSchema::new(fields))
    };

    let data_lines: Vec<&str> = data.lines().collect();
    let total_rows = data_lines.len();
    debug!(table=%table_name, schema=%schema_id, rows=total_rows, "splitting into chunks");

    let compression = Compression::BROTLI(BrotliLevel::try_new(5).unwrap());
    data_lines
        .par_chunks(chunk_size)
        .enumerate()
        .for_each(|(chunk_idx, _chunk)| {
            // Setup CSV reader with read_schema, projecting away first 4 metadata columns
            let cursor = std::io::Cursor::new(data.as_bytes());
            let total_fields = read_schema.fields().len();
            let projection: Vec<usize> = (4..total_fields).collect();
            let mut reader = ReaderBuilder::new(read_schema.clone())
                .with_header(false)
                .with_projection(projection)
                .build(cursor)
                .expect("CSV reader failed");

            // Prepare Parquet writer with *data-only* write_schema
            let out_path = out_dir.join(format!("{}—{}—chunk{}.parquet", table_name, schema_id, chunk_idx));
            let file = File::create(&out_path).expect("failed to create parquet file");
                let props = WriterProperties::builder()
                    .set_compression(compression)
                    .set_dictionary_enabled(true) // disable dictionary encoding for simplicity
                    .build();            let mut writer = ArrowWriter::try_new(file, write_schema.clone(), Some(props))
                .expect("failed to create parquet writer");

            // Process each RecordBatch
            while let Some(batch) = reader.next().transpose().unwrap() {
                let converted = convert_date_columns(batch, &headers, &write_schema);
                writer.write(&converted).unwrap();
            }

            writer.close().unwrap();
            debug!(table=%table_name, schema=%schema_id, chunk=chunk_idx, rows=_chunk.len(), "wrote rows"  );
        });
    info!(table=%table_name, schema=%schema_id, rows=total_rows, "processed file");
}

/// Convert CTL DATE columns (utf8, in UTC+10) to TimestampMicrosecondArray in the RecordBatch,
/// parsing digits directly for performance.
fn convert_date_columns(
    batch: RecordBatch,
    headers: &[Column],
    write_schema: &Arc<ArrowSchema>, // now only data columns, in the same order as `headers`
) -> RecordBatch {
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();

    // fixed +10h offset
    let offset = FixedOffset::east_opt(10 * 3600).expect("Invalid offset");

    for (i, col_meta) in headers.iter().enumerate() {
        if col_meta.ty.eq_ignore_ascii_case("DATE") {
            // data-only batch: DATE columns are at the same index as headers
            let idx = i;
            let str_arr = columns[idx]
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("DATE column not StringArray");

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

                    let date = NaiveDate::from_ymd_opt(y, m, d).unwrap();
                    let dt = NaiveDateTime::new(
                        date,
                        chrono::NaiveTime::from_hms_opt(hh, mm, ss).unwrap(),
                    );
                    // interpret `dt` in UTC+10, then get UTC microseconds
                    let dt_utc = offset.from_local_datetime(&dt).unwrap().timestamp_micros();
                    builder.append_value(dt_utc);
                }
            }

            let ts_arr = TimestampMicrosecondArray::from(builder.finish());
            columns[idx] = Arc::new(ts_arr);
        }
    }

    RecordBatch::try_new(write_schema.clone(), columns).expect("failed to build converted batch")
}
