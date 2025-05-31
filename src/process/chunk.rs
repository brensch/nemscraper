// src/process/chunk.rs

use anyhow::{anyhow, Context, Result};
use arrow::array::Array;
use arrow::array::{ArrayRef, PrimitiveBuilder, StringArray, TimestampMicrosecondArray};
use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimestampMicrosecondType};
use arrow::record_batch::RecordBatch;
use arrow_schema::TimeUnit;
use chrono::TimeZone;
use chrono::{FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use parquet::arrow::ArrowWriter;
use parquet::basic::{BrotliLevel, Compression};
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use std::fs;
use std::fs::File;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, error, info};

/// Build a "read" ArrowSchema from the base schema:
/// - prepend 4 dummy Utf8 fields (rec_type, domain, measure, seq),
/// - convert any Timestamp(µs) fields to Utf8 for CSV parsing.
fn make_read_schema(base: &ArrowSchema) -> Arc<ArrowSchema> {
    let mut fields = Vec::with_capacity(base.fields().len() + 4);

    fields.push(Field::new("rec_type", DataType::Utf8, false));
    fields.push(Field::new("domain", DataType::Utf8, false));
    fields.push(Field::new("measure", DataType::Utf8, false));
    fields.push(Field::new("seq", DataType::Utf8, false));

    for f in base.fields() {
        let dt = match f.data_type() {
            DataType::Timestamp(_, _) => DataType::Utf8,
            other => other.clone(),
        };
        fields.push(Field::new(f.name(), dt, f.is_nullable()));
    }

    Arc::new(ArrowSchema::new(fields))
}

/// Splits CSV data into record-batches of `chunk_size` rows,
/// skipping the first 4 control columns via projection, and
/// converts DATE/TIMESTAMP fields back to native types before writing Parquet.
pub fn chunk_and_write_segment(
    file_name: &str,
    arrow_schema: Arc<ArrowSchema>,
    data: &str,
    out_dir: &Path,
) {
    let chunk_size = 1_000_000;

    // 1) Derive the table name by concatenating fields 1–3 of the first line in the CSV.
    //    If the first line doesn’t have at least 3 comma-separated columns, fall back to file_name.
    let first_line = data.lines().next().unwrap_or_default();
    let header_parts: Vec<&str> = first_line.split(',').collect();
    if header_parts.len() < 3 {
        error!(file_name = %file_name, "Insufficient header fields, skipping chunk");
        return;
    }
    let table_name = format!("{}_{}", header_parts[1], header_parts[2]);

    // 2) Log only the derived table_name instead of all header fields
    debug!(table = %table_name, "splitting into batches");

    // 3) Read schema: include 4 dummy columns + actual
    let read_schema = make_read_schema(&arrow_schema);

    // 4) Projection: skip the first 4 dummy CSV columns
    let projection = (4..read_schema.fields().len()).collect::<Vec<usize>>();

    // 5) CSV reader over entire blob, with batch_size for chunking
    let cursor = Cursor::new(data.as_bytes());
    let csv_reader = ReaderBuilder::new(read_schema.clone())
        .with_header(true)
        .with_batch_size(chunk_size)
        .with_projection(projection) // skip first 4 fields
        .build(cursor)
        .context("creating CSV reader")
        .unwrap(); // or handle error gracefully

    // 6) Parquet writer properties
    let props = WriterProperties::builder()
        .set_compression(Compression::BROTLI(BrotliLevel::try_new(5).unwrap()))
        .set_dictionary_enabled(true)
        .build();

    // 7) Parallelize over record-batches
    csv_reader
        .into_iter()
        .enumerate()
        .par_bridge()
        .for_each(|(batch_idx, batch_res)| {
            match batch_res {
                Ok(batch) => {
                    // Convert any DATE/TIMESTAMP columns in this batch
                    let converted = match convert_date_columns(batch, &arrow_schema) {
                        Ok(b) => b,
                        Err(e) => {
                            error!(table = %table_name, chunk = %batch_idx, "conversion error: {:?}", e);
                            return;
                        }
                    };

                    // Build the Parquet filename using the derived table_name
                    let out_path = out_dir.join(format!("{}--{}--chunk{}.parquet", file_name, table_name, batch_idx));
                    let temp_path = out_path.with_extension("tmp");

                    // Write the file to a temporary path first
                    let file = match File::create(&temp_path) {
                        Ok(f) => f,
                        Err(e) => {
                            error!(table = %table_name, chunk = %batch_idx, "file create error: {:?}", e);
                            return;
                        }
                    };
                    let mut writer = match ArrowWriter::try_new(file, arrow_schema.clone(), Some(props.clone())) {
                        Ok(w) => w,
                        Err(e) => {
                            error!(table = %table_name, chunk = %batch_idx, "writer open error: {:?}", e);
                            return;
                        }
                    };

                    if let Err(e) = writer.write(&converted) {
                        error!(table = %table_name, chunk = %batch_idx, "write error: {:?}", e);
                    }
                    if let Err(e) = writer.close() {
                        error!(table = %table_name, chunk = %batch_idx, "close error: {:?}", e);
                    }

                    // Move the temporary file to the final destination
                    if let Err(e) = fs::rename(&temp_path, &out_path) {
                        error!(
                            table = %table_name,
                            chunk = %batch_idx,
                            temp_path = %temp_path.display(),
                            out_path = %out_path.display(),
                            "rename error: {:?}",
                            e
                        );
                    }
                    debug!(table = %table_name, chunk = %batch_idx, rows = converted.num_rows(), "wrote rows");
                }
                Err(e) => {
                    error!(table = %table_name, chunk = %batch_idx, "CSV parse error: {:?}", e);
                }
            }
        });

    debug!(table = %table_name, "processed table");
}

/// Convert CTL DATE columns (utf8, in UTC+10) to TimestampMicrosecondArray in the RecordBatch,
/// parsing digits directly for performance.
fn convert_date_columns(batch: RecordBatch, schema: &Arc<ArrowSchema>) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();

    // fixed +10h offset
    let offset =
        FixedOffset::east_opt(10 * 3600).ok_or_else(|| anyhow!("invalid fixed offset +10h"))?;

    for (i, col_meta) in schema.fields().iter().enumerate() {
        if col_meta.data_type() != &DataType::Timestamp(TimeUnit::Microsecond, None) {
            continue; // not a DATE column
        }

        let idx = i;
        let str_arr = columns[idx]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                anyhow!(
                    "column {} was expected to be StringArray for DATE, got {:?}",
                    idx,
                    columns[idx].data_type()
                )
            })?;

        let mut builder =
            PrimitiveBuilder::<TimestampMicrosecondType>::with_capacity(str_arr.len());

        for row in 0..str_arr.len() {
            if str_arr.is_null(row) {
                builder.append_null();
                continue;
            }

            let s = str_arr.value(row);
            let b = s.as_bytes();
            // allow optional surrounding quotes
            let start = if b.first() == Some(&b'"') { 1 } else { 0 };
            // need at least "YYYY/MM/DD HH:MM:SS"
            if b.len() < start + 19 {
                return Err(anyhow!("date too short at row {}: {:?}", row, s));
            }

            // fast digit-to-int parsing
            let y = (b[start] - b'0') as i32 * 1000
                + (b[start + 1] - b'0') as i32 * 100
                + (b[start + 2] - b'0') as i32 * 10
                + (b[start + 3] - b'0') as i32;
            let m = ((b[start + 5] - b'0') as u32) * 10 + (b[start + 6] - b'0') as u32;
            let d = ((b[start + 8] - b'0') as u32) * 10 + (b[start + 9] - b'0') as u32;
            let hh = ((b[start + 11] - b'0') as u32) * 10 + (b[start + 12] - b'0') as u32;
            let mm = ((b[start + 14] - b'0') as u32) * 10 + (b[start + 15] - b'0') as u32;
            let ss = ((b[start + 17] - b'0') as u32) * 10 + (b[start + 18] - b'0') as u32;

            // parse optional fractional seconds into microseconds
            let mut micros = 0u32;
            if b.len() > start + 19 && b[start + 19] == b'.' {
                // up to 6 digits of fraction
                let mut factor = 100_000;
                let mut pos = start + 20;
                while pos < b.len() && b[pos].is_ascii_digit() && factor > 0 {
                    micros += (b[pos] - b'0') as u32 * factor;
                    factor /= 10;
                    pos += 1;
                }
            }

            // safe constructors that return Option
            let date = NaiveDate::from_ymd_opt(y, m, d)
                .ok_or_else(|| anyhow!("invalid date {}/{}/{} at row {}", y, m, d, row))?;
            let time = NaiveTime::from_hms_micro_opt(hh, mm, ss, micros).ok_or_else(|| {
                anyhow!(
                    "invalid time {:02}:{:02}:{:02}.{:06} at row {}",
                    hh,
                    mm,
                    ss,
                    micros,
                    row
                )
            })?;
            let dt = NaiveDateTime::new(date, time);

            // apply offset and get UTC microseconds
            let dt_utc = offset
                .from_local_datetime(&dt)
                .earliest()
                .ok_or_else(|| anyhow!("ambiguous or invalid local datetime at row {}", row))?
                .timestamp_micros();

            builder.append_value(dt_utc);
        }

        let ts_arr = TimestampMicrosecondArray::from(builder.finish());
        columns[idx] = Arc::new(ts_arr);
    }

    RecordBatch::try_new(schema.clone(), columns).context("building converted RecordBatch")
}
