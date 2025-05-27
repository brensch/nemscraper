// data/src/process/chunk.rs

use crate::schema::Column;
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
use std::panic;
use std::{fs::File, path::Path, sync::Arc};
use tracing::{debug, error, info};

/// Split a single CSV segment into row-chunks and write each as its own Parquet file,
/// converting CTL-style DATE columns ("yyyy/mm/dd hh24:mi:ss") (in UTC+10) into true UTC timestamps.

pub fn chunk_and_write_segment(
    table_name: &str,
    schema_id: &str,
    data: &str,
    headers: Vec<Column>,
    arrow_schema: Arc<ArrowSchema>,
    out_dir: &Path,
) {
    let chunk_size = 1_000_000;
    let data_lines: Vec<&str> = data.lines().collect();
    let total_rows = data_lines.len();
    info!(table=%table_name, schema=%schema_id, rows=total_rows, "splitting into chunks");

    let compression = Compression::BROTLI(BrotliLevel::try_new(5).unwrap());

    data_lines
        .par_chunks(chunk_size)
        .enumerate()
        .for_each(|(chunk_idx, chunk_lines)| {
            // Wrap the entire chunk-processing in a panic catcher
            let result = panic::catch_unwind(|| {
                // All existing logic in a closure that returns Result<()>:
                (|| -> Result<(), anyhow::Error> {
                    // 1) Build read_schema
                    let read_schema: Arc<ArrowSchema> = {
                        let mut fields = Vec::with_capacity(arrow_schema.fields().len() + 4);
                        fields.push(Field::new("rec_type", DataType::Utf8, false));
                        fields.push(Field::new("domain", DataType::Utf8, false));
                        fields.push(Field::new("measure", DataType::Utf8, false));
                        fields.push(Field::new("seq", DataType::Utf8, false));
                        for (i, f) in arrow_schema.fields().iter().enumerate() {
                            if headers[i].ty.eq_ignore_ascii_case("DATE")
                                || headers[i].ty.starts_with("TIMESTAMP")
                            {
                                fields.push(Field::new(f.name(), DataType::Utf8, f.is_nullable()));
                            } else {
                                fields.push((**f).clone());
                            }
                        }
                        Arc::new(ArrowSchema::new(fields))
                    };

                    // 2) Build write_schema
                    let write_schema: Arc<ArrowSchema> = {
                        let mut fields = Vec::with_capacity(arrow_schema.fields().len());
                        for (i, f) in arrow_schema.fields().iter().enumerate() {
                            if headers[i].ty.eq_ignore_ascii_case("DATE")
                                || headers[i].ty.starts_with("TIMESTAMP")
                            {
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

                    // 3) CSV reader
                    let cursor = std::io::Cursor::new(data.as_bytes());
                    let projection: Vec<usize> = (4..read_schema.fields().len()).collect();
                    let mut reader = ReaderBuilder::new(read_schema.clone())
                        .with_header(false)
                        .with_projection(projection)
                        .build(cursor)
                        .context("creating CSV reader")?;

                    // 4) Parquet writer
                    let out_path = out_dir.join(format!(
                        "{}—{}—chunk{}.parquet",
                        table_name, schema_id, chunk_idx
                    ));
                    let file = File::create(&out_path)
                        .with_context(|| format!("creating {:?}", out_path))?;
                    let props = WriterProperties::builder()
                        .set_compression(compression)
                        .set_dictionary_enabled(true)
                        .build();
                    let mut writer = ArrowWriter::try_new(file, write_schema.clone(), Some(props))
                        .context("opening Parquet writer")?;

                    // 5) Write batches
                    let mut batch_idx = 0;
                    while let Some(batch) = reader
                        .next()
                        .transpose()
                        .context("reading next CSV RecordBatch")?
                    {
                        batch_idx += 1;
                        let converted = convert_date_columns(batch, &headers, &write_schema)
                            .with_context(|| {
                                format!("converting DATE columns in batch {}", batch_idx)
                            })?;
                        writer
                            .write(&converted)
                            .with_context(|| format!("writing batch {} to Parquet", batch_idx))?;
                    }

                    // 6) Close
                    writer.close().context("closing Parquet writer")?;
                    debug!(
                        table=%table_name,
                        schema=%schema_id,
                        chunk=chunk_idx,
                        rows=chunk_lines.len(),
                        "wrote rows"
                    );
                    Ok(())
                })()
            });

            match result {
                Ok(Ok(())) => {
                    // chunk succeeded
                }
                Ok(Err(e)) => {
                    // known error path
                    error!(table=%table_name, schema=%schema_id,
                        chunk=%chunk_idx, "chunk failed: {:?}", e);
                }
                Err(panic_info) => {
                    // panic occurred inside chunk logic (e.g., index out of bounds)
                    error!(table=%table_name, schema=%schema_id,
                        chunk=%chunk_idx, "internal panic: {:?}", panic_info);
                    // Attempt to log the exact row causing the issue if possible
                    // Here we assume the panic message includes the index; just log the entire chunk slice around the middle as fallback
                    let mid = chunk_lines.len() / 2;
                    let global_mid = chunk_idx * chunk_size + mid;
                    if let Some(line) = chunk_lines.get(mid) {
                        error!("Approximate offending row [{}]: {}", global_mid, line);
                    }
                    debug!(schema=?arrow_schema, "ArrowSchema at panic");
                }
            }
        });

    info!(table=%table_name, schema=%schema_id, rows=total_rows, "processed file");
}

/// Convert CTL DATE columns (utf8, in UTC+10) to TimestampMicrosecondArray in the RecordBatch,
/// parsing digits directly for performance.

fn convert_date_columns(
    batch: RecordBatch,
    headers: &[Column],
    write_schema: &Arc<ArrowSchema>,
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();

    // fixed +10h offset
    let offset =
        FixedOffset::east_opt(10 * 3600).ok_or_else(|| anyhow!("invalid fixed offset +10h"))?;

    for (i, col_meta) in headers.iter().enumerate() {
        if col_meta.ty.eq_ignore_ascii_case("DATE") || col_meta.ty.starts_with("TIMESTAMP") {
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
    }

    RecordBatch::try_new(write_schema.clone(), columns).context("building converted RecordBatch")
}
