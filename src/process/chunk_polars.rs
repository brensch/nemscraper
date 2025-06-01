use chrono::NaiveDateTime;
use polars::prelude::*;
use std::borrow::Cow;
use std::error::Error;
use std::fs;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use tracing::info;

/// 1) Trim whitespace + strip outer quotes if present.
///    Always returns an owned String.
fn clean_str(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
        trimmed[1..trimmed.len() - 1].to_string()
    } else {
        trimmed.to_string()
    }
}

/// 2) Infer “would-be” dtype from a cleaned string, treating ANY parseable
///    number (integer or float) as Float64. Otherwise String.
fn infer_dtype_from_str(s: &str) -> DataType {
    if s.parse::<f64>().is_ok() {
        DataType::Float64
    } else {
        DataType::String
    }
}

/// 3) Promote any integer‐type or Float32 to Float64 in a sample‐inferred dtype.
fn promote_sample_dtype(dt: &DataType) -> DataType {
    match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32 => DataType::Float64,
        DataType::Float64 => DataType::Float64,
        other => other.clone(),
    }
}

/// 4) After forcing schema and full read, check if any string column’s first‐row,
///    once cleaned, would change dtype (String → Float64). Return true if any do.
fn check_first_row_trim(df: &DataFrame) -> PolarsResult<bool> {
    let mut any_changed = false;

    for col_name in df.get_column_names() {
        let col = df.column(col_name)?;
        // Only inspect columns with DataType::String
        let str_ca = match col.str() {
            Ok(ca) => ca,
            Err(_) => continue,
        };
        if let Some(orig_val) = str_ca.get(0) {
            let cleaned = clean_str(orig_val);
            let new_dtype = infer_dtype_from_str(&cleaned);
            let old_dtype = col.dtype();
            if old_dtype != &new_dtype {
                println!(
                    "Column '{}' first‐row WOULD change type: {:?} (orig=\"{}\") → {:?} (cleaned=\"{}\")",
                    col_name, old_dtype, orig_val, new_dtype, cleaned
                );
                any_changed = true;
            }
            if cleaned != orig_val {
                println!(
                    "  (trimmed text changed: \"{}\" → \"{}\")",
                    orig_val, cleaned
                );
            }
        }
    }

    Ok(any_changed)
}

/// 5) Main CSV→Parquet routine:
///
///    - Automatically detect date columns matching "%Y/%m/%d %H:%M:%S" (in sample).
///    - Sample first N rows to infer a provisional schema and identify date columns.
///    - Promote any integerish/Float32 → Float64 in that schema.
///    - Re-read full CSV with `.with_dtype_overwrite(...)` so no Int64 remains.
///    - For each detected date column, `strptime(...)` into
///      `Datetime(TimeUnit::Milliseconds, Some("Australia/Sydney"))`.
///    - Build filename prefix from headers 1,2,3.
///    - If any string column’s first row (after `clean_str`) “looks numeric,” do a full‐column
///      `clean_str(...)` → cast to Float64 (or leave as String).
///    - Write to `<prefix>_<file>.parquet.tmp` → rename to `.parquet`.
pub fn csv_to_parquet(file_name: &str, data: &str, out_dir: &Path) -> Result<(), Box<dyn Error>> {
    // ─── Step A: SAMPLE‐READ (first 1,000 rows) ───────────────────────────────────
    let sample_size = 1000;
    let mut sample_cursor = Cursor::new(data.as_bytes());
    let sample_opts = CsvReadOptions::default()
        .with_has_header(true)
        .with_n_rows(Some(sample_size));
    let sample_df: DataFrame = CsvReader::new(&mut sample_cursor)
        .with_options(sample_opts)
        .finish()?;

    // ─── Step B: BUILD A FORCED‐DTYPE VECTOR AND IDENTIFY DATE COLUMNS ─────────────
    let mut forced_dtypes: Vec<DataType> = Vec::with_capacity(sample_df.width());
    let mut date_cols: Vec<PlSmallStr> = Vec::new();

    for s in sample_df.get_columns() {
        let name = s.name().clone();
        let orig_dtype = s.dtype();
        // If Polars inferred String, check if the first non-null sample parses as date.
        if orig_dtype == &DataType::String {
            if let Ok(str_ca) = s.str() {
                if let Some(val0) = str_ca.get(0) {
                    let cleaned0 = clean_str(val0);
                    if NaiveDateTime::parse_from_str(&cleaned0, "%Y/%m/%d %H:%M:%S").is_ok() {
                        // Detected date format in sample → mark as a date column
                        date_cols.push(name.clone());
                        forced_dtypes.push(DataType::String);
                        continue;
                    }
                }
            }
            // Otherwise, keep as String
            forced_dtypes.push(DataType::String);
        } else {
            // Not a string column. Promote ints/Float32 → Float64; leave others unchanged.
            let forced = promote_sample_dtype(orig_dtype);
            forced_dtypes.push(forced);
        }
    }

    // ─── Log the inferred (provisional) schema ────────────────────────────────────
    info!("Inferred schema from sample:");
    for (col_name, dtype) in sample_df
        .get_column_names()
        .iter()
        .zip(forced_dtypes.iter())
    {
        info!("  {}: {:?}", col_name, dtype);
    }

    let dtypes_ref: Arc<Vec<DataType>> = Arc::new(forced_dtypes);

    // ─── Step C: FULL‐READ with FORCED‐DTYPE VECTOR ──────────────────────────────────
    let mut full_cursor = Cursor::new(data.as_bytes());
    let full_opts = CsvReadOptions::default()
        .with_has_header(true)
        .with_dtype_overwrite(Some(dtypes_ref.clone()));
    let mut df: DataFrame = CsvReader::new(&mut full_cursor)
        .with_options(full_opts)
        .finish()?; // now ints → Float64; date columns are still String

    // ─── Step C2: FOR EACH DETECTED DATE COLUMN, PARSE AS DATETIME(GMT+10) ─────────
    for col_name in &date_cols {
        // only proceed if the DataFrame actually has this column name
        if df.get_column_names().contains(&col_name) {
            // grab the column as a Utf8Chunked (string) array
            let col = df.column(col_name)?;
            let utf8_col: &StringChunked = col.str()?;

            // format, time unit, and flags
            let fmt: Option<&str> = Some("%Y/%m/%d %H:%M:%S");
            let tu = TimeUnit::Milliseconds;
            let use_cache = true;
            let tz_aware = true;

            // static UTC+10 offset, created via Polars’s small‐string TimeZone
            //
            // SAFETY: `from_static` does no validation on the string. Here we
            // know "+10:00" is a valid fixed‐offset identifier, so this is safe.
            let tz: TimeZone = unsafe { TimeZone::from_static("+10:00") };

            // since we’re using a fixed offset, DST can’t occur—any “ambiguous” mask is ignored
            // but we still need to pass a StringChunked, so fill with empty strings
            let amb = StringChunked::full("ambiguous".into(), "", utf8_col.len());

            // parse the string column into a DatetimeChunked, attaching the +10:00 offset
            let parsed = utf8_col.as_datetime(fmt, tu, use_cache, tz_aware, Some(&tz), &amb)?;

            // replace the original column with the new datetime‐typed series
            df.replace(col_name, parsed.into_series())?;
        }
    }

    // ─── Step D: BUILD PREFIX FROM COLUMN HEADERS 1,2,3 ─────────────────────────────
    let all_trimmed_names: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|hdr| clean_str(hdr.as_str()))
        .collect();

    let prefix = if all_trimmed_names.len() >= 4 {
        format!(
            "{}_{}_{}",
            all_trimmed_names[1], all_trimmed_names[2], all_trimmed_names[3]
        )
    } else {
        String::new()
    };

    let final_name = if prefix.is_empty() {
        format!("{}.parquet", file_name)
    } else {
        format!("{}_{}.parquet", prefix, file_name)
    };
    let tmp_name = if prefix.is_empty() {
        format!("{}.parquet.tmp", file_name)
    } else {
        format!("{}_{}.parquet.tmp", prefix, file_name)
    };
    let final_path = out_dir.join(final_name);
    let tmp_path = out_dir.join(tmp_name);

    // ─── Step E: CHECK FIRST‐ROW TRIM FOR STRING COLUMNS ────────────────────────────
    let type_changed = check_first_row_trim(&df)?;
    if type_changed {
        println!("→ At least one string column’s first‐row changed type after trimming.");
        for col_name in df.get_column_names_owned() {
            // Skip if this column was parsed as datetime
            if date_cols.contains(&col_name) {
                continue;
            }
            let col = df.column(&col_name)?;
            let str_ca = match col.str() {
                Ok(ca) => ca,
                Err(_) => continue,
            };
            if let Some(orig_val) = str_ca.get(0) {
                let cleaned_first = clean_str(orig_val);
                let new_dtype = infer_dtype_from_str(&cleaned_first);
                let old_dtype = col.dtype();
                if &new_dtype != old_dtype {
                    let s: Series = col.as_series().unwrap().clone();
                    let cleaned_utf8: StringChunked =
                        s.str()?.apply(|opt| opt.map(|v| Cow::Owned(clean_str(v))));
                    let casted_series: Series = if new_dtype == DataType::Float64 {
                        cleaned_utf8.into_series().cast(&DataType::Float64)?
                    } else {
                        cleaned_utf8.into_series()
                    };
                    df.replace(&col_name, casted_series)?;
                }
            }
        }
    }

    // ─── Log the updated schema (column names and types) ────────────────────────────
    info!("Updated schema after parsing and cleanup:");
    for col_name in df.get_column_names() {
        info!("  {}: {:?}", col_name, df.column(col_name)?.dtype());
    }

    // ─── Step F: WRITE to temporary Parquet ────────────────────────────────────────
    {
        let mut tmp_file = fs::File::create(&tmp_path)?;
        ParquetWriter::new(&mut tmp_file)
            .with_compression(ParquetCompression::Brotli(Some(BrotliLevel::try_new(5)?)))
            .finish(&mut df.clone())?;
    }

    // ─── Step G: ATOMIC RENAME → final .parquet ────────────────────────────────────
    fs::rename(&tmp_path, &final_path)?;
    Ok(())
}
