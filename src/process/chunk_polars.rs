use chrono::NaiveDateTime;
use polars::prelude::*;
use std::borrow::Cow;
use std::error::Error;
use std::fs;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info};

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
///      `Datetime(TimeUnit::Milliseconds, Some("+10:00"))`.
///    - Build filename prefix from headers 1,2,3.
///    - If any string column’s first row (after `clean_str`) “looks numeric,” do a full‐column
///      `clean_str(...)` → cast to Float64 (or leave as String).
///    - Write to `<prefix>_<file>.parquet.tmp` → rename to `.parquet`.
pub fn csv_to_parquet(file_name: &str, data: &str, out_dir: &Path) -> Result<(), Box<dyn Error>> {
    debug!("starting");
    // ─── Step A: SAMPLE‐READ (first 1,000 rows) ───────────────────────────────────
    let step_a_start = Instant::now();
    let sample_size = 1000;
    let mut sample_cursor = Cursor::new(data.as_bytes());

    let sample_opts = CsvReadOptions::default()
        .with_has_header(true)
        .with_n_rows(Some(sample_size))
        .with_infer_schema_length(Some(sample_size))
        .with_ignore_errors(true);
    let sample_df: DataFrame = CsvReader::new(&mut sample_cursor)
        .with_options(sample_opts)
        .finish()?;
    let step_a_elapsed = step_a_start.elapsed();
    debug!(
        "Step A (sample-read {} rows) took {:.3}s",
        sample_size,
        step_a_elapsed.as_secs_f64()
    );
    // Log the schema of the sampled DataFrame
    debug!("Step A schema: {:#?}", sample_df.schema());

    // ─── Step B: BUILD A FORCED‐DTYPE VECTOR AND IDENTIFY DATE COLUMNS ─────────────
    let step_b_start = Instant::now();
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

    let step_b_elapsed = step_b_start.elapsed();
    debug!(
        "Step B (forced dtypes + identify date columns) took {:.3}s; found {} date columns",
        step_b_elapsed.as_secs_f64(),
        date_cols.len()
    );
    // Log forced dtypes paired with column names
    let column_names = sample_df.get_column_names();
    for (name, dt) in column_names.iter().zip(forced_dtypes.iter()) {
        debug!("  Column `{}` → forced dtype {:?}", name, dt);
    }
    debug!("Step B date columns: {:?}", date_cols);

    let dtypes_ref: Arc<Vec<DataType>> = Arc::new(forced_dtypes);

    // ─── Step C: FULL‐READ with FORCED‐DTYPE VECTOR ──────────────────────────────────
    let step_c_start = Instant::now();
    let mut full_cursor = Cursor::new(data.as_bytes());
    let full_opts = CsvReadOptions::default()
        .with_has_header(true)
        .with_dtype_overwrite(Some(dtypes_ref.clone()));
    let mut df: DataFrame = CsvReader::new(&mut full_cursor)
        .with_options(full_opts)
        .finish()?; // now ints → Float64; date columns are still String
    let step_c_elapsed = step_c_start.elapsed();
    debug!(
        "Step C (full-read with forced dtypes) took {:.3}s; DataFrame shape: {}x{}",
        step_c_elapsed.as_secs_f64(),
        df.height(),
        df.width()
    );
    // Log the schema after full read
    debug!("Step C schema: {:#?}", df.schema());

    // ─── Step C2: FOR EACH DETECTED DATE COLUMN, PARSE AS DATETIME(GMT+10) ─────────
    let step_c2_start = Instant::now();
    for col_name in &date_cols {
        if df.get_column_names().contains(&col_name) {
            let col = df.column(col_name)?;
            let utf8_col: &StringChunked = col.str()?;

            let fmt: Option<&str> = Some("%Y/%m/%d %H:%M:%S");
            let tu = TimeUnit::Milliseconds;
            let use_cache = true;
            let tz_aware = true;
            let tz: TimeZone = unsafe { TimeZone::from_static("+10:00") };
            let amb = StringChunked::full("ambiguous".into(), "", utf8_col.len());

            let parsed = utf8_col.as_datetime(fmt, tu, use_cache, tz_aware, Some(&tz), &amb)?;
            df.replace(col_name, parsed.into_series())?;
            debug!(
                "  Parsed date column `{}` into Datetime(TimeUnit::Milliseconds, \"+10:00\")",
                col_name
            );
        }
    }
    let step_c2_elapsed = step_c2_start.elapsed();
    debug!(
        "Step C2 (parse date columns: {:?}) took {:.3}s",
        date_cols,
        step_c2_elapsed.as_secs_f64()
    );
    // Log the schema after date parsing
    debug!("Step C2 schema: {:#?}", df.schema());

    // ─── Step D: BUILD PREFIX FROM COLUMN HEADERS 1,2,3 ─────────────────────────────
    let step_d_start = Instant::now();
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
    let step_d_elapsed = step_d_start.elapsed();
    debug!(
        "Step D (build filename prefix) took {:.3}s; prefix=\"{}\"",
        step_d_elapsed.as_secs_f64(),
        prefix
    );

    // ─── Step E: CHECK FIRST‐ROW TRIM FOR STRING COLUMNS ────────────────────────────
    let step_e_start = Instant::now();
    let type_changed = check_first_row_trim(&df)?;
    if type_changed {
        debug!("Step E: first-row trim changed types; applying full-column clean/cast");
        for col_name in df.get_column_names_owned() {
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
                    debug!(
                        "  Column `{}`: first-row cleaned dtype {:?} → casting entire column to {:?}",
                        col_name, old_dtype, new_dtype
                    );
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
    let step_e_elapsed = step_e_start.elapsed();
    debug!(
        "Step E (check and apply first-row trim) took {:.3}s; type_changed={}",
        step_e_elapsed.as_secs_f64(),
        type_changed
    );
    // Log the schema after potential full-column casts
    debug!("Step E schema: {:#?}", df.schema());

    // ─── Step F: WRITE to temporary Parquet ────────────────────────────────────────
    let step_f_start = Instant::now();
    {
        let mut tmp_file = fs::File::create(&tmp_path)?;
        ParquetWriter::new(&mut tmp_file)
            .with_compression(ParquetCompression::Brotli(Some(BrotliLevel::try_new(5)?)))
            .finish(&mut df.clone())?;
    }
    let step_f_elapsed = step_f_start.elapsed();
    debug!(
        "Step F (write to temp Parquet: {}) took {:.3}s",
        tmp_path.display(),
        step_f_elapsed.as_secs_f64()
    );

    // ─── Step G: ATOMIC RENAME → final .parquet ────────────────────────────────────
    let step_g_start = Instant::now();
    fs::rename(&tmp_path, &final_path)?;
    let step_g_elapsed = step_g_start.elapsed();
    debug!(
        "Step G (rename {} → {}) took {:.3}s",
        tmp_path.display(),
        final_path.display(),
        step_g_elapsed.as_secs_f64()
    );

    info!("Wrote final Parquet: {}", final_path.display());
    Ok(())
}
