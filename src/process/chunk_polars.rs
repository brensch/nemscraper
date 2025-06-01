use polars::prelude::*;
use std::borrow::Cow;
use std::error::Error;
use std::fs;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

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

/// 4) After forcing schema, check if any string column’s first‐row, once cleaned,
///    would change dtype (String → Float64). Return true if any do.
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
///    - Sample first N rows, let Polars infer a schema
///    - Promote any integerish or Float32 columns to Float64 in that schema
///    - Re-read the entire CSV with the forced schema so no Int64 remains
///    - If any String column’s first row, once cleaned, “looks numeric,” do a full-column
///      clean_str(...) → cast to Float64 (or leave as String) pass
///    - Write to `<prefix>_<file>.parquet.tmp` → rename to `.parquet`
pub fn csv_to_parquet(file_name: &str, data: &str, out_dir: &Path) -> Result<(), Box<dyn Error>> {
    // ─── Step A: SAMPLE READ (first 1,000 rows) ───────────────────────────────────
    let sample_size = 1000;
    let mut sample_cursor = Cursor::new(data.as_bytes());
    let sample_opts = CsvReadOptions::default()
        .with_has_header(true)
        .with_n_rows(Some(sample_size));
    let sample_df: DataFrame = CsvReader::new(&mut sample_cursor)
        .with_options(sample_opts)
        .finish()?;

    // ─── Step B: BUILD A FORCED-DTYPE VECTOR, PROMOTING ANY INTEGERISH → Float64 ─────
    let mut forced_dtypes: Vec<DataType> = Vec::with_capacity(sample_df.width());
    for s in sample_df.get_columns() {
        let orig_dtype = s.dtype();
        let forced_dtype = promote_sample_dtype(orig_dtype);
        forced_dtypes.push(forced_dtype);
    }
    let dtypes_ref: Arc<Vec<DataType>> = Arc::new(forced_dtypes);

    // ─── Step C: FULL READ with FORCED DTYPE VECTOR ─────────────────────────────────
    let mut full_cursor = Cursor::new(data.as_bytes());
    let full_opts = CsvReadOptions::default()
        .with_has_header(true)
        .with_dtype_overwrite(Some(dtypes_ref.clone()));
    let mut df: DataFrame = CsvReader::new(&mut full_cursor)
        .with_options(full_opts)
        .finish()?; // now any integer columns are Float64

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

        // Collect column names so we can replace in place
        let col_names: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        for col_name in col_names {
            let col = df.column(&col_name)?;
            // Only transform columns whose dtype is String
            let str_ca = match col.str() {
                Ok(ca) => ca,
                Err(_) => continue,
            };

            if let Some(orig_val) = str_ca.get(0) {
                let cleaned_first = clean_str(orig_val);
                let new_dtype = infer_dtype_from_str(&cleaned_first);
                let old_dtype = col.dtype();

                if &new_dtype != old_dtype {
                    // a) Clone the Series for full‐column work
                    let s: Series = col.as_series().unwrap().clone();

                    // b) Apply clean_str(...) to every cell, producing a StringChunked
                    let cleaned_utf8: StringChunked =
                        s.str()?.apply(|opt| opt.map(|v| Cow::Owned(clean_str(v))));

                    // c) Convert to Series, then cast if needed
                    let casted_series: Series = if new_dtype == DataType::Float64 {
                        cleaned_utf8.into_series().cast(&DataType::Float64)?
                    } else {
                        cleaned_utf8.into_series()
                    };

                    // d) Replace that column in df
                    df.replace(&col_name, casted_series)?;
                }
            }
        }
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
