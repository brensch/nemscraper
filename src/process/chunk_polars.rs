use polars::prelude::*;
use std::borrow::Cow;
use std::error::Error;
use std::fs;
use std::io::Cursor;
use std::path::Path;

// Import PlSmallStr so we can annotate the closure parameter.
use polars::prelude::PlSmallStr;

/// Reads `df` (the full DataFrame), examines each *string* column’s first‐row value,
/// and does two things in this order:
///   1) `trim()` (remove leading/trailing whitespace)
///   2) if the remaining text starts & ends with `"`, strip those outer quotes
/// After that, attempt to infer “is it an integer? a float? or just a string?”  
/// Compare that “would‐be” dtype against `series.dtype()`.  
/// If *any* column’s first‐row trimmed value would produce a different dtype than
/// Polars originally inferred, return `Ok(true)`. Otherwise return `Ok(false)`.
fn check_first_row_trim(df: &DataFrame) -> PolarsResult<bool> {
    let mut any_type_change = false;

    for col_name in df.get_column_names() {
        let column_ref: &Column = df.column(col_name)?;
        // Only look at this column if it is a string column:
        if let Ok(str_ca) = column_ref.str() {
            // If there is a non-null first‐row value, get it:
            if let Some(orig_val) = str_ca.get(0) {
                // 1) Trim whitespace
                let trimmed_ws = orig_val.trim();
                // 2) Strip outer quotes if present
                let trimmed = if trimmed_ws.starts_with('"')
                    && trimmed_ws.ends_with('"')
                    && trimmed_ws.len() >= 2
                {
                    &trimmed_ws[1..trimmed_ws.len() - 1]
                } else {
                    trimmed_ws
                };

                // Infer the “would‐be” dtype from this trimmed string:
                let new_dtype = if trimmed.parse::<i64>().is_ok() {
                    DataType::Int64
                } else if trimmed.parse::<f64>().is_ok() {
                    DataType::Float64
                } else {
                    DataType::String
                };

                let old_dtype = column_ref.dtype();
                if old_dtype != &new_dtype {
                    println!(
                        "Column '{}' first‐row WOULD change type: \
                         {:?} (orig=\"{}\") → {:?} (trimmed=\"{}\")",
                        col_name, old_dtype, orig_val, new_dtype, trimmed
                    );
                    any_type_change = true;
                }

                // Also print if the trimmed text changed compared to orig_val:
                if trimmed != orig_val {
                    println!(
                        "  (and the trimmed text itself changed: \"{}\" → \"{}\")",
                        orig_val, trimmed
                    );
                }
            }
        }
    }

    Ok(any_type_change)
}

/// Example function:
/// 1) Reads CSV normally via Polars (using a CsvReadOptions object) into a DataFrame.
/// 2) Extracts the 2nd, 3rd, and 4th column names from `df.get_column_names()` (indices 1,2,3),
///    trims each name, concatenates them with underscores as a prefix for filenames.
///    If there are fewer than 4 columns, it falls back to using just `file_name`.
/// 3) Calls `check_first_row_trim`. If it returns `true`, transforms affected columns in place.
/// 4) Writes the (possibly modified) DataFrame out to `<prefix>_<file_name>.parquet.tmp`,
///    then renames it to `<prefix>_<file_name>.parquet`.
pub fn csv_to_parquet(file_name: &str, data: &str, out_dir: &Path) -> Result<(), Box<dyn Error>> {
    // ─── 1) Read into a DataFrame so we know Polars‐inferred column names ───────────
    let mut cursor = Cursor::new(data.as_bytes());
    let options = CsvReadOptions::default()
        .with_has_header(true)
        .with_infer_schema_length(Some(100000));
    let mut df: DataFrame = CsvReader::new(&mut cursor).with_options(options).finish()?; // Polars has inferred dtypes and column names

    // ─── 2) Build `<prefix>` from columns 1,2,3 if available ─────────────────────
    //
    // Convert each &PlSmallStr → &str via as_str(), then trim and to_string():
    let all_trimmed_names: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|s: &&PlSmallStr| s.as_str().trim().to_string())
        .collect();
    let prefix = if all_trimmed_names.len() >= 4 {
        format!(
            "{}_{}_{}",
            all_trimmed_names[1], all_trimmed_names[2], all_trimmed_names[3]
        )
    } else {
        String::new()
    };

    // Construct the final and temporary filenames:
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

    // ─── 3) Check if any column’s *first* data‐row would change type after trimming ─
    let type_changed = check_first_row_trim(&df)?;
    if type_changed {
        println!("→ At least one column’s first‐row changed type after trimming.");

        // Collect column names into Vec<String> so we can mutate `df` inside the loop:
        let col_names: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();

        // ─── 4) For each string column, transform & cast if needed ─────────────────
        for col_name in col_names {
            let column_ref: &Column = df.column(&col_name)?;
            if let Ok(str_ca) = column_ref.str() {
                if let Some(orig_val) = str_ca.get(0) {
                    // Trim + strip outer quotes on row 0
                    let trimmed_ws = orig_val.trim();
                    let trimmed = if trimmed_ws.starts_with('"')
                        && trimmed_ws.ends_with('"')
                        && trimmed_ws.len() >= 2
                    {
                        &trimmed_ws[1..trimmed_ws.len() - 1]
                    } else {
                        trimmed_ws
                    };

                    // Infer new dtype from trimmed first‐row
                    let new_dtype = if trimmed.parse::<i64>().is_ok() {
                        DataType::Int64
                    } else if trimmed.parse::<f64>().is_ok() {
                        DataType::Float64
                    } else {
                        DataType::String
                    };

                    let old_dtype = column_ref.dtype().clone();
                    if old_dtype != new_dtype {
                        // a) Obtain an owned Series:
                        let s: Series = column_ref.clone().as_series().unwrap().clone();

                        // b) Apply trim+strip‐quotes to every cell (Option<Cow<str>>)
                        let trimmed_utf8: StringChunked = s.str()?.apply(|opt_val| {
                            opt_val.map(|val| {
                                let ws = val.trim();
                                if ws.starts_with('"') && ws.ends_with('"') && ws.len() >= 2 {
                                    Cow::Owned(ws[1..ws.len() - 1].to_string())
                                } else {
                                    Cow::Borrowed(ws)
                                }
                            })
                        });

                        // c) Convert to Series of strings
                        let trimmed_series: Series = trimmed_utf8.into_series();

                        // d) Cast to new dtype
                        let casted_series: Series = trimmed_series.cast(&new_dtype)?;

                        // e) Replace in DataFrame
                        df.replace(&col_name, casted_series)?;
                    }
                }
            }
        }
    }

    // ─── 5) Write to the temporary `.parquet.tmp` file ───────────────────────────
    {
        let mut tmp_file = fs::File::create(&tmp_path)?;
        ParquetWriter::new(&mut tmp_file)
            .with_compression(ParquetCompression::Brotli(Some(
                BrotliLevel::try_new(5).unwrap(),
            )))
            .finish(&mut df.clone())?;
    }

    // ─── 6) Atomically rename `.parquet.tmp` → `.parquet` ────────────────────────
    fs::rename(&tmp_path, &final_path)?;

    Ok(())
}
