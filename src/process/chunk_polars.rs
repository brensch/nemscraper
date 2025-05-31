use polars::prelude::*;
use std::borrow::Cow;
use std::error::Error;
use std::io::Cursor;
use std::path::Path;

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
/// 1) Reads CSV normally via Polars (using a CsvReadOptions object).
/// 2) Calls `check_first_row_trim`. If it returns `true`, we:
///    a) Print a summary line.
///    b) For *each* string column whose first‐row would change type, we apply
///       “trim+strip‐quotes” to **every** cell in that column, cast it to the new
///       dtype (Int64 or Float64), then replace it in the DataFrame in place.
/// 3) Write the (possibly modified) DataFrame out to Parquet.
pub fn csv_to_parquet(file_name: &str, data: &str, out_dir: &Path) -> Result<(), Box<dyn Error>> {
    // ─── 1) Wrap CSV text in a Cursor ─────────────────────────────────────────────
    let mut cursor = Cursor::new(data.as_bytes());

    // ─── 2) Build CsvReadOptions (header = true) ─────────────────────────────────
    let options = CsvReadOptions::default().with_has_header(true);

    // ─── 3) Read into a DataFrame, letting Polars infer types normally ─────────────
    let mut df: DataFrame = CsvReader::new(&mut cursor).with_options(options).finish()?; // DataFrame is now typed exactly as Polars infers

    // ─── 4) Check if any column’s *first* data‐row would change type after trimming ─
    let type_changed = check_first_row_trim(&df)?;
    if type_changed {
        println!("→ At least one column’s first‐row changed type after trimming.");

        // ─── 5) For each string column, see if its first‐row requires transformation+cast ─
        for col_name in df.get_column_names() {
            // Only transform string columns:
            let column_ref: &Column = df.column(col_name)?;
            if let Ok(str_ca) = column_ref.str() {
                if let Some(orig_val) = str_ca.get(0) {
                    // The exact same “trim‐then‐unquote” logic:
                    let trimmed_ws = orig_val.trim();
                    let trimmed = if trimmed_ws.starts_with('"')
                        && trimmed_ws.ends_with('"')
                        && trimmed_ws.len() >= 2
                    {
                        &trimmed_ws[1..trimmed_ws.len() - 1]
                    } else {
                        trimmed_ws
                    };

                    // Infer the new dtype from the trimmed first‐row:
                    let new_dtype = if trimmed.parse::<i64>().is_ok() {
                        DataType::Int64
                    } else if trimmed.parse::<f64>().is_ok() {
                        DataType::Float64
                    } else {
                        DataType::String
                    };

                    let old_dtype = column_ref.dtype().clone();
                    if old_dtype != new_dtype {
                        // ─── 6) Transform & cast the *entire* column ─────────────────────────────
                        //
                        // a) Obtain an owned `Series` from the Column:
                        let s: Series = column_ref.clone().as_series().unwrap().clone();

                        // b) Apply (trim + strip quotes) to every cell in the Utf8Chunked:
                        //    Return Option<Cow<str>> so we can borrow when possible.
                        let trimmed_utf8: StringChunked = s.str()?.apply(|opt_val| {
                            opt_val.map(|val| {
                                let ws = val.trim();
                                if ws.starts_with('"') && ws.ends_with('"') && ws.len() >= 2 {
                                    // Owned, because we're slicing out of the middle
                                    Cow::Owned(ws[1..ws.len() - 1].to_string())
                                } else {
                                    // Borrowed, just a slice of the original
                                    Cow::Borrowed(ws)
                                }
                            })
                        });

                        // Turn that back into a Series of strings:
                        let trimmed_series: Series = trimmed_utf8.into_series();

                        // c) Cast the trimmed string‐Series into the `new_dtype`:
                        let casted_series: Series = trimmed_series.cast(&new_dtype)?;

                        // d) Replace the column in the DataFrame:
                        df.replace(col_name, casted_series)?;
                    }
                }
            }
        }
    } else {
        println!("→ No first‐row type changes detected; no in‐place transform needed.");
    }

    // ─── 7) Write the (possibly modified) DataFrame to Parquet ───────────────────
    let out_path = out_dir.join(format!("{}.parquet", file_name));
    let mut file = std::fs::File::create(out_path)?;
    ParquetWriter::new(&mut file)
        .with_compression(ParquetCompression::Snappy)
        .finish(&mut df.clone())?;

    Ok(())
}
