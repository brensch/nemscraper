use chrono::NaiveDateTime;
use polars::prelude::*;
use std::borrow::Cow;
use std::error::Error;
use std::fs;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info, warn};

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

/// 2) Infer "would-be" dtype from a cleaned string, treating ANY parseable
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

/// 4) Check if any string column's first‐row, once cleaned, would change dtype.
///    Returns true if any columns would change type after cleaning.
fn detect_type_changes(df: &DataFrame) -> PolarsResult<bool> {
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

/// 5) Extract date from filename for partitioning.
///    Looks for common date patterns and returns a standardized date string for partitioning.
fn extract_date_from_filename(filename: &str) -> Option<String> {
    // Look for YYYYMMDD pattern (8 consecutive digits)
    let chars: Vec<char> = filename.chars().collect();
    for i in 0..chars.len().saturating_sub(7) {
        if chars[i..i + 8].iter().all(|c| c.is_ascii_digit()) {
            let date_str: String = chars[i..i + 8].iter().collect();
            // Validate it looks like a reasonable date
            if let (Ok(year), Ok(month), Ok(day)) = (
                date_str[0..4].parse::<u32>(),
                date_str[4..6].parse::<u32>(),
                date_str[6..8].parse::<u32>(),
            ) {
                if year >= 2000
                    && year <= 2030
                    && month >= 1
                    && month <= 12
                    && day >= 1
                    && day <= 31
                {
                    return Some(format!(
                        "{}-{}-{}",
                        &date_str[0..4],
                        &date_str[4..6],
                        &date_str[6..8]
                    ));
                }
            }
        }
    }

    // Look for YYYY-MM-DD or YYYY_MM_DD pattern
    for i in 0..chars.len().saturating_sub(9) {
        if chars.len() >= i + 10 {
            let potential_date: String = chars[i..i + 10].iter().collect();
            if (potential_date.contains('-') || potential_date.contains('_')) {
                let parts: Vec<&str> = potential_date.split(&['-', '_'][..]).collect();
                if parts.len() == 3 {
                    if let (Ok(year), Ok(month), Ok(day)) = (
                        parts[0].parse::<u32>(),
                        parts[1].parse::<u32>(),
                        parts[2].parse::<u32>(),
                    ) {
                        if year >= 2000
                            && year <= 2030
                            && month >= 1
                            && month <= 12
                            && day >= 1
                            && day <= 31
                        {
                            return Some(format!("{:04}-{:02}-{:02}", year, month, day));
                        }
                    }
                }
            }
        }
    }

    None
}

/// 6) Write DataFrame to Parquet file with date-based Hive partitioning.
///    Creates directory structure: table_name/date=YYYY-MM-DD/data.parquet
fn write_partitioned_parquet(
    df: &mut DataFrame,
    file_name: &str,
    out_dir: &Path,
    table_name: &str,
) -> Result<(), Box<dyn Error>> {
    // Extract date from filename for partitioning
    let partition_date = extract_date_from_filename(file_name).unwrap_or_else(|| {
        // Fallback to a default date if none found
        warn!(
            "No date found in filename '{}', using default partition",
            file_name
        );
        "unknown-date".to_string()
    });

    // Create table directory
    let table_dir = out_dir.join(table_name);
    fs::create_dir_all(&table_dir)?;

    // Create date partition directory
    let partition_dir = table_dir.join(format!("date={}", partition_date));
    fs::create_dir_all(&partition_dir)?;

    // Write the entire dataframe to this partition
    write_parquet_file(df, file_name, &partition_dir, 0)?;

    info!(
        "Wrote Parquet file to table '{}' with date partition '{}'",
        table_name, partition_date
    );
    Ok(())
}

/// 7) Write a single Parquet file with atomic rename.
fn write_parquet_file(
    df: &mut DataFrame,
    file_name: &str,
    out_dir: &Path,
    partition_idx: usize,
) -> Result<(), Box<dyn Error>> {
    // Generate unique filename for this partition
    let final_name = if partition_idx == 0 {
        format!("{}.parquet", file_name)
    } else {
        format!("{}_{}.parquet", file_name, partition_idx)
    };
    let tmp_name = format!("{}.tmp", final_name);

    let final_path = out_dir.join(final_name);
    let tmp_path = out_dir.join(tmp_name);

    // Write to temporary Parquet file
    {
        let mut tmp_file = fs::File::create(&tmp_path)?;
        ParquetWriter::new(&mut tmp_file)
            .with_compression(ParquetCompression::Brotli(Some(BrotliLevel::try_new(5)?)))
            .finish(df)?;
    }

    // Atomic rename to final file
    fs::rename(&tmp_path, &final_path)?;

    debug!("Wrote Parquet file: {}", final_path.display());
    Ok(())
}

/// 8) Main CSV→Parquet routine with date-based Hive partitioning:
///
///    - Automatically detect date columns matching "%Y/%m/%d %H:%M:%S" (in sample).
///    - Sample first N rows to infer a provisional schema and identify date columns.
///    - Promote any integerish/Float32 → Float64 in that schema.
///    - Re-read full CSV with .with_dtype_overwrite(...) so no Int64 remains.
///    - For each detected date column, strptime(...) into
///      Datetime(TimeUnit::Milliseconds, Some("+10:00")).
///    - Build table name from column headers 1,2,3 (0-indexed).
///    - Extract date from filename for partitioning.
///    - Apply type cleaning if needed.
///    - Write to Hive-partitioned structure: table_name/date=YYYY-MM-DD/data.parquet
pub fn csv_to_parquet(file_name: &str, data: &str, out_dir: &Path) -> Result<(), Box<dyn Error>> {
    debug!("Starting CSV to Parquet conversion with Hive partitioning");

    // ─── Step A: SAMPLE‐READ (first 1,000 rows) ───────────────────────────────────
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

    // Log forced dtypes paired with column names
    let dtypes_ref: Arc<Vec<DataType>> = Arc::new(forced_dtypes);

    // ─── Step C: FULL‐READ with FORCED‐DTYPE VECTOR ──────────────────────────────────
    let mut full_cursor = Cursor::new(data.as_bytes());
    let full_opts = CsvReadOptions::default()
        .with_has_header(true)
        .with_dtype_overwrite(Some(dtypes_ref.clone()));
    let mut df: DataFrame = CsvReader::new(&mut full_cursor)
        .with_options(full_opts)
        .finish()?; // now ints → Float64; date columns are still String

    // Log the schema after full read
    debug!("Step C schema: {:#?}", df.schema());

    // ─── Step C2: FOR EACH DETECTED DATE COLUMN, PARSE AS DATETIME(GMT+10) ─────────
    for col_name in &date_cols {
        if !df.get_column_names().contains(&col_name) {
            continue; // Skip if the column was not found in the DataFrame
        }
        let col = df.column(col_name)?;
        let utf8_col: &StringChunked = col.str()?;

        let fmt: Option<&str> = Some("%Y/%m/%d %H:%M:%S");
        let tu = TimeUnit::Milliseconds;
        let use_cache = true;
        // we are assuming the date is in GMT+10
        let tz_aware = false;
        let tz: TimeZone = unsafe { TimeZone::from_static("+10:00") };
        let amb = StringChunked::full("ambiguous".into(), "", utf8_col.len());

        let parsed_naive = utf8_col.as_datetime(fmt, tu, use_cache, tz_aware, None, &amb)?;
        let mut parsed_with_tz: DatetimeChunked = parsed_naive;
        parsed_with_tz.set_time_zone(tz)?;

        df.replace(col_name, parsed_with_tz.into_series())?;
        debug!(
            "  Parsed date column {} into Datetime(TimeUnit::Milliseconds, \"+10:00\")",
            col_name
        );
    }

    // ─── Step D: BUILD TABLE NAME FROM COLUMN HEADERS 1,2,3 ─────────────────────────────
    let all_trimmed_names: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|hdr| clean_str(hdr.as_str()))
        .collect();

    let table_name = if all_trimmed_names.len() >= 4 {
        format!(
            "{}---{}---{}",
            all_trimmed_names[1], all_trimmed_names[2], all_trimmed_names[3]
        )
    } else {
        // Fallback if less than 4 columns
        "default_table".to_string()
    };

    debug!("Table name: {}", table_name);

    // ─── Step E: CHECK AND APPLY TYPE CHANGES ────────────────────────────────────────
    let type_changed = detect_type_changes(&df)?;
    if type_changed {
        for col_name in df.get_column_names_owned() {
            if date_cols.contains(&col_name) {
                continue;
            }

            let col = df.column(&col_name)?;
            let str_ca = match col.str() {
                Ok(ca) => ca,
                Err(_) => continue,
            };

            let orig_val = match str_ca.get(0) {
                Some(val) => val,
                None => continue,
            };

            let cleaned_first = clean_str(orig_val);
            let new_dtype = infer_dtype_from_str(&cleaned_first);
            if new_dtype == *col.dtype() {
                continue;
            }

            debug!(
                "Column {}: first-row cleaned dtype {:?} → casting entire column to {:?}",
                col_name,
                col.dtype(),
                new_dtype
            );

            let s: Series = col.as_series().unwrap().clone();
            let cleaned_utf8: StringChunked =
                s.str()?.apply(|opt| opt.map(|v| Cow::Owned(clean_str(v))));

            let casted_series = if new_dtype == DataType::Float64 {
                cleaned_utf8.into_series().cast(&DataType::Float64)?
            } else {
                cleaned_utf8.into_series()
            };

            df.replace(&col_name, casted_series)?;
        }
    }

    // ─── Step F: WRITE DATE-PARTITIONED PARQUET FILES ───────────────────────────────
    write_partitioned_parquet(&mut df, file_name, out_dir, &table_name)?;

    Ok(())
}
