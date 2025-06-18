// src/main.rs
use anyhow::{Context, Result};
use arrow::array::*;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use clap::Parser;
use nemscraper::process::split::split_zip_to_parquet;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use tracing::{info, warn};
use walkdir::WalkDir;
use zip::ZipArchive;

#[derive(Parser)]
#[command(name = "csv-parquet-test")]
#[command(about = "Test binary for converting a zip file to Parquet with detailed verification")]
struct Args {
    /// Input zip file path
    zip_file: PathBuf,

    /// Output directory for Parquet files (default: ./parquet_output)
    #[arg(short, long, default_value = "parquet_output")]
    output: PathBuf,

    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,

    /// Show detailed column analysis for each parquet file
    #[arg(long)]
    detailed_columns: bool,
}

#[derive(Default, Debug)]
struct ParquetStats {
    total_files: usize,
    total_rows: u64,
    total_size_bytes: u64,
    tables_found: HashMap<String, TableStats>,
}

#[derive(Default, Debug)]
struct TableStats {
    files: usize,
    rows: u64,
    size_bytes: u64,
    partitions: HashMap<String, PartitionStats>,
}

#[derive(Default, Debug)]
struct PartitionStats {
    files: usize,
    rows: u64,
    size_bytes: u64,
}

#[derive(Debug)]
struct ColumnInfo {
    name: String,
    data_type: DataType,
    first_non_empty_value: Option<String>,
    sample_row_index: Option<usize>,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging
    let log_level = if args.verbose { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(format!("csv_parquet_test={}", log_level))
        .init();

    info!("Starting CSV to Parquet conversion test");
    info!("Input zip: {}", args.zip_file.display());
    info!("Output directory: {}", args.output.display());

    // Validate input file exists
    if !args.zip_file.exists() {
        return Err(anyhow::anyhow!(
            "Input zip file does not exist: {}",
            args.zip_file.display()
        ));
    }

    // Create output directory
    fs::create_dir_all(&args.output).context("Failed to create output directory")?;

    // Step 1: Count 'D' rows in the original ZIP for verification
    info!("Counting 'D' rows in original ZIP file...");
    let original_d_count =
        count_d_rows_in_zip(&args.zip_file).context("Failed to count D rows in ZIP file")?;
    info!("Found {} 'D' rows in original ZIP", original_d_count);

    // Step 2: Run the conversion using your existing function
    info!("Converting CSV files to Parquet...");
    // make an output directory just for this zip file
    let output_dir = args.output.join(
        args.zip_file
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("output"),
    );
    let conversion_result = split_zip_to_parquet(&args.zip_file, &output_dir)
        .context("Failed to convert CSV files to Parquet")?;

    info!("Conversion completed!");
    info!(
        "Conversion summary: {} rows, {} bytes",
        conversion_result.rows, conversion_result.bytes
    );

    // Step 3: Analyze the output parquet files
    info!("Analyzing generated Parquet files...");
    let (parquet_stats, type_issues) = analyze_parquet_files(&output_dir, args.detailed_columns)
        .context("Failed to analyze generated Parquet files")?;

    // Step 4: Print comprehensive statistics and validation
    let validation_passed = print_comprehensive_stats(
        &conversion_result,
        &parquet_stats,
        original_d_count,
        &type_issues,
    );

    // remove the output directory after analysis
    fs::remove_dir_all(&output_dir).with_context(|| {
        format!(
            "Failed to remove output directory '{}'",
            output_dir.display()
        )
    })?;

    // Exit with error code if validations failed
    if !validation_passed {
        warn!("Validation failures detected!");
        std::process::exit(1);
    }

    info!("All validations passed successfully!");
    Ok(())
}

fn count_d_rows_in_zip(zip_path: &Path) -> Result<usize> {
    let file = File::open(zip_path)
        .with_context(|| format!("Failed to open ZIP '{}'", zip_path.display()))?;
    let mut archive = ZipArchive::new(file)
        .with_context(|| format!("Failed to read ZIP '{}'", zip_path.display()))?;

    let mut total_d_count = 0;

    for i in 0..archive.len() {
        let entry = archive
            .by_index(i)
            .with_context(|| format!("Failed to access entry {} in '{}'", i, zip_path.display()))?;

        let name_lower = entry.name().to_lowercase();
        if !name_lower.ends_with(".csv") {
            continue;
        }

        let reader = BufReader::new(entry);
        let mut file_d_count = 0;

        for line_result in reader.lines() {
            let line = line_result.with_context(|| "Failed to read a CSV line")?;
            if line.chars().next() == Some('D') {
                file_d_count += 1;
            }
        }

        if file_d_count > 0 {
            info!(
                "File {}: {} 'D' rows",
                archive.by_index(i)?.name(),
                file_d_count
            );
        }
        total_d_count += file_d_count;
    }

    Ok(total_d_count)
}

#[derive(Debug)]
struct TypeIssue {
    file_path: String,
    column_name: String,
    stored_type: String,
    sample_value: String,
    suggested_type: String,
}

fn analyze_parquet_files(
    output_dir: &Path,
    detailed_columns: bool,
) -> Result<(ParquetStats, Vec<TypeIssue>)> {
    let mut stats = ParquetStats::default();
    let mut type_issues = Vec::new();

    // Collect all parquet files
    let mut parquet_files = Vec::new();
    for entry in WalkDir::new(output_dir) {
        let entry = entry.context("Failed to read directory entry")?;
        let path = entry.path();

        if path.extension().map_or(false, |ext| ext == "parquet") {
            parquet_files.push(path.to_path_buf());
        }
    }

    info!("Found {} parquet files to analyze", parquet_files.len());

    // Process files in parallel for basic stats
    let file_stats: Vec<(PathBuf, u64, u64, String, String)> = parquet_files
        .par_iter()
        .filter_map(|path| match analyze_single_parquet_file(path, output_dir) {
            Ok((row_count, file_size, table_name, partition_name)) => Some((
                path.clone(),
                row_count,
                file_size,
                table_name,
                partition_name,
            )),
            Err(e) => {
                warn!("Failed to analyze {}: {}", path.display(), e);
                None
            }
        })
        .collect();

    // Aggregate stats
    for (path, row_count, file_size, table_name, partition_name) in file_stats {
        stats.total_files += 1;
        stats.total_rows += row_count;
        stats.total_size_bytes += file_size;

        let table_stats = stats.tables_found.entry(table_name.clone()).or_default();
        table_stats.files += 1;
        table_stats.rows += row_count;
        table_stats.size_bytes += file_size;

        let partition_stats = table_stats
            .partitions
            .entry(partition_name.clone())
            .or_default();
        partition_stats.files += 1;
        partition_stats.rows += row_count;
        partition_stats.size_bytes += file_size;

        // Always show column details for first few files to give user insight
        if detailed_columns || stats.total_files < 3 {
            if let Ok(file_type_issues) = show_column_details(&path) {
                type_issues.extend(file_type_issues);
            } else {
                warn!(
                    "Failed to show column details for {}: check file",
                    path.display()
                );
            }
        }
    }

    Ok((stats, type_issues))
}

fn analyze_single_parquet_file(path: &Path, base_dir: &Path) -> Result<(u64, u64, String, String)> {
    // Get file size
    let file_size = fs::metadata(path)
        .context("Failed to get file metadata")?
        .len();

    // Read parquet file to get row count
    let file = File::open(path)
        .with_context(|| format!("Failed to open parquet file: {}", path.display()))?;

    let reader = SerializedFileReader::new(file)
        .with_context(|| format!("Failed to create parquet reader for: {}", path.display()))?;

    let row_count = reader.metadata().file_metadata().num_rows() as u64;

    // Parse path structure
    let (table_name, partition_name) = parse_path_structure(base_dir, path);

    Ok((row_count, file_size, table_name, partition_name))
}

fn show_column_details(path: &Path) -> Result<Vec<TypeIssue>> {
    println!(
        "\n--- Column Analysis for {} ---",
        path.file_name().unwrap().to_string_lossy()
    );

    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone(); // Clone the schema to avoid borrow issues

    println!("Schema fields: {}", schema.fields().len());

    // Build the reader
    let mut reader = builder.build()?;

    // Collect column info
    let mut column_info: Vec<ColumnInfo> = schema
        .fields()
        .iter()
        .map(|field| ColumnInfo {
            name: field.name().clone(),
            data_type: field.data_type().clone(),
            first_non_empty_value: None,
            sample_row_index: None,
        })
        .collect();

    // Read batches to find first non-empty values
    let mut row_offset = 0;
    let mut batches_scanned = 0;
    const MAX_BATCHES_TO_SCAN: usize = 10; // Limit scanning to avoid reading entire large files

    while let Some(batch) = reader.next() {
        let batch = batch?;
        batches_scanned += 1;

        // Scan this batch for first non-empty values
        let all_found = scan_batch_for_first_values(&batch, &mut column_info, row_offset);

        row_offset += batch.num_rows();

        // Stop if we found all values or scanned enough batches
        if all_found || batches_scanned >= MAX_BATCHES_TO_SCAN {
            break;
        }
    }

    // Print results and collect type issues
    println!(
        "{:<30} {:<25} {:<15} {}",
        "Column", "Type", "Sample Row", "First Non-Empty Value"
    );
    println!("{:-<100}", "");

    let mut type_issues = Vec::new();
    let file_name = path.file_name().unwrap().to_string_lossy().to_string();

    for col_info in &column_info {
        let sample_row = col_info
            .sample_row_index
            .map_or("N/A".to_string(), |i| i.to_string());
        let value = col_info
            .first_non_empty_value
            .as_deref()
            .unwrap_or("(not found)");
        let truncated_value = if value.len() > 40 {
            format!("{}...", &value[..37])
        } else {
            value.to_string()
        };

        println!(
            "{:<30} {:<25} {:<15} {}",
            col_info.name,
            format!("{:?}", col_info.data_type),
            sample_row,
            truncated_value
        );

        // Check for type issues: UTF8 columns that contain numeric values
        if matches!(col_info.data_type, DataType::Utf8) {
            if let Some(ref value) = col_info.first_non_empty_value {
                let trimmed = value.trim();
                if !trimmed.is_empty() {
                    // Check if this looks like it should be a Float64
                    if trimmed.parse::<f64>().is_ok() && !trimmed.parse::<i64>().is_ok() {
                        type_issues.push(TypeIssue {
                            file_path: file_name.clone(),
                            column_name: col_info.name.clone(),
                            stored_type: "Utf8".to_string(),
                            sample_value: trimmed.to_string(),
                            suggested_type: "Float64".to_string(),
                        });
                    }
                    // Check if this looks like it should be an Int64
                    else if trimmed.parse::<i64>().is_ok() {
                        type_issues.push(TypeIssue {
                            file_path: file_name.clone(),
                            column_name: col_info.name.clone(),
                            stored_type: "Utf8".to_string(),
                            sample_value: trimmed.to_string(),
                            suggested_type: "Int64".to_string(),
                        });
                    }
                }
            }
        }
    }

    println!(
        "Scanned {} batches, {} total rows",
        batches_scanned, row_offset
    );

    Ok(type_issues)
}

fn scan_batch_for_first_values(
    batch: &RecordBatch,
    column_info: &mut [ColumnInfo],
    row_offset: usize,
) -> bool {
    let mut columns_still_searching = 0;

    for (col_idx, col_info) in column_info.iter_mut().enumerate() {
        // Skip if we already found a value for this column
        if col_info.first_non_empty_value.is_some() {
            continue;
        }

        columns_still_searching += 1;
        let array = batch.column(col_idx);

        // Scan through all rows in this batch for this column
        for row_idx in 0..array.len() {
            // Skip null values
            if array.is_null(row_idx) {
                continue;
            }

            // Extract the value as a string
            let value_str = extract_array_value(array, row_idx);

            // Check if it's non-empty after trimming whitespace
            let trimmed = value_str.trim();
            if !trimmed.is_empty() && trimmed != "null" && trimmed != "NULL" {
                col_info.first_non_empty_value = Some(value_str);
                col_info.sample_row_index = Some(row_offset + row_idx);
                columns_still_searching -= 1;
                break; // Found a value for this column, move to next column
            }
        }
    }

    // Return true if all columns now have values (no more searching needed)
    columns_still_searching == 0
}

fn extract_array_value(array: &dyn Array, index: usize) -> String {
    use arrow::datatypes::DataType;

    match array.data_type() {
        DataType::Utf8 => {
            if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
                string_array.value(index).to_string()
            } else {
                "N/A".to_string()
            }
        }
        DataType::Float64 => {
            if let Some(float_array) = array.as_any().downcast_ref::<Float64Array>() {
                float_array.value(index).to_string()
            } else {
                "N/A".to_string()
            }
        }
        DataType::Float32 => {
            if let Some(float_array) = array.as_any().downcast_ref::<Float32Array>() {
                float_array.value(index).to_string()
            } else {
                "N/A".to_string()
            }
        }
        DataType::Int64 => {
            if let Some(int_array) = array.as_any().downcast_ref::<Int64Array>() {
                int_array.value(index).to_string()
            } else {
                "N/A".to_string()
            }
        }
        DataType::Int32 => {
            if let Some(int_array) = array.as_any().downcast_ref::<Int32Array>() {
                int_array.value(index).to_string()
            } else {
                "N/A".to_string()
            }
        }
        DataType::Timestamp(unit, tz) => {
            match unit {
                arrow::datatypes::TimeUnit::Millisecond => {
                    if let Some(ts_array) =
                        array.as_any().downcast_ref::<TimestampMillisecondArray>()
                    {
                        let timestamp = ts_array.value(index);
                        // Convert to readable date format
                        if let Some(tz_str) = tz {
                            format!("{}ms ({})", timestamp, tz_str)
                        } else {
                            format!("{}ms", timestamp)
                        }
                    } else {
                        "N/A".to_string()
                    }
                }
                _ => format!("Timestamp({:?})", unit),
            }
        }
        DataType::Boolean => {
            if let Some(bool_array) = array.as_any().downcast_ref::<BooleanArray>() {
                bool_array.value(index).to_string()
            } else {
                "N/A".to_string()
            }
        }
        DataType::Date32 => {
            if let Some(date_array) = array.as_any().downcast_ref::<Date32Array>() {
                date_array.value(index).to_string()
            } else {
                "N/A".to_string()
            }
        }
        _ => format!("(unsupported type: {:?})", array.data_type()),
    }
}

fn parse_path_structure(base_dir: &Path, file_path: &Path) -> (String, String) {
    let relative_path = file_path.strip_prefix(base_dir).unwrap_or(file_path);
    let components: Vec<_> = relative_path.components().collect();

    if components.len() >= 3 {
        let table_name = components[0].as_os_str().to_string_lossy().to_string();
        let partition_name = components[1].as_os_str().to_string_lossy().to_string();
        (table_name, partition_name)
    } else if components.len() >= 2 {
        let table_name = components[0].as_os_str().to_string_lossy().to_string();
        (table_name, "default".to_string())
    } else {
        ("default_table".to_string(), "default".to_string())
    }
}

fn print_comprehensive_stats(
    conversion_result: &nemscraper::process::split::RowsAndBytes,
    parquet_stats: &ParquetStats,
    original_d_count: usize,
    type_issues: &[TypeIssue],
) -> bool {
    println!("\n=== COMPREHENSIVE ANALYSIS ===");

    // Summary table like the verify function
    let delta_parquet = parquet_stats.total_rows as isize - original_d_count as isize;
    let delta_conversion = conversion_result.rows as isize - original_d_count as isize;

    println!(
        "\n{:<25} {:>15} {:>15}",
        "Source", "Row Count", "Delta vs ZIP"
    );
    println!("{:-<55}", "");
    println!(
        "{:<25} {:>15} {:>15}",
        "Original ZIP ('D' rows)", original_d_count, 0
    );
    println!(
        "{:<25} {:>15} {:>15}",
        "Conversion Result", conversion_result.rows, delta_conversion
    );
    println!(
        "{:<25} {:>15} {:>15}",
        "Parquet Analysis", parquet_stats.total_rows, delta_parquet
    );

    // Note about the conversion difference
    if delta_conversion != 0 {
        println!(
            "\n⚠️  NOTE: Conversion count differs from ZIP by {}.",
            delta_conversion
        );
        println!("   This might indicate the conversion is counting 'I,' header rows.");
        println!("   Check your CsvBatchProcessor.feed_line() logic.");
    }

    println!("\n=== PARQUET DETAILS ===");
    println!("Total files: {}", parquet_stats.total_files);
    println!(
        "Total size: {} bytes ({:.2} MB)",
        parquet_stats.total_size_bytes,
        parquet_stats.total_size_bytes as f64 / 1_048_576.0
    );

    if parquet_stats.total_files > 0 {
        println!(
            "Average rows per file: {:.0}",
            parquet_stats.total_rows as f64 / parquet_stats.total_files as f64
        );
        println!(
            "Average file size: {:.2} MB",
            parquet_stats.total_size_bytes as f64 / parquet_stats.total_files as f64 / 1_048_576.0
        );
    }

    println!("\n=== TABLE BREAKDOWN ===");
    for (table_name, table_stats) in &parquet_stats.tables_found {
        println!("Table: {}", table_name);
        println!("  Files: {}", table_stats.files);
        println!("  Rows: {}", table_stats.rows);
        println!(
            "  Size: {} bytes ({:.2} MB)",
            table_stats.size_bytes,
            table_stats.size_bytes as f64 / 1_048_576.0
        );

        if table_stats.partitions.len() > 1 {
            println!("  Partitions:");
            for (partition_name, partition_stats) in &table_stats.partitions {
                println!(
                    "    {}: {} files, {} rows, {:.2} MB",
                    partition_name,
                    partition_stats.files,
                    partition_stats.rows,
                    partition_stats.size_bytes as f64 / 1_048_576.0
                );
            }
        }
        println!();
    }

    // Type issues section
    if !type_issues.is_empty() {
        println!("=== TYPE INFERENCE ISSUES ===");
        println!(
            "Found {} columns that may have incorrect types:",
            type_issues.len()
        );
        println!(
            "{:<20} {:<30} {:<15} {:<20} {}",
            "File", "Column", "Current Type", "Sample Value", "Suggested Type"
        );
        println!("{:-<100}", "");

        for issue in type_issues {
            println!(
                "{:<20} {:<30} {:<15} {:<20} {}",
                issue.file_path,
                issue.column_name,
                issue.stored_type,
                issue.sample_value,
                issue.suggested_type
            );
        }
        println!();
    }

    // Validation
    println!("=== VALIDATION ===");
    let conversion_matches = conversion_result.rows == original_d_count as u64;
    let parquet_matches = parquet_stats.total_rows == original_d_count as u64;
    let conversion_parquet_match = conversion_result.rows == parquet_stats.total_rows;
    let no_type_issues = type_issues.is_empty();

    println!(
        "Conversion vs ZIP: {} ({} vs {})",
        if conversion_matches {
            "✅ MATCH"
        } else {
            "❌ MISMATCH"
        },
        conversion_result.rows,
        original_d_count
    );

    println!(
        "Parquet vs ZIP: {} ({} vs {})",
        if parquet_matches {
            "✅ MATCH"
        } else {
            "❌ MISMATCH"
        },
        parquet_stats.total_rows,
        original_d_count
    );

    println!(
        "Conversion vs Parquet: {} ({} vs {})",
        if conversion_parquet_match {
            "✅ MATCH"
        } else {
            "❌ MISMATCH"
        },
        conversion_result.rows,
        parquet_stats.total_rows
    );

    println!(
        "Type Inference: {} ({} issues found)",
        if no_type_issues {
            "✅ GOOD"
        } else {
            "⚠️  ISSUES"
        },
        type_issues.len()
    );

    let all_passed =
        conversion_matches && parquet_matches && conversion_parquet_match && no_type_issues;

    if all_passed {
        println!("\n🎉 ALL VALIDATIONS PASSED!");
    } else {
        println!("\n❌ VALIDATION FAILURES DETECTED!");
        if !conversion_matches {
            println!("   - Conversion row count doesn't match ZIP");
        }
        if !parquet_matches {
            println!("   - Parquet row count doesn't match ZIP");
        }
        if !conversion_parquet_match {
            println!("   - Conversion and Parquet counts don't match");
        }
        if !no_type_issues {
            println!("   - Type inference issues found (numeric data stored as strings)");
        }
    }

    all_passed
}
