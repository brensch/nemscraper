// src/main.rs

use anyhow::{Context, Result};
use glob::glob;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use zip::ZipArchive;

fn main() -> Result<()> {
    // Print current working directory (for debugging)
    println!("current dir = {:?}", std::env::current_dir()?);

    // ----------------------------------------
    // 1) Find all ZIP files under assets/zips/
    // ----------------------------------------
    let zip_pattern = "assets/zips/*.zip";
    let zip_paths: Vec<PathBuf> = glob(zip_pattern)
        .with_context(|| format!("Failed to read glob pattern '{}'", zip_pattern))?
        .filter_map(|entry| match entry {
            Ok(path) => Some(path),
            Err(err) => {
                eprintln!("Warning: glob error: {}", err);
                None
            }
        })
        .collect();

    if zip_paths.is_empty() {
        return Err(anyhow::anyhow!(
            "No ZIP files found under '{}'",
            zip_pattern
        ));
    }

    // ----------------------------------------
    // 2) Find all Parquet files under assets/parquet/
    // ----------------------------------------
    let parquet_pattern = "assets/parquet/*.parquet";
    let parquet_paths: Vec<PathBuf> = glob(parquet_pattern)
        .with_context(|| format!("Failed to read glob pattern '{}'", parquet_pattern))?
        .filter_map(|entry| match entry {
            Ok(path) => Some(path),
            Err(err) => {
                eprintln!("Warning: glob error: {}", err);
                None
            }
        })
        .collect();

    if parquet_paths.is_empty() {
        return Err(anyhow::anyhow!(
            "No Parquet files found under '{}'",
            parquet_pattern
        ));
    }

    // ----------------------------------------
    // 3) In parallel: open each ZIP and count lines starting with 'd' in each .csv
    // ----------------------------------------
    let per_zip_counts: Vec<(PathBuf, usize)> = zip_paths
        .par_iter()
        .map(|zip_path| {
            // Open the ZIP file from disk
            let file = File::open(&zip_path)
                .with_context(|| format!("Failed to open ZIP '{}'", zip_path.display()))?;
            let mut archive = ZipArchive::new(file)
                .with_context(|| format!("Failed to read ZIP archive '{}'", zip_path.display()))?;

            // Counter for lines starting with 'd' across all CSVs inside this ZIP
            let mut count_d = 0;

            // Iterate through each entry in the ZIP
            for i in 0..archive.len() {
                // We need `entry_name` before creating a BufReader,
                // so we can avoid borrowing `entry` again inside the closure.
                let mut entry = archive.by_index(i).with_context(|| {
                    format!("Failed to access entry {} in '{}'", i, zip_path.display())
                })?;
                let raw_name = entry.name().to_string();
                let name_lower = raw_name.to_lowercase();

                if !name_lower.ends_with(".csv") {
                    continue; // Only process CSV files
                }
                let csv_name = raw_name.clone();

                // Wrap this entry in BufReader to iterate lines
                let reader = BufReader::new(&mut entry);
                for line_result in reader.lines() {
                    let line = line_result
                        .with_context(|| format!("Failed to read a line in '{}'", csv_name))?;
                    if let Some(first_char) = line.chars().next() {
                        if first_char == 'D' {
                            count_d += 1;
                        }
                    }
                }
            }

            Ok((zip_path.clone(), count_d))
        })
        .collect::<Result<Vec<_>>>()?;

    // Print per-ZIP results
    let total_d_lines: usize = per_zip_counts.iter().map(|(_, cnt)| *cnt).sum();
    println!("  → Total across all ZIPs: {} line(s)\n", total_d_lines);

    // ----------------------------------------
    // 4) In parallel: open each Parquet and sum row counts
    // ----------------------------------------
    let per_parquet_counts: Vec<(PathBuf, usize)> = parquet_paths
        .par_iter()
        .map(|pq_path| {
            let file = File::open(&pq_path)
                .with_context(|| format!("Failed to open Parquet '{}'", pq_path.display()))?;
            let reader = SerializedFileReader::new(file).with_context(|| {
                format!(
                    "Failed to create Parquet reader for '{}'",
                    pq_path.display()
                )
            })?;
            let metadata = reader.metadata().file_metadata();
            let num_rows = metadata.num_rows() as usize;
            Ok((pq_path.clone(), num_rows))
        })
        .collect::<Result<Vec<_>>>()?;

    // Print per-Parquet results
    let total_rows: usize = per_parquet_counts.iter().map(|(_, cnt)| *cnt).sum();
    println!("  → Total rows across all Parquet files: {}\n", total_rows);

    println!(
        "delta between ZIP and Parquet row counts: {}",
        total_d_lines - total_rows
    );
    if total_d_lines != total_rows {
        eprintln!(
            "Warning: ZIP line count ({}) does not match Parquet row count ({})",
            total_d_lines, total_rows
        );
    } else {
        println!("Counts match! ✅");
    }

    Ok(())
}
