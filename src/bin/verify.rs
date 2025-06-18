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
    println!("current dir = {:?}\n", std::env::current_dir()?);

    // 1) Find all ZIP files under assets/zips/
    let zip_pattern = "assets/zips/*.zip";
    let zip_paths: Vec<PathBuf> = glob(zip_pattern)
        .with_context(|| format!("Failed to read glob pattern '{}'", zip_pattern))?
        .filter_map(|entry| entry.ok())
        .collect();
    if zip_paths.is_empty() {
        return Err(anyhow::anyhow!(
            "No ZIP files found under '{}'",
            zip_pattern
        ));
    }

    // 2) Find all Parquet files under assets/parquet/
    let parquet_pattern = "assets/parquet/**/*.parquet";
    let parquet_paths: Vec<PathBuf> = glob(parquet_pattern)
        .with_context(|| format!("Failed to read glob pattern '{}'", parquet_pattern))?
        .filter_map(|entry| entry.ok())
        .collect();
    if parquet_paths.is_empty() {
        return Err(anyhow::anyhow!(
            "No Parquet files found under '{}'",
            parquet_pattern
        ));
    }

    // 3) Find all Parquet files under assets/compacted/
    let compacted_pattern = "assets/compacted/**/*.parquet";
    let compacted_paths: Vec<PathBuf> = glob(compacted_pattern)
        .with_context(|| format!("Failed to read glob pattern '{}'", compacted_pattern))?
        .filter_map(|entry| entry.ok())
        .collect();
    if compacted_paths.is_empty() {
        return Err(anyhow::anyhow!(
            "No Parquet files found under '{}'",
            compacted_pattern
        ));
    }

    // 4) In parallel: count 'D'-lines in each ZIP
    let per_zip_counts: Vec<usize> = zip_paths
        .par_iter()
        .map(|zip_path| -> Result<usize> {
            let file = File::open(zip_path)
                .with_context(|| format!("Failed to open ZIP '{}'", zip_path.display()))?;
            let mut archive = ZipArchive::new(file)
                .with_context(|| format!("Failed to read ZIP '{}'", zip_path.display()))?;

            let mut count_d = 0;
            for i in 0..archive.len() {
                let mut entry = archive.by_index(i).with_context(|| {
                    format!("Failed to access entry {} in '{}'", i, zip_path.display())
                })?;
                let name_lower = entry.name().to_lowercase();
                if !name_lower.ends_with(".csv") {
                    continue;
                }
                let reader = BufReader::new(&mut entry);
                for line in reader.lines() {
                    let line = line.with_context(|| "Failed to read a CSV line")?;
                    if line.starts_with('D') {
                        count_d += 1;
                    }
                }
            }
            Ok(count_d)
        })
        .collect::<Result<Vec<_>>>()?;
    let total_zip: usize = per_zip_counts.iter().sum();

    // 5) In parallel: sum rows in assets/parquet
    let per_parquet_counts: Vec<usize> = parquet_paths
        .par_iter()
        .map(|pq_path| -> Result<usize> {
            let file = File::open(pq_path)
                .with_context(|| format!("Failed to open Parquet '{}'", pq_path.display()))?;
            let reader = SerializedFileReader::new(file)
                .with_context(|| format!("Failed to read Parquet '{}'", pq_path.display()))?;
            Ok(reader.metadata().file_metadata().num_rows() as usize)
        })
        .collect::<Result<Vec<_>>>()?;
    let total_parquet: usize = per_parquet_counts.iter().sum();

    // 6) In parallel: sum rows in assets/compacted
    let per_compacted_counts: Vec<usize> = compacted_paths
        .par_iter()
        .map(|pq_path| -> Result<usize> {
            let file = File::open(pq_path)
                .with_context(|| format!("Failed to open Parquet '{}'", pq_path.display()))?;
            let reader = SerializedFileReader::new(file)
                .with_context(|| format!("Failed to read Parquet '{}'", pq_path.display()))?;
            Ok(reader.metadata().file_metadata().num_rows() as usize)
        })
        .collect::<Result<Vec<_>>>()?;
    let total_compacted: usize = per_compacted_counts.iter().sum();

    // 7) Print summary table
    //
    // delta = count(dir) - count(assets/zips)
    let delta_parquet = total_parquet as isize - total_zip as isize;
    let delta_compacted = total_compacted as isize - total_zip as isize;

    println!(
        "\n{: <25} {:>15} {:>15}",
        "Directory", "Count", "Delta vs zip"
    );
    println!("{:-<55}", "");
    println!("{: <25} {:>15} {:>15}", "assets/zips", total_zip, 0);
    println!(
        "{: <25} {:>15} {:>15}",
        "assets/parquet", total_parquet, delta_parquet
    );
    println!(
        "{: <25} {:>15} {:>15}",
        "assets/compacted", total_compacted, delta_compacted
    );

    Ok(())
}
