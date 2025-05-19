// src/main.rs

use anyhow::{Context, Result};
use duckdb::arrow::util::pretty::print_batches;
use duckdb::Connection;
use std::path::Path;

fn main() -> Result<()> {
    // 1) Open an in-memory DuckDB
    let conn = Connection::open_in_memory().context("failed to open in-memory DuckDB")?;

    // 2) Point at your chunked Parquet files
    let input_dir = Path::new("tests/output");
    let parquet_pattern = input_dir
        .join("*.parquet")
        .to_string_lossy()
        .replace('\'', "''"); // escape any quotes

    // 3) Read all chunks in one go into a single merged table
    conn.execute(
        &format!(
            "CREATE OR REPLACE TABLE merged AS \
             SELECT * FROM parquet_scan('{}')",
            parquet_pattern
        ),
        [],
    )?;

    // 4) Optional: show how many rows we got
    let total_rows: i64 = conn.query_row("SELECT COUNT(*) FROM merged", [], |row| row.get(0))?;
    println!("▶ Merged table has {} rows", total_rows);

    // 6) Export that merged table as one highly-compressed Parquet
    let output_file = input_dir.join("merged_zstd.parquet");
    let output_path = output_file.to_string_lossy().replace('\'', "''");
    conn.execute(
        &format!(
            "COPY merged TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD)",
            output_path
        ),
        [],
    )?;
    println!("✅ Wrote compressed Parquet to {}", output_file.display());

    Ok(())
}
