// src/main.rs

use anyhow::{Context, Result};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::arrow::util::pretty::print_batches;
use duckdb::Connection;
use std::{fs, path::Path};

fn main() -> Result<()> {
    // 1) Open an in-memory DuckDB
    let conn = Connection::open_in_memory().context("failed to open in-memory DuckDB")?;

    let input_dir = Path::new("tests/output");
    for entry in fs::read_dir(input_dir).context("reading tests/output directory")? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("parquet") {
            continue;
        }

        // Prepare safe identifiers and paths
        let input_path = path.to_string_lossy().replace('\'', "''");
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .context("invalid file stem")?;
        let safe_stem: String = stem
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        let table_name = format!("t_{}", safe_stem);

        // 2) Load parquet into DuckDB
        conn.execute(&format!("DROP TABLE IF EXISTS {}", table_name), [])?;
        conn.execute(
            &format!(
                "CREATE TABLE {t} AS SELECT * FROM parquet_scan('{p}')",
                t = table_name,
                p = input_path
            ),
            [],
        )?;

        // 3a) Print row count
        let count: i64 =
            conn.query_row(&format!("SELECT COUNT(*) FROM {}", table_name), [], |row| {
                row.get(0)
            })?;
        println!("Table `{}` ← {} rows", table_name, count);

        // 3b) Print *all* rows via Arrow batches
        let mut stmt = conn.prepare(&format!("SELECT * FROM {}", table_name))?;
        let batches: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
        println!("\n📋 Rows in `{}`:", table_name);
        print_batches(&batches); // :contentReference[oaicite:0]{index=0}

        // 4) Export compressed copy
        let output_file = input_dir.join(format!("{}_compressed.parquet", stem));
        let output_path = output_file.to_string_lossy().replace('\'', "''");
        conn.execute(
            &format!(
                "COPY {t} TO '{o}' (FORMAT PARQUET, COMPRESSION ZSTD)",
                t = table_name,
                o = output_path
            ),
            [],
        )?;
        println!("✅ {} → {}\n", path.display(), output_file.display());
    }

    // Finally, show all tables in the in-memory DB
    println!("All DuckDB tables:");
    for table in conn
        .prepare("SHOW TABLES")?
        .query_map([], |r| r.get::<_, String>(0))?
    {
        println!(" - {}", table?);
    }

    Ok(())
}
