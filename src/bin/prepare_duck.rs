use anyhow::Result;
use duckdb::Connection;
use std::{fs, path::Path};

fn main() -> Result<()> {
    // 1. Configuration
    let parquet_dir = "parquet/parquet_date";
    let db_path = "duckdb.db";

    // 2. If an old DB exists, remove it so we start fresh
    if Path::new(db_path).exists() {
        fs::remove_file(db_path)?;
    }

    // 3. Open (or create) the DuckDB database file
    //    (Connection::open creates the file if it doesn't exist) :contentReference[oaicite:0]{index=0}
    let conn = Connection::open(db_path)?;

    // 4. Ingest each .parquet file as its own table
    for entry in fs::read_dir(parquet_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
            let file_path = path.to_string_lossy();
            let table_name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap()
                .replace('-', "_"); // ensure valid SQL identifier

            let sql = format!(
                "CREATE TABLE IF NOT EXISTS \"{table_name}\" \
                 AS SELECT * FROM read_parquet('{file_path}');",
                table_name = table_name,
                file_path = file_path
            );

            conn.execute_batch(&sql)?;
        }
    }

    println!(
        "✅ All parquet files ingested — remaining artifact: {}",
        db_path
    );
    Ok(())
}
