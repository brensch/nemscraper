use std::{
    collections::HashMap,
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use duckdb::Connection;
use num_cpus;
use regex::Regex;
use tracing::{debug, error, info};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

fn main() -> Result<(), Box<dyn Error>> {
    // ─── initialize tracing at DEBUG ─────────────────
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::new("debug"))
        .init();
    info!("nemscraper_munge starting up");

    let input_dir = Path::new("./parquet");
    let output_dir = Path::new("./munged");
    fs::create_dir_all(output_dir)?;
    debug!(output_dir = %output_dir.display(), "Ensured output dir exists");

    // open DuckDB & set threads
    let conn = Connection::open_in_memory()?;
    debug!("Opened in-memory DuckDB");
    let cores = num_cpus::get();
    debug!(cores, "Detected CPU cores");
    conn.execute(&format!("SET threads = {}", cores), [])?;
    debug!(cores, "Configured DuckDB to use threads");

    // collect all parquet paths, grouped by table and full date (YYYYMMDD)
    let mut groups: HashMap<String, HashMap<String, Vec<PathBuf>>> = HashMap::new();
    let date_re = Regex::new(r"(\d{8})")?;
    info!(input_dir = %input_dir.display(), "Scanning parquet files");

    for entry in fs::read_dir(input_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("parquet") {
            debug!(path = %path.display(), "Skipping (not parquet)");
            continue;
        }

        let fname = path.file_name().unwrap().to_string_lossy().to_string();
        let parts: Vec<&str> = fname.split('\u{2014}').collect();
        if parts.len() < 3 {
            error!(filename = %fname, "Bad filename, skipping");
            continue;
        }
        let table = parts[1].to_string();
        let prefix = parts[0];
        let date = date_re
            .captures(prefix)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str())
            .unwrap_or("");
        if date.len() != 8 {
            error!(filename = %fname, "No full YYYYMMDD in filename, skipping");
            continue;
        }
        let ymd = date; // full YYYYMMDD

        groups
            .entry(table.clone())
            .or_default()
            .entry(ymd.to_string())
            .or_default()
            .push(path.clone());

        debug!(fname, table, ymd, "Grouped file");
    }

    // for each daily‐group: concat, write, then inspect sizes & compression
    for (table, by_day) in groups {
        for (ymd, files) in by_day {
            info!(table, ymd, count = files.len(), "Processing daily group");

            // 1) sum input sizes
            let input_bytes: u64 = files
                .iter()
                .map(|p| fs::metadata(p).map(|m| m.len()).unwrap_or(0))
                .sum();
            debug!(input_bytes, "Total input bytes for this group");

            // 2) build and run COPY … UNION ALL
            let selects: Vec<String> = files
                .iter()
                .map(|p| format!("SELECT * FROM read_parquet('{}')", p.display()))
                .collect();
            let union_sql = selects.join(" UNION ALL ");
            let out = output_dir.join(format!("{}—{}.parquet", ymd, table));
            let copy_sql = format!(
                "COPY ( {} ) \
                    TO '{}' \
                    (FORMAT PARQUET, COMPRESSION zstd, COMPRESSION_LEVEL 22);",
                union_sql,
                out.display()
            );
            debug!(out = %out.display(), "Running COPY");
            conn.execute(&copy_sql, [])?;
            info!(path = %out.display(), "Wrote output file");

            // 3) stat the output file
            let output_bytes = fs::metadata(&out)?.len();
            info!(output_bytes, "Output bytes for this file");

            // 4) inspect parquet_metadata via prepare + query_row loop
            let meta_sql = format!(
                "SELECT compression, \
                        SUM(total_compressed_size)   AS comp_bytes, \
                        SUM(total_uncompressed_size) AS uncomp_bytes \
                 FROM parquet_metadata('{}') \
                 GROUP BY compression;",
                out.display()
            );
            let mut stmt = conn.prepare(&meta_sql)?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let codec: String = row.get("compression")?;
                let comp: i64 = row.get("comp_bytes")?;
                let uncomp: i64 = row.get("uncomp_bytes")?;
                info!(
                    codec,
                    comp_bytes = comp,
                    uncomp_bytes = uncomp,
                    "Parquet metadata per codec"
                );
            }
        }
    }

    info!("nemscraper_munge complete");
    Ok(())
}
