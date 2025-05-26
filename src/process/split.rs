// data/src/process/split.rs

use crate::process::chunk::chunk_and_write_segment;
use crate::schema::build_arrow_schema;
use crate::schema::find_column_types;
use anyhow::{anyhow, Context, Result};
use arrow::datatypes::Schema as ArrowSchema;
use num_cpus;
use rayon::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    fs,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};
use tracing::{error, info, instrument};
use zip::ZipArchive;

/// Splits each CSV file in the ZIP into chunks and writes Parquet files,
/// using a pre-built lookup of column types per table.
#[instrument(level = "info", skip(zip_path, out_dir, column_lookup), fields(zip = %zip_path.as_ref().display()))]
pub fn split_zip_to_parquet<P: AsRef<Path>, Q: AsRef<Path>>(
    zip_path: P,
    out_dir: Q,
    column_lookup: Arc<HashMap<String, HashMap<String, HashSet<String>>>>,
) -> Result<()> {
    // Initialize Rayon thread pool
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_cpus::get())
        .build_global()
        .ok();

    let start = Instant::now();
    info!("starting split_zip_to_parquet");

    let zip_path = zip_path.as_ref().to_path_buf();
    let out_dir = out_dir.as_ref().to_path_buf();
    fs::create_dir_all(&out_dir)?;

    let file = File::open(&zip_path)?;
    let mut archive = ZipArchive::new(file)?;

    for idx in 0..archive.len() {
        let mut entry = archive.by_index(idx)?;
        let name = entry.name().to_string();
        if !name.to_lowercase().ends_with(".csv") {
            continue;
        }

        // Read the entire CSV into memory once
        let mut buf = Vec::new();
        entry.read_to_end(&mut buf)?;
        let text = Arc::new(String::from_utf8_lossy(&buf).to_string());

        // 1) Identify all (table_name, header_start, data_start, end_pos, date) segments
        let segments = {
            let mut segs = Vec::new();
            let mut pos = 0;
            let mut header_start = 0;
            let mut data_start = 0;
            let mut current_table: Option<String> = None;
            let mut seen_footer = false;
            let mut date = String::new();

            for chunk in text.split_inclusive('\n') {
                if chunk.starts_with("C,") {
                    if seen_footer {
                        break;
                    }
                    let parts: Vec<&str> = chunk.trim_end_matches('\n').split(',').collect();
                    date = parts[5].replace('/', "").chars().take(6).collect();
                    seen_footer = true;
                    pos += chunk.len();
                    continue;
                }
                if chunk.starts_with("I,") {
                    if let Some(table) = current_table.take() {
                        segs.push((table, header_start, data_start, pos, date.clone()));
                    }
                    header_start = pos;
                    let parts: Vec<&str> = chunk.trim_end_matches('\n').split(',').collect();
                    let table_name = if parts[2].starts_with(parts[1]) {
                        parts[2].to_string()
                    } else {
                        format!("{}_{}", parts[1], parts[2])
                    };
                    current_table = Some(table_name);
                    data_start = pos + chunk.len();
                }
                pos += chunk.len();
            }
            if let Some(table) = current_table {
                segs.push((table, header_start, data_start, pos, date));
            }
            segs
        };

        // 2) Process each segment in parallel; any error aborts run
        segments
            .into_par_iter()
            .try_for_each(|(table, hs, ds, es, date)| -> Result<()> {
                let header_str = &text[hs..ds];
                let header_line = header_str
                    .lines()
                    .next()
                    .ok_or_else(|| anyhow!("empty header for table `{}` on {}", table, date))?;

                // **skip first 4 metadata columns** rec_type/domain/measure/seq
                let header_names: Vec<String> = header_line
                    .split(',')
                    .skip(4)
                    .map(String::from)
                    .collect();

                // Resolve columns by name, warn on missing, error only if table missing
                let cols = find_column_types(&column_lookup, &table, &header_names)
                    .inspect_err(|e| {
                        error!(table = %table, month = %date, error = %e, "column type resolution error");
                    })?;

                // Build an Arrow schema from resolved columns
                let arrow_schema: Arc<ArrowSchema> = build_arrow_schema(&cols);

                // Write the parquet using resolved types
                chunk_and_write_segment(
                    &name,
                    &table,
                    header_str,
                    &text[ds..es],
                    cols.clone(),
                    arrow_schema.clone(),
                    &out_dir,
                );
                Ok(())
            })?;
    }

    Ok(())
}
