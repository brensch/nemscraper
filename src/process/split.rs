// src/process/split.rs
use crate::process::chunk::chunk_and_write_segment;
use crate::schema::build_arrow_schema;
use crate::schema::derive_types;
use crate::schema::store::SchemaStore;
use anyhow::Context;
use anyhow::Result;
use arrow::datatypes::Schema as ArrowSchema;
use csv::ReaderBuilder;
use num_cpus;
use rayon::prelude::*;
use serde_json;
use std::io::{Cursor, Read};
use std::{
    collections::{HashMap, HashSet},
    fs,
    fs::File,
    path::Path,
    sync::Arc,
};
use tracing::{info, instrument, warn};
use zip::ZipArchive;

/// Splits each CSV file in the ZIP into chunks and writes Parquet files,
/// using a pre-built lookup of column types per table, with a fallback
/// to `derive_types` when lookup fails, emitting schema proposals.
#[instrument(level = "info", skip(zip_path, out_dir, schema_store), fields(zip = %zip_path.as_ref().display()))]
pub fn split_zip_to_parquet<P: AsRef<Path>, Q: AsRef<Path>, R: AsRef<Path>>(
    zip_path: P,
    out_dir: Q,
    schema_store: SchemaStore,
) -> Result<()> {
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_cpus::get())
        .build_global()
        .ok();

    info!("starting split_zip_to_parquet");

    let file = File::open(&zip_path)?;
    let mut archive = ZipArchive::new(file)?;

    for idx in 0..archive.len() {
        let mut entry = archive.by_index(idx)?;
        let name = entry.name().to_string();
        if !name.to_lowercase().ends_with(".csv") {
            continue;
        }

        let mut buf = Vec::new();
        entry.read_to_end(&mut buf)?;
        let text = Arc::new(String::from_utf8_lossy(&buf).to_string());

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

        segments
            .into_par_iter()
            .try_for_each(|(table, hs, ds, es, date)| -> Result<()> {
                let headers: Vec<String> = text[hs..ds]
                    .lines()
                    .next()
                    .unwrap()
                    .split(',')
                    .skip(4)
                    .map(String::from)
                    .collect();

                let arrow_schema = schema_store.get_schema(&table, &headers, &text, ds, es);
                chunk_and_write_segment(
                    &name,
                    &table,
                    &text[ds..es],
                    cols.clone(),
                    arrow_schema.clone(),
                    out_dir,
                );
                Ok(())
            })?;
    }

    Ok(())
}
