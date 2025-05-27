// src/process/split.rs
use crate::process::chunk::chunk_and_write_segment;
use crate::schema::evolution::derive_types;
use crate::schema::types::{calculate_fields_hash, CtlSchema, MonthSchema};
use crate::schema::{build_arrow_schema, find_column_types};
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
#[instrument(level = "info", skip(zip_path, out_dir, column_lookup, schema_proposals_dir), fields(zip = %zip_path.as_ref().display()))]
pub fn split_zip_to_parquet<P: AsRef<Path>, Q: AsRef<Path>, R: AsRef<Path>>(
    zip_path: P,
    out_dir: Q,
    column_lookup: Arc<HashMap<String, HashMap<String, HashSet<String>>>>,
    schema_proposals_dir: R,
) -> Result<()> {
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_cpus::get())
        .build_global()
        .ok();

    info!("starting split_zip_to_parquet");

    let out_dir = out_dir.as_ref();
    let proposals_dir = schema_proposals_dir.as_ref();
    let temp_dir = proposals_dir.join("tmp");

    fs::create_dir_all(out_dir)?;
    fs::create_dir_all(proposals_dir)?;
    fs::create_dir_all(&temp_dir)?;

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

                let cols = match find_column_types(&column_lookup, &table, &headers) {
                    Ok(c) => c,
                    Err(e) => {
                        warn!(table=%table,month=%date,error=%e,"lookup failed, deriving types");
                        let mut rdr = ReaderBuilder::new()
                            .has_headers(false)
                            .from_reader(Cursor::new(&text[ds..es]));

                        // Collect up to 1_000 sample rows, skipping the control columns:
                        let mut sample_rows: Vec<Vec<String>> = Vec::with_capacity(1000);
                        for result in rdr.records().skip(1).take(1000) {
                            let record = result.context("parsing CSV record for derive_types")?;
                            // Skip the first 4 columns (C,I metadata) then collect the rest:
                            let row: Vec<String> =
                                record.iter().skip(4).map(|s| s.to_string()).collect();
                            sample_rows.push(row);
                        }
                        let proposal_cols = derive_types(&table, &headers, &sample_rows)?;

                        // wrap in MonthSchema/CtlSchema
                        let ctl = CtlSchema {
                            table: table.clone(),
                            month: date.clone(),
                            columns: proposal_cols.clone(),
                        };
                        let month_schema = MonthSchema {
                            month: date.clone(),
                            schemas: vec![ctl],
                        };

                        // compute filename by hash of columns
                        let mut ordered = proposal_cols.clone();
                        ordered.sort_by(|a, b| a.name.cmp(&b.name));
                        let hash = calculate_fields_hash(&ordered);
                        let filename = format!("{}.json", hash);
                        let final_path = proposals_dir.join(&filename);

                        if !final_path.exists() {
                            let tmp_path = temp_dir.join(&filename);
                            let json = serde_json::to_string_pretty(&month_schema)?;
                            fs::write(&tmp_path, json)?;
                            fs::rename(&tmp_path, &final_path)?;
                            info!(proposal=%filename,"wrote schema proposal");
                        } else {
                            info!(proposal=%filename,"proposal exists, skipping");
                        }

                        proposal_cols
                    }
                };

                let arrow_schema: Arc<ArrowSchema> = build_arrow_schema(&cols);
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
