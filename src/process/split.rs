// data/src/process/split.rs
use crate::process::chunk::chunk_and_write_segment;
use crate::schema::find_schema_evolution;
use crate::schema::SchemaEvolution;
use anyhow::Result;
use num_cpus;
use rayon::prelude::*;
use std::{
    collections::HashMap,
    fs,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};
use tracing::{info, instrument, warn};
use zip::ZipArchive;

/// Splits each CSV file in the ZIP into chunks and writes Parquet files,
/// using a pre-built lookup of schema evolutions.
#[instrument(level = "info", skip(zip_path, out_dir, lookup), fields(zip = %zip_path.as_ref().display()))]
pub fn split_zip_to_parquet<P: AsRef<Path>, Q: AsRef<Path>>(
    zip_path: P,
    out_dir: Q,
    lookup: Arc<HashMap<String, Vec<Arc<SchemaEvolution>>>>,
) -> Result<()> {
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

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        let name = entry.name().to_string();
        if !name.to_lowercase().ends_with(".csv") {
            continue;
        }

        let mut buf = Vec::new();
        entry.read_to_end(&mut buf)?;
        let text = Arc::new(String::from_utf8_lossy(&buf).to_string());

        // determine segments
        let segments = {
            let mut segs = Vec::new();
            let mut pos = 0;
            let mut header_start = 0;
            let mut data_start = 0;
            let mut current_schema = None;
            let mut seen_footer = false;

            for chunk in text.split_inclusive('\n') {
                if chunk.starts_with("C,") {
                    if seen_footer {
                        break;
                    }
                    seen_footer = true;
                    pos += chunk.len();
                    continue;
                }
                if chunk.starts_with("I,") {
                    if let Some(schema_id) = current_schema.take() {
                        segs.push((schema_id, header_start, data_start, pos));
                    }
                    header_start = pos;
                    let parts: Vec<&str> = chunk.trim_end_matches('\n').split(',').collect();
                    current_schema = Some(format!("{}_{}", parts[1], parts[2]));
                    data_start = pos + chunk.len();
                }
                pos += chunk.len();
            }
            if let Some(schema_id) = current_schema {
                segs.push((schema_id, header_start, data_start, pos));
            }
            segs
        };

        // process segments
        segments
            .into_par_iter()
            .for_each(|(schema_id, hs, ds, es)| {
                let header = &text[hs..ds];
                let data = &text[ds..es];

                let table = Path::new(&name)
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or(&name);

                let first_line = header.lines().next().unwrap();
                let cols: Vec<&str> = first_line.split(',').collect();
                let st_dt = cols[6];
                let effective_month: String = st_dt
                    .split_whitespace()
                    .next()
                    .unwrap()
                    .replace('/', "")
                    .chars()
                    .take(6)
                    .collect();

                if let Some(evo) = find_schema_evolution(&lookup, table, &effective_month) {
                    let arrow_schema = evo.arrow_schema.clone();
                    chunk_and_write_segment(
                        &name,
                        &schema_id,
                        header,
                        data,
                        arrow_schema,
                        &out_dir,
                    );
                } else {
                    warn!(
                        table = table,
                        month = effective_month,
                        "no schema evolution found, skipping segment"
                    );
                }
            });
    }

    info!("completed in {:?}", start.elapsed());
    Ok(())
}
