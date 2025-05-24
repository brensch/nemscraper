// data/src/process/split.rs
use crate::process::chunk::chunk_and_write_segment;
use crate::schema::{find_schema_evolution, SchemaEvolution};
use anyhow::{anyhow, Context, Result};
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
use tracing::{error, info, instrument};
use zip::ZipArchive;

/// Splits each CSV file in the ZIP into chunks and writes Parquet files,
/// using a pre-built lookup of schema evolutions.  For each segment:
/// 1. Parse out the CSV header row into `Vec<String>`.
/// 2. Call `find_schema_evolution(.., &header_names)`, which will error
///    if no matching evolution covers that month **or** the headers differ.
/// 3. Only if it passes, write the Parquet via `chunk_and_write_segment`.
#[instrument(level = "info", skip(zip_path, out_dir, lookup), fields(zip = %zip_path.as_ref().display()))]
pub fn split_zip_to_parquet<P: AsRef<Path>, Q: AsRef<Path>>(
    zip_path: P,
    out_dir: Q,
    lookup: Arc<HashMap<String, Vec<Arc<SchemaEvolution>>>>,
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

        // 1) Identify all (schema_id, header_start, data_start, end_pos, date) segments
        let segments = {
            let mut segs = Vec::new();
            let mut pos = 0;
            let mut header_start = 0;
            let mut data_start = 0;
            let mut current_schema: Option<String> = None;
            let mut seen_footer = false;
            let mut date = String::new();

            for chunk in text.split_inclusive('\n') {
                if chunk.starts_with("C,") {
                    if seen_footer {
                        break;
                    }
                    // get the date of the file from the header
                    let parts: Vec<&str> = chunk.trim_end_matches('\n').split(',').collect();
                    // remove / from the 6th element of the line, and take the first 6 characters
                    date = parts[5].replace('/', "").chars().take(6).collect();

                    seen_footer = true;
                    pos += chunk.len();
                    continue;
                }
                if chunk.starts_with("I,") {
                    // When switching schemas, flush previous
                    if let Some(schema_id) = current_schema.take() {
                        segs.push((schema_id, header_start, data_start, pos, date.clone()));
                    }
                    header_start = pos;
                    let parts: Vec<&str> = chunk.trim_end_matches('\n').split(',').collect();
                    let schema_id = if parts[2].starts_with(parts[1]) {
                        parts[2].to_string()
                    } else {
                        format!("{}_{}", parts[1], parts[2])
                    };
                    current_schema = Some(schema_id);
                    data_start = pos + chunk.len();
                }
                pos += chunk.len();
            }
            // Flush last segment
            if let Some(schema_id) = current_schema {
                segs.push((schema_id, header_start, data_start, pos, date));
            }
            segs
        };

        // 2) Process each segment in parallel, validating headers; any error aborts entire run
        segments
            .into_par_iter()
            .try_for_each(|(schema_id, hs, ds, es, date)| -> Result<()> {
                // Extract the raw header text (usually a single CSV header line)
                let header_str = &text[hs..ds];
                let header_line = header_str
                    .lines()
                    .next()
                    .ok_or_else(|| anyhow!("empty header for table `{}` on {}", schema_id, date))?;
                let header_names: Vec<String> =
                    header_line.split(',').map(|s| s.to_string()).collect();

                // Find the matching evolution *and* verify those header names
                let evo = find_schema_evolution(&lookup, &schema_id, &date, &header_names)
                    // Log the full error chain to your logger:
                    .inspect_err(|e| {
                        error!(
                            table = %schema_id,
                            month = %date,
                            error = %e,
                            "schema validation error"
                        )
                    })?;

                // Now write the parquet using the validated schema
                chunk_and_write_segment(
                    &name,
                    &schema_id,
                    header_str,
                    &text[ds..es],
                    evo.columns.clone(),
                    evo.arrow_schema.clone(),
                    &out_dir,
                );
                Ok(())
            })?;
    }

    info!("completed in {:?}", start.elapsed());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::split_zip_to_parquet;
    use crate::schema;
    use crate::schema::SchemaEvolution;
    use anyhow::Result;
    use reqwest::Client;
    use std::{
        env, fs,
        iter::Skip,
        path::{Path, PathBuf},
        sync::Arc,
    };
    use tempfile::TempDir;
    use tracing_subscriber::{EnvFilter, FmtSubscriber};

    /// This test will:
    /// 1) fetch all CTL schemas into `schemas/`
    /// 2) extract schema evolutions and build the lookup
    /// 3) run split_zip_to_parquet on the ZIP at $ZIP_PATH
    /// 4) verify that at least one Parquet file was generated
    #[tokio::test]
    async fn test_split_zip_to_parquet() -> Result<()> {
        let subscriber = FmtSubscriber::builder()
            .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                // ADJUST your_crate_name to your actual crate name (e.g., nemscraper)
                EnvFilter::new("info,your_crate_name::duck=trace,your_crate_name::process=trace")
            }))
            .with_test_writer()
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);

        // 1) Get the ZIP path from the environment
        let zip_path =
            env::var("ZIP_PATH").expect("you must set ZIP_PATH to point at a .zip for testing");

        // 2) Prepare a clean temp directory for outputs
        let out_dir: PathBuf = env::current_dir()?.join("parquet");
        fs::create_dir_all(&out_dir)?;

        // 3) Re-run the schema fetch + evolution logic from `main`
        let schemas_dir = Path::new("schemas");
        fs::create_dir_all(schemas_dir)?;
        schema::fetch_all(&Client::new(), schemas_dir).await?;
        let evolutions = schema::extract_schema_evolutions(schemas_dir)?;
        let lookup = Arc::new(SchemaEvolution::build_lookup(evolutions));

        // 4) Invoke the function under test
        split_zip_to_parquet(&zip_path, out_dir.as_path(), lookup)?;

        // 5) Assert that something got written
        let entries: Vec<_> = fs::read_dir(out_dir.as_path())?
            .filter_map(Result::ok)
            .collect();
        assert!(
            !entries.is_empty(),
            "Expected at least one parquet file in {:?}, found none",
            out_dir.as_path()
        );

        Ok(())
    }
}
