use crate::process::chunk::chunk_and_write_segment;
use crate::schema::store::SchemaStore;
use anyhow::Result;
use num_cpus;
use rayon::prelude::*;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use tracing::{instrument, warn};
use zip::ZipArchive;

/// Splits each CSV file in the ZIP into chunks and writes Parquet files,
/// using a pre-built lookup of column types per table, with a fallback
/// to `derive_types` when lookup fails, emitting schema proposals.
#[instrument(level = "debug", skip(zip_path, out_dir, schema_store), fields(zip = %zip_path.as_ref().display()))]
pub fn split_zip_to_parquet<P: AsRef<Path>, Q: AsRef<Path>>(
    zip_path: P,
    out_dir: Q,
    schema_store: Arc<SchemaStore>,
) -> Result<()> {
    // configure Rayon
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_cpus::get())
        .build_global()
        .ok();

    // open ZIP
    let file = File::open(&zip_path)?;
    let mut archive = ZipArchive::new(file)?;

    for idx in 0..archive.len() {
        let mut entry = archive.by_index(idx)?;
        let file_name = entry.name().to_string();
        if !file_name.to_lowercase().ends_with(".csv") {
            continue;
        }

        // read entire CSV into Arc<String>
        let mut buf = Vec::new();
        entry.read_to_end(&mut buf)?;
        let text = Arc::new(String::from_utf8_lossy(&buf).into_owned());
        let text_str: &str = &text;

        // 1) Find and strip off the first and last C‐lines.
        //    We look for the first '\n' (end of metadata) and the last "\nC," (start of footer).
        let first_data = text_str.find('\n').map(|i| i + 1).unwrap_or_else(|| {
            warn!(
                "Error in {}: every CSV must have a metadata C-line at the top",
                zip_path.as_ref().display()
            );
            0
        });

        let footer_start = text_str.rfind("\nC,").unwrap_or_else(|| {
            warn!(
                "Error in {}: every CSV must have a footer C-line at the bottom",
                zip_path.as_ref().display()
            );
            text_str.len()
        });

        // This is the big middle blob containing only I‐ and D‐lines:
        let core = &text_str[first_data..footer_start];

        // 2) Find the start‐of‐line offsets for every `I,` within that blob.
        //    We always include 0 (the first I is at the very start of `core`),
        //    then scan for every `\nI,` and record the index+1.
        let mut header_starts = Vec::new();
        header_starts.push(0);
        for (rel, _) in core.match_indices("\nI,") {
            header_starts.push(rel + 1);
        }
        header_starts.sort_unstable();

        // 3) Build your segments by pairing each header_start with the next one (or end of blob).
        let mut segments = Vec::with_capacity(header_starts.len());
        for window in header_starts.windows(2) {
            let hs = window[0];
            let es = window[1];
            segments.push((hs + first_data, es + first_data));
        }
        // last segment goes to the footer_start:
        if let Some(&last_hs) = header_starts.last() {
            segments.push((last_hs + first_data, footer_start));
        }

        let out_path = out_dir.as_ref().to_path_buf(); // grab a concrete PathBuf
        let out_path = std::sync::Arc::new(out_path);

        // process segments in parallel
        segments.into_par_iter().for_each(|(start, end)| {
            // build or retrieve ArrowSchema for this table
            let arrow_schema = schema_store
                .get_schema(&text_str, start, end)
                .unwrap_or_else(|e| panic!("schema error: {:?}", e));

            // chunk and write without further slicing of text
            chunk_and_write_segment(
                &file_name,
                arrow_schema.clone(),
                &text_str[start..end],
                &*out_path,
            );
        });
    }

    Ok(())
}
