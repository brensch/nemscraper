use anyhow::Result;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use tracing::{debug, instrument, warn};
use zip::ZipArchive;

use super::chunk_polars::csv_to_parquet;

/// Splits each CSV file in the ZIP into chunks and writes Parquet files,
/// using a pre-built lookup of column types per table, with a fallback
/// to `derive_types` when lookup fails, emitting schema proposals.
#[instrument(level = "debug", skip(zip_path, out_dir), fields(zip = %zip_path.as_ref().display()))]
pub fn split_zip_to_parquet<P: AsRef<Path>, Q: AsRef<Path>>(
    zip_path: P,
    out_dir: Q,
    // schema_store: Arc<SchemaStore>,
) -> Result<()> {
    // 1) Open the ZIP file
    debug!("Opening ZIP file: {}", zip_path.as_ref().display());
    let file = File::open(&zip_path)?;
    let mut archive = ZipArchive::new(file)?;
    let total_entries = archive.len();
    debug!("ZIP archive contains {} entries", total_entries);

    // 2) Iterate over each entry in the ZIP
    for idx in 0..total_entries {
        let mut entry = archive.by_index(idx)?;
        let file_name = entry.name().to_string();

        // Skip non-CSV entries
        if !file_name.to_lowercase().ends_with(".csv") {
            debug!("Skipping non-CSV entry: {}", file_name);
            continue;
        }
        debug!("Processing CSV entry: {}", file_name);

        // 3) Read the entire CSV into memory
        let mut buf = Vec::new();
        entry.read_to_end(&mut buf)?;
        debug!("Read {} bytes from {}", buf.len(), file_name);

        let text = String::from_utf8_lossy(&buf).into_owned();
        let text_str: &str = &text;

        // 4) Find and strip off the first and last C-lines
        let first_data = text_str.find('\n').map(|i| i + 1).unwrap_or_else(|| {
            warn!(
                "Error in {}: every CSV must have a metadata C-line at the top",
                zip_path.as_ref().display()
            );
            0
        });
        debug!("First data offset: {}", first_data);

        let footer_start = text_str.rfind("\nC,").unwrap_or_else(|| {
            warn!(
                "Error in {}: every CSV must have a footer C-line at the bottom",
                zip_path.as_ref().display()
            );
            text_str.len()
        });
        debug!("Footer start offset: {}", footer_start);

        // Extract the core (only I- and D-lines)
        let core = &text_str[first_data..footer_start];
        debug!("Core length (bytes): {}", core.len());

        // 5) Find start-of-line offsets for every `I,` within that blob
        let mut header_starts = Vec::new();
        header_starts.push(0);
        for (rel, _) in core.match_indices("\nI,") {
            header_starts.push(rel + 1);
        }
        header_starts.sort_unstable();
        debug!("Found {} header starts", header_starts.len());

        // 6) Build segments by pairing each start with the next (or end of blob)
        let mut segments = Vec::with_capacity(header_starts.len());
        for window in header_starts.windows(2) {
            let hs = window[0];
            let es = window[1];
            segments.push((hs + first_data, es + first_data));
        }
        // Last segment goes to footer_start
        if let Some(&last_hs) = header_starts.last() {
            segments.push((last_hs + first_data, footer_start));
        }
        debug!(
            "Total segments to process for {}: {}",
            file_name,
            segments.len()
        );

        // 7) Prepare output path
        let out_path = out_dir.as_ref().to_path_buf();

        // 8) Process each segment sequentially
        for (seg_idx, (start, end)) in segments.into_iter().enumerate() {
            debug!(
                "Segment {} for {}: byte range [{}, {})",
                seg_idx, file_name, start, end
            );

            // Convert this slice of CSV to Parquet
            csv_to_parquet(&file_name, &text_str[start..end], &out_path).unwrap_or_else(|e| {
                panic!(
                    "csv to parquet error on {} segment {}: {:?}",
                    file_name, seg_idx, e
                )
            });
            debug!("Finished writing segment {} for {}", seg_idx, file_name);
        }

        debug!("Completed all segments for {}", file_name);
    }

    debug!("Completed processing all entries in ZIP");
    Ok(())
}
