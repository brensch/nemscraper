use anyhow::Result;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use tracing::{debug, instrument, warn};
use zip::ZipArchive;

use super::chunk_polars::csv_to_parquet;

/// Splits each CSV file in the ZIP into segments (each starting with an `I,` line
/// and including all subsequent `D,` lines) and writes Parquet files. At no point
/// do we read the entire CSV into memory—only one segment is buffered at a time.
#[instrument(level = "debug", skip(zip_path, out_dir), fields(zip = %zip_path.as_ref().display()))]
pub fn split_zip_to_parquet<P: AsRef<Path>, Q: AsRef<Path>>(zip_path: P, out_dir: Q) -> Result<()> {
    // 1) Open the ZIP file
    debug!("Opening ZIP file: {}", zip_path.as_ref().display());
    let file = File::open(&zip_path)?;
    let mut archive = ZipArchive::new(file)?;
    let total_entries = archive.len();
    debug!("ZIP archive contains {} entries", total_entries);

    // 2) Iterate over each entry in the ZIP
    for idx in 0..total_entries {
        // Extract the entry and capture its name up front
        let entry = archive.by_index(idx)?;
        let file_name = entry.name().to_string();

        // Skip non-CSV entries
        if !file_name.to_lowercase().ends_with(".csv") {
            debug!("Skipping non-CSV entry: {}", file_name);
            continue;
        }
        debug!("Processing CSV entry: {}", file_name);

        // 3) Wrap the entry in a buffered reader so we can read line-by-line
        let mut reader = BufReader::new(entry);

        // 4) Skip the very first “C-” metadata line at the top
        let mut first_line = String::new();
        let bytes_read = reader.read_line(&mut first_line)?;
        if bytes_read == 0 || !first_line.starts_with('C') {
            warn!(
                "Expected top metadata C-line in {}, but didn't find it; proceeding anyway.",
                file_name
            );
        } else {
            debug!("Skipped top C-line: {}", first_line.trim_end());
        }

        // 5) Now iterate line-by-line until we hit the footer (another C-line).
        //    Whenever we see an “I,” line, that starts a new segment. We buffer
        //    until the next “I,” (or footer), then call csv_to_parquet on that chunk.

        let mut segment_buf = String::new();
        let mut first_segment = true;

        loop {
            let mut line = String::new();
            let bytes = reader.read_line(&mut line)?;
            if bytes == 0 {
                // EOF reached; flush last segment if any
                if !segment_buf.is_empty() {
                    debug!(
                        "EOF: flushing final segment ({} bytes) for {}",
                        segment_buf.len(),
                        file_name
                    );
                    csv_to_parquet(&file_name, &segment_buf, out_dir.as_ref()).unwrap_or_else(
                        |e| {
                            panic!(
                                "csv_to_parquet error on {} final segment: {:?}",
                                file_name, e
                            )
                        },
                    );
                    segment_buf.clear();
                }
                break;
            }

            // If this line begins with “C,” we assume it's the footer and stop.
            if line.starts_with("C,") {
                debug!(
                    "Encountered footer C-line in {}: {}",
                    file_name,
                    line.trim_end()
                );
                // Flush out the last buffered segment (if any) before breaking
                if !segment_buf.is_empty() {
                    debug!(
                        "Footer: flushing last segment ({} bytes) for {}",
                        segment_buf.len(),
                        file_name
                    );
                    csv_to_parquet(&file_name, &segment_buf, out_dir.as_ref()).unwrap_or_else(
                        |e| {
                            panic!(
                                "csv_to_parquet error on {} footer segment: {:?}",
                                file_name, e
                            )
                        },
                    );
                    segment_buf.clear();
                }
                break;
            }

            // If the line starts with "I,", it’s the start of a new segment
            if line.starts_with("I,") {
                if first_segment {
                    // Start buffering the very first segment
                    first_segment = false;
                    segment_buf.push_str(&line);
                } else {
                    // We already had buffered a previous segment—flush it now
                    debug!(
                        "Found new I-line: flushing segment ({} bytes) for {}",
                        segment_buf.len(),
                        file_name
                    );
                    csv_to_parquet(&file_name, &segment_buf, out_dir.as_ref()).unwrap_or_else(
                        |e| panic!("csv_to_parquet error on {} segment: {:?}", file_name, e),
                    );
                    // Clear and start a fresh buffer for the next segment
                    segment_buf.clear();
                    segment_buf.push_str(&line);
                }
            } else {
                // Not an "I," or "C," line. If we are in a segment, append it.
                if !first_segment {
                    segment_buf.push_str(&line);
                } else {
                    // We haven't seen any "I," yet, but this is data (D-lines).
                    // In well-formed CSVs, lines preceding the first I-line are usually metadata—
                    // but just in case, collect them if we’ve already started a segment. Otherwise ignore.
                }
            }
        }

        debug!("Completed all segments for {}", file_name);
    }

    debug!("Completed processing all entries in ZIP");
    Ok(())
}
