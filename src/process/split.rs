use anyhow::Result;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use tracing::{debug, instrument, warn};
use zip::ZipArchive;

use super::chunk_polars::csv_to_parquet;

/// Splits each CSV inside the ZIP into ~100 MiB chunks (by bytes) and writes Parquet files.
/// Streams line‐by‐line so it never buffers the entire CSV in memory.
#[instrument(level = "debug", skip(zip_path, out_dir), fields(zip = %zip_path.as_ref().display()))]
pub fn split_zip_to_parquet<P: AsRef<Path>, Q: AsRef<Path>>(
    zip_path: P,
    out_dir: Q,
    // schema_store: Arc<SchemaStore>,
) -> Result<()> {
    const MAX_BATCH_BYTES: usize = 100 * 1024 * 1024; // 100 MiB

    debug!("Opening ZIP file: {}", zip_path.as_ref().display());
    let file = File::open(&zip_path)?;
    let mut archive = ZipArchive::new(file)?;
    let total_entries = archive.len();
    debug!("ZIP archive contains {} entries", total_entries);

    for idx in 0..total_entries {
        let mut entry = archive.by_index(idx)?;
        let file_name = entry.name().to_string();

        if !file_name.to_lowercase().ends_with(".csv") {
            debug!("Skipping non-CSV entry: {}", file_name);
            continue;
        }
        debug!("Processing CSV entry: {}", file_name);

        // Wrap the ZIP entry in a BufReader so we can read lines incrementally.
        let mut reader = BufReader::new(entry);

        // 1) Skip the top C-line (metadata header). If missing, warn and continue.
        {
            let mut first_line = String::new();
            let n = reader.read_line(&mut first_line)?;
            if n == 0 || !first_line.trim_start().starts_with('C') {
                warn!(
                    "Expected a top C-line in {}; got {:?}. Continuing anyway.",
                    zip_path.as_ref().display(),
                    first_line
                );
            }
        }

        // 2) Buffer lines until either we hit the footer or reach ~100 MiB.
        let mut buf = String::new();
        let mut batch = String::new();
        let mut batch_index = 0;

        loop {
            buf.clear();
            let bytes_read = reader.read_line(&mut buf)?;
            if bytes_read == 0 {
                // EOF reached
                break;
            }

            // If this line is the footer C-line, stop reading further.
            if buf.trim_start().starts_with("C,") {
                debug!("Hit footer C-line; stopping read of {}", file_name);
                break;
            }

            // Append this line to our batch buffer
            batch.push_str(&buf);

            // If batch has grown to MAX_BATCH_BYTES or more, flush it now.
            if batch.len() >= MAX_BATCH_BYTES {
                debug!(
                    "Flushing batch {} for {} ({} bytes)",
                    batch_index,
                    file_name,
                    batch.len()
                );
                let batch_name = format!("{}-batch-{}", file_name, batch_index);
                csv_to_parquet(&batch_name, batch.as_str(), out_dir.as_ref()).unwrap_or_else(|e| {
                    panic!(
                        "csv_to_parquet error on {} batch {}: {:?}",
                        file_name, batch_index, e
                    )
                });
                batch_index += 1;
                batch.clear();
            }
        }

        // 3) After EOF or footer, flush any remaining data in `batch`.
        if !batch.is_empty() {
            debug!(
                "Flushing final batch {} for {} ({} bytes)",
                batch_index,
                file_name,
                batch.len()
            );
            let batch_name = format!("{}-batch-{}", file_name, batch_index);
            csv_to_parquet(&batch_name, batch.as_str(), out_dir.as_ref()).unwrap_or_else(|e| {
                panic!(
                    "csv_to_parquet error on {} final batch {}: {:?}",
                    file_name, batch_index, e
                )
            });
            batch.clear();
        }

        debug!("Completed all batches for {}", file_name);
    }

    debug!("Completed processing all entries in ZIP");
    Ok(())
}
