use anyhow::Result;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use tracing::{debug, instrument, warn};
use zip::ZipArchive;

use super::chunk_polars::csv_to_parquet;

/// Splits each CSV inside the ZIP into ~100 MiB chunks, but also breaks on every `I,` line:
/// whenever we see an `I,` at the start of a row, that marks a schema change. We flush the
/// old chunk (if any), then begin a new chunk whose first line is that `I,` header. We also
/// never let any single chunk exceed MAX_BATCH_BYTES—if it would, we flush and start a fresh
/// chunk, always re‐adding the same `I,` header at the top.
///
/// Returns the total number of data‐rows processed (i.e. each `I,`‐header plus each subsequent
/// data line belonging to a schema).
#[instrument(level = "debug", skip(zip_path, out_dir), fields(zip = %zip_path.as_ref().display()))]
pub fn split_zip_to_parquet<P: AsRef<Path>, Q: AsRef<Path>>(
    zip_path: P,
    out_dir: Q,
    // schema_store: Arc<SchemaStore>,
) -> Result<i64> {
    const MAX_BATCH_BYTES: usize = 100 * 1024 * 1024; // 100 MiB

    debug!("Opening ZIP file: {}", zip_path.as_ref().display());
    let file = File::open(&zip_path)?;
    let mut archive = ZipArchive::new(file)?;
    let total_entries = archive.len();
    debug!("ZIP archive contains {} entries", total_entries);

    // Count of data rows (each `I,` header counts as one, plus each non-header line that goes in a batch)
    let mut total_rows: i64 = 0;

    for idx in 0..total_entries {
        let entry = archive.by_index(idx)?;
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

        // 2) Maintain:
        //    - `current_i_line`: the last-seen "I," header that defines the current schema.
        //    - `batch`: the accumulating chunk (always starts with current_i_line, once set).
        // Whenever we see a new `I,` line, we flush the old batch (if nonempty),
        // then begin batch = that `I,` line. We also flush if batch.len() ≥ MAX_BATCH_BYTES,
        // but always re-add current_i_line at the top of the new chunk.
        let mut buf = String::new();
        let mut current_i_line: Option<String> = None;
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

            // Check if this line starts with "I," → new schema
            if buf.trim_start().starts_with("I,") {
                // Count this I-line as a data row
                total_rows += 1;

                // 2a) If we already have a batch (i.e. current_i_line.is_some()), flush it now.
                if !batch.is_empty() {
                    debug!(
                        "Flushing batch {} for {} ({} bytes) due to new I-line",
                        batch_index,
                        file_name,
                        batch.len()
                    );
                    let batch_name = format!("{}-batch-{}", file_name, batch_index);
                    csv_to_parquet(&batch_name, batch.as_str(), out_dir.as_ref()).unwrap_or_else(
                        |e| {
                            panic!(
                                "csv_to_parquet error on {} batch {}: {:?}",
                                file_name, batch_index, e
                            )
                        },
                    );
                    batch_index += 1;
                    batch.clear();
                }

                // 2b) Now start a brand‐new batch whose first line is this I, header
                current_i_line = Some(buf.clone());
                batch.push_str(&buf);
                continue;
            }

            // 2c) Any non-I line:
            match &current_i_line {
                Some(i_line) => {
                    // We are inside a schema group. Append this line to batch.
                    batch.push_str(&buf);
                    // Count this data line as a row
                    total_rows += 1;

                    // 2d) If batch has grown too large, flush it but keep the same I-line.
                    if batch.len() >= MAX_BATCH_BYTES {
                        debug!(
                            "Flushing batch {} for {} ({} bytes) due to size limit",
                            batch_index,
                            file_name,
                            batch.len()
                        );
                        let batch_name = format!("{}-batch-{}", file_name, batch_index);
                        csv_to_parquet(&batch_name, batch.as_str(), out_dir.as_ref())
                            .unwrap_or_else(|e| {
                                panic!(
                                    "csv_to_parquet error on {} batch {}: {:?}",
                                    file_name, batch_index, e
                                )
                            });
                        batch_index += 1;

                        // Start a fresh batch containing only the I-line at top
                        batch.clear();
                        batch.push_str(i_line);
                    }
                }
                None => {
                    // We haven't seen any I, yet → skip lines until the first I,
                    // because we don't know the schema. In practice, every CSV after
                    // the C header should start with "I,".
                    continue;
                }
            }
        }

        // 3) After EOF or footer, flush any remaining data in batch.
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

    debug!(
        "Completed processing all entries in ZIP; total rows = {}",
        total_rows
    );
    Ok(total_rows)
}
