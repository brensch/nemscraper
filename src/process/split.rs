use anyhow::{Context, Result};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use tracing::{debug, instrument, warn};
use zip::ZipArchive;

use super::csv_batch_processor::CsvBatchProcessor;

/// Splits each CSV inside the ZIP into ~100 MiB chunks, breaking on every `I,` line
/// (new schema), and returns total rows processed.
#[instrument(level = "debug", skip(zip_path, out_dir), fields(zip = %zip_path.as_ref().display()))]
pub fn split_zip_to_parquet<P: AsRef<Path>, Q: AsRef<Path>>(
    zip_path: P,
    out_dir: Q,
) -> Result<i64> {
    const MAX_BATCH_BYTES: usize = 100 * 1024 * 1024; // 100 MiB

    debug!("Opening ZIP file: {}", zip_path.as_ref().display());
    let file = File::open(&zip_path).with_context(|| format!("opening {:?}", zip_path.as_ref()))?;
    let mut archive = ZipArchive::new(file).context("constructing ZipArchive from File")?;
    let total_entries = archive.len();

    let mut total_rows: i64 = 0;

    for idx in 0..total_entries {
        let entry = archive.by_index(idx)?;
        let file_name = entry.name().to_string();

        // Skip anything that isn’t a .csv
        if !file_name.to_lowercase().ends_with(".csv") {
            debug!("Skipping non-CSV entry: {}", file_name);
            continue;
        }
        debug!("Processing CSV entry: {}", file_name);

        // Wrap entry in BufReader to read line by line
        let reader = BufReader::new(entry);
        let rows_in_this =
            process_csv_entry(reader, &file_name, out_dir.as_ref(), MAX_BATCH_BYTES)?;
        total_rows += rows_in_this;
    }

    debug!(
        "Completed processing all entries in ZIP; total rows = {}",
        total_rows
    );
    Ok(total_rows)
}

/// Process a single CSV entry (streaming from `reader`), skipping its top `C` header,
/// then iterating line by line until EOF or a footer `C,`. Returns rows counted.
fn process_csv_entry<R: BufRead>(
    mut reader: R,
    file_name: &str,
    out_dir: &Path,
    max_batch_bytes: usize,
) -> Result<i64> {
    // 1) Skip the very first “C” line (metadata header). If missing, warn and proceed.
    skip_top_c_header(&mut reader, file_name);

    // 2) Create a batch‐processor that holds state (current_i_line, batch string, batch_index, row_count).
    let mut batcher = CsvBatchProcessor::new(max_batch_bytes);

    // 3) Loop through every subsequent line until EOF or a “C,” footer
    let mut buf = String::new();
    loop {
        buf.clear();
        let bytes_read = reader.read_line(&mut buf)?;
        if bytes_read == 0 {
            // EOF
            break;
        }

        let trimmed = buf.trim_start();
        if trimmed.starts_with("C,") {
            // footer reached → stop reading further
            break;
        }

        // Feed this line into the processor (it will flush or append as needed)
        batcher.feed_line(&buf, file_name, out_dir)?;
    }

    // 4) Flush any remaining batch at EOF.
    batcher.flush_final(file_name, out_dir)?;
    Ok(batcher.row_count())
}

/// Try to read exactly one line and check if it starts with a “C”. If not, warn and continue.
fn skip_top_c_header<R: BufRead>(reader: &mut R, file_name: &str) {
    let mut peek = String::new();
    match reader.read_line(&mut peek) {
        Ok(n) => {
            if n == 0 || !peek.trim_start().starts_with('C') {
                warn!(
                    "Expected a top C-line in {} but got {:?}. Continuing anyway.",
                    file_name, peek
                );
            }
        }
        Err(e) => {
            warn!(
                "Unable to read top C-line in {}: {:?}. Continuing anyway.",
                file_name, e
            );
        }
    }
}
