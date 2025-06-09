use anyhow::{Context, Result};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use tracing::{debug, instrument, warn};
use zip::ZipArchive;

use super::csv_batch_processor::CsvBatchProcessor;

/// A simple accumulator for rows and bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowsAndBytes {
    pub rows: u64,
    pub bytes: u64,
}

impl RowsAndBytes {
    pub const ZERO: Self = RowsAndBytes { rows: 0, bytes: 0 };

    /// Add another RowsAndBytes into `self`, saturating on overflow.
    pub fn add(&mut self, other: RowsAndBytes) {
        self.rows = self.rows.saturating_add(other.rows);
        self.bytes = self.bytes.saturating_add(other.bytes);
    }
}

/// Splits each CSV inside the ZIP into ~100 MiB chunks, breaking on every `I,` line
/// (new schema), and returns total rows processed.
#[instrument(level = "debug", skip(zip_path, out_dir), fields(zip = %zip_path.as_ref().display()))]
pub fn split_zip_to_parquet<P: AsRef<Path>, Q: AsRef<Path>>(
    zip_path: P,
    out_dir: Q,
) -> Result<RowsAndBytes> {
    const MAX_BATCH_BYTES: usize = 100 * 1024 * 1024; // 100 MiB

    let file = File::open(&zip_path).with_context(|| format!("opening {:?}", zip_path.as_ref()))?;
    let mut archive = ZipArchive::new(file).context("constructing ZipArchive from File")?;

    let mut totals = RowsAndBytes::ZERO;

    for idx in 0..archive.len() {
        let entry = archive.by_index(idx)?;
        let name = entry.name().to_string();

        if !name.to_lowercase().ends_with(".csv") {
            continue;
        }

        let reader = BufReader::new(entry);
        // process_csv_entry now returns RowsAndBytes
        let entry_counts = process_csv_entry(reader, &name, out_dir.as_ref(), MAX_BATCH_BYTES)
            .with_context(|| format!("processing CSV entry {}", name))?;

        totals.add(entry_counts);
    }

    debug!(
        "Completed ZIP: total rows = {}, total bytes = {}",
        totals.rows, totals.bytes
    );
    Ok(totals)
}

/// Process a single CSV entry (streaming from `reader`), skipping its top `C` header,
/// then iterating line by line until EOF or a footer `C,`. Returns rows counted.
fn process_csv_entry<R: BufRead>(
    mut reader: R,
    file_name: &str,
    out_dir: &Path,
    max_batch_bytes: usize,
) -> Result<RowsAndBytes> {
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
    let rows = batcher.row_count();
    let bytes = batcher.parquet_bytes();
    Ok(RowsAndBytes { rows, bytes })
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

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use glob::glob;
    use std::{fs, path::PathBuf};
    use tempfile::tempdir;
    use tracing::{info, Level};
    use tracing_subscriber::{fmt, EnvFilter};

    fn init_logging() {
        // initialize tracing subscriber once at INFO level
        let _ = fmt()
            .with_env_filter(EnvFilter::new("debug"))
            .with_target(false)
            .try_init();
    }

    #[test]
    fn integration_split_zip_to_parquet_real_file() -> Result<()> {
        init_logging();

        // 1) Path to a real ZIP file in your repo
        let zip_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("./test_assets/PUBLIC_MTPASADUIDAVAILABILITY_202207201800_0000000367313233.zip");

        // 2) Create a temp directory for output
        let output_dir = tempdir()?;

        // 3) Call the function under test
        let RowsAndBytes { rows, bytes } = split_zip_to_parquet(&zip_path, output_dir.path())?;

        // 4) Basic sanity checks
        assert!(rows > 0, "Expected at least one row, got {}", rows);
        assert!(bytes > 0, "Expected some bytes, got {}", bytes);

        // 5) Recursively find all .parquet files under the output directory
        let pattern = format!("{}/**/*.parquet", output_dir.path().display());
        let parquet_files: Vec<_> = glob(&pattern)?.filter_map(Result::ok).collect();
        info!("Parquet files generated: {:?}", parquet_files);
        assert!(
            !parquet_files.is_empty(),
            "No .parquet files found in {:?}",
            output_dir.path()
        );

        Ok(())
    }

    #[test]
    fn integration_split_big_zip() -> Result<()> {
        init_logging();

        // 1) Path to a real ZIP file in your repo
        let zip_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("./test_assets/PUBLIC_NEXT_DAY_FPPMW_20250414_0000000459215330.zip");

        // 2) Create a temp directory for output
        let output_dir = tempdir()?;

        // 3) Call the function under test
        let RowsAndBytes { rows, bytes } = split_zip_to_parquet(&zip_path, output_dir.path())?;

        // 4) Basic sanity checks
        assert!(rows > 0, "Expected at least one row, got {}", rows);
        assert!(bytes > 0, "Expected some bytes, got {}", bytes);

        // 5) Recursively find all .parquet files under the output directory
        let pattern = format!("{}/**/*.parquet", output_dir.path().display());
        let parquet_files: Vec<_> = glob(&pattern)?.filter_map(Result::ok).collect();
        info!("Parquet files generated: {:?}", parquet_files);
        assert!(
            !parquet_files.is_empty(),
            "No .parquet files found in {:?}",
            output_dir.path()
        );

        Ok(())
    }

    #[test]
    fn integration_split_gets6() -> Result<()> {
        init_logging();

        // 1) Path to a real ZIP file in your repo
        let zip_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("./test_assets/PUBLIC_FPP_RUN_202506091945_0000000466987122.zip");

        // 2) Create a temp directory for output
        let output_dir = tempdir()?;

        // 3) Call the function under test
        let RowsAndBytes { rows, bytes } = split_zip_to_parquet(&zip_path, output_dir.path())?;

        // 4) Basic sanity checks
        assert!(rows > 0, "Expected at least one row, got {}", rows);
        assert!(bytes > 0, "Expected some bytes, got {}", bytes);

        // 5) Recursively find all .parquet files under the output directory
        let pattern = format!("{}/**/*.parquet", output_dir.path().display());
        let parquet_files: Vec<_> = glob(&pattern)?.filter_map(Result::ok).collect();
        info!("Parquet files generated: {:?}", parquet_files);
        assert!(
            !parquet_files.is_empty(),
            "No .parquet files found in {:?}",
            output_dir.path()
        );

        Ok(())
    }
}
