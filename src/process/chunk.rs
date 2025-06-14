use anyhow::{anyhow, Context, Result};
use arrow::{
    csv::ReaderBuilder,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::ArrowWriter,
    basic::{BrotliLevel, Compression},
    file::properties::WriterProperties,
};
use std::{
    fs,
    fs::File,
    io::{BufRead, BufReader, Cursor},
    path::Path,
    sync::Arc,
    time::Instant,
};

use crate::process::{
    convert::convert_to_final_types,
    schema::{analyze_batch_for_schema, SchemaInfo},
    trimming::apply_trimming,
    utils::{clean_str, extract_date_from_filename},
};
use tracing::{debug, warn};

/// Streams CSV data in-memory into Parquet files in batches, with timing diagnostics.
pub struct StreamingCsvProcessor {
    max_batch_rows: usize,
    headers: Option<Vec<String>>,
    schema_info: Option<SchemaInfo>,
    current_batch_lines: Vec<String>,
    batch_index: usize,
    row_count: u64,
    parquet_bytes: u64,
    writer: Option<ArrowWriter<File>>,
    output_path: Option<std::path::PathBuf>,
}

impl StreamingCsvProcessor {
    pub fn new(max_batch_rows: usize) -> Self {
        Self {
            max_batch_rows,
            headers: None,
            schema_info: None,
            current_batch_lines: Vec::new(),
            batch_index: 0,
            row_count: 0,
            parquet_bytes: 0,
            writer: None,
            output_path: None,
        }
    }

    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    pub fn parquet_bytes(&self) -> u64 {
        self.parquet_bytes
    }

    pub fn initialize(&mut self, file_name: &str, header_line: &str, out_dir: &Path) -> Result<()> {
        let headers: Vec<String> = header_line.trim().split(',').map(clean_str).collect();

        let partition_date = extract_date_from_filename(file_name).unwrap_or_else(|| {
            warn!(
                "No date found in filename '{}'; using default partition",
                file_name
            );
            "unknown-date".to_string()
        });

        // derive table name
        let table_name = if headers.len() >= 4 {
            format!("{}---{}---{}", headers[1], headers[2], headers[3])
        } else {
            "default_table".to_string()
        };

        let table_dir = out_dir.join(&table_name);
        fs::create_dir_all(&table_dir).context("creating table directory")?;
        let partition_dir = table_dir.join(format!("date={}", partition_date));
        fs::create_dir_all(&partition_dir).context("creating partition directory")?;

        let final_name = format!("{}.parquet", file_name);
        let tmp_name = format!("{}.tmp", final_name);
        let tmp_path = partition_dir.join(&tmp_name);

        self.headers = Some(headers);
        self.output_path = Some(tmp_path);

        debug!("Initialized processor for table: {}", table_name);
        Ok(())
    }

    pub fn feed_row(&mut self, line: &str) -> Result<()> {
        self.current_batch_lines.push(line.to_string());
        self.row_count += 1;

        if self.current_batch_lines.len() >= self.max_batch_rows {
            self.flush_current_batch()?;
        }
        Ok(())
    }

    fn flush_current_batch(&mut self) -> Result<()> {
        let t0 = Instant::now();
        if self.current_batch_lines.is_empty() {
            return Ok(());
        }

        let headers = self
            .headers
            .as_ref()
            .ok_or_else(|| anyhow!("Headers not initialized"))?;

        // build CSV in-memory
        let mut csv = headers.join(",");
        csv.push('\n');
        for ln in &self.current_batch_lines {
            csv.push_str(ln);
            if !ln.ends_with('\n') {
                csv.push('\n');
            }
        }
        let t1 = Instant::now();
        debug!("build CSV content took {:?}", t1 - t0);

        // schema inference + writer on first batch
        if self.schema_info.is_none() {
            let t_schema = Instant::now();
            let schema_info = self.infer_schema_from_batch(&csv, headers)?;
            self.schema_info = Some(schema_info);
            self.setup_parquet_writer()?;
            debug!(
                "schema inference + writer setup took {:?}",
                Instant::now() - t_schema
            );
        }

        // convert CSV → Arrow batch
        let t_conv = Instant::now();
        let batch = self.convert_csv_to_arrow_batch(&csv)?;
        debug!(
            "convert_csv_to_arrow_batch took {:?}",
            Instant::now() - t_conv
        );

        // write to Parquet
        let t_write = Instant::now();
        if let Some(w) = self.writer.as_mut() {
            w.write(&batch).context("writing batch to Parquet")?;
        }
        debug!("parquet write took {:?}", Instant::now() - t_write);
        debug!("flush_current_batch total took {:?}", Instant::now() - t0);

        self.batch_index += 1;
        self.current_batch_lines.clear();
        Ok(())
    }

    fn infer_schema_from_batch(&self, csv: &str, headers: &[String]) -> Result<SchemaInfo> {
        // initial string schema
        let fields: Vec<Field> = headers
            .iter()
            .map(|n| Field::new(n, DataType::Utf8, true))
            .collect();
        let string_schema = Schema::new(fields);

        let cursor = Cursor::new(csv.as_bytes());
        let mut rdr = ReaderBuilder::new(Arc::new(string_schema))
            .with_header(true)
            .with_batch_size(1_000)
            .build(cursor)
            .context("creating CSV reader for schema inference")?;

        let first = rdr.next().ok_or_else(|| anyhow!("No data in CSV"))??;
        analyze_batch_for_schema(&first, headers)
    }

    fn setup_parquet_writer(&mut self) -> Result<()> {
        let si = self
            .schema_info
            .as_ref()
            .ok_or_else(|| anyhow!("Schema info not available"))?;
        let out_path = self
            .output_path
            .as_ref()
            .ok_or_else(|| anyhow!("Output path not set"))?;

        let tmp = File::create(out_path).context("creating temp Parquet file")?;
        let props = WriterProperties::builder()
            .set_compression(Compression::BROTLI(BrotliLevel::try_new(5)?))
            .build();

        let writer = ArrowWriter::try_new(tmp, Arc::new(si.schema.clone()), Some(props))
            .context("initializing Parquet writer")?;
        self.writer = Some(writer);
        Ok(())
    }

    fn convert_csv_to_arrow_batch(&self, csv: &str) -> Result<RecordBatch> {
        let headers = self.headers.as_ref().unwrap();
        // build string schema for this batch
        let fields: Vec<Field> = headers
            .iter()
            .map(|n| Field::new(n, DataType::Utf8, true))
            .collect();
        let schema = Schema::new(fields);

        let cursor = Cursor::new(csv.as_bytes());
        let mut rdr = ReaderBuilder::new(Arc::new(schema))
            .with_header(true)
            .with_batch_size(self.max_batch_rows)
            .build(cursor)
            .context("creating CSV reader for batch conversion")?;

        let batch = rdr.next().ok_or_else(|| anyhow!("No data in batch"))??;
        let trimmed = apply_trimming(&batch, &self.schema_info.as_ref().unwrap().trim_columns)?;
        convert_to_final_types(&trimmed, self.schema_info.as_ref().unwrap())
    }

    /// Flush remaining data, close writer, and rename file
    pub fn finalize(&mut self) -> Result<u64> {
        if !self.current_batch_lines.is_empty() {
            self.flush_current_batch()?;
        }
        if let Some(mut w) = self.writer.take() {
            w.close().context("closing Parquet writer")?;
        }

        let out_path = self
            .output_path
            .as_ref()
            .ok_or_else(|| anyhow!("Output path not set"))?;
        let meta = fs::metadata(out_path).context("getting file metadata")?;
        let bytes = meta.len();
        self.parquet_bytes = bytes;

        let final_path = out_path.with_extension("parquet");
        fs::rename(out_path, &final_path).context("renaming Parquet file")?;

        debug!(
            "Finalized Parquet file: {} bytes to {}",
            bytes,
            final_path.display()
        );
        Ok(bytes)
    }
}

/// Top-level helper: stream a CSV string → Parquet streaming
pub fn csv_to_parquet_streaming<R: BufRead>(
    mut reader: R,
    file_name: &str,
    out_dir: &Path,
) -> Result<u64> {
    const MAX_BATCH_ROWS: usize = 18_192;
    let mut proc = StreamingCsvProcessor::new(MAX_BATCH_ROWS);

    // header
    let mut hdr = String::new();
    reader.read_line(&mut hdr)?;
    proc.initialize(file_name, &hdr, out_dir)?;

    let mut line = String::new();
    loop {
        line.clear();
        let n = reader.read_line(&mut line)?;
        if n == 0 {
            break;
        }
        if line.trim().is_empty() {
            continue;
        }
        proc.feed_row(&line)?;
    }

    proc.finalize()
}

/// Legacy compatibility: full CSV string → Parquet
pub fn csv_to_parquet(file_name: &str, data: &str, out_dir: &Path) -> Result<u64> {
    let cursor = Cursor::new(data.as_bytes());
    let buf = BufReader::new(cursor);
    csv_to_parquet_streaming(buf, file_name, out_dir)
}
