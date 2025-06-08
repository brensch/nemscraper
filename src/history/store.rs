use anyhow::{Context, Result};
use arrow::{
    array::{ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema, TimeUnit},
    record_batch::RecordBatch,
};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use glob::glob;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use std::{
    collections::HashSet,
    fs::{self, File},
    io::BufWriter,
    marker::PhantomData,
    path::PathBuf,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

/// Trait representing a row in the history table.
/// - Defines schema, to_arrays, unique_key for writes.
/// - Provides column indices and a small extractor for dedupe scanning.
pub trait HistoryRow: Sized {
    /// Partition date (UTC naive) for hive partitioning
    fn partition_date(&self) -> NaiveDate;
    /// Arrow schema for this row type
    fn schema() -> ArrowSchema;
    /// Convert this row into column arrays matching the schema
    fn to_arrays(&self) -> Vec<ArrayRef>;
    /// Unique dedupe key for this row (used when writing)
    fn unique_key(&self) -> String;
    /// Column index for key in schema
    const KEY_COLUMN: usize;
    /// Column index for timestamp in schema
    const TIME_COLUMN: usize;
    /// Extract unique key from an existing batch row (for scanning)
    fn extract_key(batch: &RecordBatch, row: usize) -> String {
        let arr = batch
            .column(Self::KEY_COLUMN)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("KEY_COLUMN type must be StringArray");
        let key = arr.value(row);
        let ts_arr = batch
            .column(Self::TIME_COLUMN)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("TIME_COLUMN type must be TimestampMicrosecondArray");
        let ts = ts_arr.value(row);
        format!("{}--{}", key, ts)
    }
}

/// Generic hive-partitioned history table.
pub struct TableHistory<R: HistoryRow> {
    base_dir: PathBuf,
    table: String,
    schema: Arc<ArrowSchema>,
    seen: Arc<Mutex<HashSet<String>>>,
    _marker: PhantomData<R>,
}

impl<R: HistoryRow + Send + Sync + 'static> TableHistory<R> {
    /// Create, scan existing data into `seen`, and spawn a background vacuum loop.
    pub fn new(base_dir: impl Into<PathBuf>, table: &str) -> Result<Arc<Self>> {
        let base_dir = base_dir.into();
        let table_dir = base_dir.join(table);
        fs::create_dir_all(&table_dir)
            .with_context(|| format!("could not create `{}`", table_dir.display()))?;

        let schema = Arc::new(R::schema());
        let mut seen_set = HashSet::new();

        // Scan dedupe keys only
        for part in fs::read_dir(&table_dir)? {
            let part = part?;
            if !part.file_type()?.is_dir() {
                continue;
            }
            let part_dir = part.path();
            for entry in glob(&format!("{}/*.parquet", part_dir.display()))? {
                let path = entry?;
                let file = File::open(&path)
                    .with_context(|| format!("failed to open `{}`", path.display()))?;
                let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)?
                    .with_batch_size(1024)
                    .build()?;
                while let Some(batch) = reader.next().transpose()? {
                    for i in 0..batch.num_rows() {
                        let key = R::extract_key(&batch, i);
                        seen_set.insert(key);
                    }
                }
            }
        }

        let tbl = Arc::new(Self {
            base_dir,
            table: table.to_string(),
            schema,
            seen: Arc::new(Mutex::new(seen_set)),
            _marker: PhantomData,
        });

        // spawn vacuum loop: runs every 30s
        {
            let tbl_clone = Arc::clone(&tbl);
            thread::spawn(move || loop {
                thread::sleep(Duration::from_secs(30));
                if let Err(e) = tbl_clone.vacuum() {
                    eprintln!("vacuum error: {:?}", e);
                }
            });
        }

        Ok(tbl)
    }

    /// Add a new row of type R
    pub fn add(&self, row: &R) -> Result<()> {
        let key = row.unique_key();
        {
            let mut seen = self.seen.lock().unwrap();
            if !seen.insert(key.clone()) {
                return Ok(());
            }
        }

        let date = row.partition_date();
        let arrays = row.to_arrays();
        let partition = format!("date={}", date.format("%Y%m%d"));
        let dir = self.base_dir.join(&self.table).join(partition);
        fs::create_dir_all(&dir)?;

        let ts = Utc::now().timestamp_micros();
        let fname = format!("{}---{}.parquet", key, ts);
        let tmp = dir.join(format!("{}.tmp", fname));
        let final_path = dir.join(&fname);

        let file = File::create(&tmp)?;
        let mut writer = ArrowWriter::try_new(BufWriter::new(file), self.schema.clone(), None)?;
        let batch = RecordBatch::try_new(self.schema.clone(), arrays)?;
        writer.write(&batch)?;
        writer.close()?;
        fs::rename(&tmp, &final_path)?;
        Ok(())
    }

    /// Check if a row exists by its dedupe key
    pub fn get(&self, key: String) -> bool {
        let seen = self.seen.lock().unwrap();
        seen.contains(&key)
    }

    /// Vacuum each partition into one consolidated file
    pub fn vacuum(&self) -> Result<()> {
        let table_dir = self.base_dir.join(&self.table);
        for part in fs::read_dir(&table_dir)? {
            let part = part?;
            if !part.file_type()?.is_dir() {
                continue;
            }
            let dir = part.path();

            let files = glob(&format!("{}/*.parquet", dir.display()))?
                .filter_map(Result::ok)
                .collect::<Vec<_>>();
            if files.is_empty() {
                continue;
            }

            let tmp = dir.join("consolidated.parquet.tmp");
            let cons = dir.join("consolidated.parquet");
            let file = File::create(&tmp)?;
            let mut writer = ArrowWriter::try_new(BufWriter::new(file), self.schema.clone(), None)?;

            for p in &files {
                let f = File::open(p)?;
                let mut reader = ParquetRecordBatchReaderBuilder::try_new(f)?
                    .with_batch_size(1024)
                    .build()?;
                while let Some(batch) = reader.next().transpose()? {
                    writer.write(&batch)?;
                }
            }
            writer.close()?;
            fs::rename(&tmp, &cons)?;

            for p in files {
                if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
                    if name != "consolidated.parquet" {
                        fs::remove_file(p)?;
                    }
                }
            }
        }
        Ok(())
    }
}

// ------------------------------------------------------------------------------------------------
// Define row types for `downloaded` and `processed`, implementing HistoryRow
// ------------------------------------------------------------------------------------------------

pub struct DownloadedRow {
    pub filename: String,
    pub url: String,
    pub size_bytes: i64,
    pub download_start: DateTime<Utc>,
    pub download_end: DateTime<Utc>,
}

impl HistoryRow for DownloadedRow {
    const KEY_COLUMN: usize = 0;
    const TIME_COLUMN: usize = 4;

    fn partition_date(&self) -> NaiveDate {
        // now trivial:
        self.download_end.date_naive()
    }

    fn schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            Field::new("filename", ArrowDataType::Utf8, false),
            Field::new("url", ArrowDataType::Utf8, false),
            Field::new("size_bytes", ArrowDataType::Int64, false),
            Field::new(
                "download_start",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new(
                "download_end",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ])
    }

    fn to_arrays(&self) -> Vec<ArrayRef> {
        vec![
            Arc::new(StringArray::from(vec![self.filename.clone()])),
            Arc::new(StringArray::from(vec![self.url.clone()])),
            Arc::new(Int64Array::from(vec![self.size_bytes])),
            Arc::new(TimestampMicrosecondArray::from(vec![self
                .download_start
                .timestamp_micros()])),
            Arc::new(TimestampMicrosecondArray::from(vec![self
                .download_end
                .timestamp_micros()])),
        ]
    }

    fn unique_key(&self) -> String {
        format!(
            "{}--{}",
            self.filename,
            self.download_end.timestamp_micros()
        )
    }
}

// And similarly for your ProcessedRow:

pub struct ProcessedRow {
    pub filename: String,
    pub total_rows: i64,
    pub size_bytes: i64,
    pub processing_start: DateTime<Utc>,
    pub processing_end: DateTime<Utc>,
}

impl HistoryRow for ProcessedRow {
    const KEY_COLUMN: usize = 0;
    const TIME_COLUMN: usize = 4;

    fn partition_date(&self) -> NaiveDate {
        self.processing_end.date_naive()
    }

    fn schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            Field::new("filename", ArrowDataType::Utf8, false),
            Field::new("total_rows", ArrowDataType::Int64, false),
            Field::new("size_bytes", ArrowDataType::Int64, false),
            Field::new(
                "processing_start",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new(
                "processing_end",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ])
    }

    fn to_arrays(&self) -> Vec<ArrayRef> {
        vec![
            Arc::new(StringArray::from(vec![self.filename.clone()])),
            Arc::new(Int64Array::from(vec![self.total_rows])),
            Arc::new(Int64Array::from(vec![self.size_bytes])),
            Arc::new(TimestampMicrosecondArray::from(vec![self
                .processing_start
                .timestamp_micros()])),
            Arc::new(TimestampMicrosecondArray::from(vec![self
                .processing_end
                .timestamp_micros()])),
        ]
    }

    fn unique_key(&self) -> String {
        format!(
            "{}--{}",
            self.filename,
            self.processing_end.timestamp_micros()
        )
    }
}

impl TableHistory<DownloadedRow> {
    pub fn new_downloaded(base: impl Into<PathBuf>) -> Result<Arc<Self>> {
        TableHistory::new(base, "downloaded")
    }
}
impl TableHistory<ProcessedRow> {
    pub fn new_processed(base: impl Into<PathBuf>) -> Result<Arc<Self>> {
        TableHistory::new(base, "processed")
    }
}

// Usage example:
// let downloaded = TableHistory::<DownloadedRow>::new_downloaded("./history_dir")?;
// let processed  = TableHistory::<ProcessedRow>::new_processed("./history_dir")?;
// downloaded.add(&DownloadedRow { filename: "file.csv".into(), url: "http://...".into(), size_bytes: 123, download_start: Utc::now().timestamp_micros(), download_end: Utc::now().timestamp_micros() })?;
// processed.add(&ProcessedRow { filename: "file.csv".into(), total_rows: 100, size_bytes: 456, processing_start: Utc::now().timestamp_micros(), processing_end: Utc::now().timestamp_micros() })?;
