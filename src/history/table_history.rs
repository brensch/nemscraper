use anyhow::{Context, Result};
use arrow::{
    array::{ArrayRef, StringArray, TimestampMicrosecondArray, UInt64Array},
    datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema, TimeUnit},
    record_batch::RecordBatch,
};
use chrono::{DateTime, NaiveDate, Utc};
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
            .expect("KEY_COLUMN must be StringArray");
        let key = arr.value(row);
        let ts_arr = batch
            .column(Self::TIME_COLUMN)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("TIME_COLUMN must be TimestampMicrosecondArray");
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
    /// Create and scan existing data into `seen`. Does *not* start vacuum loop.
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

        Ok(Arc::new(Self {
            base_dir,
            table: table.to_string(),
            schema,
            seen: Arc::new(Mutex::new(seen_set)),
            _marker: PhantomData,
        }))
    }

    /// Spawn a background vacuum loop (every 30s).
    pub fn start_vacuum_loop(self: &Arc<Self>) {
        let tbl_clone = Arc::clone(self);
        thread::spawn(move || loop {
            thread::sleep(Duration::from_secs(30));
            if let Err(e) = tbl_clone.vacuum() {
                eprintln!("vacuum error: {:?}", e);
            }
        });
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
            let cons = dir.join("consolidated.parquet");
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

// ----- Tests -----
#[cfg(test)]
mod tests {
    use crate::history::processed::ProcessedRow;

    use super::*;
    use chrono::{Duration, Utc};
    use glob::glob;
    use tempfile::tempdir;

    #[test]
    fn test_add_and_get() {
        let tmp = tempdir().unwrap();
        let hist = TableHistory::<ProcessedRow>::new_processed(tmp.path()).unwrap();
        hist.start_vacuum_loop();

        let now = Utc::now();
        let row = ProcessedRow {
            filename: "file1.csv".to_string(),
            total_rows: 10,
            size_bytes: 100,
            processing_start: now,
            processing_end: now,
        };

        assert!(!hist.get(row.unique_key()));
        hist.add(&row).unwrap();
        assert!(hist.get(row.unique_key()));

        let date_str = now.date_naive().format("%Y%m%d").to_string();
        let part_dir = tmp
            .path()
            .join("processed")
            .join(format!("date={}", date_str));
        let files: Vec<_> = glob(&format!("{}/*.parquet", part_dir.display()))
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        assert_eq!(files.len(), 1);
    }

    #[test]
    fn test_deduplication() {
        let tmp = tempdir().unwrap();
        let hist = TableHistory::<ProcessedRow>::new_processed(tmp.path()).unwrap();
        hist.start_vacuum_loop();

        let now = Utc::now();
        let row = ProcessedRow {
            filename: "file2.csv".to_string(),
            total_rows: 20,
            size_bytes: 200,
            processing_start: now,
            processing_end: now,
        };

        hist.add(&row).unwrap();
        let date_str = now.date_naive().format("%Y%m%d").to_string();
        let pattern = format!(
            "{}/processed/date={}/**/*.parquet",
            tmp.path().display(),
            date_str
        );
        let count1 = glob(&pattern).unwrap().filter_map(Result::ok).count();
        assert_eq!(count1, 1);

        hist.add(&row).unwrap();
        let count2 = glob(&pattern).unwrap().filter_map(Result::ok).count();
        assert_eq!(count1, count2);
    }

    #[test]
    fn test_vacuum_consolidates() {
        let tmp = tempdir().unwrap();
        let hist = TableHistory::<ProcessedRow>::new_processed(tmp.path()).unwrap();

        let now = Utc::now();
        let row1 = ProcessedRow {
            filename: "file3.csv".to_string(),
            total_rows: 30,
            size_bytes: 300,
            processing_start: now,
            processing_end: now,
        };
        let later = now + Duration::microseconds(1);
        let row2 = ProcessedRow {
            filename: "file4.csv".to_string(),
            total_rows: 40,
            size_bytes: 400,
            processing_start: now,
            processing_end: later,
        };

        hist.add(&row1).unwrap();
        hist.add(&row2).unwrap();

        let date_str = now.date_naive().format("%Y%m%d").to_string();
        let glob_pattern = format!(
            "{}/processed/date={}/**/*.parquet",
            tmp.path().display(),
            date_str
        );

        // ensure two files before vacuum
        let before = glob(&glob_pattern).unwrap().filter_map(Result::ok).count();
        assert_eq!(before, 2);

        hist.vacuum().unwrap();

        // after vacuum, only consolidated.parquet remains
        let after_paths: Vec<_> = glob(&glob_pattern)
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        assert_eq!(after_paths.len(), 1);
        assert_eq!(
            after_paths[0].file_name().unwrap().to_string_lossy(),
            "consolidated.parquet"
        );
    }

    #[test]
    fn test_persistence_across_restarts() {
        let tmp = tempdir().unwrap();
        // first instance: write one row
        let key: String;
        {
            let hist = TableHistory::<ProcessedRow>::new_processed(tmp.path()).unwrap();
            hist.start_vacuum_loop();
            let now = Utc::now();
            let row = ProcessedRow {
                filename: "file5.csv".to_string(),
                total_rows: 50,
                size_bytes: 500,
                processing_start: now,
                processing_end: now,
            };
            key = row.unique_key();
            hist.add(&row).unwrap();
            assert!(hist.get(key.clone()));
        }

        // second instance: should pick up the existing file
        let hist2 = TableHistory::<ProcessedRow>::new_processed(tmp.path()).unwrap();
        assert!(hist2.get(key));
    }
}
