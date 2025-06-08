use anyhow::{Context, Result};
use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::Schema as ArrowSchema,
    record_batch::RecordBatch,
};
use chrono::{NaiveDate, Utc};
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
    /// Extract unique key from an existing batch row (for scanning)
    fn extract_key(batch: &RecordBatch, row: usize) -> String {
        let arr = batch
            .column(Self::KEY_COLUMN)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("KEY_COLUMN must be StringArray");
        arr.value(row).to_string()
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

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use arrow::{
        array::{ArrayRef, StringArray, UInt64Array},
        datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema},
    };
    use chrono::NaiveDate;
    use std::sync::Arc;
    use tempfile::tempdir;

    /// A minimal test row: just a string key, a partition date, and a u64 value.
    struct DummyRow {
        key: String,
        date: NaiveDate,
        value: u64,
    }

    impl HistoryRow for DummyRow {
        fn partition_date(&self) -> NaiveDate {
            self.date
        }

        fn schema() -> ArrowSchema {
            ArrowSchema::new(vec![
                Field::new("key", ArrowDataType::Utf8, false),
                Field::new("value", ArrowDataType::UInt64, false),
            ])
        }

        fn to_arrays(&self) -> Vec<ArrayRef> {
            vec![
                Arc::new(StringArray::from(vec![self.key.clone()])),
                Arc::new(UInt64Array::from(vec![self.value])),
            ]
        }

        fn unique_key(&self) -> String {
            self.key.clone()
        }

        const KEY_COLUMN: usize = 0;
    }

    #[test]
    fn add_get_vacuum_and_scan_again() -> Result<()> {
        // 1) create empty TableHistory in a temp dir
        let tmp = tempdir()?;
        let base_dir = tmp.path();
        let table_name = "test_table";

        // new() should see nothing
        let hist = TableHistory::<DummyRow>::new(base_dir, table_name)?;
        assert!(!hist.get("row1".into()), "should not find row1 yet");

        // 2) add first row and check get()
        let row1 = DummyRow {
            key: "row1".into(),
            date: NaiveDate::from_ymd_opt(2025, 6, 8).unwrap(),
            value: 42,
        };
        hist.add(&row1)?;
        assert!(hist.get("row1".into()), "row1 should now be present");

        // 3) add second row under a different key
        let row2 = DummyRow {
            key: "row2".into(),
            date: NaiveDate::from_ymd_opt(2025, 6, 8).unwrap(),
            value: 99,
        };
        hist.add(&row2)?;
        assert!(hist.get("row2".into()), "row2 should also be present");

        // 4) vacuum consolidates all parquet files
        hist.vacuum()?;

        // 5) drop original and re-open to scan on-disk
        drop(hist);
        let hist2 = TableHistory::<DummyRow>::new(base_dir, table_name)?;

        // 6) both keys should survive across restarts
        assert!(hist2.get("row1".into()), "row1 must survive a restart");
        assert!(hist2.get("row2".into()), "row2 must survive a restart");

        Ok(())
    }
}
