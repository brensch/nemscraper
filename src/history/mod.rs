// src/history/mod.rs

use anyhow::{Context, Result};
use arrow::array::{StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use glob::glob;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::{
    collections::HashSet,
    fs,
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

/// A simple history manager backed by Parquet files.
pub struct History {
    history_dir: PathBuf,
}

impl History {
    /// Construct a new History store at `history_dir`, creating the directory if needed.
    pub fn new(history_dir: impl Into<PathBuf>) -> Result<Self> {
        let history_dir = history_dir.into();
        fs::create_dir_all(&history_dir)
            .with_context(|| format!("creating history directory {:?}", &history_dir))?;
        Ok(Self { history_dir })
    }

    /// Record an event for `zip_name` (e.g. "downloaded", "processed").
    /// Writes a single-row Parquet file named `<zip>_<event>_<ts>.parquet`.
    pub fn record_event(&self, zip_name: &str, event: &str) -> Result<()> {
        // Timestamp in microseconds
        let ts = Utc::now().timestamp_micros();
        let filename = format!("{}_{}_{}.parquet", zip_name, event, ts);
        let path = self.history_dir.join(filename);

        // Define schema: zip_name, event, event_time
        let schema = Schema::new(vec![
            Field::new("zip_name", DataType::Utf8, false),
            Field::new("event", DataType::Utf8, false),
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]);

        // Build Arrow arrays
        let arr_zip =
            Arc::new(StringArray::from(vec![zip_name.to_string()])) as Arc<dyn arrow::array::Array>;
        let arr_event =
            Arc::new(StringArray::from(vec![event.to_string()])) as Arc<dyn arrow::array::Array>;
        let arr_time = Arc::new(TimestampMicrosecondArray::from_iter_values(vec![ts]))
            as Arc<dyn arrow::array::Array>;

        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![arr_zip, arr_event, arr_time])
                .context("building history record batch")?;
        let file =
            File::create(&path).with_context(|| format!("creating history file {:?}", &path))?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props))
            .context("creating Arrow writer for history")?;
        writer.write(&batch).context("writing history batch")?;
        writer.close().context("closing history writer")?;
        Ok(())
    }

    /// Load all distinct `zip_name`s for the given `event` by scanning filenames.
    /// Uses glob pattern matching on `<zip>_<event>_*.parquet`.
    pub fn load_event_names(&self, event: &str) -> Result<HashSet<String>> {
        let mut set = HashSet::new();
        let pattern = format!("{}/**/*_{}_*.parquet", self.history_dir.display(), event);
        for entry in glob(&pattern)? {
            if let Ok(path) = entry {
                if let Some(fname) = path.file_stem().and_then(|s| s.to_str()) {
                    // fname = "<zip>_<event>_<ts>"
                    if let Some(idx) = fname.rfind(&format!("_{}_", event)) {
                        let zip = &fname[..idx];
                        set.insert(zip.to_string());
                    }
                }
            }
        }
        Ok(set)
    }
}
