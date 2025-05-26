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
    fmt, fs,
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

/// Events that can be recorded in history.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum Event {
    Downloaded,
    Processed,
    Retried,
}

impl Event {
    /// Returns the string representation used in filenames.
    pub fn as_str(&self) -> &'static str {
        match self {
            Event::Downloaded => "downloaded",
            Event::Processed => "processed",
            Event::Retried => "retried",
        }
    }
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

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

    /// Record an event for `zip_name`.
    /// Writes a single-row Parquet file named `<zip>_<event>_<ts>.parquet`.
    pub fn record_event(&self, zip_name: &str, event: Event) -> Result<()> {
        // Timestamp in microseconds
        let ts = Utc::now().timestamp_micros();
        let filename = format!("{}---{}---{}.parquet", zip_name, event.as_str(), ts);
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
        let arr_event = Arc::new(StringArray::from(vec![event.as_str().to_string()]))
            as Arc<dyn arrow::array::Array>;
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
    pub fn load_event_names(&self, event: Event) -> Result<HashSet<String>> {
        let mut set = HashSet::new();
        let pattern = format!(
            "{}/**/*---{}---*.parquet",
            self.history_dir.display(),
            event.as_str()
        );
        for entry in glob(&pattern)? {
            if let Ok(path) = entry {
                if let Some(fname) = path.file_stem().and_then(|s| s.to_str()) {
                    // fname = "<zip>_<event>_<ts>"
                    if let Some(idx) = fname.rfind(&format!("---{}---", event.as_str())) {
                        let zip = &fname[..idx];
                        set.insert(zip.to_string());
                    }
                }
            }
        }
        Ok(set)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::fs;
    use std::thread::sleep;
    use std::time::Duration;
    use tempfile::TempDir;

    #[test]
    fn record_event_and_load_event_names() -> Result<()> {
        // 1) Create a temporary directory for history
        let tmp = TempDir::new().expect("failed to create temp dir");
        let history_dir = tmp.path().join("history_store");

        // 2) Construct the History; should create the directory
        let history = History::new(&history_dir)?;
        assert!(history_dir.exists() && history_dir.is_dir());

        // 3) Record two events for two different zip_names
        let zip1 = "archive1.zip";
        let zip2 = "archive2.zip";
        let event = Event::Processed;

        history.record_event(zip1, event)?;
        // Sleep a tiny bit so timestamps differ
        sleep(Duration::from_millis(1));
        history.record_event(zip2, event)?;

        // 4) Ensure that parquet files actually exist on disk
        let entries: Vec<_> = fs::read_dir(&history_dir)?
            .map(|e| e.expect("read_dir error").path())
            .collect();
        assert_eq!(entries.len(), 2, "should have written 2 parquet files");

        // 5) Load back the set of zip names for our event
        let names = history.load_event_names(event)?;
        let mut expected = HashSet::new();
        expected.insert(zip1.to_string());
        expected.insert(zip2.to_string());

        assert_eq!(names, expected);
        Ok(())
    }

    #[test]
    fn load_event_names_from_existing_directory() -> Result<()> {
        // specify an actual directory for history relative to current path
        let history_dir = PathBuf::from("./assets/history");

        // Create dummy files matching and not matching the pattern
        let files = vec![
            format!("zipA_{}_123.parquet", Event::Downloaded.as_str()),
            format!("zipB_{}_456.parquet", Event::Downloaded.as_str()),
            format!("zipA_{}_789.parquet", Event::Processed.as_str()),
            "not_history.txt".to_string(),
            format!("zipC_other_000.parquet"),
        ];
        for f in &files {
            fs::write(history_dir.join(f), b"")?;
        }

        let history = History::new(&history_dir)?;
        let names = history.load_event_names(Event::Downloaded)?;

        let mut expected = HashSet::new();
        expected.insert("zipA".to_string());
        expected.insert("zipB".to_string());

        assert_eq!(names, expected);
        Ok(())
    }
}
