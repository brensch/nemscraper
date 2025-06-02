// src/history/mod.rs

use anyhow::{Context, Result};
use arrow::array::Array;
use arrow::array::{ArrayRef, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDateTime, Utc};
use glob::glob;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use std::collections::HashMap;
use std::{
    collections::HashSet,
    fs::{self, File},
    io::BufWriter,
    path::PathBuf,
    sync::{Arc, Mutex},
};

/// Represents the two states we track for each file:
/// - Downloaded: the file has been downloaded.
/// - Processed: the file has been processed.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum State {
    Downloaded,
    Processed,
}

impl State {
    /// Convert the enum to a string for writing to Parquet.
    fn as_str(&self) -> &'static str {
        match self {
            State::Downloaded => "Downloaded",
            State::Processed => "Processed",
        }
    }

    /// Attempt to parse a string (from Parquet) back into a State.
    fn from_str(s: &str) -> Option<Self> {
        match s {
            "Downloaded" => Some(State::Downloaded),
            "Processed" => Some(State::Processed),
            _ => None,
        }
    }
}

/// A simple history store that keeps, in memory, a `HashSet<(String, State)>` of
/// seen CSV (or ZIP) filenames and their associated state. Each time you call
/// `add(file_name, state)`, it writes exactly one tiny Parquet file containing
/// `{ file_name: &str, state: &str, event_time: i64 }` (microseconds since epoch),
/// then inserts into the in‐memory set. On `new(...)`, it loads any existing
/// `.parquet` files from disk to rebuild the set. You can later point DuckDB
/// at `history_dir/*.parquet` for reporting.
pub struct History {
    /// Directory where each single‐row Parquet file lives.
    history_dir: PathBuf,

    /// The in‐memory set of all `(file_name, state)` pairs already recorded.
    set: Arc<Mutex<HashSet<(String, State)>>>,
}

impl History {
    /// Create (or open) a History in `history_dir`. If the directory doesn’t exist,
    /// create it. Then glob for `*.parquet` in that directory, open each file,
    /// read the `"file_name"` and `"state"` columns via Arrow, and populate the
    /// in‐memory `HashSet<(String, State)>`.
    pub fn new(history_dir: impl Into<PathBuf>) -> Result<Self> {
        let history_dir = history_dir.into();
        fs::create_dir_all(&history_dir).with_context(|| {
            format!(
                "could not create or open history directory `{}`",
                history_dir.display()
            )
        })?;

        let set = Arc::new(Mutex::new(HashSet::new()));

        // 1) Scan for any existing `*.parquet` files in the directory
        let pattern = format!("{}/{}", history_dir.display(), "*.parquet");
        for entry in glob(&pattern).context("invalid glob pattern for existing Parquet files")? {
            let path = match entry {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("warning: cannot read glob entry: {:?}", e);
                    continue;
                }
            };

            // Skip any non‐`.parquet` files
            if path.extension().and_then(|e| e.to_str()) != Some("parquet") {
                continue;
            }

            // Open the file directly (it implements ChunkReader)
            let file = File::open(&path)
                .with_context(|| format!("failed to open `{}`", path.display()))?;

            // Build a RecordBatchReader (batch size = 1024) over the raw File
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).with_context(|| {
                format!(
                    "failed to create RecordBatchReaderBuilder for `{}`",
                    path.display()
                )
            })?;
            let mut record_batch_reader =
                builder.with_batch_size(1024).build().with_context(|| {
                    format!("failed to build RecordBatchReader for `{}`", path.display())
                })?;

            // For each record batch, extract columns: "file_name" (column 0) and "state" (column 1).
            while let Some(batch) = record_batch_reader
                .next()
                .transpose()
                .with_context(|| format!("error reading RecordBatch from `{}`", path.display()))?
            {
                // Column 0 must be "file_name" (Utf8)
                let fname_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "column 0 was not a StringArray in Parquet file `{}`",
                            path.display()
                        )
                    })?;
                // Column 1 must be "state" (Utf8)
                let state_array = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "column 1 was not a StringArray in Parquet file `{}`",
                            path.display()
                        )
                    })?;

                let mut guard = set.lock().unwrap();
                for i in 0..fname_array.len() {
                    if fname_array.is_valid(i) && state_array.is_valid(i) {
                        let fname = fname_array.value(i);
                        let state_str = state_array.value(i);
                        if let Some(state) = State::from_str(state_str) {
                            guard.insert((fname.to_string(), state));
                        }
                    }
                }
            }
        }

        Ok(History {
            history_dir,
            set: set.clone(),
        })
    }

    /// Returns `true` if `(file_name, state)` is already in the history.
    pub fn get(&self, file_name: &str, state: &State) -> bool {
        let guard = self.set.lock().unwrap();
        guard.contains(&(file_name.to_string(), state.clone()))
    }

    /// Add `(file_name, state)` to the history. If it’s already present, do nothing.
    ///
    /// Otherwise:
    ///  (1) Build a one‐row `RecordBatch` with columns:
    ///        - "file_name": Utf8 (the string you passed)
    ///        - "state": Utf8 (the state as a string)
    ///        - "event_time": Int64 (UTC timestamp in microseconds)
    ///  (2) Write it out under
    ///      `history_dir/<sanitized>---<state>---<ts>.parquet.tmp`
    ///      using `ArrowWriter`
    ///  (3) Atomically rename to `.parquet`
    ///  (4) Insert the `(file_name, state)` pair into the in‐memory `HashSet`
    pub fn add(&self, file_name: &str, state: State) -> Result<()> {
        // 1) Lock the in‐memory set
        let mut guard = self.set.lock().unwrap();

        // If already recorded, do nothing.
        if guard.contains(&(file_name.to_string(), state.clone())) {
            return Ok(());
        }

        // 2) Not found—prepare a new Parquet filename
        let now = Utc::now();
        let ts_micros = now.timestamp_micros();

        // Sanitize `file_name` so it can appear safely in a filesystem name:
        let safe_fname: String = file_name
            .chars()
            .map(|c| if c == '/' || c == '\\' { '_' } else { c })
            .collect();
        let state_str = state.as_str();

        let parquet_filename = format!("{}---{}---{}.parquet", safe_fname, state_str, ts_micros);
        let tmp_filename = format!("{}---{}---{}.parquet.tmp", safe_fname, state_str, ts_micros);

        let final_path = self.history_dir.join(&parquet_filename);
        let tmp_path = self.history_dir.join(&tmp_filename);

        // 3) Build an Arrow schema: three fields
        //      "file_name": Utf8 (non‐null)
        //      "state": Utf8 (non‐null)
        //      "event_time": Int64 (non‐null)
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("file_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("event_time", DataType::Int64, false),
        ]));

        // 4) Build one‐row RecordBatch
        let mut fname_builder = StringBuilder::new();
        let mut state_builder = StringBuilder::new();
        let mut ts_builder = Int64Builder::new();

        fname_builder.append_value(file_name);
        state_builder.append_value(state_str);
        ts_builder.append_value(ts_micros);

        let fname_array = Arc::new(fname_builder.finish()) as ArrayRef;
        let state_array = Arc::new(state_builder.finish()) as ArrayRef;
        let ts_array = Arc::new(ts_builder.finish()) as ArrayRef;

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![fname_array, state_array, ts_array],
        )
        .context("creating one‐row RecordBatch")?;

        // 5) Write that batch out to `<tmp_path>` using ArrowWriter
        {
            let file = File::create(&tmp_path)
                .with_context(|| format!("could not create `{}`", tmp_path.display()))?;
            let buf_writer = BufWriter::new(file);

            let mut writer = ArrowWriter::try_new(buf_writer, arrow_schema.clone(), None)
                .context("creating ArrowWriter for single‐row Parquet")?;

            writer
                .write(&batch)
                .context("writing one‐row Parquet via ArrowWriter")?;
            writer
                .close()
                .context("closing ArrowWriter / finalizing Parquet file")?;
        }

        // 6) Atomically rename `.tmp` → `.parquet`
        fs::rename(&tmp_path, &final_path).with_context(|| {
            format!(
                "renaming `{}` → `{}`",
                tmp_path.display(),
                final_path.display()
            )
        })?;

        // 7) Record in the in‐memory set
        guard.insert((file_name.to_string(), state));
        Ok(())
    }

    /// Consolidate (“vacuum”) all tiny per‐row Parquet files into one file per UTC‐day &
    /// per State.  After writing the consolidated file, delete the originals.
    /// The in‐memory HashSet remains valid, since nothing ever gets removed from it
    /// until the entire history directory is reloaded on `new()`.
    pub fn vacuum(&self) -> Result<()> {
        // 1) Scan for all "*.parquet" files in history_dir and group by (date, state).
        let mut buckets: HashMap<(chrono::NaiveDate, State), Vec<PathBuf>> = HashMap::new();
        let pattern = format!("{}/{}", self.history_dir.display(), "*.parquet");
        for entry in glob(&pattern).context("invalid glob pattern for vacuum")? {
            let path = match entry {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("warning: cannot read glob entry: {:?}", e);
                    continue;
                }
            };
            if !path.is_file() {
                continue;
            }

            // Expect file_name = "<safe_fname>---<State>---<ts>.parquet"
            let file_name = match path.file_name().and_then(|f| f.to_str()) {
                Some(n) => n,
                None => continue,
            };
            let parts: Vec<&str> = file_name.split("---").collect();
            if parts.len() != 3 {
                // Either not our tiny file or already a consolidated file
                continue;
            }

            let state_str = parts[1];
            let ts_and_ext = parts[2];
            if !ts_and_ext.ends_with(".parquet") {
                continue;
            }
            let ts_str = &ts_and_ext[..ts_and_ext.len() - ".parquet".len()];
            if ts_str == "consolidated" {
                // Skip any already‐consolidated files
                continue;
            }

            // Parse ts as microseconds
            let ts_micros: i64 = match ts_str.parse() {
                Ok(v) => v,
                Err(_) => continue,
            };

            // Convert to UTC date
            let dt: DateTime<Utc> = DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(
                    ts_micros / 1_000_000,
                    ((ts_micros % 1_000_000) * 1_000) as u32,
                ),
                Utc,
            );
            let date = dt.date_naive();

            // Parse State
            let state = match State::from_str(state_str) {
                Some(s) => s,
                None => continue,
            };

            buckets.entry((date, state)).or_default().push(path.clone());
        }

        // 2) For each (date, state) bucket: merge → write one consolidated file → delete originals
        for ((date, state), paths) in buckets {
            if paths.is_empty() {
                continue;
            }

            let date_str = date.format("%Y%m%d").to_string();
            let consolidated_name =
                format!("{}---{}---consolidated.parquet", date_str, state.as_str());
            let consolidated_tmp = format!(
                "{}---{}---consolidated.parquet.tmp",
                date_str,
                state.as_str()
            );

            let consolidated_path = self.history_dir.join(&consolidated_name);
            let tmp_path = self.history_dir.join(&consolidated_tmp);

            // Prepare schema: ["file_name":Utf8, "state":Utf8, "event_time":Int64]
            let arrow_schema = {
                let fields = vec![
                    Field::new("file_name", DataType::Utf8, false),
                    Field::new("state", DataType::Utf8, false),
                    Field::new("event_time", DataType::Int64, false),
                ];
                Arc::new(ArrowSchema::new(fields))
            };

            // (a) Create the ArrowWriter to tmp_path
            let tmp_file = File::create(&tmp_path)
                .with_context(|| format!("could not create `{}`", tmp_path.display()))?;
            let buf_writer = BufWriter::new(tmp_file);
            let mut writer = ArrowWriter::try_new(buf_writer, arrow_schema.clone(), None)
                .context("creating ArrowWriter for consolidated Parquet")?;

            // (b) For each tiny file: open → read batches → write into the ArrowWriter
            for small_path in &paths {
                let small_file = File::open(&small_path)
                    .with_context(|| format!("failed to open `{}`", small_path.display()))?;
                let builder =
                    ParquetRecordBatchReaderBuilder::try_new(small_file).with_context(|| {
                        format!(
                            "failed to create RecordBatchReaderBuilder for `{}`",
                            small_path.display()
                        )
                    })?;
                let mut batch_reader =
                    builder.with_batch_size(1024).build().with_context(|| {
                        format!(
                            "failed to build RecordBatchReader for `{}`",
                            small_path.display()
                        )
                    })?;

                while let Some(batch) = batch_reader.next().transpose().with_context(|| {
                    format!("error reading RecordBatch from `{}`", small_path.display())
                })? {
                    writer.write(&batch).with_context(|| {
                        format!(
                            "writing batch from `{}` to consolidated file",
                            small_path.display()
                        )
                    })?;
                }
            }

            // (c) Close & finalize the consolidated Parquet
            writer
                .close()
                .context("closing ArrowWriter for consolidated Parquet")?;

            // (d) Rename tmp → final
            fs::rename(&tmp_path, &consolidated_path).with_context(|| {
                format!(
                    "renaming `{}` → `{}`",
                    tmp_path.display(),
                    consolidated_path.display()
                )
            })?;

            // (e) Delete all the tiny files in this bucket
            for small_path in &paths {
                fs::remove_file(&small_path).with_context(|| {
                    format!("failed to delete small file `{}`", small_path.display())
                })?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;
    use tempfile::TempDir;

    #[test]
    fn add_and_get_downloaded_state() -> Result<()> {
        let tmp = TempDir::new()?;
        let hist_dir = tmp.path().join("history_store");
        let history = History::new(&hist_dir)?;

        assert!(!history.get("foo.csv", &State::Downloaded));
        history.add("foo.csv", State::Downloaded)?;
        assert!(history.get("foo.csv", &State::Downloaded));

        // Calling add a second time for the same file & state is a no-op
        history.add("foo.csv", State::Downloaded)?;
        assert!(history.get("foo.csv", &State::Downloaded));

        // Exactly one `.parquet` file on disk (Downloaded state)
        let files: Vec<_> = fs::read_dir(&hist_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.file_name())
            .collect();
        assert_eq!(files.len(), 1, "expected 1 parquet file, got {:?}", files);

        Ok(())
    }

    #[test]
    fn add_and_get_processed_state() -> Result<()> {
        let tmp = TempDir::new()?;
        let hist_dir = tmp.path().join("history_store");
        let history = History::new(&hist_dir)?;

        assert!(!history.get("bar.csv", &State::Processed));
        history.add("bar.csv", State::Processed)?;
        assert!(history.get("bar.csv", &State::Processed));

        // Downloaded state should still be false
        assert!(!history.get("bar.csv", &State::Downloaded));

        // Exactly one `.parquet` file on disk (Processed state)
        let files: Vec<_> = fs::read_dir(&hist_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.file_name())
            .collect();
        assert_eq!(files.len(), 1, "expected 1 parquet file, got {:?}", files);

        Ok(())
    }

    #[test]
    fn persist_and_reload_both_states() -> Result<()> {
        let tmp = TempDir::new()?;
        let hist_dir = tmp.path().join("history_store");

        // 1) Create, add two names with different states, then drop
        {
            let history = History::new(&hist_dir)?;
            history.add("A.csv", State::Downloaded)?;
            sleep(Duration::from_millis(1));
            history.add("B.csv", State::Processed)?;
            // now two tiny Parquet files should exist
        }

        // 2) Re-open and ensure both (file, state) pairs are present
        {
            let history2 = History::new(&hist_dir)?;
            assert!(history2.get("A.csv", &State::Downloaded));
            assert!(!history2.get("A.csv", &State::Processed));
            assert!(history2.get("B.csv", &State::Processed));
            assert!(!history2.get("B.csv", &State::Downloaded));
            assert!(!history2.get("C.csv", &State::Downloaded));
        }

        Ok(())
    }

    #[test]
    fn many_adds_performance_smoke() -> Result<()> {
        let tmp = TempDir::new()?;
        let hist_dir = tmp.path().join("history_store");
        let history = History::new(&hist_dir)?;

        const N: usize = 500;
        for i in 0..N {
            let fname = format!("file_{}.csv", i);
            history.add(&fname, State::Downloaded)?;
        }

        // We should have N tiny Parquet files on disk
        let count = fs::read_dir(&hist_dir)?.count();
        assert_eq!(count, N, "expected {} files on disk, found {}", N, count);

        Ok(())
    }
}
