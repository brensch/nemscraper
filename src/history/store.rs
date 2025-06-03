use super::state::State;
use anyhow::{Context, Result};
use arrow::{
    array::{StringArray, TimestampMicrosecondArray},
    datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema, TimeUnit},
    record_batch::RecordBatch,
};
use chrono::TimeZone;
use chrono::{NaiveDate, Utc};
use glob::glob;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::BufWriter,
    path::PathBuf,
    sync::{Arc, Mutex},
};

/// `History` manages a directory of per‐row Parquet files and can “vacuum”
/// them into one consolidated file per (UTC) date & state. Each row now also
/// carries a `count` field. Internally, we keep `seen` as an `Arc<Mutex<…>>`
/// so that multiple threads can safely call `add` or `get` at once.
///
/// Calling `new(...)` will scan the directory and populate that HashSet from
/// all existing tiny‐file Parquets, so that `add(...)` won’t re‐insert duplicates.
pub struct History {
    history_dir: PathBuf,
    /// Tracks which `(safe_fname, state, ts_micros)` have already been added.
    /// Wrapped in `Arc<Mutex<…>>` for thread safety.
    seen: Arc<Mutex<HashSet<(String, State, i64)>>>,
}

impl History {
    /// Create a new `History` pointing at `history_dir`. If the directory does not exist,
    /// this returns an error. Otherwise, it scans every `*.parquet` file in that directory,
    /// parses out `(safe_fname, state, ts_micros)`, and populates `self.seen` so that
    /// subsequent `add(...)` calls won’t re‐insert duplicates.
    ///
    /// Note: Accepts `impl Into<PathBuf>` for greater flexibility.
    pub fn new(history_dir: impl Into<PathBuf>) -> Result<Self> {
        let history_dir: PathBuf = history_dir.into();
        if !history_dir.is_dir() {
            anyhow::bail!(
                "history_dir `{}` does not exist or is not a directory",
                history_dir.display()
            );
        }

        let mut initial_set = HashSet::new();
        let pattern = format!("{}/{}", history_dir.display(), "*.parquet");

        for entry in glob(&pattern).context("invalid glob pattern for History::new")? {
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
            let file_name = match path.file_name().and_then(|f| f.to_str()) {
                Some(n) => n,
                None => continue,
            };

            // Expect either:
            // - "<safe_fname>---<State>---<ts>.parquet"
            // - "<YYYYMMDD>---<State>---consolidated.parquet"
            let parts: Vec<&str> = file_name.split("---").collect();
            if parts.len() != 3 {
                continue;
            }

            // Parse state
            let state = match State::from_str(parts[1]) {
                Some(s) => s,
                None => continue,
            };

            if parts[2] == "consolidated.parquet" {
                // We do not insert consolidated files into `seen` because they are aggregates.
                continue;
            }

            // Filename is "<safe_fname>---<State>---<ts>.parquet"
            if !parts[2].ends_with(".parquet") {
                continue;
            }
            let ts_str = &parts[2][..parts[2].len() - ".parquet".len()];
            let ts_micros: i64 = match ts_str.parse() {
                Ok(v) => v,
                Err(_) => continue,
            };

            // `safe_fname` is everything before the first '---'
            let safe_fname = parts[0].to_string();
            initial_set.insert((safe_fname, state, ts_micros));
        }

        Ok(History {
            history_dir,
            seen: Arc::new(Mutex::new(initial_set)),
        })
    }

    /// Append a single event.  
    /// - `safe_fname`: a unique identifier for this row  
    /// - `state`: Downloaded or Processed  
    /// - `count`: the integer count to record in the "count" column  
    ///  
    /// The timestamp is taken as `Utc::now()`.  If an entry with the same  
    /// `(safe_fname, state, ts_micros)` already exists, this is a no‐op.
    pub fn add(&self, safe_fname: &str, state: State, count: i64) -> Result<()> {
        // 1) Grab the current UTC timestamp in microseconds:
        let now_utc = Utc::now();
        let ts_micros = now_utc.timestamp_micros();

        // 2) Check for duplicates under the mutex. If already seen, skip writing.
        {
            let mut seen_guard = self.seen.lock().unwrap();
            if seen_guard.contains(&(safe_fname.to_string(), state, ts_micros)) {
                return Ok(());
            }
            // Mark it as seen immediately so we don’t try again on race.
            seen_guard.insert((safe_fname.to_string(), state, ts_micros));
        }

        // 3) Build the on-disk filename "<safe_fname>---<State>---<ts>.parquet"
        let file_name = format!(
            "{}---{}---{}.parquet",
            safe_fname,
            state.as_str(),
            ts_micros
        );
        let final_path = self.history_dir.join(&file_name);

        // 4) Build the temporary filename "<safe_fname>---<State>---<ts>.parquet.tmp"
        let tmp_file_name = format!(
            "{}---{}---{}.parquet.tmp",
            safe_fname,
            state.as_str(),
            ts_micros
        );
        let tmp_path = self.history_dir.join(&tmp_file_name);

        // 5) Define an Arrow schema that includes:
        //    ["file_name":Utf8, "state":Utf8, "event_time":Timestamp(µs), "count":Int64]
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("file_name", ArrowDataType::Utf8, false),
            Field::new("state", ArrowDataType::Utf8, false),
            Field::new(
                "event_time",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("count", ArrowDataType::Int64, false),
        ]));

        // 6) Create the Parquet file & ArrowWriter on the .tmp path
        let tmp_file = File::create(&tmp_path)
            .with_context(|| format!("could not create temporary file `{}`", tmp_path.display()))?;
        let buf_writer = BufWriter::new(tmp_file);
        let mut writer = ArrowWriter::try_new(buf_writer, schema.clone(), None)
            .context("creating ArrowWriter for tiny Parquet")?;

        // 7) Build each column array (single-row):
        let file_name_array = StringArray::from(vec![file_name.clone()]);
        let state_array = StringArray::from(vec![state.as_str().to_string()]);
        let event_time_array = TimestampMicrosecondArray::from(vec![ts_micros]);
        let count_array = arrow::array::Int64Array::from(vec![count]);

        // 8) Package into a RecordBatch
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(file_name_array),
                Arc::new(state_array),
                Arc::new(event_time_array),
                Arc::new(count_array),
            ],
        )
        .context("building RecordBatch for tiny Parquet")?;

        // 9) Write out and close
        writer
            .write(&batch)
            .context("writing batch to tiny Parquet")?;
        writer
            .close()
            .context("closing ArrowWriter for tiny Parquet")?;

        // 10) Rename the .tmp file to its final .parquet filename
        fs::rename(&tmp_path, &final_path).with_context(|| {
            format!(
                "failed to rename `{}` to `{}`",
                tmp_path.display(),
                final_path.display()
            )
        })?;

        Ok(())
    }

    /// Returns `true` if `(file_name, state)` is already in the history.
    pub fn get(&self, file_name: &str, state: &State) -> bool {
        let guard = self.seen.lock().unwrap();
        guard
            .iter()
            .any(|(fname, st, _)| fname == file_name && st == state)
    }

    /// “Vacuum” the entire `history_dir` by grouping every "*.parquet" file
    /// (both tiny per‐row files _and_ any existing “consolidated” files) into
    /// one output per (UTC‐date, state). After successfully writing the new
    /// consolidated file, delete only the old tiny files (leave any old
    /// consolidated files alone).
    pub fn vacuum(&self) -> Result<()> {
        // 1) Scan for all "*.parquet" in history_dir and group by (date, state).
        let mut buckets: HashMap<(NaiveDate, State), Vec<PathBuf>> = HashMap::new();
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

            let file_name = match path.file_name().and_then(|f| f.to_str()) {
                Some(n) => n,
                None => continue,
            };
            let parts: Vec<&str> = file_name.split("---").collect();
            if parts.len() != 3 {
                // Not our expected naming convention
                continue;
            }

            // Parse state
            let state = match State::from_str(parts[1]) {
                Some(s) => s,
                None => continue,
            };

            // Determine the UTC‐date bucket key
            let date = if parts[2] == "consolidated.parquet" {
                // Filename was "<YYYYMMDD>---<State>---consolidated.parquet"
                match NaiveDate::parse_from_str(parts[0], "%Y%m%d") {
                    Ok(d) => d,
                    Err(_) => continue,
                }
            } else if parts[2].ends_with(".parquet") {
                // Filename was "<safe_fname>---<State>---<ts>.parquet"
                let ts_str = &parts[2][..parts[2].len() - ".parquet".len()];
                let ts_micros: i64 = match ts_str.parse() {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let seconds = ts_micros / 1_000_000;
                let nanoseconds = ((ts_micros % 1_000_000) * 1_000) as u32;
                let dt_utc = Utc
                    .timestamp_opt(seconds, nanoseconds)
                    .single()
                    .expect("invalid timestamp");
                dt_utc.date_naive()
            } else {
                continue;
            };

            buckets.entry((date, state)).or_default().push(path.clone());
        }

        // 2) For each bucket (date, state), merge everything into one consolidated file.
        for ((date, state), paths) in &buckets {
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

            // Schema now includes a real timestamp type
            let arrow_schema = Arc::new(ArrowSchema::new(vec![
                Field::new("file_name", ArrowDataType::Utf8, false),
                Field::new("state", ArrowDataType::Utf8, false),
                Field::new(
                    "event_time",
                    ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
                Field::new("count", ArrowDataType::Int64, false),
            ]));

            // (a) Create ArrowWriter targeting tmp_path
            let tmp_file = File::create(&tmp_path)
                .with_context(|| format!("could not create `{}`", tmp_path.display()))?;
            let buf_writer = BufWriter::new(tmp_file);
            let mut writer = ArrowWriter::try_new(buf_writer, arrow_schema.clone(), None)
                .context("creating ArrowWriter for consolidated Parquet")?;

            // (b) Read each file in `paths`, except the old consolidated (skip that), and dump its batches into the new writer.
            for p in paths.iter() {
                let file =
                    File::open(p).with_context(|| format!("failed to open `{}`", p.display()))?;
                let builder =
                    ParquetRecordBatchReaderBuilder::try_new(file).with_context(|| {
                        format!(
                            "failed to create RecordBatchReaderBuilder for `{}`",
                            p.display()
                        )
                    })?;
                let mut batch_reader =
                    builder.with_batch_size(1024).build().with_context(|| {
                        format!("failed to build RecordBatchReader for `{}`", p.display())
                    })?;

                while let Some(batch) = batch_reader
                    .next()
                    .transpose()
                    .with_context(|| format!("error reading RecordBatch from `{}`", p.display()))?
                {
                    writer.write(&batch).with_context(|| {
                        format!("writing batch from `{}` to consolidated file", p.display())
                    })?;
                }
            }

            // (c) Close & finalize the new consolidated Parquet
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

            // (e) Delete all tiny files in this bucket (skip the old consolidated)
            for p in paths.iter() {
                if let Some(fname) = p.file_name().and_then(|f| f.to_str()) {
                    if fname == consolidated_name {
                        continue;
                    }
                }
                fs::remove_file(p)
                    .with_context(|| format!("failed to delete file `{}`", p.display()))?;
            }
        }

        Ok(())
    }
}
