use anyhow::{Context, Result};
use arrow::datatypes::Schema as ArrowSchema;
use csv::ReaderBuilder;
use serde_json;
use std::{
    collections::{HashMap, HashSet},
    fs,
    io::{self, Cursor, Write},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};
use tracing::{error, warn};

use super::arrow::build_arrow_schema;
use super::write_columns;
use super::Column;
use crate::schema::derive; // provides derive_types

/// Thread-safe cache of table → columns, each table locked independently.
pub struct SchemaStore {
    /// Map: table_name → RwLock<Vec<Column>>
    map: RwLock<HashMap<String, Arc<RwLock<Vec<Column>>>>>,
    dir: PathBuf,
}

impl SchemaStore {
    /// Initialize by loading any existing `<table>_columns.json` in `dir`.
    pub fn new<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        let mut initial = HashMap::new();

        // Attempt to read directory; if it fails, skip initial load
        let entries = match fs::read_dir(&dir) {
            Ok(e) => e,
            Err(_) => {
                return Ok(Self {
                    map: RwLock::new(initial),
                    dir,
                })
            }
        };

        for entry in entries.filter_map(Result::ok) {
            let path = entry.path();
            let fname = match path.file_name().and_then(|n| n.to_str()) {
                Some(f) => f,
                None => continue,
            };
            if !fname.ends_with("_columns.json") {
                continue;
            }

            let table = fname.trim_end_matches("_columns.json").to_string();
            match fs::File::open(&path).and_then(|f| {
                serde_json::from_reader(f)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
            }) {
                Ok(cols) => {
                    initial.insert(table, Arc::new(RwLock::new(cols)));
                }
                Err(e) => error!("Skipping corrupt {:?}: {}", path, e),
            }
        }

        Ok(Self {
            map: RwLock::new(initial),
            dir,
        })
    }

    /// Return columns for the table found at `data_start`, deriving/persisting if needed.
    /// The first row at `data_start` is parsed for headers and table name.
    pub fn get_columns(
        &self,
        text: &str,
        data_start: usize,
        data_end: usize,
    ) -> Result<Vec<Column>> {
        const SAMPLE_LIMIT: usize = 1_000;

        // ----- 1) Extract the very first line to derive `table_name` and header names -----
        let slice = &text[data_start..data_end];
        let header_end = slice.find('\n').unwrap_or(slice.len());
        let header_line = &slice[..header_end];

        // split on commas
        let parts: Vec<&str> = header_line.split(',').collect();
        let table_name: String = if parts.len() > 2 && parts[2].starts_with(parts[1]) {
            // e.g. I,Table1,Table1,...
            parts[2].to_string()
        } else if parts.len() > 2 {
            // e.g. I,Foo,Bar,... → "Foo_Bar"
            format!("{}_{}", parts[1], parts[2])
        } else {
            warn!("malformed header: {}", header_line);
            "unknown".into()
        };

        // now drop the header prefix (we’re going to skip it when sampling rows)
        let data_slice = &slice[header_end.saturating_add(1)..];

        // ----- 2) Acquire or insert the per‐table lock using `table_name` -----
        let table_lock = {
            let map_r = self.map.read().unwrap();
            if let Some(lock) = map_r.get(&table_name) {
                Arc::clone(lock)
            } else {
                drop(map_r);
                let mut map_w = self.map.write().unwrap();
                let lock = Arc::new(RwLock::new(Vec::new()));
                map_w.insert(table_name.clone(), Arc::clone(&lock));
                lock
            }
        };

        // ----- 3) Fast‐path: if we already have all these columns, return immediately -----
        let header_names: Vec<String> =
            header_line.split(',').skip(4).map(str::to_string).collect();
        {
            let cols_r = table_lock.read().unwrap();
            let present: HashSet<&String> = cols_r.iter().map(|c| &c.name).collect();
            if header_names.iter().all(|n| present.contains(n)) {
                return Ok(cols_r.clone());
            }
        }

        // ----- 4) Write‐lock & double‐check -----
        let mut cols_w = table_lock.write().unwrap();
        let present: HashSet<&String> = cols_w.iter().map(|c| &c.name).collect();
        if header_names.iter().all(|n| present.contains(n)) {
            return Ok(cols_w.clone());
        }

        // ----- 5) Sample up to SAMPLE_LIMIT rows to derive missing columns -----
        let mut sample_rows = Vec::with_capacity(SAMPLE_LIMIT);
        let mut rdr = ReaderBuilder::new()
            .has_headers(false)
            .from_reader(Cursor::new(data_slice.as_bytes()));

        // skip what was the header row (we sliced it off already),
        // then take SAMPLE_LIMIT
        for result in rdr.records().take(SAMPLE_LIMIT) {
            let record = result.context("parsing CSV record for derive_types")?;
            let row: Vec<String> = record
                .iter()
                .skip(4)
                .take(header_names.len())
                .map(|s| s.to_string())
                .collect();
            sample_rows.push(row);
        }

        // ----- 6) Derive types & persist -----
        let derived = derive::derive_types(&table_name, &header_names, &sample_rows)
            .with_context(|| format!("deriving types for {}", table_name))?;

        if derived.is_empty() {
            warn!(
                "derive_types returned empty for {}, defaulting to utf8-only",
                table_name
            );
        }

        write_columns(&table_name, &self.dir, &derived)
            .with_context(|| format!("writing columns for {}", table_name))?;

        // ----- 7) Cache & return -----
        *cols_w = derived.clone();
        Ok(derived)
    }

    /// Return an ArrowSchema for `table_name`, building from columns.
    pub fn get_schema(
        &self,
        text: &str,
        data_start: usize,
        data_end: usize,
    ) -> Result<Arc<ArrowSchema>> {
        let cols = self.get_columns(text, data_start, data_end)?;
        Ok(build_arrow_schema(&cols))
    }
}
