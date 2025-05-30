use anyhow::Result;
use serde_json;
use std::{
    collections::HashMap,
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
};

use super::Column;

/// Read, merge and write back the column list for `table_name`
///
/// - `table_name`: logical name, used to form `<table_name>_columns.json`  
/// - `dir`: directory containing that JSON file  
/// - `new_cols`: incoming `Column` definitions to add or override
pub fn write_columns<P: AsRef<Path>>(table_name: &str, dir: P, new_cols: &[Column]) -> Result<()> {
    // 1) Build path ".../<table_name>_columns.json"
    let dir = dir.as_ref();
    let file_name = format!("{}_columns.json", table_name);
    let path: PathBuf = dir.join(&file_name);

    // 2) Load existing columns, or start empty
    let existing: Vec<Column> = if path.exists() {
        let f = fs::File::open(&path)
            .map_err(|e| io::Error::new(e.kind(), format!("opening {}: {}", file_name, e)))?;
        serde_json::from_reader(f).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("parsing {}: {}", file_name, e),
            )
        })?
    } else {
        Vec::new()
    };

    // 3) Merge by name: existing → map, then insert/override with new_cols
    let mut by_name: HashMap<String, Column> =
        existing.into_iter().map(|c| (c.name.clone(), c)).collect();
    for col in new_cols {
        by_name.insert(col.name.clone(), col.clone());
    }

    // 4) Sort the merged columns alphabetically by name
    let mut merged: Vec<Column> = by_name.into_values().collect();
    merged.sort_by(|a, b| a.name.cmp(&b.name));

    // 5) Write atomically: to tmp file, then rename over original
    let tmp_path = dir.join(format!(".{}_columns.json.tmp", table_name));
    let mut tmp = fs::File::create(&tmp_path)
        .map_err(|e| io::Error::new(e.kind(), format!("creating {:?}: {}", tmp_path, e)))?;

    // pretty-print with a trailing newline
    serde_json::to_writer_pretty(&mut tmp, &merged)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("serializing JSON: {}", e)))?;
    tmp.write_all(b"\n")?;

    fs::rename(&tmp_path, &path).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("renaming {:?} -> {:?}: {}", tmp_path, path, e),
        )
    })?;

    Ok(())
}
