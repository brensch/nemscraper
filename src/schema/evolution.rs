//! Reworked schema extraction: track column types per table name across all input JSONs,
//! and map header names to Column definitions without date logic.

use anyhow::{anyhow, Context, Result};
use serde_json;
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
};
use tracing::{debug, warn};

use super::types::{Column, MonthSchema};

/// Read all `<YYYYMM>.json` in `input_dir` and produce a lookup of column types per table.
/// Returns a map from table name -> (column name -> set of types encountered).
pub fn extract_column_types<P: AsRef<Path>>(
    input_dir: P,
) -> Result<HashMap<String, HashMap<String, HashSet<String>>>> {
    let input = input_dir.as_ref();
    let mut column_types: HashMap<String, HashMap<String, HashSet<String>>> = HashMap::new();

    // Gather all month files
    let month_files: Vec<_> = fs::read_dir(input)
        .with_context(|| format!("reading {:?}", input))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.is_file()
                && p.extension().and_then(|s| s.to_str()) == Some("json")
                && p.file_stem()
                    .and_then(|s| s.to_str())
                    .map_or(false, |stem| {
                        stem.len() == 6 && stem.chars().all(|c| c.is_ascii_digit())
                    })
        })
        .collect();

    for file in month_files {
        let data: MonthSchema = {
            let text = fs::read_to_string(&file).with_context(|| format!("read {:?}", file))?;
            serde_json::from_str(&text).with_context(|| format!("parse {:?}", file))?
        };
        for cs in data.schemas {
            let table_entry = column_types.entry(cs.table.clone()).or_default();
            for col in cs.columns {
                table_entry
                    .entry(col.name.clone())
                    .or_default()
                    .insert(col.ty.clone());
            }
        }
    }

    Ok(column_types)
}

/// Given a lookup from `extract_column_types`, a table name, and header names,
/// return a Vec<Column> matching each header to its type.
/// - If a header name is not found, defaults to `utf8`, logs a warning, but still includes it.
/// - Errors only if the table name is missing.
pub fn find_column_types(
    column_lookup: &HashMap<String, HashMap<String, HashSet<String>>>,
    table_name: &str,
    header_names: &[String],
) -> Result<Vec<Column>> {
    let table_map = column_lookup
        .get(table_name)
        .ok_or_else(|| anyhow!("no column types found for table `{}`", table_name))?;

    let mut cols = Vec::with_capacity(header_names.len());
    for header in header_names {
        match table_map.get(header) {
            Some(types) => {
                let ty = if types.len() == 1 {
                    types.iter().next().unwrap().clone()
                } else {
                    debug!("multiple types found for column `{}`: {:?}", header, types);
                    types.iter().next().unwrap().clone()
                };
                cols.push(Column {
                    name: header.clone(),
                    ty,
                    format: None,
                });
            }
            None => {
                warn!("no column type found for `{}`, defaulting to utf8", header);
                cols.push(Column {
                    name: header.clone(),
                    ty: "utf8".to_string(),
                    format: None,
                });
            }
        }
    }

    Ok(cols)
}
