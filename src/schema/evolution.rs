//! Reworked schema extraction: track column types per table name across all input JSONs,
//! and map header names to Column definitions without date logic.

use anyhow::{anyhow, Context, Result};
use chrono::NaiveDateTime;
use serde_json;
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
};
use tracing::{debug, warn};

use super::types::{Column, MonthSchema};

/// Read **all** `.json` files in any of the `input_dirs`, parse out their
/// `month` plus `schemas`, and build a map of (table → column → seen types).
pub fn extract_column_types<P, I>(
    input_dirs: I,
) -> Result<HashMap<String, HashMap<String, HashSet<String>>>>
where
    P: AsRef<Path>,
    I: IntoIterator<Item = P>,
{
    let mut column_types: HashMap<String, HashMap<String, HashSet<String>>> = HashMap::new();
    let mut json_files = Vec::new();

    // 1) Gather every `.json` file from each provided directory
    for input_dir in input_dirs {
        let dir = input_dir.as_ref();
        for entry in fs::read_dir(dir).with_context(|| format!("reading directory {:?}", dir))? {
            let path = entry?.path();
            if path.is_file()
                && path
                    .extension()
                    .and_then(|s| s.to_str())
                    .map_or(false, |ext| ext.eq_ignore_ascii_case("json"))
            {
                json_files.push(path);
            }
        }
    }

    // 2) Parse each file and merge its column‐type info
    for file in json_files {
        // read + parse
        let text = fs::read_to_string(&file).with_context(|| format!("reading {:?}", file))?;
        let data: MonthSchema =
            serde_json::from_str(&text).with_context(|| format!("parsing {:?}", file))?;

        // merge schemas
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

/// Fallback schema derivation when no CTL info exists:
/// for each column, look through up to the first 1 000 rows
/// to infer every non‐empty value’s type; if they’re all the same,
/// use that (ty, format), otherwise default to utf8.
pub fn derive_types(
    table_name: &str,
    header_names: &[String],
    rows: &[Vec<String>],
) -> Result<Vec<Column>> {
    // 1) Prepare state: for each column we track
    //    - the first seen type+format (Option<(ty, fmt)>)
    //    - whether we've already spotted an inconsistency
    let mut seen: Vec<Option<(String, Option<String>)>> = vec![None; header_names.len()];
    let mut inconsistent: Vec<bool> = vec![false; header_names.len()];
    let mut examples_scanned = vec![0usize; header_names.len()]; // count of samples per col

    // 2) Walk rows
    for row in rows.iter().take(1_000) {
        for (i, cell) in row.iter().enumerate() {
            let v = cell.trim();
            if v.is_empty() {
                continue;
            }

            // Skip if we've already marked inconsistent and seen > 1 sample
            if inconsistent[i] {
                // Still count up to 1 000 rows, but no further checks needed
                examples_scanned[i] += 1;
                continue;
            }

            // Infer this sample’s type
            let inferred = infer_type_and_format(v);

            match &seen[i] {
                None => {
                    // first non‐empty sample for this column
                    seen[i] = Some(inferred.clone());
                }
                Some(prev) => {
                    if *prev != inferred {
                        // mismatch! mark inconsistent
                        debug!(
                            "derive_types: column `{}` in table `{}` has conflicting types: {:?} vs {:?}",
                            header_names[i], table_name, prev, inferred
                        );
                        inconsistent[i] = true;
                    }
                }
            }

            examples_scanned[i] += 1;
        }
        // if every column has at least one sample *and* is either consistent or flagged,
        // we could stop early; but here we scan the full 1 000 rows to be thorough
    }

    // 3) Build result columns
    let mut cols = Vec::with_capacity(header_names.len());
    for (i, name) in header_names.iter().enumerate() {
        let (ty, format) = if inconsistent[i] {
            warn!(
                "derive_types: inconsistent samples for `{}` on table `{}`, defaulting to utf8",
                name, table_name
            );
            ("utf8".into(), None)
        } else {
            match &seen[i] {
                Some((t, f)) => (t.clone(), f.clone()),
                None => {
                    warn!(
                        "derive_types: no non-empty sample for `{}` on table `{}`, defaulting to utf8",
                        name, table_name
                    );
                    ("utf8".into(), None)
                }
            }
        };

        cols.push(Column {
            name: name.clone(),
            ty,
            format,
        });
    }

    Ok(cols)
}

fn infer_type_and_format(raw: &str) -> (String, Option<String>) {
    // strip wrapping quotes
    let v = raw.trim().trim_matches('"');

    // 1) numeric ⇒ FLOAT + EXTERNAL
    if v.parse::<i64>().is_ok() || v.parse::<f64>().is_ok() {
        return ("FLOAT".into(), Some("EXTERNAL".into()));
    }

    // 2) datetime full
    const SLASH_TS: &str = "%Y/%m/%d %H:%M:%S";
    const DASH_TS: &str = "%Y-%m-%d %H:%M:%S";
    if NaiveDateTime::parse_from_str(v, SLASH_TS).is_ok() {
        return ("DATE".into(), Some("\"yyyy/mm/dd hh24:mi:ss\"".into()));
    }
    if NaiveDateTime::parse_from_str(v, DASH_TS).is_ok() {
        return ("DATE".into(), Some("\"yyyy-mm-dd hh24:mi:ss\"".into()));
    }

    // 3) string ⇒ CHAR(length)
    let len = v.len();
    (format!("CHAR({})", len), None)
}
// In your lib.rs or extract.rs, keep only the test module below:

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::{
        fs::File,
        io::{self, Write},
        path::Path,
    };
    use tempfile::tempdir;

    /// Manual schema existence check:
    /// Reads JSON schema files from a real directory and prompts for a table name.
    #[test]
    #[ignore]
    fn manual_schema_check() -> Result<(), anyhow::Error> {
        // Path to your real schema directory
        let schema_dir = Path::new("./assets/schemas");
        let schema_dir_temp = Path::new("./assets/schemas_temp");
        assert!(schema_dir.is_dir(), "'schemas' directory not found");

        // Build lookup from JSON files in that directory
        let lookup = extract_column_types(vec![schema_dir, schema_dir_temp])?;
        println!("Available tables: {:?}", lookup.keys());

        let col_types = find_column_types(
            &lookup,
            "P5MIN_FCAS_REQ_CONSTRAINT",
            &["BIDTYPE".to_string(), "REGIONAL_ENABLEMENT".to_string()],
        )
        .context("Failed to find column types ")?;

        println!("Column types: {:?}", col_types);
        // let name = "MCC_CONSTRAINTSOLUTION"; // Replace with the table you want to check
        // assert!(
        //     lookup.contains_key(name),
        //     "Table '{}' not found in schemas",
        //     name
        // );
        // println!("Table '{}' exists!", name);
        Ok(())
    }
}
