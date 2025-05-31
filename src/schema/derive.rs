use anyhow::anyhow;
use anyhow::Result;
use chrono::NaiveDateTime;
use tracing::{debug, warn};

use super::Column;

/// For each column, look at up to SAMPLE_LIMIT rows:
///  - Ignore empty cells
///  - On the first non-empty sample, remember its (type, format)
///  - On subsequent samples, if it differs, stop and mark inconsistent
///  - Finally, if inconsistent *or* no samples found, default to utf8
pub fn derive_types(
    table_name: &str,
    header_names: &[String],
    sample_rows: &[Vec<String>],
) -> Result<Vec<Column>> {
    if header_names.is_empty() {
        return Err(anyhow!("derive_types: `{}` has no headers", table_name));
    }

    // Check for rows that are longer than headers and warn once
    if sample_rows.iter().any(|r| r.len() > header_names.len()) {
        warn!(
            "derive_types: some rows in `{}` have more cells than headers ({} headers)",
            table_name,
            header_names.len()
        );
    }

    let mut cols = Vec::with_capacity(header_names.len());

    for (idx, raw_name) in header_names.iter().enumerate() {
        // Strip all leading/trailing whitespace (spaces, tabs, \r, \n, etc.)
        let col_name = raw_name.trim();
        if col_name.is_empty() {
            return Err(anyhow!(
                "derive_types: header at index {} in `{}` is empty after trimming",
                idx,
                table_name
            ));
        }

        let mut first_sample: Option<(String, Option<String>)> = None;
        let mut inconsistent = false;

        // Scan the sample rows for this column index
        for row in sample_rows {
            let cell = row.get(idx).map(|s| s.trim()).unwrap_or("");
            if cell.is_empty() {
                continue;
            }

            let inferred = infer_type_and_format(cell);

            match &first_sample {
                None => {
                    first_sample = Some(inferred.clone());
                }
                Some(prev) if prev != &inferred => {
                    debug!(
                        "derive_types: column `{}` in `{}` conflict: {:?} vs {:?}",
                        col_name, table_name, prev, inferred
                    );
                    inconsistent = true;
                    break;
                }
                _ => {}
            }
        }

        let (ty, format) = match (inconsistent, first_sample) {
            (true, _) => {
                debug!(
                    "derive_types: inconsistent samples for `{}` in `{}`, defaulting to utf8",
                    col_name, table_name
                );
                ("utf8".into(), None)
            }
            (false, Some((t, f))) => (t, f),
            (false, None) => {
                debug!(
                    "derive_types: no samples for `{}` in `{}`, defaulting to utf8",
                    col_name, table_name
                );
                ("utf8".into(), None)
            }
        };

        cols.push(Column {
            name: col_name.to_string(),
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
