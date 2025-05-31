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
    rows: &[Vec<String>],
) -> Result<Vec<Column>> {
    // How many rows to sample per column
    const SAMPLE_LIMIT: usize = 1_000;
    let num_cols = header_names.len();
    let mut cols = Vec::with_capacity(num_cols);

    // OPTIONAL: warn once if any rows are longer than the header
    if rows.iter().any(|r| r.len() > num_cols) {
        warn!(
            "derive_types: some rows in table `{}` have more cells than headers ({} headers)",
            table_name, num_cols
        );
    }

    // Process each column independently
    for (col_idx, col_name) in header_names.iter().enumerate() {
        // Track the first non-empty sample (type, format)
        let mut first_sample: Option<(String, Option<String>)> = None;
        // If we ever see a conflicting sample, we can stop early
        let mut inconsistent = false;

        // Scan up to SAMPLE_LIMIT rows
        for row in rows.iter().take(SAMPLE_LIMIT) {
            // Safely get the cell; treat missing cells as empty
            let cell = row.get(col_idx).map(|s| s.trim()).unwrap_or("");
            if cell.is_empty() {
                continue;
            }

            // Infer this cell’s type+format
            let inferred = infer_type_and_format(cell);

            match &first_sample {
                // No sample yet → record this one
                None => first_sample = Some(inferred.clone()),

                // We already have a sample → check for mismatch
                Some(prev) if prev != &inferred => {
                    debug!(
                        "derive_types: column `{}` in table `{}` conflict: {:?} vs {:?}",
                        col_name, table_name, prev, inferred
                    );
                    inconsistent = true;
                    break;
                }

                _ => {}
            }
        }

        // Decide final type & format
        let (ty, format) = match (inconsistent, first_sample) {
            // We detected conflicting samples → default
            (true, _) => {
                debug!(
                    "derive_types: inconsistent samples for `{}` in table `{}`, defaulting to utf8",
                    col_name, table_name
                );
                ("utf8".into(), None)
            }

            // No conflict, and we have at least one sample → use it
            (false, Some((t, f))) => (t, f),

            // No conflict but also no samples at all → default
            (false, None) => {
                debug!(
                    "derive_types: no samples for `{}` in table `{}`, defaulting to utf8",
                    col_name, table_name
                );
                ("utf8".into(), None)
            }
        };

        cols.push(Column {
            name: col_name.clone(),
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
