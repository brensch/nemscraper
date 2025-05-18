// src/process/mod.rs
use anyhow::{Context, Result};
use csv::ReaderBuilder;
use std::{collections::BTreeMap, fs::File, path::Path};
use tracing::{debug, trace, warn}; // Added tracing
use zip::ZipArchive;

#[derive(Debug)]
pub struct RawTable {
    /// Column names, from the “I” row of the specific CSV file.
    /// While SchemaEvolution provides canonical names, these are what the file claims.
    pub headers: Vec<String>,
    /// Each “D” row, as a Vec of Strings (one per field).
    pub rows: Vec<Vec<String>>,
    /// The effective month (YYYYMM) for the data in this table, derived from the 'C' row of the CSV.
    pub effective_month: String,
}

/// Parses a date string like "YYYY/MM/DD" or "YYYY/MM/DD hh:mm:ss" into "YYYYMM".
/// Returns None if parsing fails.
fn parse_date_to_yyyymm(date_str: &str) -> Option<String> {
    // Handle potential quotes around the date string if it comes from CSV parsing directly.
    let cleaned_date_str = date_str.trim_matches('"');

    if cleaned_date_str.len() >= 7 {
        // "YYYY/MM"
        let year_str = &cleaned_date_str[0..4];
        let month_str = &cleaned_date_str[5..7];
        if year_str.chars().all(char::is_numeric) && month_str.chars().all(char::is_numeric) {
            return Some(format!("{}{}", year_str, month_str));
        }
    }
    warn!(date_str, "Failed to parse date string into YYYYMM format");
    None
}

/// Open `zip_path`, find all `.csv` entries, and for each:
/// - Extracts the effective month from the "C" row.
/// - On “I” rows: starts a new schema table (keyed by schema name), storing headers and the effective_month.
/// - On “D” rows: pushes the data row into the current schema's table.
///
/// Returns a BTreeMap mapping schema names (e.g., "FORECAST_DEFAULT_CF") to RawTable data.
#[tracing::instrument(level = "debug", skip(zip_path), fields(path = %zip_path.as_ref().display()))]
pub fn load_aemo_zip<P: AsRef<Path>>(zip_path: P) -> Result<BTreeMap<String, RawTable>> {
    let path_display = zip_path.as_ref().display().to_string();
    debug!("Opening ZIP file");
    let file = File::open(zip_path.as_ref())
        .with_context(|| format!("Failed to open ZIP file: {}", path_display))?;
    let mut archive = ZipArchive::new(file)
        .with_context(|| format!("Failed to read ZIP archive: {}", path_display))?;

    let mut tables: BTreeMap<String, RawTable> = BTreeMap::new();

    for i in 0..archive.len() {
        let file_in_zip = archive.by_index(i).with_context(|| {
            format!("Failed to get file at index {} in ZIP: {}", i, path_display)
        })?;

        let file_name = file_in_zip.name().to_string(); // Get file name for logging
        let file_processing_span =
            tracing::debug_span!("csv_processing", zip_file_name = %file_name);
        let _enter = file_processing_span.enter();

        if file_in_zip.is_file() && file_name.to_lowercase().ends_with(".csv") {
            trace!("Processing CSV file entry");
            let mut rdr = ReaderBuilder::new()
                .has_headers(false)
                .flexible(true) // Handles varying numbers of fields if any
                .from_reader(file_in_zip); // file_in_zip is consumed here

            let mut current_schema_identifier: Option<String> = None;
            let mut file_effective_month: Option<String> = None;

            for (record_idx, result) in rdr.records().enumerate() {
                let record = result.with_context(|| {
                    format!(
                        "Failed to read record {} from CSV '{}' in ZIP '{}'",
                        record_idx, file_name, path_display
                    )
                })?;

                trace!(
                    record_num = record_idx,
                    first_field = record.get(0).unwrap_or("N/A"),
                    "Read record"
                );

                match record.get(0).map(|s| s.trim()) {
                    Some("C") => {
                        // "C" row, typically the first row. Extract file effective date.
                        // Date is usually at record[6] (0-indexed). Example: "2024/12/14"
                        if let Some(date_str) = record.get(5) {
                            if let Some(yyyymm) = parse_date_to_yyyymm(date_str) {
                                file_effective_month = Some(yyyymm);
                                debug!(
                                    effective_month =
                                        file_effective_month.as_deref().unwrap_or("N/A"),
                                    "Determined file effective month from 'C' row."
                                );
                            } else {
                                warn!(
                                    raw_date = date_str,
                                    "Could not parse effective month from 'C' row date field."
                                );
                            }
                        }
                    }
                    Some("I") => {
                        // "I" row: Schema definition.
                        // record[1]_record[2] is the schema name, record[4..] are column names.

                        // get the schema name by concatenating record[1] and record[2]
                        // record[1] is the schema type (e.g., "FPP")
                        let schema_type =
                            record.get(1).map(|s| s.trim().to_string()).ok_or_else(|| {
                                warn!("'I' row missing schema type at index 1.");
                                anyhow::anyhow!(
                                    "'I' row missing schema type (index 1) in {}",
                                    file_name
                                )
                            })?;
                        // record[2] is the schema name (e.g., "FORECAST_DEFAULT_CF")
                        let schema_name =
                            record.get(2).map(|s| s.trim().to_string()).ok_or_else(|| {
                                warn!("'I' row missing schema name at index 2.");
                                anyhow::anyhow!(
                                    "'I' row missing schema name (index 2) in {}",
                                    file_name
                                )
                            })?;

                        let schema_identifier = format!("{}_{}", schema_type, schema_name);

                        let cols: Vec<String> = record
                            .iter()
                            .skip(4)
                            .map(|s| s.trim().to_string())
                            .collect();
                        trace!(schema_identifier, num_cols = cols.len(), "Parsed 'I' row.");

                        if file_effective_month.is_none() {
                            warn!(schema_identifier, "'I' row encountered before a 'C' row or 'C' row date was unparsable. Effective month for this table is unknown.");
                            // Decide on a fallback or skip. For now, we'll use a placeholder.
                            // This situation should be rare if files are well-formed.
                        }

                        let table_effective_month = file_effective_month.clone().unwrap_or_else(|| {
                            warn!(schema_identifier, "Using placeholder for effective_month due to missing 'C' row date.");
                            "UNKNOWN_MONTH".to_string()
                        });

                        let table_entry =
                            tables.entry(schema_identifier.clone()).or_insert_with(|| {
                                debug!(
                                    schema_identifier,
                                    effective_month = table_effective_month,
                                    "Creating new RawTable entry."
                                );
                                RawTable {
                                    headers: Vec::new(), // Will be set below
                                    rows: Vec::new(),
                                    effective_month: table_effective_month,
                                }
                            });
                        table_entry.headers = cols; // Overwrite/set headers

                        // If the effective_month was determined *after* a previous 'I' row for the same schema name
                        // (e.g. multiple CSVs for same table in one zip, or malformed single CSV), update it.
                        // This logic assumes one effective_month per CSV file, taken from the first 'C' row.
                        if let Some(ref month) = file_effective_month {
                            if table_entry.effective_month == "UNKNOWN_MONTH"
                                || table_entry.effective_month != *month
                            {
                                trace!(
                                    schema_identifier,
                                    old_month = table_entry.effective_month,
                                    new_month = month,
                                    "Updating effective_month for existing table entry."
                                );
                                table_entry.effective_month = month.clone();
                            }
                        }

                        current_schema_identifier = Some(schema_identifier);
                    }
                    Some("D") => {
                        // "D" row: Data row.
                        if let Some(ref current_name) = current_schema_identifier {
                            if let Some(table_entry) = tables.get_mut(current_name) {
                                let row_data: Vec<String> =
                                    record.iter().skip(4).map(|s| s.to_string()).collect(); // Keep original strings, don't trim yet
                                table_entry.rows.push(row_data);
                            } else {
                                // This case should ideally not happen if "I" always precedes "D" for a schema.
                                warn!(current_schema_name = current_name, "Encountered 'D' row for schema not yet initialized with an 'I' row. Skipping row.");
                            }
                        } else {
                            warn!("Encountered 'D' row without a preceding 'I' row in the current CSV file. Skipping row.");
                        }
                    }
                    _ => {
                        // Other row types, or empty first field. Ignored.
                        trace!(
                            record_num = record_idx,
                            first_field = record.get(0).unwrap_or("N/A"),
                            "Ignoring row type or empty first field."
                        );
                    }
                }
            }
        } else if file_in_zip.is_file() {
            trace!(
                file_type = clean_file_type(&file_name),
                "Skipping non-CSV file entry."
            );
        }
    }
    debug!(
        num_tables_extracted = tables.len(),
        "Finished processing ZIP file."
    );
    Ok(tables)
}

// Helper to avoid panic on invalid UTF-8 in filenames for logging
fn clean_file_type(file_name: &str) -> String {
    if file_name.to_lowercase().ends_with(".csv") {
        "CSV".to_string()
    } else {
        Path::new(file_name)
            .extension()
            .and_then(|os_str| os_str.to_str())
            .map(|s| s.to_uppercase())
            .unwrap_or_else(|| "Unknown".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::io::{Cursor, Write};
    use tempfile::NamedTempFile;
    use tracing_subscriber::{EnvFilter, FmtSubscriber};
    use zip::write::FileOptions;
    use zip::CompressionMethod; // For test logging

    fn init_test_logging() {
        // Initialize tracing for tests, if not already done globally
        let subscriber = FmtSubscriber::builder()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| EnvFilter::new("info,nemscraper::process=trace")),
            )
            .with_test_writer() // Redirect logs to the test output
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber); // Use `let _ =` to ignore errors if already set
    }

    #[test]
    fn test_load_aemo_zip_example() -> Result<()> {
        init_test_logging();
        // build the exact example CSV you provided
        let content = r#"C,SETP.WORLD,FPP_DCF,AEMO,PUBLIC,"2024/12/14",18:02:37,0000000443338900,FPP,0000000443338900
I,FPP,FORECAST_DEFAULT_CF,1,FPP_UNITID,CONSTRAINTID,EFFECTIVE_START_DATETIME,EFFECTIVE_END_DATETIME,VERSIONNO,BIDTYPE,REGIONID,DEFAULT_CONTRIBUTION_FACTOR,DCF_REASON_FLAG,DCF_ABS_NEGATIVE_PERF_TOTAL,SETTLEMENTS_UNITID
D,FPP,FORECAST_DEFAULT_CF,1,ADPBA1,F_I+GFT_TG_R5,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,RAISEREG,SA1,-0.00011729,0,520.67692,ADPBA1
D,FPP,FORECAST_DEFAULT_CF,1,ADPBA1,F_I+LREG_0210,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,LOWERREG,SA1,-0.00035807,0,415.19675,ADPBA1
I,FPP,FORECAST_RESIDUAL_DCF,1,CONSTRAINTID,EFFECTIVE_START_DATETIME,EFFECTIVE_END_DATETIME,VERSIONNO,BIDTYPE,RESIDUAL_DCF,RESIDUAL_DCF_REASON_FLAG,DCF_ABS_NEGATIVE_PERF_TOTAL
D,FPP,FORECAST_RESIDUAL_DCF,1,F_I+GFT_TG_R5,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,RAISEREG,-0.22384015,0,520.67692
D,FPP,FORECAST_RESIDUAL_DCF,1,F_I+LREG_0210,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,LOWERREG,-0.19742411,0,415.19675
"#;

        // write ZIP
        let mut buf = Vec::new();
        {
            let mut zip = zip::ZipWriter::new(Cursor::new(&mut buf));
            let options: FileOptions<'_, ()> =
                FileOptions::default().compression_method(CompressionMethod::Stored);
            zip.start_file("aemo.csv", options)?;
            zip.write_all(content.as_bytes())?;
            zip.finish()?;
        }

        let mut tmp = NamedTempFile::new()?;
        tmp.write_all(&buf)?;

        // run loader
        let tables = load_aemo_zip(tmp.path())?;

        // we should get two schemas
        assert_eq!(tables.len(), 2);
        let default_cf = tables
            .get("FPP_FORECAST_DEFAULT_CF")
            .expect("FPP_FORECAST_DEFAULT_CF table not found");
        let residual_dcf = tables
            .get("FPP_FORECAST_RESIDUAL_DCF")
            .expect("FPP_FORECAST_RESIDUAL_DCF table not found");

        // Check effective month (from "C" row)
        assert_eq!(default_cf.effective_month, "202412");
        assert_eq!(residual_dcf.effective_month, "202412");

        // correct headers?
        assert_eq!(
            default_cf.headers,
            vec![
                "FPP_UNITID",
                "CONSTRAINTID",
                "EFFECTIVE_START_DATETIME",
                "EFFECTIVE_END_DATETIME",
                "VERSIONNO",
                "BIDTYPE",
                "REGIONID",
                "DEFAULT_CONTRIBUTION_FACTOR",
                "DCF_REASON_FLAG",
                "DCF_ABS_NEGATIVE_PERF_TOTAL",
                "SETTLEMENTS_UNITID"
            ]
        );
        assert_eq!(default_cf.rows.len(), 2); // Updated count based on provided sample

        assert_eq!(
            residual_dcf.headers,
            vec![
                "CONSTRAINTID",
                "EFFECTIVE_START_DATETIME",
                "EFFECTIVE_END_DATETIME",
                "VERSIONNO",
                "BIDTYPE",
                "RESIDUAL_DCF",
                "RESIDUAL_DCF_REASON_FLAG",
                "DCF_ABS_NEGATIVE_PERF_TOTAL"
            ]
        );
        assert_eq!(residual_dcf.rows.len(), 2); // Updated count

        Ok(())
    }
}
