// src/process/mod.rs
use anyhow::{Context, Result};
use csv::ReaderBuilder;

use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::{BufReader, Cursor, Read},
    path::Path,
};
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
#[tracing::instrument(level = "info", skip(zip_path), fields(path = %zip_path.as_ref().display()))]
pub fn load_aemo_zip<P: AsRef<Path>>(zip_path: P) -> Result<BTreeMap<String, RawTable>> {
    // 1) Open the ZIP once
    let file = File::open(&zip_path)
        .with_context(|| format!("Failed to open ZIP file: {:?}", zip_path.as_ref()))?;
    let mut archive = ZipArchive::new(file)
        .with_context(|| format!("Failed to read ZIP archive: {:?}", zip_path.as_ref()))?;

    // 2) Extract each .csv entry into memory, in archive order
    let mut buffers: Vec<(String, Vec<u8>)> = Vec::with_capacity(archive.len());
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i).with_context(|| {
            format!(
                "Failed to access ZIP entry #{} in {:?}",
                i,
                zip_path.as_ref()
            )
        })?;
        let name = entry.name().to_string();

        if entry.is_file() && name.to_lowercase().ends_with(".csv") {
            let mut buf = Vec::with_capacity(entry.size() as usize);
            entry
                .read_to_end(&mut buf)
                .with_context(|| format!("Failed to read {} into memory", name))?;
            buffers.push((name, buf));
        }
    }
    // drop the archive (and its file handle) now that we've buffered everything
    drop(archive);

    // 3) Now parse each CSV buffer **in order**, preserving every row’s sequence
    let mut tables: BTreeMap<String, RawTable> = BTreeMap::new();
    for (file_name, data) in buffers {
        let mut rdr = ReaderBuilder::new()
            .has_headers(false)
            .flexible(true) // keep this so records with different field-counts work
            .from_reader(Cursor::new(data));

        let mut current_schema: Option<String> = None;
        let mut file_month: Option<String> = None;

        for (idx, result) in rdr.records().enumerate() {
            let record = result
                .with_context(|| format!("CSV parse error in {} at record {}", file_name, idx))?;

            match record.get(0).map(str::trim) {
                Some("C") => {
                    if let Some(ds) = record.get(5) {
                        file_month = parse_date_to_yyyymm(ds);
                    }
                }
                Some("I") => {
                    let schema_id = {
                        let t = record.get(1).unwrap().trim();
                        let n = record.get(2).unwrap().trim();
                        format!("{}_{}", t, n)
                    };
                    let cols: Vec<String> = record.iter().skip(4).map(|s| s.to_string()).collect();
                    let month = file_month.clone().unwrap_or_else(|| "UNKNOWN_MONTH".into());

                    // guaranteed to return you &mut RawTable (in order)
                    let tbl = tables.entry(schema_id.clone()).or_insert_with(|| RawTable {
                        headers: Vec::new(),
                        rows: Vec::new(),
                        effective_month: month.clone(),
                    });

                    // overwrite headers (once per I-row, in-order)
                    tbl.headers = cols;
                    current_schema = Some(schema_id);
                }
                Some("D") => {
                    if let Some(ref schema_id) = current_schema {
                        let row: Vec<String> =
                            record.iter().skip(4).map(|s| s.to_string()).collect();
                        tables
                            .get_mut(schema_id)
                            .expect("schema must exist")
                            .rows
                            .push(row);
                    }
                }
                _ => {
                    // skip blanks or other rows
                }
            }
        }
    }

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
    use std::env;
    use std::io::{Cursor, Write};
    use std::sync::Arc;
    use std::time::Instant;
    use tempfile::NamedTempFile;
    use tracing_subscriber::{EnvFilter, FmtSubscriber};
    use zip::write::FileOptions;
    use zip::CompressionMethod; // For test logging

    fn init_test_logging() {
        // Initialize tracing for tests, if not already done globally
        let subscriber = FmtSubscriber::builder()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| EnvFilter::new("info,nemscraper::process=debug")),
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

    use crate::{duck, schema};

    /// Benchmark test: set ZIP_PATH to your real file and run with `-- --nocapture`
    /// Benchmark test: set `ZIP_PATH` to your real file and run with `-- --nocapture`
    #[test]
    fn bench_load_aemo_zip() -> Result<()> {
        init_test_logging();

        // 1) Load the ZIP and parse into in-memory tables
        let zip_path_str =
            env::var("ZIP_PATH").expect("Please set ZIP_PATH env var to the path of your ZIP file");
        let zip_path = Path::new(&zip_path_str);
        println!("→ Loading ZIP: {}", zip_path.display());
        let start = Instant::now();
        let tables = load_aemo_zip(zip_path)?;
        let elapsed = start.elapsed();
        println!(
            "✔ load_aemo_zip({}) extracted {} tables in {:?}",
            zip_path.display(),
            tables.len(),
            elapsed
        );

        // 2) Extract schema evolutions from disk
        let schemas_dir = Path::new("schemas");
        println!(
            "→ Extracting schema evolutions from {}",
            schemas_dir.display()
        );
        let evolutions = schema::extract_schema_evolutions(schemas_dir)?;
        println!("✔ found {} schema evolutions", evolutions.len());

        // 3) Create a temporary DuckDB file
        // let db_file = NamedTempFile::new()?;
        // let db_path = db_file.path().to_str().unwrap();
        println!("→ Creating DuckDB ");
        let conn = duck::open_mem_db()?;

        // 4) Create each versioned table in DuckDB
        let mut created = 0;
        for evo in &evolutions {
            let tbl_name = format!("{}_{}", evo.table_name, evo.fields_hash);
            duck::create_table_from_schema(&conn, &tbl_name, &evo.columns)?;
            created += 1;
        }
        println!("✔ created {} tables in DuckDB", created);

        // 5) Build lookup: table_name -> sorted Vec<Arc<SchemaEvolution>>
        let mut lookup: HashMap<String, Vec<Arc<schema::SchemaEvolution>>> = HashMap::new();
        for evo in &evolutions {
            lookup
                .entry(evo.table_name.clone())
                .or_default()
                .push(Arc::new(evo.clone()));
        }
        for versions in lookup.values_mut() {
            versions.sort_by_key(|e| e.start_month.clone());
        }

        // 6) Insert each RawTable into its matching versioned table
        let mut inserted = 0;
        for (tbl_id, raw) in &tables {
            if let Some(evo) = schema::find_schema_evolution(&lookup, tbl_id, &raw.effective_month)
            {
                let target = format!("{}_{}", evo.table_name, evo.fields_hash);
                duck::insert_raw_table_data(&conn, &target, raw, &evo)?;
                inserted += 1;
            } else {
                panic!(
                    "No schema evolution found for {}@{}",
                    tbl_id, raw.effective_month
                );
            }
        }
        println!("✔ inserted {} tables into DuckDB", inserted);

        Ok(())
    }
}
