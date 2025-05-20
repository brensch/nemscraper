// // src/history/mod.rs

// use anyhow::{Context, Result};
// use chrono::{DateTime, NaiveDateTime, Utc};
// use duckdb::{
//     params,
//     types::{TimeUnit, Value},
//     Appender, Connection, Error as DuckDbError,
// }; // Added Value and TimeUnit for previous manual Value::Timestamp attempt, not strictly needed if NaiveDateTime:ToSql works
// use std::{collections::BTreeMap, path::Path};
// use tracing::{debug, info, warn};

// const HISTORY_TABLE_NAME: &str = "csv_processing_history";

// /// Represents a record of a table processed from a CSV file within a ZIP archive.
// #[derive(Debug, Clone, PartialEq)]
// pub struct CsvDownloadRecord {
//     pub table_schema_name: String,
//     pub source_zip_filename: String,
//     pub row_count: i64,
//     pub effective_month: String,
//     pub processed_at: DateTime<Utc>,
// }

// /// Ensures the history table exists in the DuckDB database.
// pub fn ensure_history_table_exists(conn: &Connection) -> Result<()> {
//     conn.execute(
//         &format!(
//             "CREATE TABLE IF NOT EXISTS {} (
//                 table_schema_name VARCHAR NOT NULL,
//                 source_zip_filename VARCHAR NOT NULL,
//                 row_count BIGINT NOT NULL,
//                 effective_month VARCHAR(6) NOT NULL,
//                 processed_at TIMESTAMP NOT NULL,
//                 PRIMARY KEY (table_schema_name, source_zip_filename, effective_month)
//             );",
//             HISTORY_TABLE_NAME
//         ),
//         [],
//     )
//     .with_context(|| {
//         format!(
//             "Failed to create or ensure history table '{}' exists",
//             HISTORY_TABLE_NAME
//         )
//     })?;
//     debug!("Ensured history table '{}' exists.", HISTORY_TABLE_NAME);
//     Ok(())
// }

// pub fn record_processed_csv_data<P: AsRef<Path>>(
//     conn: &Connection,
//     source_zip_path: P,
//     processed_tables_data: &BTreeMap<String, RawTable>,
// ) -> Result<()> {
//     let zip_filename_str = source_zip_path
//         .as_ref()
//         .file_name()
//         .and_then(|name| name.to_str())
//         .map(String::from)
//         .unwrap_or_else(|| {
//             let path_display = source_zip_path.as_ref().display().to_string();
//             warn!(path = %path_display, "Could not extract filename from source_zip_path, using full path string.");
//             path_display
//         });

//     if processed_tables_data.is_empty() {
//         info!(zip_filename = %zip_filename_str, "No tables to record from this ZIP file.");
//         return Ok(());
//     }

//     let mut appender = conn.appender(HISTORY_TABLE_NAME).with_context(|| {
//         format!(
//             "Failed to create appender for table '{}'",
//             HISTORY_TABLE_NAME
//         )
//     })?;

//     let current_time_utc = Utc::now();
//     // Assuming NaiveDateTime: ToSql works if chrono feature is enabled for duckdb crate
//     let current_time_for_db: NaiveDateTime = current_time_utc.naive_utc();

//     for (table_schema_name, raw_table) in processed_tables_data {
//         appender
//             .append_row(params![
//                 table_schema_name,
//                 &zip_filename_str,
//                 raw_table.rows.len() as i64,
//                 &raw_table.effective_month,
//                 current_time_for_db,
//             ])
//             .with_context(|| {
//                 format!(
//                     "Failed to append row to appender for table '{}' from ZIP '{}'",
//                     table_schema_name, zip_filename_str
//                 )
//             })?;
//     }

//     info!(zip_filename = %zip_filename_str, num_tables_recorded = processed_tables_data.len(), "Finished recording processed CSV data for ZIP using appender.");
//     Ok(())
// }

// /// Retrieves a unique list of ZIP filenames that have been
// /// processed and recorded in the history table.
// pub fn get_processed_zipfiles(conn: &Connection) -> Result<Vec<String>> {
//     let mut query_stmt = conn
//         .prepare(&format!(
//             "SELECT DISTINCT source_zip_filename FROM {} ORDER BY source_zip_filename;",
//             HISTORY_TABLE_NAME
//         ))
//         .with_context(|| {
//             format!(
//                 "Failed to prepare statement for distinct ZIP filenames from '{}'",
//                 HISTORY_TABLE_NAME
//             )
//         })?;

//     let zip_filenames_iter = query_stmt
//         .query_map([], |row| row.get(0))
//         .with_context(|| {
//             format!(
//                 "Failed to query distinct ZIP filenames from '{}'",
//                 HISTORY_TABLE_NAME
//             )
//         })?;

//     let mut zip_filenames = Vec::new();
//     for filename_result in zip_filenames_iter {
//         zip_filenames.push(
//             filename_result
//                 .with_context(|| "Failed to retrieve a ZIP filename from query results")?,
//         );
//     }

//     debug!(
//         "Retrieved {} distinct processed ZIP filenames.",
//         zip_filenames.len()
//     );
//     Ok(zip_filenames)
// }
