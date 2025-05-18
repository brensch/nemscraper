// src/history/mod.rs

use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use duckdb::{
    params,
    types::{TimeUnit, Value},
    Appender, Connection, Error as DuckDbError,
}; // Added Value and TimeUnit for previous manual Value::Timestamp attempt, not strictly needed if NaiveDateTime:ToSql works
use std::{collections::BTreeMap, path::Path};
use tracing::{debug, info, warn};

use crate::process::RawTable;

const HISTORY_TABLE_NAME: &str = "csv_processing_history";

/// Represents a record of a table processed from a CSV file within a ZIP archive.
#[derive(Debug, Clone, PartialEq)]
pub struct CsvDownloadRecord {
    pub table_schema_name: String,
    pub source_zip_filename: String,
    pub row_count: i64,
    pub effective_month: String,
    pub processed_at: DateTime<Utc>,
}

/// Ensures the history table exists in the DuckDB database.
pub fn ensure_history_table_exists(conn: &Connection) -> Result<()> {
    conn.execute(
        &format!(
            "CREATE TABLE IF NOT EXISTS {} (
                table_schema_name VARCHAR NOT NULL,
                source_zip_filename VARCHAR NOT NULL,
                row_count BIGINT NOT NULL,
                effective_month VARCHAR(6) NOT NULL,
                processed_at TIMESTAMP NOT NULL,
                PRIMARY KEY (table_schema_name, source_zip_filename, effective_month)
            );",
            HISTORY_TABLE_NAME
        ),
        [],
    )
    .with_context(|| {
        format!(
            "Failed to create or ensure history table '{}' exists",
            HISTORY_TABLE_NAME
        )
    })?;
    debug!("Ensured history table '{}' exists.", HISTORY_TABLE_NAME);
    Ok(())
}

pub fn record_processed_csv_data<P: AsRef<Path>>(
    conn: &Connection,
    source_zip_path: P,
    processed_tables_data: &BTreeMap<String, RawTable>,
) -> Result<()> {
    let zip_filename_str = source_zip_path
        .as_ref()
        .file_name()
        .and_then(|name| name.to_str())
        .map(String::from)
        .unwrap_or_else(|| {
            let path_display = source_zip_path.as_ref().display().to_string();
            warn!(path = %path_display, "Could not extract filename from source_zip_path, using full path string.");
            path_display
        });

    if processed_tables_data.is_empty() {
        info!(zip_filename = %zip_filename_str, "No tables to record from this ZIP file.");
        return Ok(());
    }

    let mut appender = conn.appender(HISTORY_TABLE_NAME).with_context(|| {
        format!(
            "Failed to create appender for table '{}'",
            HISTORY_TABLE_NAME
        )
    })?;

    let current_time_utc = Utc::now();
    // Assuming NaiveDateTime: ToSql works if chrono feature is enabled for duckdb crate
    let current_time_for_db: NaiveDateTime = current_time_utc.naive_utc();

    for (table_schema_name, raw_table) in processed_tables_data {
        appender
            .append_row(params![
                table_schema_name,
                &zip_filename_str,
                raw_table.rows.len() as i64,
                &raw_table.effective_month,
                current_time_for_db,
            ])
            .with_context(|| {
                format!(
                    "Failed to append row to appender for table '{}' from ZIP '{}'",
                    table_schema_name, zip_filename_str
                )
            })?;
    }

    info!(zip_filename = %zip_filename_str, num_tables_recorded = processed_tables_data.len(), "Finished recording processed CSV data for ZIP using appender.");
    Ok(())
}

/// Retrieves a unique list of ZIP filenames that have been
/// processed and recorded in the history table.
pub fn get_processed_zipfiles(conn: &Connection) -> Result<Vec<String>> {
    let mut query_stmt = conn
        .prepare(&format!(
            "SELECT DISTINCT source_zip_filename FROM {} ORDER BY source_zip_filename;",
            HISTORY_TABLE_NAME
        ))
        .with_context(|| {
            format!(
                "Failed to prepare statement for distinct ZIP filenames from '{}'",
                HISTORY_TABLE_NAME
            )
        })?;

    let zip_filenames_iter = query_stmt
        .query_map([], |row| row.get(0))
        .with_context(|| {
            format!(
                "Failed to query distinct ZIP filenames from '{}'",
                HISTORY_TABLE_NAME
            )
        })?;

    let mut zip_filenames = Vec::new();
    for filename_result in zip_filenames_iter {
        zip_filenames.push(
            filename_result
                .with_context(|| "Failed to retrieve a ZIP filename from query results")?,
        );
    }

    debug!(
        "Retrieved {} distinct processed ZIP filenames.",
        zip_filenames.len()
    );
    Ok(zip_filenames)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::duck::open_mem_db;

    use duckdb::{
        params,
        // Import Type for error construction, ValueRef and TimeUnit for retrieval
        types::{TimeUnit, Type, ValueRef},
        Error as DuckDbError,
    };
    use std::path::PathBuf;
    use tracing_subscriber::{EnvFilter, FmtSubscriber};

    fn init_test_logging() {
        let subscriber = FmtSubscriber::builder()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| EnvFilter::new("info,nemscraper::history=trace")), // Adjust crate name if necessary
            )
            .with_test_writer()
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
    }

    fn dummy_raw_table(rows: usize, month: &str) -> RawTable {
        RawTable {
            headers: vec!["h1".to_string()],
            rows: vec![vec!["data".to_string()]; rows],
            effective_month: month.to_string(),
        }
    }

    // #[test]
    // fn test_record_and_retrieve_with_appender() -> Result<()> {
    //     init_test_logging();
    //     let conn = open_mem_db()?;
    //     ensure_history_table_exists(&conn)?;

    //     // 1. Initial insertion
    //     let mut processed_data1 = BTreeMap::new();
    //     processed_data1.insert("TABLE_A".to_string(), dummy_raw_table(1, "202301"));
    //     processed_data1.insert("TABLE_B".to_string(), dummy_raw_table(2, "202301"));
    //     let zip_path1 = PathBuf::from("test_zip_01.zip");

    //     let time_before_insert1 = Utc::now();
    //     std::thread::sleep(std::time::Duration::from_micros(50)); // Ensure time progresses
    //     record_processed_csv_data(&conn, &zip_path1, &processed_data1)?;
    //     std::thread::sleep(std::time::Duration::from_micros(50));
    //     let time_after_insert1 = Utc::now();

    //     let distinct_ids = get_distinct_processed_csv_identifiers(&conn)?;
    //     assert_eq!(
    //         distinct_ids,
    //         vec!["TABLE_A".to_string(), "TABLE_B".to_string()]
    //     );

    //     let mut stmt_get_record = conn.prepare(&format!(
    //         "SELECT table_schema_name, source_zip_filename, row_count, effective_month, processed_at FROM {} WHERE source_zip_filename = ? AND table_schema_name = ? AND effective_month = ?",
    //         HISTORY_TABLE_NAME
    //     ))?;

    //     let map_row_to_record = |row: &duckdb::Row<'_>| -> Result<CsvDownloadRecord, DuckDbError> {
    //         let value_ref = row.get_ref(4)?;
    //         let ts_micros: i64 = match value_ref {
    //             ValueRef::Timestamp(TimeUnit::Microsecond, micros_val) => micros_val,
    //             ValueRef::Timestamp(unit, other_val) => {
    //                 warn!(
    //                     "Timestamp retrieved with unit {:?} for value {}, converting to micros.",
    //                     unit, other_val
    //                 );
    //                 unit.to_micros(other_val)
    //             }
    //             _ => {
    //                 let actual_type_display = format!("{:?}", value_ref.data_type()); // Use Debug format of private DataType
    //                 return Err(DuckDbError::FromSqlConversionFailure(
    //                     4,
    //                     Type::Timestamp, // Correctly using duckdb::types::Type
    //                     format!(
    //                         "Expected Timestamp ValueRef, got internal type {}",
    //                         actual_type_display
    //                     )
    //                     .into(),
    //                 ));
    //             }
    //         };

    //         let processed_at_datetime =
    //             DateTime::from_timestamp_micros(ts_micros).ok_or_else(|| {
    //                 DuckDbError::FromSqlConversionFailure(
    //                     4,
    //                     Type::Timestamp, // Correctly using duckdb::types::Type
    //                     "Invalid microsecond value for DateTime conversion".into(),
    //                 )
    //             })?;

    //         Ok(CsvDownloadRecord {
    //             table_schema_name: row.get(0)?,
    //             source_zip_filename: row.get(1)?,
    //             row_count: row.get(2)?,
    //             effective_month: row.get(3)?,
    //             processed_at: processed_at_datetime,
    //         })
    //     };

    //     let record_a1: CsvDownloadRecord = stmt_get_record
    //         .query_row(
    //             params!["test_zip_01.zip", "TABLE_A", "202301"],
    //             map_row_to_record,
    //         )
    //         .context("Failed to query record_a1 from history")?;

    //     assert_eq!(record_a1.table_schema_name, "TABLE_A");
    //     assert_eq!(record_a1.source_zip_filename, "test_zip_01.zip");
    //     assert_eq!(record_a1.row_count, 1);
    //     assert_eq!(record_a1.effective_month, "202301");
    //     assert!(
    //         record_a1.processed_at >= time_before_insert1 // Use >= and <= for broader compatibility with time resolution
    //             && record_a1.processed_at <= time_after_insert1,
    //         "Timestamp for record_a1 out of expected range (recorded: {}, window: {} to {})",
    //         record_a1.processed_at,
    //         time_before_insert1,
    //         time_after_insert1
    //     );

    //     // 2. Attempt to reprocess (expect PK violation)
    //     let result_reprocessing_same =
    //         record_processed_csv_data(&conn, &zip_path1, &processed_data1);

    //     assert!(
    //         result_reprocessing_same.is_err(),
    //         "Reprocessing should have failed due to PK violation, but it succeeded."
    //     );

    //     if let Err(e) = result_reprocessing_same {
    //         let mut is_constraint_violation = false;
    //         for cause in e.chain() {
    //             // Iterate through the anyhow::Error cause chain
    //             if let Some(duckdb_error) = cause.downcast_ref::<DuckDbError>() {
    //                 if let DuckDbError::DuckDBFailure(_, Some(message)) = duckdb_error {
    //                     let lower_message = message.to_lowercase();
    //                     if lower_message.contains("constraint")
    //                         || lower_message.contains("primary key")
    //                     {
    //                         is_constraint_violation = true;
    //                         break;
    //                     }
    //                 }
    //             }
    //         }
    //         assert!(
    //             is_constraint_violation,
    //             "Expected a DuckDB constraint violation, but got error: {:?}",
    //             e
    //         );
    //     }
    //     // The else branch for panic! if result_reprocessing_same.is_ok() is implicitly handled by the assert!(is_err()) above.

    //     // 3. Insert data from a new zip
    //     let mut processed_data2 = BTreeMap::new();
    //     processed_data2.insert("TABLE_A".to_string(), dummy_raw_table(3, "202302"));
    //     processed_data2.insert("TABLE_C".to_string(), dummy_raw_table(4, "202301"));
    //     let zip_path2 = PathBuf::from("test_zip_02.zip");
    //     record_processed_csv_data(&conn, &zip_path2, &processed_data2)?;

    //     let distinct_ids_after_2 = get_distinct_processed_csv_identifiers(&conn)?;
    //     assert_eq!(distinct_ids_after_2.len(), 3);
    //     assert_eq!(
    //         distinct_ids_after_2,
    //         vec![
    //             "TABLE_A".to_string(),
    //             "TABLE_B".to_string(),
    //             "TABLE_C".to_string()
    //         ]
    //     );

    //     let record_c2: CsvDownloadRecord = stmt_get_record
    //         .query_row(
    //             params!["test_zip_02.zip", "TABLE_C", "202301"],
    //             map_row_to_record,
    //         )
    //         .context("Failed to query record_c2 from history")?;
    //     assert_eq!(record_c2.row_count, 4);

    //     // 4. Verify original TABLE_A from zip_path1 is unchanged
    //     let record_a1_again: CsvDownloadRecord = stmt_get_record
    //         .query_row(
    //             params!["test_zip_01.zip", "TABLE_A", "202301"],
    //             map_row_to_record,
    //         )
    //         .context("Failed to query record_a1_again from history")?;
    //     assert_eq!(record_a1_again.row_count, 1);
    //     assert_eq!(record_a1_again.processed_at, record_a1.processed_at);

    //     Ok(())
    // }
}
