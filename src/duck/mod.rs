// src/duck/mod.rs

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, FixedOffset, NaiveDateTime, TimeZone, Utc}; use duckdb::types::TimeUnit;
// Added DateTime, Utc
use duckdb::{types::Value, Appender, Connection, ToSql};
use tracing;

use crate::process::RawTable;
use crate::schema::Column as NemSchemaColumn;

// --- Helper for chrono format string conversion ---
/// Converts a NEM-style date format string to a chrono-compatible format string.
fn nem_format_to_chrono_format(nem_format: &str) -> String {
    nem_format
        .replace("YYYY", "%Y")
        .replace("YY", "%y")
        .replace("MM", "%m")
        .replace("DD", "%d")
        .replace("HH24", "%H")
        .replace("hh24", "%H")
        .replace("HH", "%H") // Assuming HH is 24-hour, adjust if 12-hour
        .replace("hh", "%I") // Assuming hh is 12-hour
        .replace("MI", "%M")
        .replace("mi", "%M")
        .replace("SS", "%S")
        .replace("ss", "%S")
        // remove quotes that might be present in NEM format
        .replace("\"", "")
}

// --- Functions for schema-based table creation ---

/// Maps a schema column type (and optional format) to a DuckDB SQL type.
pub fn map_schema_type_to_duckdb_type(
    schema_col_type: &str,
    schema_col_format: Option<&str>,
) -> String {
    let u_type = schema_col_type.to_uppercase();
    match u_type.as_str() {
        s if s.starts_with("TIMESTAMP") => "TIMESTAMPTZ".to_string(),
        s if s.starts_with("DATE") => {
            if let Some(fmt) = schema_col_format {
                let fmt_upper = fmt.to_uppercase();
                if fmt_upper.contains("HH24")
                    || fmt_upper.contains("HH")
                    || fmt_upper.contains("MI")
                    || fmt_upper.contains("SS")
                {
                    "TIMESTAMPTZ".to_string()
                } else {
                    "DATE".to_string()
                }
            } else {
                "DATE".to_string()
            }
        }
        s if s.starts_with("FLOAT") || s.starts_with("NUMBER") || s.starts_with("DECIMAL") => {
            "DOUBLE".to_string()
        }
        s if s.starts_with("INT") => "INTEGER".to_string(),
        s if s.starts_with("BIGINT") => "BIGINT".to_string(),
        s if s.starts_with("SMALLINT") => "SMALLINT".to_string(),
        s if s.starts_with("TINYINT") => "TINYINT".to_string(),
        s if s.starts_with("CHAR")
            || s.starts_with("VARCHAR")
            || s.starts_with("TEXT")
            || s.starts_with("STRING") =>
        {
            "VARCHAR".to_string()
        }
        s if s.starts_with("BOOL") => "BOOLEAN".to_string(),
        s if s.starts_with("BLOB") || s.starts_with("BYTEA") => "BLOB".to_string(),
        _ => {
            tracing::warn!(
                unknown_type = schema_col_type,
                "Unknown schema type. Defaulting to VARCHAR."
            );
            "VARCHAR".to_string()
        }
    }
}

/// Creates a table in DuckDB based on the provided schema definition.
#[tracing::instrument(level = "debug", skip(conn, columns), fields(table_name))]
pub fn create_table_from_schema(
    conn: &Connection,
    table_name: &str,
    columns: &[NemSchemaColumn],
) -> Result<()> {
    tracing::Span::current().record("table_name", &table_name);
    tracing::debug!(?columns, "Creating table from schema.");
    if columns.is_empty() {
        return Err(anyhow!(
            "Cannot create table '{}': no columns defined.",
            table_name
        ));
    }

    let mut create_table_sql = format!("CREATE TABLE IF NOT EXISTS \"{}\" (\n", table_name);

    for (i, col) in columns.iter().enumerate() {
        let duckdb_type = map_schema_type_to_duckdb_type(&col.ty, col.format.as_deref());
        create_table_sql.push_str(&format!("    \"{}\" {}", col.name, duckdb_type));
        if i < columns.len() - 1 {
            create_table_sql.push_str(",\n");
        } else {
            create_table_sql.push_str("\n");
        }
    }
    create_table_sql.push_str(");");

    tracing::trace!(sql_statement = %create_table_sql, "Generated CREATE TABLE SQL.");

    conn.execute(&create_table_sql, []).with_context(|| {
        format!(
            "Failed to create table '{}' with SQL: \n{}",
            table_name, create_table_sql
        )
    })?;

    // debug: dump the on‐disk columns
    let mut info = Vec::new();
    let mut stmt = conn.prepare(&format!("PRAGMA table_info(\"{}\")", table_name))?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        let cid: i32 = r.get(0)?;
        let name: String = r.get(1)?;
        let dtype: String = r.get(2)?;
        info.push((cid, name, dtype));
    }
    tracing::debug!(?info, "Actual columns for table");

    tracing::debug!(
        "Table '{}' created successfully or already exists.",
        table_name
    );
    Ok(())
}

// --- Function for inserting RawTable data ---

/// Parses a string value into a DuckDB `Value` based on the target schema column type.
/// Handles empty strings as NULL.
/// For timestamp-like fields, assumes input string is in NEM time (+10:00) and converts to an ISO string.
/// Parses a string value into a DuckDB `Value` based on the target schema column type.
/// Handles empty strings as NULL.
/// For timestamp-like fields, assumes input string is in NEM time (+10:00) and converts to native timestamp.
fn parse_string_to_duckdb_value(value_str: &str, target_column: &NemSchemaColumn) -> Result<Value> {
    let trimmed_value = value_str.trim();

    if trimmed_value.is_empty() {
        return Ok(Value::Null);
    }

    // Determine the target DuckDB type once
    let final_duckdb_type =
        map_schema_type_to_duckdb_type(&target_column.ty, target_column.format.as_deref());

    match final_duckdb_type.as_str() {
        "TIMESTAMPTZ" => {
            let format_str = target_column
                .format
                .as_deref()
                // Default to a common ISO-like format if specific NEM format isn't there,
                // though NEM usually provides one for timestamps.
                .unwrap_or("YYYY/MM/DD HH24:MI:SS");
            let chrono_format_str = nem_format_to_chrono_format(format_str);

            match NaiveDateTime::parse_from_str(trimmed_value, &chrono_format_str) {
                Ok(naive_dt) => {
                    let nem_offset = FixedOffset::east_opt(10 * 3600).unwrap(); // +10 hours
                    match nem_offset.from_local_datetime(&naive_dt).single() {
                        Some(dt_with_offset) => {
                            // Convert to UTC timestamp with microsecond precision
                            let utc_dt: DateTime<Utc> = dt_with_offset.into();
                            
                            // Calculate microseconds
                            let timestamp_micros = utc_dt.timestamp() * 1_000_000 
                                + i64::from(utc_dt.timestamp_subsec_micros());
                            
                            // Use native timestamp type
                            Ok(Value::Timestamp(TimeUnit::Microsecond, timestamp_micros))
                        },
                        None => Err(anyhow!(
                            "Failed to interpret '{}' with offset +10:00 for column '{}' (ambiguous or invalid local time). Value: {}, Format: {}",
                            target_column.name, trimmed_value, naive_dt, chrono_format_str
                        )),
                    }
                }
                Err(e) => Err(anyhow!(
                    "Failed to parse '{}' as NaiveDateTime with format '{}' (original NEM format: '{}') for column '{}': {}",
                    trimmed_value, chrono_format_str, format_str, target_column.name, e
                )),
            }
        }
        "DATE" => {
            if let Some(fmt) = target_column.format.as_deref() {
                let chrono_format_str = nem_format_to_chrono_format(fmt);
                
                // Try to parse as DateTime first
                if let Ok(naive_dt) = NaiveDateTime::parse_from_str(trimmed_value, &chrono_format_str) {
                    // If it contains time components, use TimestampTz
                    let nem_offset = FixedOffset::east_opt(10 * 3600).unwrap();
                    if let Some(dt_with_offset) = nem_offset.from_local_datetime(&naive_dt).single() {
                        let utc_dt: DateTime<Utc> = dt_with_offset.into();
                        let timestamp_micros = utc_dt.timestamp() * 1_000_000 
                            + i64::from(utc_dt.timestamp_subsec_micros());
                        return Ok(Value::Timestamp(TimeUnit::Microsecond, timestamp_micros));
                    }
                }
                
                // Try to parse as Date only
                if let Ok(naive_date) = chrono::NaiveDate::parse_from_str(trimmed_value, &chrono_format_str) {
                    // Convert to days since epoch (1970-01-01)
                    let unix_epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let days_since_epoch = naive_date.signed_duration_since(unix_epoch).num_days() as i32;
                    return Ok(Value::Date32(days_since_epoch));
                }
                
                // If both parsing attempts failed, return an error
                Err(anyhow!(
                    "Failed to parse '{}' as DATE with chrono format '{}' (original NEM format: '{}') for column '{}'.",
                    trimmed_value, chrono_format_str, fmt, target_column.name
                ))
            } else {
                // Try to parse using common date formats
                if let Ok(naive_date) = chrono::NaiveDate::parse_from_str(trimmed_value, "%Y-%m-%d") {
                    let unix_epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let days_since_epoch = naive_date.signed_duration_since(unix_epoch).num_days() as i32;
                    Ok(Value::Date32(days_since_epoch))
                } else if let Ok(naive_date) = chrono::NaiveDate::parse_from_str(trimmed_value, "%Y/%m/%d") {
                    let unix_epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let days_since_epoch = naive_date.signed_duration_since(unix_epoch).num_days() as i32;
                    Ok(Value::Date32(days_since_epoch))
                } else {
                    // Last resort, pass as text and let DuckDB try to parse it
                    Ok(Value::Text(trimmed_value.to_string()))
                }
            }
        }
        "DOUBLE" => trimmed_value
            .parse::<f64>()
            .map(Value::Double)
            .map_err(|e| {
                anyhow!(
                    "Failed to parse '{}' as f64 for column '{}': {}",
                    trimmed_value,
                    target_column.name,
                    e
                )
            }),
        "INTEGER" => trimmed_value.parse::<i32>().map(Value::Int).map_err(|e| {
            anyhow!(
                "Failed to parse '{}' as i32 for column '{}': {}",
                trimmed_value,
                target_column.name,
                e
            )
        }),
        "BIGINT" => trimmed_value
            .parse::<i64>()
            .map(Value::BigInt)
            .map_err(|e| {
                anyhow!(
                    "Failed to parse '{}' as i64 for column '{}': {}",
                    trimmed_value,
                    target_column.name,
                    e
                )
            }),
        "SMALLINT" => trimmed_value
            .parse::<i16>()
            .map(Value::SmallInt)
            .map_err(|e| {
                anyhow!(
                    "Failed to parse '{}' as i16 for column '{}': {}",
                    trimmed_value,
                    target_column.name,
                    e
                )
            }),
        "TINYINT" => trimmed_value
            .parse::<i8>()
            .map(Value::TinyInt)
            .map_err(|e| {
                anyhow!(
                    "Failed to parse '{}' as i8 for column '{}': {}",
                    trimmed_value,
                    target_column.name,
                    e
                )
            }),
        "BOOLEAN" => match trimmed_value.to_uppercase().as_str() {
            "TRUE" | "T" | "1" | "YES" | "Y" => Ok(Value::Boolean(true)),
            "FALSE" | "F" | "0" | "NO" | "N" => Ok(Value::Boolean(false)),
            _ => Err(anyhow!(
                "Failed to parse '{}' as boolean for column '{}'",
                trimmed_value,
                target_column.name
            )),
        },
        _ => Ok(Value::Text(trimmed_value.to_string())), // VARCHAR, BLOB (as text hex?), UNKNOWN
    }
}





/// Inserts data from a `RawTable` into the specified DuckDB table,
/// but first verifies that the on‐disk schema matches the expected schema.
#[tracing::instrument(
    level = "debug",
    skip(conn, raw_table_data, schema_definition),
    fields(target_table = %duckdb_table_name, num_raw_rows = raw_table_data.rows.len())
)]
pub fn insert_raw_table_data(
    conn: &Connection,
    duckdb_table_name: &str,
    raw_table_data: &RawTable,
    schema_definition: &crate::schema::SchemaEvolution,
) -> Result<usize> {
    //  Quick exit if nothing to do
    if raw_table_data.rows.is_empty() {
        tracing::debug!("No rows to insert into table '{}'.", duckdb_table_name);
        return Ok(0);
    }

    //  Verify expected schema is non‐empty
    let expected_cols: Vec<String> = schema_definition
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect();
    if expected_cols.is_empty() {
        return Err(anyhow!(
            "Schema definition for table '{}' has no columns.",
            duckdb_table_name
        ));
    }







    // 4) Open an Appender (will also error if table really doesn’t exist)
    let mut appender = match conn.appender(duckdb_table_name) {
        Ok(appender) => appender,
        Err(e) => {
            tracing::error!(table = %duckdb_table_name, raw_error = %e, "duckdb::Connection::appender() failed");
            return Err(e).context(format!(
                "Failed to create appender for table '{}'",
                duckdb_table_name
            ));
        }
    };

    //  Append rows, skipping any with the wrong length
    let expected_col_count = expected_cols.len();
    let mut successfully_appended = 0;
    for (idx, string_row) in raw_table_data.rows.iter().enumerate() {
        if string_row.len() != expected_col_count {
            tracing::error!(
                row_num = idx + 1,
                expected_cols = expected_col_count,
                actual_cols = string_row.len(),
                table = duckdb_table_name,
                "Skipping row due to column-count mismatch"
            );
            continue;
        }

        // convert each cell to Value…
        let mut duck_vals = Vec::with_capacity(expected_col_count);
        let mut row_ok = true;
        for (i, cell) in string_row.iter().enumerate() {
            let col = &schema_definition.columns[i];
            match crate::duck::parse_string_to_duckdb_value(cell, col) {
                Ok(v) => duck_vals.push(v),
                Err(err) => {
                    tracing::warn!(
                        row_num = idx + 1,
                        col_idx = i + 1,
                        col_name = %col.name,
                        raw = %cell,
                        error = %err,
                        table = duckdb_table_name,
                        "Conversion failed; skipping row"
                    );
                    row_ok = false;
                    break;
                }
            }
        }
        if !row_ok {
            continue;
        }

        // …and append
        let params: Vec<&dyn ToSql> = duck_vals.iter().map(|v| v as &dyn ToSql).collect();
        if let Err(e) = appender.append_row(params.as_slice()) {
            tracing::warn!(
                row_num = idx + 1,
                table = duckdb_table_name,
                error = %e,
                "Appender.append_row() failed; skipping row"
            );
        } else {
            successfully_appended += 1;
        }
    }

    // 6) Flush everything
    appender
        .flush()
        .with_context(|| format!("Failed to flush appender for table '{}'", duckdb_table_name))?;
    tracing::info!(
        inserted_rows = successfully_appended,
        total_rows = raw_table_data.rows.len(),
        table = duckdb_table_name,
        "Finished inserting data."
    );

    Ok(successfully_appended)
}

/// Open an in-memory DuckDB instance
pub fn open_mem_db() -> Result<Connection> {
    Ok(Connection::open_in_memory()?)
}

/// Open or create a file-based DuckDB instance
pub fn open_file_db(path: &str) -> Result<Connection> {
    // Ok(Connection::open_in_memory()?)

    Connection::open(path).context(format!("Failed to open DuckDB file at {}", path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaEvolution;
    use anyhow::Result;
    // TimeUnit might not be needed if directly getting chrono types
    // use duckdb::types::TimeUnit;
    use tracing_subscriber::{EnvFilter, FmtSubscriber};

    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
    use std::time::Instant;

    fn init_test_logging() {
        let subscriber = FmtSubscriber::builder()
            .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                // ADJUST your_crate_name to your actual crate name (e.g., nemscraper)
                EnvFilter::new("info,your_crate_name::duck=trace,your_crate_name::process=trace")
            }))
            .with_test_writer()
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
    }

    #[test]
    fn test_nem_format_to_chrono_format_conversion() {
        assert_eq!(
            nem_format_to_chrono_format("YYYY/MM/DD HH24:MI:SS"),
            "%Y/%m/%d %H:%M:%S"
        );
        assert_eq!(
            nem_format_to_chrono_format("\"YYYY-MM-DD hh:mi:ss\""),
            "%Y-%m-%d %I:%M:%S"
        );
        assert_eq!(nem_format_to_chrono_format("YYYYMMDD"), "%Y%m%d");
    }

    #[test]
    fn test_map_schema_types() {
        init_test_logging();
        assert_eq!(
            map_schema_type_to_duckdb_type("DATE", Some("\"yyyy/mm/dd hh24:mi:ss\"")),
            "TIMESTAMPTZ"
        );
        assert_eq!(
            map_schema_type_to_duckdb_type("DATE", Some("YYYY/MM/DD HH24:MI:SS")),
            "TIMESTAMPTZ"
        );
        assert_eq!(
            map_schema_type_to_duckdb_type("DATE", Some("YYYY-MM-DD hh:mi:ss")),
            "TIMESTAMPTZ"
        );
        assert_eq!(
            map_schema_type_to_duckdb_type("TIMESTAMP", None),
            "TIMESTAMPTZ"
        );
        assert_eq!(
            map_schema_type_to_duckdb_type("TIMESTAMP(3)", None),
            "TIMESTAMPTZ"
        );
        assert_eq!(map_schema_type_to_duckdb_type("DATE", None), "DATE");
        assert_eq!(map_schema_type_to_duckdb_type("FLOAT", None), "DOUBLE");
        assert_eq!(
            map_schema_type_to_duckdb_type("date", Some("yyyy-mm-dd")),
            "DATE"
        );
        assert_eq!(
            map_schema_type_to_duckdb_type("date", Some("YYYY-MM-DD HH:MI:SS")),
            "TIMESTAMPTZ"
        );
    }

    #[test]
    fn test_create_and_insert_raw_data_with_timestamptz() -> Result<()> {
        init_test_logging();
        let conn = open_mem_db().context("Failed to open in-memory DB for test")?;
        // Setting session timezone to UTC ensures that when DuckDB converts TIMESTAMPTZ to string
        // for display or for direct retrieval (if not directly into DateTime<Utc>),
        // it uses UTC, making assertions predictable.
        conn.execute("SET TimeZone='UTC';", [])?;

        let table_base_name = "TZ_TEST_DATA_TABLE";
        let schema_hash = "tztesthash123";
        let duckdb_table_name = format!("{}_{}", table_base_name, schema_hash);

        let schema_cols = vec![
            NemSchemaColumn {
                name: "ID".to_string(),
                ty: "INTEGER".to_string(),
                format: None,
            },
            NemSchemaColumn {
                name: "NAME".to_string(),
                ty: "VARCHAR(50)".to_string(),
                format: None,
            },
            NemSchemaColumn {
                name: "EVENT_TIME_NEM".to_string(),
                ty: "DATE".to_string(),
                format: Some("YYYY/MM/DD HH24:MI:SS".to_string()), // This will map to TIMESTAMPTZ
            },
            NemSchemaColumn {
                name: "EXPLICIT_TS_NEM".to_string(),
                ty: "TIMESTAMP".to_string(), // This will map to TIMESTAMPTZ
                format: Some("YYYY-MM-DD HH24:MI:SS".to_string()),
            },
            NemSchemaColumn {
                name: "SIMPLE_DATE".to_string(),
                ty: "DATE".to_string(),
                format: Some("YYYY-MM-DD".to_string()),
            },
        ];

        let test_schema_evolution = SchemaEvolution {
            table_name: table_base_name.to_string(),
            fields_hash: schema_hash.to_string(),
            start_month: "202301".to_string(),
            end_month: "202312".to_string(),
            columns: schema_cols.clone(),
        };

        create_table_from_schema(&conn, &duckdb_table_name, &test_schema_evolution.columns)
            .context("Failed to create table from schema")?;

        let raw_table = RawTable {
            headers: vec![
                "ID".to_string(),
                "NAME".to_string(),
                "EVENT_TIME_NEM".to_string(),
                "EXPLICIT_TS_NEM".to_string(),
                "SIMPLE_DATE".to_string(),
            ],
            rows: vec![
                vec![
                    "1".to_string(),
                    "Alice".to_string(),
                    "2023/01/15 10:30:00".to_string(), // NEM +10:00
                    "2024-03-01 23:00:00".to_string(), // NEM +10:00
                    "2023-01-15".to_string(),
                ],
                vec![
                    "2".to_string(),
                    "Bob".to_string(),
                    "2023/02/20 00:30:00".to_string(), // NEM +10:00 (becomes 14:30 previous day UTC)
                    "2024-04-10 12:00:00".to_string(), // NEM +10:00
                    "2023-02-20".to_string(),
                ],
                vec![
                    "3".to_string(),
                    "Charlie".to_string(),
                    "".to_string(),  // Empty timestamp -> NULL
                    " ".to_string(), // Empty timestamp -> NULL
                    "2023-03-10".to_string(),
                ],
            ],
            effective_month: "202301".to_string(),
        };

        let inserted_count = insert_raw_table_data(
            &conn,
            &duckdb_table_name,
            &raw_table,
            &test_schema_evolution,
        )?;
        assert_eq!(
            inserted_count, 3,
            "Expected 3 rows to be successfully inserted"
        );

        let mut stmt_type_check = conn.prepare(&format!(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'main' AND table_name = '{}' ORDER BY column_name",
            duckdb_table_name
        ))?;
        let mut type_rows = stmt_type_check.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;

        assert_eq!(
            type_rows.next().unwrap()?,
            (
                "EVENT_TIME_NEM".to_string(),
                "TIMESTAMP WITH TIME ZONE".to_string()
            )
        );
        assert_eq!(
            type_rows.next().unwrap()?,
            (
                "EXPLICIT_TS_NEM".to_string(),
                "TIMESTAMP WITH TIME ZONE".to_string()
            )
        );
        assert_eq!(
            type_rows.next().unwrap()?,
            ("ID".to_string(), "INTEGER".to_string())
        );
        assert_eq!(
            type_rows.next().unwrap()?,
            ("NAME".to_string(), "VARCHAR".to_string())
        );
        assert_eq!(
            type_rows.next().unwrap()?,
            ("SIMPLE_DATE".to_string(), "DATE".to_string())
        );

        let mut stmt = conn.prepare(&format!(
            "SELECT ID, EVENT_TIME_NEM, EXPLICIT_TS_NEM, SIMPLE_DATE FROM \"{}\" ORDER BY ID",
            duckdb_table_name
        ))?;
        let mut query_rows = stmt.query([])?;

        // Row 1:
        // EVENT_TIME_NEM: "2023/01/15 10:30:00" (+10:00) is 2023-01-15 00:30:00 UTC
        // EXPLICIT_TS_NEM: "2024-03-01 23:00:00" (+10:00) is 2024-03-01 13:00:00 UTC
        let r1 = query_rows.next()?.unwrap();
        assert_eq!(r1.get::<_, i32>(0)?, 1);
        let event_time_r1_dt: DateTime<Utc> = r1.get(1)?;
        let expected_dt1_utc = Utc.with_ymd_and_hms(2023, 1, 15, 0, 30, 0).unwrap();
        assert_eq!(event_time_r1_dt, expected_dt1_utc);

        let explicit_ts_r1_dt: DateTime<Utc> = r1.get(2)?;
        let expected_explicit_dt1_utc = Utc.with_ymd_and_hms(2024, 3, 1, 13, 0, 0).unwrap();
        assert_eq!(explicit_ts_r1_dt, expected_explicit_dt1_utc);

        let simple_date_r1: chrono::NaiveDate = r1.get(3)?;
        assert_eq!(
            simple_date_r1,
            chrono::NaiveDate::from_ymd_opt(2023, 1, 15).unwrap()
        );

        // Row 2:
        // EVENT_TIME_NEM: "2023/02/20 00:30:00" (+10:00) is 2023-02-19 14:30:00 UTC
        // EXPLICIT_TS_NEM: "2024-04-10 12:00:00" (+10:00) is 2024-04-10 02:00:00 UTC
        let r2 = query_rows.next()?.unwrap();
        assert_eq!(r2.get::<_, i32>(0)?, 2);
        let event_time_r2_dt: DateTime<Utc> = r2.get(1)?;
        let expected_dt2_utc = Utc.with_ymd_and_hms(2023, 2, 19, 14, 30, 0).unwrap();
        assert_eq!(event_time_r2_dt, expected_dt2_utc);

        let explicit_ts_r2_dt: DateTime<Utc> = r2.get(2)?;
        let expected_explicit_dt2_utc = Utc.with_ymd_and_hms(2024, 4, 10, 2, 0, 0).unwrap();
        assert_eq!(explicit_ts_r2_dt, expected_explicit_dt2_utc);

        // Row 3 (NULL timestamps)
        let r3 = query_rows.next()?.unwrap();
        assert_eq!(r3.get::<_, i32>(0)?, 3);
        // For nullable chrono types, get Option<DateTime<Utc>>
        let event_time_r3_opt: Option<DateTime<Utc>> = r3.get(1)?;
        assert!(
            event_time_r3_opt.is_none(),
            "Expected EVENT_TIME_NEM to be NULL for row 3"
        );

        let explicit_ts_r3_opt: Option<DateTime<Utc>> = r3.get(2)?;
        assert!(
            explicit_ts_r3_opt.is_none(),
            "Expected EXPLICIT_TS_NEM to be NULL for row 3"
        );

        assert!(query_rows.next()?.is_none(), "Expected no more rows");

        Ok(())
    }

    #[test]
    #[ignore] // This test can take a while, mark as ignored
    fn test_insert_1m_rows_performance() -> Result<()> {
        init_test_logging();
        let conn = open_mem_db().context("Failed to open in-memory DB for performance test")?;
        conn.execute("SET TimeZone='UTC';", [])?;

        let table_base_name = "PERF_TEST_DATA_TABLE";
        let schema_hash = "perfhash1Mtz";
        let duckdb_table_name = format!("{}_{}", table_base_name, schema_hash);

        let schema_cols = vec![
            NemSchemaColumn {
                name: "ID".to_string(),
                ty: "INTEGER".to_string(),
                format: None,
            },
            NemSchemaColumn {
                name: "NAME".to_string(),
                ty: "VARCHAR(50)".to_string(),
                format: None,
            },
            NemSchemaColumn {
                name: "VALUE".to_string(),
                ty: "FLOAT".to_string(),
                format: None,
            },
            NemSchemaColumn {
                name: "EVENT_TIME".to_string(),
                ty: "DATE".to_string(),
                format: Some("YYYY/MM/DD HH24:MI:SS".to_string()), // Will become TIMESTAMPTZ
            },
            NemSchemaColumn {
                name: "IS_ACTIVE".to_string(),
                ty: "BOOLEAN".to_string(),
                format: None,
            },
        ];

        let test_schema_evolution = SchemaEvolution {
            table_name: table_base_name.to_string(),
            fields_hash: schema_hash.to_string(),
            start_month: "202401".to_string(),
            end_month: "202412".to_string(),
            columns: schema_cols.clone(),
        };

        create_table_from_schema(&conn, &duckdb_table_name, &test_schema_evolution.columns)
            .context("Failed to create table for performance test")?;

        tracing::info!("Starting data generation for 1 million rows...");
        let num_rows_to_generate: usize = 100_000; // Reduced for faster CI/testing; was 1_000_000
        let mut rows_data: Vec<Vec<String>> = Vec::with_capacity(num_rows_to_generate);
        let mut rng = thread_rng();

        for i in 0..num_rows_to_generate {
            let id = (i + 1).to_string();
            let name_len = rng.gen_range(5..=15);
            let name: String = (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(name_len)
                .map(char::from)
                .collect();
            let value = rng.gen_range(0.0..10000.0).to_string();
            let year = rng.gen_range(2020..=2024);
            let month = rng.gen_range(1..=12);
            let day = rng.gen_range(1..=28); // Keep day simple
            let hour = rng.gen_range(0..=23);
            let minute = rng.gen_range(0..=59);
            let second = rng.gen_range(0..=59);
            let event_time = format!(
                "{:04}/{:02}/{:02} {:02}:{:02}:{:02}",
                year, month, day, hour, minute, second
            );
            let is_active = if rng.gen() { "true" } else { "false" }.to_string();
            rows_data.push(vec![id, name, value, event_time, is_active]);
        }
        tracing::info!(
            "Data generation complete. Generated {} rows.",
            rows_data.len()
        );

        let raw_table_to_insert = RawTable {
            headers: vec![
                "ID".to_string(),
                "NAME".to_string(),
                "VALUE".to_string(),
                "EVENT_TIME".to_string(),
                "IS_ACTIVE".to_string(),
            ],
            rows: rows_data,
            effective_month: "202401".to_string(),
        };

        tracing::info!(
            "Starting insertion of {} rows into table '{}'...",
            num_rows_to_generate,
            duckdb_table_name
        );
        let start_time = Instant::now();

        let inserted_count = insert_raw_table_data(
            &conn,
            &duckdb_table_name,
            &raw_table_to_insert,
            &test_schema_evolution,
        )?;

        let duration = start_time.elapsed();
        let rows_per_second = if duration.as_secs_f64() > 0.0 {
            inserted_count as f64 / duration.as_secs_f64()
        } else {
            f64::INFINITY
        };

        tracing::info!(
            "Insertion complete. Inserted {} rows into '{}' in {:.2?}. Average: {:.2} rows/sec",
            inserted_count,
            duckdb_table_name,
            duration,
            rows_per_second
        );

        assert_eq!(
            inserted_count, num_rows_to_generate,
            "Expected all {} generated rows to be inserted, but {} were inserted.",
            num_rows_to_generate, inserted_count
        );

        let count: i64 = conn
            .query_row(
                &format!("SELECT COUNT(*) FROM \"{}\"", duckdb_table_name),
                [],
                |row| row.get(0),
            )
            .context("Failed to query count from performance test table")?;

        assert_eq!(
            count, num_rows_to_generate as i64,
            "Database row count mismatch after performance test."
        );
        Ok(())
    }
}
