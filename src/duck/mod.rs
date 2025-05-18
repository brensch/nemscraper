// src/duck/mod.rs

use anyhow::{anyhow, Context, Result};
use duckdb::{types::Value, Appender, Connection, ToSql}; // params_from_iter is not needed with this approach
use std::time::Instant;
use tracing;

use crate::process::RawTable;
use crate::schema::Column as NemSchemaColumn;

// --- Functions for schema-based table creation ---

/// Maps a schema column type (and optional format) to a DuckDB SQL type.
pub fn map_schema_type_to_duckdb_type(
    schema_col_type: &str,
    schema_col_format: Option<&str>,
) -> String {
    let u_type = schema_col_type.to_uppercase();
    match u_type.as_str() {
        s if s.starts_with("DATE") => {
            if let Some(fmt) = schema_col_format {
                if fmt.contains("hh24:mi:ss")
                    || fmt.contains("HH24:MI:SS")
                    || fmt.contains("hh:mi:ss")
                    || fmt.contains("HH:MI:SS")
                {
                    "TIMESTAMP".to_string()
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

    tracing::debug!(
        "Table '{}' created successfully or already exists.",
        table_name
    );
    Ok(())
}

// --- Function for inserting RawTable data ---

/// Parses a string value into a DuckDB `Value` based on the target schema column type.
/// Handles empty strings as NULL.
fn parse_string_to_duckdb_value(value_str: &str, target_column: &NemSchemaColumn) -> Result<Value> {
    let trimmed_value = value_str.trim();

    if trimmed_value.is_empty() {
        return Ok(Value::Null);
    }

    let schema_type_upper = target_column.ty.to_uppercase();

    match schema_type_upper.as_str() {
        s if s.starts_with("DATE") => Ok(Value::Text(trimmed_value.to_string())),
        s if s.starts_with("FLOAT") || s.starts_with("NUMBER") || s.starts_with("DECIMAL") => {
            trimmed_value
                .parse::<f64>()
                .map(Value::Double)
                .map_err(|e| {
                    anyhow!(
                        "Failed to parse '{}' as f64 for column '{}': {}",
                        trimmed_value,
                        target_column.name,
                        e
                    )
                })
        }
        s if s.starts_with("INT") => {
            trimmed_value.parse::<i32>().map(Value::Int).map_err(|e| {
                // Value::Integer for i32
                anyhow!(
                    "Failed to parse '{}' as i32 for column '{}': {}",
                    trimmed_value,
                    target_column.name,
                    e
                )
            })
        }
        s if s.starts_with("BIGINT") => {
            trimmed_value
                .parse::<i64>()
                .map(Value::BigInt)
                .map_err(|e| {
                    anyhow!(
                        "Failed to parse '{}' as i64 for column '{}': {}",
                        trimmed_value,
                        target_column.name,
                        e
                    )
                })
        }
        s if s.starts_with("SMALLINT") => trimmed_value
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
        s if s.starts_with("TINYINT") => {
            trimmed_value
                .parse::<i8>()
                .map(Value::TinyInt)
                .map_err(|e| {
                    anyhow!(
                        "Failed to parse '{}' as i8 for column '{}': {}",
                        trimmed_value,
                        target_column.name,
                        e
                    )
                })
        }
        s if s.starts_with("BOOL") => match trimmed_value.to_uppercase().as_str() {
            "TRUE" | "T" | "1" | "YES" | "Y" => Ok(Value::Boolean(true)),
            "FALSE" | "F" | "0" | "NO" | "N" => Ok(Value::Boolean(false)),
            _ => Err(anyhow!(
                "Failed to parse '{}' as boolean for column '{}'",
                trimmed_value,
                target_column.name
            )),
        },
        _ => Ok(Value::Text(trimmed_value.to_string())),
    }
}

/// Inserts data from a `RawTable` into the specified DuckDB table,
/// using the provided `SchemaEvolution` for column definitions and type conversions.
#[tracing::instrument(level = "debug", skip(conn, raw_table_data, schema_definition), fields(target_table = %duckdb_table_name, num_raw_rows = raw_table_data.rows.len()))]
pub fn insert_raw_table_data(
    conn: &Connection,
    duckdb_table_name: &str,
    raw_table_data: &RawTable,
    schema_definition: &crate::schema::SchemaEvolution,
) -> Result<usize> {
    if raw_table_data.rows.is_empty() {
        tracing::debug!("No rows to insert into table '{}'.", duckdb_table_name);
        return Ok(0);
    }

    let expected_col_count = schema_definition.columns.len();
    if expected_col_count == 0 {
        return Err(anyhow!(
            "Schema definition for table '{}' has no columns.",
            duckdb_table_name
        ));
    }

    tracing::info!(
        num_rows = raw_table_data.rows.len(),
        table = duckdb_table_name,
        "Preparing to insert data into DuckDB table."
    );

    let mut appender: Appender = conn.appender(duckdb_table_name).with_context(|| {
        format!(
            "Failed to create appender for table '{}'",
            duckdb_table_name
        )
    })?;

    let mut successfully_appended_rows = 0;
    for (row_idx, string_row) in raw_table_data.rows.iter().enumerate() {
        if string_row.len() != expected_col_count {
            tracing::warn!(
                row_num = row_idx + 1,
                expected_cols = expected_col_count,
                actual_cols = string_row.len(),
                table = duckdb_table_name,
                "Skipping row due to column count mismatch. First few values: {:?}",
                string_row.iter().take(3).collect::<Vec<_>>()
            );
            continue;
        }

        let mut duckdb_row_values: Vec<Value> = Vec::with_capacity(expected_col_count);
        let mut conversion_failed = false;
        for (col_idx, cell_value_str) in string_row.iter().enumerate() {
            let target_column = &schema_definition.columns[col_idx];
            match parse_string_to_duckdb_value(cell_value_str, target_column) {
                Ok(duck_value) => {
                    duckdb_row_values.push(duck_value);
                }
                Err(e) => {
                    tracing::warn!(
                        row_num = row_idx + 1,
                        col_num = col_idx + 1,
                        col_name = %target_column.name,
                        raw_value = %cell_value_str,
                        error = %e,
                        table = duckdb_table_name,
                        "Failed to parse value. Skipping row."
                    );
                    conversion_failed = true;
                    break;
                }
            }
        }

        if !conversion_failed {
            // Collect references to the Value objects for the current row
            let params_for_row: Vec<&dyn ToSql> =
                duckdb_row_values.iter().map(|v| v as &dyn ToSql).collect();

            // Pass a slice of these references to append_row
            if let Err(e) = appender.append_row(&params_for_row[..]) {
                tracing::warn!(
                    row_num = row_idx + 1,
                    error = %e,
                    table = duckdb_table_name,
                    "Failed to append row. Skipping row."
                );
            } else {
                successfully_appended_rows += 1;
            }
        }
    }

    appender
        .flush()
        .with_context(|| format!("Failed to flush appender for table '{}'", duckdb_table_name))?;
    tracing::info!(
        inserted_rows = successfully_appended_rows,
        total_raw_rows = raw_table_data.rows.len(),
        table = duckdb_table_name,
        "Finished inserting data into DuckDB table."
    );
    Ok(successfully_appended_rows)
}

/// Open an in-memory DuckDB instance
pub fn open_mem_db() -> Result<Connection> {
    Ok(Connection::open_in_memory()?)
}

/// Open or create a file-based DuckDB instance
pub fn open_file_db(path: &str) -> Result<Connection> {
    Connection::open(path).context(format!("Failed to open DuckDB file at {}", path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaEvolution;
    use anyhow::Result;
    use tracing_subscriber::{EnvFilter, FmtSubscriber};

    fn init_test_logging() {
        let subscriber = FmtSubscriber::builder()
            .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                EnvFilter::new("info,nemscraper::duck=trace,nemscraper::process=trace")
            }))
            .with_test_writer()
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
    }

    #[test]
    fn test_map_schema_types() {
        init_test_logging();
        assert_eq!(
            map_schema_type_to_duckdb_type("DATE", Some("\"yyyy/mm/dd hh24:mi:ss\"")),
            "TIMESTAMP"
        );
        // ... (rest of map_schema_types tests remain the same)
        assert_eq!(map_schema_type_to_duckdb_type("INTEGER", None), "INTEGER");
        assert_eq!(map_schema_type_to_duckdb_type("INT", None), "INTEGER");
        assert_eq!(
            map_schema_type_to_duckdb_type("UNKNOWN_TYPE", None),
            "VARCHAR"
        );
    }

    #[test]
    fn test_create_and_insert_raw_data() -> Result<()> {
        init_test_logging();
        let conn = open_mem_db().context("Failed to open in-memory DB for test")?;

        let table_base_name = "TEST_DATA_TABLE";
        let schema_hash = "testhash123";
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
                format: Some("YYYY/MM/DD HH24:MI:SS".to_string()),
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
                "VALUE".to_string(),
                "EVENT_TIME".to_string(),
                "IS_ACTIVE".to_string(),
            ],
            rows: vec![
                vec![
                    "1".to_string(),
                    "Alice".to_string(),
                    "123.45".to_string(),
                    "2023/01/15 10:30:00".to_string(),
                    "true".to_string(),
                ],
                vec![
                    "2".to_string(),
                    "Bob".to_string(),
                    "67.89".to_string(),
                    "2023/02/20 12:00:00".to_string(),
                    "0".to_string(),
                ],
                vec![
                    "3".to_string(),
                    "Charlie".to_string(),
                    "".to_string(),
                    "2023/03/10 08:00:00".to_string(),
                    "F".to_string(),
                ],
                vec![
                    "4".to_string(),
                    "David".to_string(),
                    "99.0".to_string(),
                    "  ".to_string(),
                    "YES".to_string(),
                ],
                vec![
                    "5".to_string(),
                    "Eve".to_string(),
                    "INVALID_FLOAT".to_string(),
                    "2023/05/05 05:05:05".to_string(),
                    "N".to_string(),
                ],
                vec![
                    "6".to_string(),
                    "Valid".to_string(),
                    "1.0".to_string(),
                    "2023/06/01 00:00:00".to_string(),
                    "T".to_string(),
                    "EXTRA_COL".to_string(),
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
            inserted_count, 4,
            "Expected 4 rows to be successfully inserted"
        );

        let mut stmt = conn.prepare(&format!(
            "SELECT ID, NAME, VALUE, EVENT_TIME, IS_ACTIVE FROM \"{}\" ORDER BY ID",
            duckdb_table_name
        ))?;
        let mut query_rows = stmt.query([])?;

        let r = query_rows.next()?.unwrap();
        assert_eq!(r.get::<_, i32>(0)?, 1);
        assert_eq!(r.get::<_, String>(1)?, "Alice");
        assert_eq!(r.get::<_, f64>(2)?, 123.45);
        assert_eq!(r.get::<_, bool>(4)?, true);

        let r = query_rows.next()?.unwrap();
        assert_eq!(r.get::<_, i32>(0)?, 2);
        assert_eq!(r.get::<_, String>(1)?, "Bob");
        assert_eq!(r.get::<_, f64>(2)?, 67.89);
        assert_eq!(r.get::<_, bool>(4)?, false);

        let r = query_rows.next()?.unwrap();
        assert_eq!(r.get::<_, i32>(0)?, 3);
        assert_eq!(r.get::<_, String>(1)?, "Charlie");
        let value_col3: Option<f64> = r.get(2)?;
        assert!(value_col3.is_none());
        assert_eq!(r.get::<_, bool>(4)?, false);

        let r = query_rows.next()?.unwrap();
        assert_eq!(r.get::<_, i32>(0)?, 4);
        assert_eq!(r.get::<_, String>(1)?, "David");
        assert_eq!(r.get::<_, f64>(2)?, 99.0);
        assert_eq!(r.get::<_, bool>(4)?, true);

        assert!(query_rows.next()?.is_none());

        Ok(())
    }
}
