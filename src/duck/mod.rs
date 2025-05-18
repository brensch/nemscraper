// src/duck/mod.rs
// Add to Cargo.toml:
// duckdb = { version = "1.2.2", default-features = false, features = ["bundled"] } // Or your current version
// rand = "0.8"
// anyhow = "1.0"
// nemscraper = { path = ".." } // Or however your crate is referenced if schema is in lib

use anyhow::{Context, Result};
use duckdb::{Connection, ToSql};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::time::Instant;

// Assuming your schema module is part of the 'nemscraper' crate (library)
// If schema_stats.rs uses `nemscraper::schema::...`, then your library is `nemscraper`.
// Make sure `nemscraper/src/lib.rs` has `pub mod schema;`
// and `nemscraper/src/schema.rs` has `pub struct Column;` etc.
use crate::schema::Column;

/// A row with various DuckDB-compatible types
#[derive(Debug)]
pub struct TestRow {
    pub id: i32,
    pub value: f64,
    pub flag: bool,
    pub name: String,
}

/// Generate `n` random `TestRow`s, reserving capacity up front for speed
pub fn generate_test_data(n: usize) -> Vec<TestRow> {
    let mut rng = StdRng::seed_from_u64(0x_d00d_f00d);
    let mut rows = Vec::with_capacity(n);
    for i in 0..n {
        rows.push(TestRow {
            id: i as i32,
            value: rng.gen_range(0.0..1_000_000.0),
            flag: rng.gen_bool(0.5),
            name: format!("name_{}", rng.gen_range(0..1_000_000)),
        });
    }
    rows
}

/// Open an in-memory DuckDB instance
pub fn open_mem_db() -> Result<Connection> {
    Ok(Connection::open_in_memory()?)
}

/// Open or create a file-based DuckDB instance
pub fn open_file_db(path: &str) -> Result<Connection> {
    Connection::open(path).context(format!("Failed to open DuckDB file at {}", path))
}

/// Ensure the `test_data` table exists
pub fn setup_test_table(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS test_data(
            id INTEGER,
            value DOUBLE,
            flag BOOLEAN,
            name VARCHAR
        );",
        [],
    )?;
    Ok(())
}

/// Bulk-insert rows via `Appender::append_rows`, returns elapsed seconds
pub fn insert_batch_rows(conn: &Connection, data: &[TestRow]) -> Result<f64> {
    let mut appender = conn.appender("test_data")?;
    let start = Instant::now();

    // Each element is an array of &dyn ToSql matching table columns
    appender.append_rows(
        data.iter()
            .map(|row| [&row.id as &dyn ToSql, &row.value, &row.flag, &row.name]),
    )?;

    appender.flush()?;
    Ok(start.elapsed().as_secs_f64())
}

// --- New functions for schema-based table creation ---

/// Maps a schema column type (and optional format) to a DuckDB SQL type.
///
/// # Arguments
/// * `schema_col_type` - The type string from `nemscraper::schema::Column.ty` (e.g., "DATE", "FLOAT", "CHAR(10)").
/// * `schema_col_format` - The optional format string from `nemscraper::schema::Column.format`.
///
/// # Returns
/// A string representing the corresponding DuckDB SQL type.
pub fn map_schema_type_to_duckdb_type(
    schema_col_type: &str,
    schema_col_format: Option<&str>,
) -> String {
    let u_type = schema_col_type.to_uppercase();
    match u_type.as_str() {
        s if s.starts_with("DATE") => {
            if let Some(fmt) = schema_col_format {
                if fmt.contains("hh24:mi:ss") || fmt.contains("HH24:MI:SS") {
                    "TIMESTAMP".to_string()
                } else {
                    "DATE".to_string()
                }
            } else {
                "DATE".to_string()
            }
        }
        s if s.starts_with("FLOAT") || s.starts_with("NUMBER") || s.starts_with("DECIMAL") => {
            // For simplicity, mapping to DOUBLE. For more precision, DECIMAL(p,s)
            // would require parsing p,s from the type string if available.
            "DOUBLE".to_string()
        }
        s if s.starts_with("INT") => "INTEGER".to_string(), // Handles "INTEGER", "INT4", etc.
        s if s.starts_with("BIGINT") => "BIGINT".to_string(),
        s if s.starts_with("SMALLINT") => "SMALLINT".to_string(),
        s if s.starts_with("TINYINT") => "TINYINT".to_string(),
        s if s.starts_with("CHAR")
            || s.starts_with("VARCHAR")
            || s.starts_with("TEXT")
            || s.starts_with("STRING") =>
        {
            "VARCHAR".to_string() // DuckDB's VARCHAR is flexible
        }
        s if s.starts_with("BOOL") => "BOOLEAN".to_string(),
        s if s.starts_with("BLOB") || s.starts_with("BYTEA") => "BLOB".to_string(),
        // Add more specific mappings as needed based on your schema's type system
        _ => {
            // Default to VARCHAR for unknown types, with a warning or log
            eprintln!(
                "Warning: Unknown schema type '{}'. Defaulting to VARCHAR.",
                schema_col_type
            );
            "VARCHAR".to_string()
        }
    }
}

/// Creates a table in DuckDB based on the provided schema definition.
///
/// # Arguments
/// * `conn` - A reference to the DuckDB `Connection`.
/// * `table_name` - The name of the table to create.
/// * `columns` - A slice of `nemscraper::schema::Column` defining the table structure.
///
/// # Returns
/// `Ok(())` if the table is created successfully, or an `anyhow::Error` on failure.
pub fn create_table_from_schema(
    conn: &Connection,
    table_name: &str,
    columns: &[Column],
) -> Result<()> {
    if columns.is_empty() {
        return Err(anyhow::anyhow!(
            "Cannot create table '{}': no columns defined.",
            table_name
        ));
    }

    let mut create_table_sql = format!("CREATE TABLE IF NOT EXISTS \"{}\" (\n", table_name); // Quote table name

    for (i, col) in columns.iter().enumerate() {
        let duckdb_type = map_schema_type_to_duckdb_type(&col.ty, col.format.as_deref());
        // Quote column names to handle spaces, keywords, or special characters
        create_table_sql.push_str(&format!("    \"{}\" {}", col.name, duckdb_type));
        if i < columns.len() - 1 {
            create_table_sql.push_str(",\n");
        } else {
            create_table_sql.push_str("\n");
        }
    }
    create_table_sql.push_str(");");

    tracing::debug!("Executing SQL for table creation: \n{}", create_table_sql); // Use tracing for logging

    conn.execute(&create_table_sql, []).with_context(|| {
        format!(
            "Failed to create table '{}' with SQL: \n{}",
            table_name, create_table_sql
        )
    })?;

    tracing::info!(
        "Table '{}' created successfully or already exists.",
        table_name
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Column;
    use anyhow::Result;
    use duckdb::ToSql; // Already imported
    use std::time::Instant; // Already imported // Import Column for test

    /// Helper to initialize tracing for tests, ensuring it's only done once.
    fn init_test_logging() {
        use std::sync::Once;
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let filter = tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,duck=trace")); // trace for duck module
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_test_writer() // Directs output to test harness
                .try_init()
                .ok(); // Use try_init to avoid panic if already initialized
        });
    }

    /// Benchmark bulk insertion into in-memory DB using our helper
    #[test]
    fn bench_in_memory() -> Result<()> {
        init_test_logging();
        let n = 1_000_000;
        let data = generate_test_data(n);

        let conn = open_mem_db()?;
        setup_test_table(&conn)?;
        let secs = insert_batch_rows(&conn, &data)?;
        tracing::info!("In-memory insertion of {} rows took {:.3}s", n, secs);

        // Verify row count
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM test_data;", [], |r| r.get(0))?;
        assert_eq!(count, n as i64);
        Ok(())
    }

    /// Same benchmark, but inlines the &dyn ToSql dispatch
    #[test]
    fn bench_in_memory_dyn() -> Result<()> {
        init_test_logging();
        let n = 1_000_000; // Reduced for faster CI, was 10_000_000
        let data = generate_test_data(n);

        let conn = open_mem_db()?;
        setup_test_table(&conn)?;

        let mut appender = conn.appender("test_data")?;
        let start = Instant::now();

        // Inline dynamic dispatch of each row
        appender.append_rows(
            data.iter()
                .map(|row| [&row.id as &dyn ToSql, &row.value, &row.flag, &row.name]),
        )?;
        appender.flush()?;

        let secs = start.elapsed().as_secs_f64();
        tracing::info!("Inline dynamic insertion of {} rows took {:.3}s", n, secs);

        // Verify row count
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM test_data;", [], |r| r.get(0))?;
        assert_eq!(count, n as i64);
        Ok(())
    }

    #[test]
    fn test_map_schema_types() {
        init_test_logging();
        assert_eq!(
            map_schema_type_to_duckdb_type("DATE", Some("\"yyyy/mm/dd hh24:mi:ss\"")),
            "TIMESTAMP"
        );
        assert_eq!(
            map_schema_type_to_duckdb_type("DATE", Some("YYYY/MM/DD HH24:MI:SS")),
            "TIMESTAMP"
        );
        assert_eq!(map_schema_type_to_duckdb_type("date", None), "DATE");
        assert_eq!(
            map_schema_type_to_duckdb_type("Date", Some("YYYY-MM-DD")),
            "DATE"
        );
        assert_eq!(
            map_schema_type_to_duckdb_type("FLOAT", Some("EXTERNAL")),
            "DOUBLE"
        );
        assert_eq!(map_schema_type_to_duckdb_type("NUMBER", None), "DOUBLE");
        assert_eq!(map_schema_type_to_duckdb_type("INTEGER", None), "INTEGER");
        assert_eq!(map_schema_type_to_duckdb_type("INT", None), "INTEGER");
        assert_eq!(map_schema_type_to_duckdb_type("CHAR(10)", None), "VARCHAR");
        assert_eq!(
            map_schema_type_to_duckdb_type("VARCHAR(255)", None),
            "VARCHAR"
        );
        assert_eq!(map_schema_type_to_duckdb_type("TEXT", None), "VARCHAR");
        assert_eq!(map_schema_type_to_duckdb_type("BOOLEAN", None), "BOOLEAN");
        assert_eq!(map_schema_type_to_duckdb_type("BLOB", None), "BLOB");
        assert_eq!(
            map_schema_type_to_duckdb_type("UNKNOWN_TYPE", None),
            "VARCHAR"
        ); // Test default
    }

    #[test]
    fn test_create_table_from_nem_schema() -> Result<()> {
        init_test_logging();
        let conn = open_mem_db().context("Failed to open in-memory DB for test")?;

        let table_name = "MY_TEST_TABLE";
        let columns = vec![
            Column {
                name: "EFFECTIVEDATE".to_string(),
                ty: "DATE".to_string(),
                format: Some("\"yyyy/mm/dd hh24:mi:ss\"".to_string()),
            },
            Column {
                name: "VERSION NO".to_string(), // Column name with space
                ty: "FLOAT".to_string(),
                format: Some("EXTERNAL".to_string()),
            },
            Column {
                name: "SERVICE".to_string(),
                ty: "CHAR(10)".to_string(),
                format: None,
            },
            Column {
                name: "Amount".to_string(), // Mixed case
                ty: "NUMBER".to_string(),
                format: None,
            },
            Column {
                name: "IS_VALID".to_string(),
                ty: "BOOLEAN".to_string(),
                format: None,
            },
        ];

        create_table_from_schema(&conn, table_name, &columns)
            .context("Failed to create table from schema")?;

        // Verify table creation by querying its structure from information_schema
        let mut stmt = conn.prepare(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position;",
        )?;
        let mut rows = stmt.query([table_name])?;

        let row1 = rows.next()?.unwrap();
        assert_eq!(row1.get::<_, String>(0)?, "EFFECTIVEDATE");
        assert_eq!(row1.get::<_, String>(1)?, "TIMESTAMP"); // Mapped from DATE + time format

        let row2 = rows.next()?.unwrap();
        assert_eq!(row2.get::<_, String>(0)?, "VERSION NO"); // Check quoted name
        assert_eq!(row2.get::<_, String>(1)?, "DOUBLE");

        let row3 = rows.next()?.unwrap();
        assert_eq!(row3.get::<_, String>(0)?, "SERVICE");
        assert_eq!(row3.get::<_, String>(1)?, "VARCHAR");

        let row4 = rows.next()?.unwrap();
        assert_eq!(row4.get::<_, String>(0)?, "Amount");
        assert_eq!(row4.get::<_, String>(1)?, "DOUBLE");

        let row5 = rows.next()?.unwrap();
        assert_eq!(row5.get::<_, String>(0)?, "IS_VALID");
        assert_eq!(row5.get::<_, String>(1)?, "BOOLEAN");

        // Optionally, try to insert some data (though types must match exactly)
        // For this test, verifying schema is sufficient.
        // Example: conn.execute("INSERT INTO MY_TEST_TABLE VALUES (now(), 1.0, 'test', 123.45, true)", [])?;
        // let count: i64 = conn.query_row("SELECT COUNT(*) FROM MY_TEST_TABLE;", [], |r| r.get(0))?;
        // assert_eq!(count, 1);

        Ok(())
    }

    #[test]
    fn test_create_table_empty_columns() -> Result<()> {
        init_test_logging();
        let conn = open_mem_db()?;
        let table_name = "EMPTY_TABLE";
        let columns: Vec<Column> = vec![];

        let result = create_table_from_schema(&conn, table_name, &columns);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("no columns defined"));
        }
        Ok(())
    }
}
