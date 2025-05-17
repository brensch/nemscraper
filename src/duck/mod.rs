// src/duck/mod.rs
// Add to Cargo.toml:
// duckdb = { version = "1.2.2", default-features = false, features = ["bundled"] }
// rand = "0.8"
// anyhow = "1.0"

use anyhow::Result;
use duckdb::{Connection, ToSql};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::time::Instant;

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

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    /// Benchmark bulk insertion into in-memory DB
    #[test]
    fn bench_in_memory() -> Result<()> {
        let n = 1_000_000;
        let data = generate_test_data(n);

        let conn = open_mem_db()?;
        setup_test_table(&conn)?;
        let secs = insert_batch_rows(&conn, &data)?;
        println!("In-memory insertion of {} rows took {:.3}s", n, secs);

        // Verify row count
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM test_data;", [], |r| r.get(0))?;
        assert_eq!(count, n as i64);
        Ok(())
    }
}
