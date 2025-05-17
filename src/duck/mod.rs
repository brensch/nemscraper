use anyhow::Result;
use duckdb::{Connection, ToSql};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::time::Instant;

/// Example struct with a variety of DuckDB‐compatible types
pub struct TestRow {
    pub id: i32,
    pub value: f64,
    pub flag: bool,
    pub name: String,
}

/// Generate `n` random TestRow instances using a fixed seed
pub fn generate_test_data(n: usize) -> Vec<TestRow> {
    let mut rng = StdRng::seed_from_u64(0x_d00d_f00d);
    (0..n)
        .map(|i| TestRow {
            id: i as i32,
            value: rng.gen(),
            flag: rng.gen_bool(0.5),
            name: format!("name_{}", rng.gen_range(0..1_000_000)),
        })
        .collect()
}

/// Open a DuckDB database on disk at `path`, creating the file if it doesn't exist.
pub fn open_disk_db(path: &str) -> Result<Connection> {
    let conn = Connection::open(path)?;
    Ok(conn)
}

/// Open a DuckDB in‐memory database
pub fn open_mem_db() -> Result<Connection> {
    let conn = Connection::open_in_memory()?;
    Ok(conn)
}

/// Prepare the `test_data` table in the given connection
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

/// Insert rows via Appender::append_rows in bulk, using arrays of &dyn ToSql
/// returning elapsed seconds
pub fn insert_batch_rows(conn: &Connection, data: &[TestRow]) -> Result<f64> {
    let mut appender = conn.appender("test_data")?;
    let start = Instant::now();

    appender.append_rows(data.iter().map(|row| {
        [
            &row.id as &dyn ToSql,
            &row.value as &dyn ToSql,
            &row.flag as &dyn ToSql,
            &row.name as &dyn ToSql,
        ]
    }))?;
    appender.flush()?;
    Ok(start.elapsed().as_secs_f64())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    /// Benchmarks bulk insertion into disk and memory databases, timing each.
    #[test]
    fn bench_disk_vs_memory() -> Result<()> {
        let n = 10_000_000;
        let data = generate_test_data(n);

        // Disk-backed DB
        let conn_disk = open_disk_db("test_data.duckdb")?;
        setup_test_table(&conn_disk)?;
        let t_disk = insert_batch_rows(&conn_disk, &data)?;
        println!("Disk DB insertion of {} rows took {:.3}s", n, t_disk);

        // Memory DB
        let conn_mem = open_mem_db()?;
        setup_test_table(&conn_mem)?;
        let t_mem = insert_batch_rows(&conn_mem, &data)?;
        println!("In-memory DB insertion of {} rows took {:.3}s", n, t_mem);

        // Sanity check row counts
        let c_disk: i64 =
            conn_disk.query_row("SELECT COUNT(*) FROM test_data;", [], |r| r.get(0))?;
        let c_mem: i64 = conn_mem.query_row("SELECT COUNT(*) FROM test_data;", [], |r| r.get(0))?;
        assert_eq!(c_disk, n as i64);
        assert_eq!(c_mem, n as i64);

        Ok(())
    }
}
