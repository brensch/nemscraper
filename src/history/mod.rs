use anyhow::{Context, Result};
use chrono::Utc;
use duckdb::{params, Connection};
use glob::glob;

use std::{
    collections::HashSet,
    fmt, fs,
    path::PathBuf,
    sync::{Arc, Mutex},
};

/// Events that can be recorded in history.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum Event {
    Downloaded,
    Processed,
    Retried,
}

impl Event {
    /// Returns the string representation used in filenames.
    pub fn as_str(&self) -> &'static str {
        match self {
            Event::Downloaded => "downloaded",
            Event::Processed => "processed",
            Event::Retried => "retried",
        }
    }
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A history manager backed by an in-memory DuckDB instance *and* Parquet files.
pub struct History {
    history_dir: PathBuf,
    conn: Arc<Mutex<Connection>>,
}

impl History {
    /// Construct a new History store at `history_dir`, creating the directory if needed,
    /// and loading any existing `.parquet` history files into the in-memory DB.
    pub fn new(history_dir: impl Into<PathBuf>) -> Result<Self> {
        let history_dir = history_dir.into();
        fs::create_dir_all(&history_dir)?;

        let conn = Arc::new(Mutex::new(Connection::open_in_memory()?));
        {
            let db = conn.lock().unwrap();

            // 1) Create the in-memory table
            db.execute(
                r#"
            CREATE TABLE IF NOT EXISTS history (
              zip_name    VARCHAR,
              event       VARCHAR,
              event_time  TIMESTAMP,
              file_name   VARCHAR
            )
            "#,
                params![],
            )?;

            // 2) Only if there are any .parquet files, bulk-load them
            let glob_pattern = format!("{}/{}.parquet", history_dir.display(), "*");
            let mut any = false;
            for entry in glob(&glob_pattern)? {
                if entry.is_ok() {
                    any = true;
                    break;
                }
            }

            if any {
                let safe_pattern = glob_pattern.replace('\'', "''");
                let load_sql = format!(
                    r#"
                INSERT INTO history
                SELECT zip_name::VARCHAR,
                       event::VARCHAR,
                       event_time::TIMESTAMP,
                       filename::VARCHAR AS file_name
                FROM read_parquet('{pattern}', filename=true)
                "#,
                    pattern = safe_pattern
                );
                db.execute(&load_sql, params![])?;
            }
        }

        Ok(History { history_dir, conn })
    }

    /// Record an event for `zip_name` in one DB transaction and parquet write.
    pub fn record_event(&self, zip_name: &str, event: Event) -> Result<()> {
        // Grab the lock so nobody else races us on the DB or the FS.
        let db = self.conn.lock().unwrap();

        // Timestamp and filenames
        // Use a chrono DateTime and its ISO8601 representation for DuckDB
        let now = Utc::now();
        let ts = now.timestamp_micros(); // still use for filename
        let ts_iso = now.to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
        let fname = format!("{}---{}---{}.parquet", zip_name, event.as_str(), ts);
        let final_path = self.history_dir.join(&fname);
        let tmp_path = final_path.with_extension("parquet.tmp");

        // Begin a single transaction
        db.execute("BEGIN", params![])?;

        // 1) Insert into the in-memory history table
        db.execute(
            "INSERT INTO history (zip_name, event, event_time, file_name) VALUES (?, ?, ?, ?)",
            params![zip_name, event.as_str(), ts_iso, &fname],
        )
        .context("inserting new history row into DuckDB")?;

        // 2) Emit exactly that row out to Parquet via DuckDB’s COPY command
        //
        //    COPY (SELECT zip_name, event, event_time
        //          FROM history
        //          WHERE file_name = '<fname>')
        //    TO '<tmp_path>' (FORMAT PARQUET, COMPRESSION SNAPPY)
        //
        let copy_sql = format!(
            "COPY (SELECT zip_name, event, event_time \
                  FROM history WHERE file_name = '{fname}') \
             TO '{path}' (FORMAT PARQUET, COMPRESSION SNAPPY)",
            fname = fname.replace('\'', "''"),
            path = tmp_path.to_string_lossy().replace('\'', "''"),
        );
        db.execute(&copy_sql, params![])
            .context("writing parquet via DuckDB COPY")?;

        // Commit the DB side
        db.execute("COMMIT", params![])?;

        // 3) Finally, atomically rename the .tmp → .parquet
        fs::rename(&tmp_path, &final_path)
            .with_context(|| format!("renaming {:?} to {:?}", tmp_path, final_path))?;

        Ok(())
    }

    /// Get the latest history file path for a given `zip_name` and `event`.
    pub fn get_one(&self, zip_name: &str, event: Event) -> Result<Option<PathBuf>> {
        let db = self.conn.lock().unwrap();
        let mut stmt = db
            .prepare(
                "SELECT file_name FROM history
                 WHERE zip_name = ? AND event = ?
                 ORDER BY event_time DESC LIMIT 1",
            )
            .context("preparing get_latest_event_file query")?;
        let mut rows = stmt
            .query_map(params![&zip_name, &event.as_str()], |row| row.get(0))
            .context("executing get_latest_event_file query")?;
        if let Some(res) = rows.next() {
            let fname: String = res.context("reading file_name from row")?;
            Ok(Some(self.history_dir.join(fname)))
        } else {
            Ok(None)
        }
    }

    /// Return all csv files for a given `event`.
    pub fn get_all(&self, event: Event) -> Result<HashSet<String>> {
        let db = self.conn.lock().unwrap();
        let mut stmt = db
            .prepare(
                "SELECT zip_name
                 FROM history
                 WHERE event = ?
                 ORDER BY event_time DESC",
            )
            .context("preparing get_all query")?;
        let rows = stmt
            .query_map(params![event.as_str()], |row| row.get::<_, String>(0))
            .context("executing get_all query")?;

        let mut set = HashSet::new();
        for row in rows {
            set.insert(row.context("reading zip_name from row")?);
        }
        Ok(set)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        thread::sleep,
        time::{Duration, Instant},
    };
    use tempfile::TempDir;

    #[test]
    fn record_and_get_one() -> Result<()> {
        // — setup a clean history directory —
        let tmp = TempDir::new().unwrap();
        let hist_dir = tmp.path().join("history_store");
        let history = History::new(&hist_dir)?;

        // — record three events: zip1, zip2, then zip1 again —
        let zip1 = "archive1.zip";
        let zip2 = "archive2.zip";
        history.record_event(zip1, Event::Processed)?;
        sleep(Duration::from_millis(1));
        history.record_event(zip2, Event::Processed)?;
        sleep(Duration::from_millis(1));
        history.record_event(zip1, Event::Processed)?;

        // the directory should now contain exactly 3 Parquet files
        let files: Vec<_> = fs::read_dir(&hist_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.file_name())
            .collect();
        assert_eq!(files.len(), 3, "expected 3 parquet files, got {:?}", files);

        // — latest for zip1 —
        let latest1 = history
            .get_one(zip1, Event::Processed)?
            .expect("zip1 should have a latest file");
        let fname1 = latest1.file_name().unwrap().to_str().unwrap();
        assert!(fname1.starts_with("archive1.zip---processed---"));

        // — only file for zip2 —
        let latest2 = history
            .get_one(zip2, Event::Processed)?
            .expect("zip2 should have a file");
        let fname2 = latest2.file_name().unwrap().to_str().unwrap();
        assert!(fname2.starts_with("archive2.zip---processed---"));

        // — unknown zip returns None —
        assert!(history.get_one("no_such.zip", Event::Downloaded)?.is_none());

        Ok(())
    }

    #[test]
    fn persist_and_reload_store() -> Result<()> {
        // — first instance: write two events —
        let tmp = TempDir::new().unwrap();
        let hist_dir = tmp.path().join("history_store");
        {
            let history = History::new(&hist_dir)?;
            history.record_event("X.zip", Event::Downloaded)?;
            sleep(Duration::from_millis(1));
            history.record_event("Y.zip", Event::Downloaded)?;
        }

        // — new instance: should pick up the on-disk files —
        let history2 = History::new(&hist_dir)?;
        let got_x = history2.get_one("X.zip", Event::Downloaded)?;
        let got_y = history2.get_one("Y.zip", Event::Downloaded)?;
        assert!(got_x.is_some(), "expected X.zip after reload");
        assert!(got_y.is_some(), "expected Y.zip after reload");

        // — verify filenames —
        let fx = got_x
            .unwrap()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        let fy = got_y
            .unwrap()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        assert!(fx.starts_with("X.zip---downloaded---"));
        assert!(fy.starts_with("Y.zip---downloaded---"));

        Ok(())
    }

    #[test]
    fn many_events_performance_smoke() -> Result<()> {
        // — smoke-test writing 1 000 events across 10 zip names —
        let tmp = TempDir::new().unwrap();
        let hist_dir = tmp.path().join("history_store");
        let history = History::new(&hist_dir)?;

        const N: usize = 1_000;
        let start = Instant::now();
        let mut last = None;

        for i in 0..N {
            let zip = format!("bulk{}.zip", i % 10);
            history.record_event(&zip, Event::Processed)?;
            if i == N - 1 {
                last = history.get_one(&zip, Event::Processed)?;
            }
        }
        let elapsed = start.elapsed();
        println!("Wrote {} events in {:?}", N, elapsed);

        // — sanity: last get_one returns correct file for bulk9.zip —
        let path = last.expect("expected last event");
        let fname = path.file_name().unwrap().to_str().unwrap();
        assert!(fname.starts_with("bulk9.zip---processed---"));

        // — ensure we have N files on disk —
        let count = fs::read_dir(&hist_dir)?.count();
        assert_eq!(count, N, "expected {} parquet files, found {}", N, count);

        Ok(())
    }
}
