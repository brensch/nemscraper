//! lib.rs  ‒ crate `schema` (Tokio async, single shared Client, no headers)
//
//! Call `schema::fetch_all(&client, "schemas").await?;`
//! with a `reqwest::Client` configured (headers, proxy, TLS, …) by the caller.
//! The calling application is responsible for initializing a `tracing` subscriber.

use anyhow::{Context, Result};
use futures::{stream::FuturesUnordered, StreamExt};
use regex::Regex;
use reqwest::Client;
use scraper::{Html, Selector};
use serde::Serialize;
use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};
use tokio::{fs as tokio_fs, io::AsyncWriteExt, time::sleep};
use tracing::{debug, error, info, instrument, span, trace, warn, Level}; // Added trace
                                                                         // EnvFilter and FmtSpan would be used by the main application, not directly by library init
                                                                         // use tracing_subscriber::EnvFilter;
                                                                         // use tracing_subscriber::fmt::format::FmtSpan;
use std::time::{Duration, Instant};
use url::Url; // For timing operations

/* ───────────────────────────── data types ───────────────────────────── */

#[derive(Debug, Serialize, PartialEq)]
pub struct Column {
    pub name: String,
    pub ty: String,
    pub format: Option<String>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct CtlSchema {
    pub table: String,
    pub month: String,
    pub columns: Vec<Column>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct MonthSchema {
    pub month: String,
    pub schemas: Vec<CtlSchema>,
}

/* ──────────────────────────── CTL parsing ──────────────────────────── */

#[instrument(level = "debug", skip(contents), fields(content_len = contents.len()))]
pub fn parse_ctl(contents: &str) -> Result<CtlSchema> {
    debug!("Starting CTL parsing");
    let month = Regex::new(r"(?mi)INFILE[^\n]*?(\d{6})\D*\d{6}[^\n]*\.[cC][sS][vV]")?
        .captures(contents)
        .and_then(|c| c.get(1).map(|m| m.as_str().to_owned()))
        .ok_or_else(|| {
            warn!("Month not found in INFILE during CTL parsing");
            anyhow::anyhow!("month not found in INFILE")
        })?;
    trace!(month, "Successfully parsed month from INFILE");

    let table = Regex::new(r"(?mi)^APPEND INTO TABLE\s+(\w+)")?
        .captures(contents)
        .and_then(|c| c.get(1).map(|m| m.as_str().to_owned()))
        .ok_or_else(|| {
            warn!("Table name not found during CTL parsing");
            anyhow::anyhow!("table name not found")
        })?;
    trace!(table_name = table, "Successfully parsed table name");

    let cols_block = Regex::new(r"(?s)TRAILING NULLCOLS\s*\(\s*(.*?)\s*\)\s*(?:--|$)")?
        .captures(contents)
        .and_then(|c| c.get(1).map(|m| m.as_str()))
        .ok_or_else(|| {
            warn!("Column block not found during CTL parsing");
            anyhow::anyhow!("column block not found")
        })?;
    trace!("Successfully extracted columns block");

    let mut columns = Vec::new();
    for raw in cols_block.split(',') {
        let s = raw.trim();
        if s.is_empty() {
            continue;
        }
        let mut parts = s.split_whitespace();
        let name = parts.next().context("bad column")?.to_owned();
        let ty = parts.next().context("bad column")?.to_owned();
        if ty.eq_ignore_ascii_case("FILLER") {
            trace!(column_name = name, "Skipping FILLER column");
            continue;
        }
        let format = parts.next().map(|first| {
            let mut owned = first.to_owned();
            for p in parts {
                owned.push(' ');
                owned.push_str(p);
            }
            owned
        });
        trace!(column_name = name, column_type = ty, column_format = ?format, "Parsed column");
        columns.push(Column { name, ty, format });
    }
    debug!(num_columns = columns.len(), "Finished CTL parsing");
    Ok(CtlSchema {
        table,
        month,
        columns,
    })
}

/* ──────────────────── helpers (no header mutation) ──────────────────── */

#[instrument(level = "debug", skip(client), fields(url = %url))]
async fn get_text_core(client: &Client, url: &Url) -> Result<String> {
    debug!("Fetching text content");
    Ok(client
        .get(url.clone())
        .send()
        .await
        .with_context(|| format!("Failed to send GET request to {}", url))?
        .error_for_status()
        .with_context(|| format!("HTTP error status for {}", url))?
        .text()
        .await
        .with_context(|| format!("Failed to decode text from {}", url))?)
}

#[instrument(level = "debug", skip(client), fields(url = %url, max_retries, initial_backoff_ms))]
async fn get_text_with_retry(
    client: &Client,
    url: &Url,
    max_retries: u32,
    initial_backoff_ms: u64,
) -> Result<String> {
    let mut attempts = 0;
    debug!("Attempting to get text with retries");
    loop {
        match get_text_core(client, url).await {
            Ok(text) => {
                debug!(attempts, "Successfully fetched text");
                return Ok(text);
            }
            Err(e) => {
                attempts += 1;
                if attempts > max_retries {
                    error!(
                        final_attempt = attempts -1,
                        error = %e,
                        "Failed to download after maximum retries"
                    );
                    return Err(e).with_context(|| {
                        format!("Failed to download {} after {} retries", url, max_retries)
                    });
                }
                let backoff_duration_ms = initial_backoff_ms * (2u64.pow(attempts - 1));
                warn!(
                    attempt = attempts,
                    max_attempts = max_retries,
                    delay_ms = backoff_duration_ms,
                    error = %e, // Keep error context for warning
                    "Download attempt failed. Retrying..."
                );
                sleep(Duration::from_millis(backoff_duration_ms)).await;
            }
        }
    }
}

#[instrument(level = "debug", skip(client), fields(directory_url = %dir))]
async fn list_dirs(client: &Client, dir: &Url) -> Result<Vec<Url>> {
    debug!("Listing subdirectories");
    let body = get_text_core(client, dir)
        .await
        .with_context(|| format!("Failed to get directory listing HTML for {}", dir))?;
    let doc = Html::parse_document(&body);
    let sel = Selector::parse("a[href]").unwrap();
    let urls = doc
        .select(&sel)
        .filter_map(|e| {
            let href = e.value().attr("href")?;
            if !href.ends_with('/') {
                return None;
            }
            if e.text().any(|t| t.contains("Parent Directory")) {
                return None;
            }
            match dir.join(href) {
                Ok(url) => {
                    trace!(url = %url, "Found directory link");
                    Some(url)
                }
                Err(e) => {
                    warn!(href, error = %e, "Failed to join URL for directory listing");
                    None
                }
            }
        })
        .collect::<Vec<_>>();
    debug!(count = urls.len(), "Finished listing subdirectories");
    Ok(urls)
}

#[instrument(level = "debug", skip(client), fields(ctl_dir_url = %ctl_dir))]
async fn list_ctl_urls(client: &Client, ctl_dir: &Url) -> Result<Vec<Url>> {
    debug!("Listing CTL files");
    let body = get_text_core(client, ctl_dir)
        .await
        .with_context(|| format!("Failed to get CTL directory listing HTML for {}", ctl_dir))?;
    let doc = Html::parse_document(&body);
    let sel = Selector::parse("a[href]").unwrap();
    let re = Regex::new(r"(?i)\.ctl$").unwrap();
    let urls = doc
        .select(&sel)
        .filter_map(|e| {
            let h = e.value().attr("href")?;
            if re.is_match(h) {
                match ctl_dir.join(h) {
                    Ok(url) => {
                        trace!(url = %url, "Found CTL file link");
                        Some(url)
                    }
                    Err(e) => {
                        warn!(href = h, error = %e, "Failed to join URL for CTL file");
                        None
                    }
                }
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    debug!(count = urls.len(), "Finished listing CTL files");
    Ok(urls)
}

fn month_code_from_url(u: &Url) -> Option<String> {
    let re = Regex::new(r"MMSDM_(\d{4})_(\d{2})/?$").ok()?;
    let caps = re.captures(u.path())?;
    Some(format!("{}{}", &caps[1], &caps[2]))
}

/* ───────────────────────── public API ───────────────────────── */

/// Process every unseen month (concurrency 8) into `schemas/<YYYYMM>.json`.
///
/// The caller controls all headers & settings on `client`.
/// **Important**: The calling application is responsible for initializing a `tracing`
/// subscriber (e.g., `tracing_subscriber::fmt().init();`) before calling this function
/// if log output is desired.
#[instrument(level = "info", skip(client, output_dir), fields(output_path = %output_dir.as_ref().display()))]
pub async fn fetch_all<P: AsRef<Path>>(client: &Client, output_dir: P) -> Result<Vec<MonthSchema>> {
    // Logger initialization is now expected to be done by the caller (e.g., in main.rs)
    info!("Starting schema fetch_all process."); // Kept as info: main entry point
    let output_dir = output_dir.as_ref();
    fs::create_dir_all(output_dir)
        .with_context(|| format!("Failed to create output directory: {:?}", output_dir))?;
    debug!(directory = %output_dir.display(), "Output directory ensured.");

    let done: HashSet<String> = fs::read_dir(output_dir)?
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().into_string().ok())
        .filter_map(|n| (n.ends_with(".json") && n.len() == 11).then(|| n[..6].to_owned()))
        .collect();
    debug!(
        processed_months_count = done.len(),
        "Loaded set of already processed months."
    );

    let base = Url::parse("https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/")?;
    debug!(base_url = %base, "Listing year directories.");
    let mut month_urls = Vec::<Url>::new();

    for year_url in list_dirs(client, &base).await.context("listing years")? {
        debug!(year_url = %year_url, "Listing month directories for year.");
        month_urls.extend(
            list_dirs(client, &year_url)
                .await
                .with_context(|| format!("months in {year_url}"))?,
        );
    }
    debug!(
        total_month_urls_found = month_urls.len(),
        "Finished discovering all month URLs."
    );

    const MAX_CONCURRENCY: usize = 4;
    const CTL_MAX_RETRIES: u32 = 3;
    const CTL_INITIAL_BACKOFF_MS: u64 = 500;

    let mut tasks = FuturesUnordered::new();
    let mut results = Vec::<MonthSchema>::new();
    let mut tasks_spawned = 0;

    for month_url in month_urls {
        let month_code = match month_code_from_url(&month_url) {
            Some(c) => c,
            None => {
                warn!(url = %month_url, "Could not extract month code from URL. Skipping.");
                continue;
            }
        };

        if done.contains(&month_code) {
            info!(month = month_code, "Skipping month (already on disk)"); // Kept info: user-visible skip
            continue;
        }

        let ctl_dir = match month_url.join("MMSDM_Historical_Data_SQLLoader/CTL/") {
            Ok(u) => u,
            Err(e) => {
                error!(month_url = %month_url, error = %e, "Error forming CTL directory URL. Skipping month.");
                continue;
            }
        };

        debug!(month = month_code, ctl_dir = %ctl_dir, "Queueing month for processing");
        let client_clone = client.clone();
        let out_dir_clone = PathBuf::from(output_dir);
        tasks.push(process_month(
            client_clone,
            month_code.clone(),
            ctl_dir,
            out_dir_clone,
            CTL_MAX_RETRIES,
            CTL_INITIAL_BACKOFF_MS,
        ));
        tasks_spawned += 1;

        if tasks.len() >= MAX_CONCURRENCY {
            if let Some(res) = tasks.next().await {
                match res {
                    Ok(Some(ms)) => {
                        info!(
                            month = ms.month,
                            schemas_count = ms.schemas.len(),
                            "Successfully processed and saved month"
                        );
                        results.push(ms);
                    }
                    Ok(None) => { /* Month processed but yielded no schemas, already logged in process_month */
                    }
                    Err(e) => error!(error = %e, "Error processing a month task"),
                }
            }
        }
    }
    debug!(
        tasks_initially_spawned = tasks_spawned,
        "All processable months queued. Waiting for remaining tasks."
    );

    while let Some(res) = tasks.next().await {
        match res {
            Ok(Some(ms)) => {
                info!(
                    month = ms.month,
                    schemas_count = ms.schemas.len(),
                    "Successfully processed and saved month (from remaining tasks)"
                );
                results.push(ms);
            }
            Ok(None) => { /* Already logged */ }
            Err(e) => error!(error = %e, "Error processing a month task (from remaining tasks)"),
        }
    }
    info!(
        total_months_processed_in_run = results.len(),
        "Schema fetch_all process completed."
    ); // Kept as info: final summary
    Ok(results)
}

/* ───────────────────── per-month worker ───────────────────── */
#[instrument(level = "info", skip(client, output_dir, ctl_max_retries, ctl_initial_backoff_ms), fields(month_code = code, ctl_dir = %ctl_dir, output_path = %output_dir.display()))]
async fn process_month(
    client: Client,
    code: String,
    ctl_dir: Url,
    output_dir: PathBuf,
    ctl_max_retries: u32,
    ctl_initial_backoff_ms: u64,
) -> Result<Option<MonthSchema>> {
    // Note: The `info!` log for "Starting to process month" was removed as per previous request
    // to reduce verbosity and rely on the `#[instrument]` span.
    // If you want it back, it would be:
    // debug!("Starting to process month"); // Or info! if preferred

    let ctl_urls = match list_ctl_urls(&client, &ctl_dir).await {
        Ok(v) if !v.is_empty() => {
            debug!(count = v.len(), "Found CTL URLs for month");
            v
        }
        Ok(_) => {
            warn!("No CTLs found in directory for month {}", code);
            return Ok(None);
        }
        Err(e) => {
            error!(error = %e, "Failed to list CTL URLs for month {}. Skipping month.", code);
            return Err(e).with_context(|| format!("Failed listing CTLs for month {}", code));
        }
    };

    // The `info!` log for ctl_count was here, now covered by the debug log above
    // and the final summary. If desired:
    // debug!(ctl_count = ctl_urls.len(), "Processing CTL URLs for month");

    let mut schemas = Vec::new();
    let mut total_download_duration = Duration::ZERO;
    let mut total_parse_duration = Duration::ZERO;

    for (idx, url) in ctl_urls.iter().enumerate() {
        let ctl_span = span!(Level::DEBUG, "ctl_processing", ctl_url = %url, ctl_index = idx + 1, total_ctls = ctl_urls.len());
        let _enter = ctl_span.enter();

        debug!("Starting download for CTL file");
        let download_start_time = Instant::now();
        let text = match get_text_with_retry(&client, url, ctl_max_retries, ctl_initial_backoff_ms)
            .await
        {
            Ok(t) => {
                total_download_duration += download_start_time.elapsed();
                debug!(
                    duration_ms = download_start_time.elapsed().as_millis(),
                    "CTL file downloaded successfully"
                );
                t
            }
            Err(e) => {
                warn!(error = %e, "Skipping CTL file due to persistent download error after retries");
                continue;
            }
        };

        debug!("Starting parsing for CTL file content");
        let parse_start_time = Instant::now();
        match parse_ctl(&text) {
            Ok(s) => {
                total_parse_duration += parse_start_time.elapsed();
                debug!(
                    table_name = s.table,
                    duration_ms = parse_start_time.elapsed().as_millis(),
                    "Successfully parsed CTL file"
                );
                schemas.push(s);
            }
            Err(e) => {
                // Log the parse error, but continue with other CTL files.
                // The duration for this failed parse is not added to total_parse_duration.
                warn!(error = %e, "Failed to parse CTL file content, skipping this file.");
            }
        }
    }

    if schemas.is_empty() {
        warn!(
            "Month {} produced no schemas (no valid CTLs found or all failed processing)",
            code
        );
        return Ok(None);
    }

    let path = output_dir.join(format!("{code}.json"));
    debug!(file_path = %path.display(), num_schemas = schemas.len(), "Writing schemas to JSON file");

    let write_start_time = Instant::now();
    let mut f = tokio_fs::File::create(&path).await.with_context(|| {
        format!(
            "Failed to create output file for month {}: {:?}",
            code, path
        )
    })?;

    let json_data = serde_json::to_string_pretty(&schemas)
        .with_context(|| format!("Failed to serialize schemas for month {}", code))?;

    f.write_all(json_data.as_bytes()).await.with_context(|| {
        format!(
            "Failed to write schemas to disk for month {}: {:?}",
            code, path
        )
    })?;
    let file_write_duration_ms = write_start_time.elapsed().as_millis();

    // Updated final info log with timing fields
    info!(
        month_code = code,
        schemas_written = schemas.len(),
        total_download_time_ms = total_download_duration.as_millis(),
        total_ctl_parse_time_ms = total_parse_duration.as_millis(),
        file_write_time_ms = file_write_duration_ms,
        "Successfully processed month and wrote schemas to disk."
    );

    Ok(Some(MonthSchema {
        month: code,
        schemas,
    }))
}

/* ───────────────────────────── tests ───────────────────────────── */
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Once;
    use tokio::runtime::Runtime;
    use tracing_subscriber::fmt::format::FmtSpan;
    use tracing_subscriber::EnvFilter; // For test logger setup // For test logger setup

    // Test logger initialization remains, as tests are separate execution contexts.
    fn init_test_logging_for_tests() {
        static TEST_INIT_LOGGER: Once = Once::new();
        TEST_INIT_LOGGER.call_once(|| {
            let filter = EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,schema=debug")); // Default for tests

            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_test_writer() // Directs output to test harness
                .with_span_events(FmtSpan::CLOSE)
                .init();
            info!("Tracing logger initialized for schema tests.");
        });
    }

    #[test]
    fn month_regex_variants() {
        init_test_logging_for_tests();
        let re = Regex::new(r"(?mi)INFILE[^\n]*?(\d{6})\D*\d{6}[^\n]*\.[cC][sS][vV]").unwrap();
        let cases = [
            ("INFILE F_201008010000.csv", "201008"),
            ("INFILE F_201008_010000.csv", "201008"),
            ("INFILE path/F_201008-010000.CSV", "201008"),
        ];
        for (s, want) in cases {
            assert_eq!(re.captures(s).unwrap()[1].to_string(), want);
        }
    }

    #[test]
    fn parse_minimal_ctl() {
        init_test_logging_for_tests();
        let ctl = r#"
LOAD DATA
INFILE FILE_202201010000.csv
APPEND INTO TABLE FOO
TRAILING NULLCOLS (foo FILLER, BAR DATE "yyyy/mm/dd")
"#;
        let s = parse_ctl(ctl).unwrap();
        assert_eq!(s.month, "202201");
        assert_eq!(s.table, "FOO");
        assert_eq!(s.columns.len(), 1);
    }

    #[test]
    #[ignore] // This test requires network access and can take time
    fn integration_one_month() {
        init_test_logging_for_tests(); // Initialize logger for test output
        info!("Starting integration test: integration_one_month");
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            // In a real application, main.rs would have set up the tracing subscriber.
            // For this test, init_test_logging_for_tests() does it.

            let tmp = tempfile::tempdir().unwrap();
            let client = Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap();

            debug!(temp_dir = %tmp.path().display(), "Running fetch_all for integration test");
            match fetch_all(&client, tmp.path()).await {
                Ok(out) => {
                    info!(
                        processed_months_count = out.len(),
                        "Integration test fetch_all completed successfully"
                    );
                }
                Err(e) => {
                    error!(error = %e, "Integration test fetch_all failed");
                    panic!("Integration test failed: {:?}", e);
                }
            }
        });
        info!("Finished integration test: integration_one_month");
    }
}
