//! lib.rs ‒ crate `schema` (Tokio async, single shared Client, no headers)
//
//! Call `schema::fetch_all(&client, "schemas").await?;`
//! with a `reqwest::Client` configured (headers, proxy, TLS, …) by the caller.
//! The calling application is responsible for initializing a `tracing` subscriber.

use anyhow::{Context, Result};
use futures::{stream::FuturesUnordered, StreamExt};
use hex;
use regex::Regex;
use reqwest::Client;
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize}; // Added Deserialize
use sha2::{Digest, Sha256};
use std::sync::Arc;
// For hashing
use std::time::{Duration, Instant};
use std::{
    collections::{HashMap, HashSet}, // Added BTreeMap and HashMap
    fs,
    path::{Path, PathBuf},
};
use tokio::{fs as tokio_fs, io::AsyncWriteExt, time::sleep};
use tracing::{debug, error, info, instrument, span, trace, warn, Level};
use url::Url; // For converting hash to string

/* ───────────────────────────── data types ───────────────────────────── */

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq, Hash)] // Added Deserialize, Clone, Eq, Hash
pub struct Column {
    pub name: String,
    pub ty: String,
    pub format: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)] // Added Deserialize
pub struct CtlSchema {
    pub table: String,
    pub month: String, // This month is from the CTL, might differ from filename if file is misplaced
    pub columns: Vec<Column>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)] // Added Deserialize
pub struct MonthSchema {
    pub month: String, // This is the YYYYMM from the filename/directory structure
    pub schemas: Vec<CtlSchema>,
}

/// Represents a specific version of a table schema and its lifespan.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
pub struct SchemaEvolution {
    pub table_name: String,
    pub fields_hash: String, // SHA256 hash of the sorted column names, types, and formats
    pub start_month: String, // YYYYMM
    pub end_month: String,   // YYYYMM (inclusive)
    #[serde(skip_serializing_if = "Vec::is_empty")] // To keep JSON clean
    pub columns: Vec<Column>, // Store the actual columns for this version
}

/* ──────────────────────────── CTL parsing ──────────────────────────── */

#[instrument(level = "debug", skip(contents), fields(content_len = contents.len()))]
pub fn parse_ctl(contents: &str) -> Result<CtlSchema> {
    debug!("Starting CTL parsing");
    let month_in_ctl = Regex::new(r"(?mi)INFILE[^\n]*?(\d{6})\D*\d{6}[^\n]*\.[cC][sS][vV]")?
        .captures(contents)
        .and_then(|c| c.get(1).map(|m| m.as_str().to_owned()))
        .ok_or_else(|| {
            warn!("Month not found in INFILE during CTL parsing");
            anyhow::anyhow!("month not found in INFILE")
        })?;
    trace!(month_in_ctl, "Successfully parsed month from INFILE");

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
        month: month_in_ctl, // Use month parsed from CTL content
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
    info!("Starting schema fetch_all process.");
    let output_dir = output_dir.as_ref();
    fs::create_dir_all(output_dir)
        .with_context(|| format!("Failed to create output directory: {:?}", output_dir))?;
    debug!(directory = %output_dir.display(), "Output directory ensured.");

    let done: HashSet<String> = fs::read_dir(output_dir)?
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().into_string().ok())
        .filter_map(|n| {
            (n.ends_with(".json") && n.len() == "YYYYMM.json".len()).then(|| n[..6].to_owned())
        }) // Corrected length check
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

    // Sort month_urls to process them chronologically, helpful for debugging and consistency
    month_urls.sort_by_key(|url| month_code_from_url(url));

    for month_url in month_urls {
        let month_code = match month_code_from_url(&month_url) {
            Some(c) => c,
            None => {
                warn!(url = %month_url, "Could not extract month code from URL. Skipping.");
                continue;
            }
        };

        if done.contains(&month_code) {
            info!(month = month_code, "Skipping month (already on disk)");
            // Optionally load existing data if needed downstream, or just skip for fetch_all
            // For `extract_schema_evolutions`, we'll read all files anyway.
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
    );
    Ok(results)
}

/* ───────────────────── per-month worker ───────────────────── */
#[instrument(level = "info", skip(client, output_dir, ctl_max_retries, ctl_initial_backoff_ms), fields(month_code = code, ctl_dir = %ctl_dir, output_path = %output_dir.display()))]
async fn process_month(
    client: Client,
    code: String, // This is the YYYYMM for the month being processed.
    ctl_dir: Url,
    output_dir: PathBuf,
    ctl_max_retries: u32,
    ctl_initial_backoff_ms: u64,
) -> Result<Option<MonthSchema>> {
    // Return type is Option<MonthSchema>
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

    let mut ctl_schemas = Vec::new(); // Changed from `schemas` to `ctl_schemas` to avoid confusion
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
                // `mut s` to potentially correct month if needed
                total_parse_duration += parse_start_time.elapsed();
                // The month in `s.month` comes from the CTL file.
                // The `code` argument is the month derived from the directory structure.
                // They should ideally match. If not, it's a data inconsistency.
                // For now, we trust the `code` (directory month) for the MonthSchema grouping.
                // The `s.month` (CTL month) is preserved within CtlSchema.
                if s.month != code {
                    warn!(ctl_month = s.month, dir_month = code, table_name = s.table, ctl_url = %url,
                          "Month mismatch between CTL content and directory structure. Using directory month for grouping.");
                }

                debug!(
                    table_name = s.table,
                    ctl_month = s.month, // Log the month from CTL
                    duration_ms = parse_start_time.elapsed().as_millis(),
                    "Successfully parsed CTL file"
                );
                ctl_schemas.push(s);
            }
            Err(e) => {
                warn!(error = %e, "Failed to parse CTL file content, skipping this file.");
            }
        }
    }

    if ctl_schemas.is_empty() {
        warn!(
            "Month {} produced no schemas (no valid CTLs found or all failed processing)",
            code
        );
        return Ok(None);
    }

    let month_schema_for_file = MonthSchema {
        month: code.clone(), // Use the directory-derived month `code` for the MonthSchema
        schemas: ctl_schemas,
    };

    let path = output_dir.join(format!("{}.json", code)); // Corrected filename format
    debug!(file_path = %path.display(), num_schemas = month_schema_for_file.schemas.len(), "Writing schemas to JSON file");

    let write_start_time = Instant::now();
    let mut f = tokio_fs::File::create(&path).await.with_context(|| {
        format!(
            "Failed to create output file for month {}: {:?}",
            code, path
        )
    })?;

    // We are serializing Vec<CtlSchema> as per original logic, but wrapped in MonthSchema for context
    let json_data = serde_json::to_string_pretty(&month_schema_for_file)
        .with_context(|| format!("Failed to serialize MonthSchema for month {}", code))?;

    f.write_all(json_data.as_bytes()).await.with_context(|| {
        format!(
            "Failed to write schemas to disk for month {}: {:?}",
            code, path
        )
    })?;
    let file_write_duration_ms = write_start_time.elapsed().as_millis();

    info!(
        month_code = code,
        schemas_written = month_schema_for_file.schemas.len(),
        total_download_time_ms = total_download_duration.as_millis(),
        total_ctl_parse_time_ms = total_parse_duration.as_millis(),
        file_write_time_ms = file_write_duration_ms,
        "Successfully processed month and wrote schemas to disk."
    );

    Ok(Some(month_schema_for_file)) // Return the constructed MonthSchema
}

/* ─────────────────── Schema Evolution Extraction ─────────────────── */

/// Calculates a stable hash for a list of columns.
/// Columns are sorted by name before hashing to ensure order doesn't affect the hash.
fn calculate_fields_hash(columns: &[Column]) -> String {
    let mut hasher = Sha256::new();
    let mut sorted_columns = columns.to_vec();
    // Sort by name, then type, then format to ensure a canonical representation
    sorted_columns.sort_by(|a, b| {
        a.name
            .cmp(&b.name)
            .then_with(|| a.ty.cmp(&b.ty))
            .then_with(|| a.format.cmp(&b.format))
    });

    for col in sorted_columns {
        hasher.update(col.name.as_bytes());
        hasher.update(col.ty.as_bytes());
        if let Some(ref format) = col.format {
            hasher.update(format.as_bytes());
        }
        hasher.update(b";"); // Separator
    }
    hex::encode(hasher.finalize())
}

/// Extracts schema evolution information from previously generated JSON files.
///
/// Reads all `<YYYYMM>.json` files from the `input_dir`, processes them in chronological order,
/// and identifies periods where table schemas remain consistent.
///
/// # Arguments
/// * `input_dir` - The directory containing the `<YYYYMM>.json` files.
///
/// # Returns
/// A `Result` containing a `Vec<SchemaEvolution>` detailing the history of each table schema,
/// or an `anyhow::Error` if an issue occurs.
#[instrument(level = "info", skip(input_dir), fields(input_path = %input_dir.as_ref().display()))]
pub fn extract_schema_evolutions<P: AsRef<Path>>(input_dir: P) -> Result<Vec<SchemaEvolution>> {
    info!("Starting schema evolution extraction.");
    let input_dir = input_dir.as_ref();

    let mut month_files = Vec::new();
    for entry in fs::read_dir(input_dir)
        .with_context(|| format!("Failed to read input directory: {:?}", input_dir))?
    {
        let entry = entry.with_context(|| "Failed to read directory entry")?;
        let path = entry.path();
        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext == "json" {
                    if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                        // Expect YYYYMM format
                        if stem.len() == 6 && stem.chars().all(|c| c.is_digit(10)) {
                            // Check if the year part is reasonable (e.g., 1990-2099)
                            let year = stem[0..4].parse::<u32>().unwrap_or(0);
                            let month_val = stem[4..6].parse::<u32>().unwrap_or(0);
                            if (1990..=2099).contains(&year) && (1..=12).contains(&month_val) {
                                month_files.push(path.clone());
                            } else {
                                warn!(
                                    "Skipping file with invalid month format in name: {:?}",
                                    path
                                );
                            }
                        } else {
                            warn!("Skipping file with non-YYYYMM name: {:?}", path);
                        }
                    }
                }
            }
        }
    }

    // Sort files by month (YYYYMM from filename)
    month_files.sort_by_key(|path| {
        path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string()
    });

    debug!(
        num_month_files = month_files.len(),
        "Found and sorted month JSON files."
    );

    if month_files.is_empty() {
        info!("No month JSON files found in the input directory. No evolution to extract.");
        return Ok(Vec::new());
    }

    let mut evolutions: Vec<SchemaEvolution> = Vec::new();
    // Tracks the current active schema for each table: TableName -> (FieldsHash, StartMonth, Columns)
    let mut active_schemas: HashMap<String, (String, String, Vec<Column>)> = HashMap::new();

    let mut previous_month_code: Option<String> = None;

    for month_file_path in &month_files {
        let file_name_month = month_file_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string();

        debug!(month_file = %month_file_path.display(), month_code = file_name_month, "Processing month file");

        let file_contents = fs::read_to_string(month_file_path)
            .with_context(|| format!("Failed to read month file: {:?}", month_file_path))?;

        let month_data: MonthSchema = serde_json::from_str(&file_contents)
            .with_context(|| format!("Failed to parse JSON from file: {:?}", month_file_path))?;

        // Ensure the month in the filename matches the month in the MonthSchema object
        if month_data.month != file_name_month {
            warn!(
                file_month = file_name_month, schema_object_month = month_data.month, file_path = %month_file_path.display(),
                "Mismatch between filename month and MonthSchema.month field. Using filename month."
            );
            // Potentially skip this file or handle inconsistency, for now, we proceed using file_name_month.
        }

        let current_month_code = file_name_month;

        // Keep track of tables seen in this month to handle tables that disappear
        let mut tables_in_current_month: HashSet<String> = HashSet::new();

        for ctl_schema in month_data.schemas {
            tables_in_current_month.insert(ctl_schema.table.clone());
            let fields_hash = calculate_fields_hash(&ctl_schema.columns);

            if let Some(active_entry) = active_schemas.get_mut(&ctl_schema.table) {
                // Table already has an active schema
                if active_entry.0 != fields_hash {
                    // Schema changed for this table
                    debug!(
                        table = ctl_schema.table,
                        old_hash = active_entry.0,
                        new_hash = fields_hash,
                        end_month = previous_month_code.as_deref().unwrap_or("N/A"),
                        "Schema changed"
                    );
                    evolutions.push(SchemaEvolution {
                        table_name: ctl_schema.table.clone(),
                        fields_hash: active_entry.0.clone(),
                        start_month: active_entry.1.clone(),
                        end_month: previous_month_code
                            .clone()
                            .unwrap_or_else(|| active_entry.1.clone()), // End in previous month
                        columns: active_entry.2.clone(),
                    });
                    // Start new schema
                    *active_entry = (
                        fields_hash,
                        current_month_code.clone(),
                        ctl_schema.columns.clone(),
                    );
                } else {
                    // Schema is the same, just continue. The end_month will be updated if it changes later or at the end.
                }
            } else {
                // New table encountered
                debug!(
                    table = ctl_schema.table,
                    hash = fields_hash,
                    start_month = current_month_code,
                    "New table schema started"
                );
                active_schemas.insert(
                    ctl_schema.table.clone(),
                    (
                        fields_hash,
                        current_month_code.clone(),
                        ctl_schema.columns.clone(),
                    ),
                );
            }
        }

        // Check for tables that were active but are not in the current month's schemas
        // These tables' schemas are considered ended in the `previous_month_code`.
        let mut tables_to_remove_from_active = Vec::new();
        if let Some(ref prev_m) = previous_month_code {
            // Only if there *was* a previous month
            for (table_name, (hash, start_m, cols)) in &active_schemas {
                if !tables_in_current_month.contains(table_name) {
                    debug!(
                        table = table_name,
                        hash = hash,
                        start_month = start_m,
                        end_month = prev_m,
                        "Schema ended (table not present in current month)"
                    );
                    evolutions.push(SchemaEvolution {
                        table_name: table_name.clone(),
                        fields_hash: hash.clone(),
                        start_month: start_m.clone(),
                        end_month: prev_m.clone(),
                        columns: cols.clone(),
                    });
                    tables_to_remove_from_active.push(table_name.clone());
                }
            }
        }
        for table_name in tables_to_remove_from_active {
            active_schemas.remove(&table_name);
        }

        previous_month_code = Some(current_month_code);
    }

    // After iterating through all month files, close any remaining active schemas
    // Their end_month will be the last processed month_code
    if let Some(last_month_code) = previous_month_code {
        for (table_name, (hash, start_month, columns)) in active_schemas {
            debug!(
                table = table_name,
                hash = hash,
                start_month = start_month,
                end_month = last_month_code,
                "Closing active schema at end of processing"
            );
            evolutions.push(SchemaEvolution {
                table_name,
                fields_hash: hash,
                start_month,
                end_month: last_month_code.clone(),
                columns,
            });
        }
    }

    // Sort evolutions for deterministic output, useful for tests and consistency
    evolutions.sort_by(|a, b| {
        a.table_name
            .cmp(&b.table_name)
            .then_with(|| a.start_month.cmp(&b.start_month))
            .then_with(|| a.end_month.cmp(&b.end_month))
    });

    info!(
        num_evolutions = evolutions.len(),
        "Schema evolution extraction completed."
    );
    Ok(evolutions)
}

// Helper function to find the correct schema evolution for a given table and month
pub fn find_schema_evolution(
    schema_lookup: &HashMap<String, Vec<Arc<SchemaEvolution>>>,
    table_name: &str, // Base AEMO table name, e.g., "DISPATCHPRICE"
    effective_month: &str,
) -> Option<Arc<SchemaEvolution>> {
    if let Some(evolutions_for_table) = schema_lookup.get(table_name) {
        for evo in evolutions_for_table {
            // Ensure the month falls within the schema's valid range
            // CORRECTED LINE: Compare &str with &str using .as_str()
            if effective_month >= evo.start_month.as_str()
                && effective_month <= evo.end_month.as_str()
            {
                return Some(evo.clone());
            }
        }
        tracing::warn!(
            table_name,
            effective_month,
            available_evolutions = ?evolutions_for_table.iter().map(|e| format!("{}-{}", e.start_month, e.end_month)).collect::<Vec<_>>(),
            "No schema evolution found for the given month."
        );
    } else {
        tracing::warn!(
            table_name,
            "No schema evolutions found for this table name in the lookup map."
        );
    }
    None
}

/* ───────────────────────────── tests ───────────────────────────── */
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Once;
    use tempfile::tempdir;
    use tokio::runtime::Runtime;
    use tracing_subscriber::fmt::format::FmtSpan;
    use tracing_subscriber::EnvFilter; // For creating temporary directories in tests

    static TEST_INIT_LOGGER: Once = Once::new();
    fn init_test_logging() {
        // Renamed for clarity
        TEST_INIT_LOGGER.call_once(|| {
            let filter = EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,schema=debug"));

            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_test_writer()
                .with_span_events(FmtSpan::CLOSE) // FmtSpan::NEW | FmtSpan::CLOSE for more detail
                .try_init() // Use try_init to avoid panic if already initialized
                .ok(); // Ignore error if it's already initialized by another test
            info!("Test tracing logger initialized for schema crate tests.");
        });
    }

    #[test]
    fn month_regex_variants() {
        init_test_logging();
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
        init_test_logging();
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
        assert_eq!(s.columns[0].name, "BAR");
        assert_eq!(s.columns[0].ty, "DATE");
        assert_eq!(s.columns[0].format, Some("\"yyyy/mm/dd\"".to_string()));
    }

    #[test]
    #[ignore] // This test requires network access and can take time
    fn integration_fetch_all_basic() {
        // Renamed for clarity
        init_test_logging();
        info!("Starting integration test: integration_fetch_all_basic");
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let tmp = tempdir().unwrap();
            let client = Client::builder()
                .timeout(Duration::from_secs(60)) // Increased timeout
                .build()
                .unwrap();

            debug!(temp_dir = %tmp.path().display(), "Running fetch_all for integration test");
            // To make this test faster and more reliable, we should limit the scope.
            // For now, it fetches everything, which can be a lot.
            // Consider modifying `Workspace_all` or its callers to fetch only a specific year/month for testing.
            match fetch_all(&client, tmp.path()).await {
                Ok(out) => {
                    info!(
                        processed_months_count = out.len(),
                        "Integration test fetch_all completed successfully"
                    );
                    // Add assertions here if needed, e.g., check if any files were created.
                    let entries = fs::read_dir(tmp.path()).unwrap().count();
                    if out.is_empty() {
                        // This might happen if all data is already "done" or no data is found for the period.
                        // For a robust test, we'd need to ensure a clean state or mock the "done" list.
                        warn!("fetch_all returned no new MonthSchema objects. This might be okay if all data was already processed or no data for the default range.");
                    } else {
                        assert!(entries > 0, "Expected some JSON files to be created.");
                        assert_eq!(entries, out.len(), "Number of created files should match number of processed months.");
                    }
                }
                Err(e) => {
                    error!(error = %e, "Integration test fetch_all failed");
                    panic!("Integration test failed: {:?}", e);
                }
            }
        });
        info!("Finished integration test: integration_fetch_all_basic");
    }

    #[test]
    fn test_calculate_fields_hash_stability() {
        init_test_logging();
        let cols1 = vec![
            Column {
                name: "A".to_string(),
                ty: "VARCHAR".to_string(),
                format: None,
            },
            Column {
                name: "B".to_string(),
                ty: "NUMBER".to_string(),
                format: Some("FMT".to_string()),
            },
        ];
        let cols2 = vec![
            // Same as cols1 but different order
            Column {
                name: "B".to_string(),
                ty: "NUMBER".to_string(),
                format: Some("FMT".to_string()),
            },
            Column {
                name: "A".to_string(),
                ty: "VARCHAR".to_string(),
                format: None,
            },
        ];
        let cols3 = vec![
            // Different
            Column {
                name: "A".to_string(),
                ty: "VARCHAR2".to_string(),
                format: None,
            }, // type changed
            Column {
                name: "B".to_string(),
                ty: "NUMBER".to_string(),
                format: Some("FMT".to_string()),
            },
        ];

        let hash1 = calculate_fields_hash(&cols1);
        let hash2 = calculate_fields_hash(&cols2);
        let hash3 = calculate_fields_hash(&cols3);

        assert_eq!(
            hash1, hash2,
            "Hashes should be the same for columns in different order."
        );
        assert_ne!(
            hash1, hash3,
            "Hashes should differ for different column definitions."
        );
        assert!(!hash1.is_empty(), "Hash should not be empty.");
    }

    #[test]
    fn test_extract_schema_evolutions() -> Result<()> {
        init_test_logging();
        let tmp_dir = tempdir().context("Failed to create temp dir for test")?;
        let output_path = tmp_dir.path();

        // --- Create dummy JSON files ---

        // Month 1: TableA v1, TableB v1
        let cols_a_v1 = vec![Column {
            name: "col1".to_string(),
            ty: "VARCHAR".to_string(),
            format: None,
        }];
        let cols_b_v1 = vec![Column {
            name: "data".to_string(),
            ty: "NUMBER".to_string(),
            format: None,
        }];
        let month1_schemas = vec![
            CtlSchema {
                table: "TableA".to_string(),
                month: "202301".to_string(),
                columns: cols_a_v1.clone(),
            },
            CtlSchema {
                table: "TableB".to_string(),
                month: "202301".to_string(),
                columns: cols_b_v1.clone(),
            },
        ];
        let month1_data = MonthSchema {
            month: "202301".to_string(),
            schemas: month1_schemas,
        };
        fs::write(
            output_path.join("202301.json"),
            serde_json::to_string_pretty(&month1_data)?,
        )?;

        // Month 2: TableA v1 (same), TableB v2 (changed)
        let cols_b_v2 = vec![
            Column {
                name: "data".to_string(),
                ty: "NUMBER".to_string(),
                format: None,
            },
            Column {
                name: "extra".to_string(),
                ty: "DATE".to_string(),
                format: Some("YYYYMMDD".to_string()),
            },
        ];
        let month2_schemas = vec![
            CtlSchema {
                table: "TableA".to_string(),
                month: "202302".to_string(),
                columns: cols_a_v1.clone(),
            },
            CtlSchema {
                table: "TableB".to_string(),
                month: "202302".to_string(),
                columns: cols_b_v2.clone(),
            },
        ];
        let month2_data = MonthSchema {
            month: "202302".to_string(),
            schemas: month2_schemas,
        };
        fs::write(
            output_path.join("202302.json"),
            serde_json::to_string_pretty(&month2_data)?,
        )?;

        // Month 3: TableA v2 (changed), TableB v2 (same), TableC v1 (new)
        let cols_a_v2 = vec![
            Column {
                name: "col1".to_string(),
                ty: "VARCHAR".to_string(),
                format: None,
            },
            Column {
                name: "col2".to_string(),
                ty: "INT".to_string(),
                format: None,
            },
        ];
        let cols_c_v1 = vec![Column {
            name: "id".to_string(),
            ty: "UUID".to_string(),
            format: None,
        }];
        let month3_schemas = vec![
            CtlSchema {
                table: "TableA".to_string(),
                month: "202303".to_string(),
                columns: cols_a_v2.clone(),
            },
            CtlSchema {
                table: "TableB".to_string(),
                month: "202303".to_string(),
                columns: cols_b_v2.clone(),
            },
            CtlSchema {
                table: "TableC".to_string(),
                month: "202303".to_string(),
                columns: cols_c_v1.clone(),
            },
        ];
        let month3_data = MonthSchema {
            month: "202303".to_string(),
            schemas: month3_schemas,
        };
        fs::write(
            output_path.join("202303.json"),
            serde_json::to_string_pretty(&month3_data)?,
        )?;

        // Month 4: TableA v2 (same), TableC v1 (same) - TableB disappears
        let month4_schemas = vec![
            CtlSchema {
                table: "TableA".to_string(),
                month: "202304".to_string(),
                columns: cols_a_v2.clone(),
            },
            CtlSchema {
                table: "TableC".to_string(),
                month: "202304".to_string(),
                columns: cols_c_v1.clone(),
            },
        ];
        let month4_data = MonthSchema {
            month: "202304".to_string(),
            schemas: month4_schemas,
        };
        fs::write(
            output_path.join("202304.json"),
            serde_json::to_string_pretty(&month4_data)?,
        )?;

        // --- Call the function ---
        let evolutions = extract_schema_evolutions(output_path)?;

        // --- Assertions ---
        assert_eq!(evolutions.len(), 5, "Expected 5 schema evolution periods");

        let hash_a_v1 = calculate_fields_hash(&cols_a_v1);
        let hash_a_v2 = calculate_fields_hash(&cols_a_v2);
        let hash_b_v1 = calculate_fields_hash(&cols_b_v1);
        let hash_b_v2 = calculate_fields_hash(&cols_b_v2);
        let hash_c_v1 = calculate_fields_hash(&cols_c_v1);

        // Expected evolutions (order matters due to sorting in the function)
        let expected_evolutions = vec![
            SchemaEvolution {
                // TableA v1
                table_name: "TableA".to_string(),
                fields_hash: hash_a_v1.clone(),
                start_month: "202301".to_string(),
                end_month: "202302".to_string(),
                columns: cols_a_v1.clone(),
            },
            SchemaEvolution {
                // TableA v2
                table_name: "TableA".to_string(),
                fields_hash: hash_a_v2.clone(),
                start_month: "202303".to_string(),
                end_month: "202304".to_string(),
                columns: cols_a_v2.clone(),
            },
            SchemaEvolution {
                // TableB v1
                table_name: "TableB".to_string(),
                fields_hash: hash_b_v1.clone(),
                start_month: "202301".to_string(),
                end_month: "202301".to_string(),
                columns: cols_b_v1.clone(),
            },
            SchemaEvolution {
                // TableB v2
                table_name: "TableB".to_string(),
                fields_hash: hash_b_v2.clone(),
                start_month: "202302".to_string(),
                end_month: "202303".to_string(),
                columns: cols_b_v2.clone(),
            },
            SchemaEvolution {
                // TableC v1
                table_name: "TableC".to_string(),
                fields_hash: hash_c_v1.clone(),
                start_month: "202303".to_string(),
                end_month: "202304".to_string(),
                columns: cols_c_v1.clone(),
            },
        ];

        // To compare, it's easier if both are converted to a HashSet or sorted consistently.
        // The function already sorts them.
        assert_eq!(evolutions.len(), expected_evolutions.len());
        for (i, evo) in evolutions.iter().enumerate() {
            assert_eq!(
                evo, &expected_evolutions[i],
                "Mismatch at evolution index {}",
                i
            );
        }

        Ok(())
    }
}
