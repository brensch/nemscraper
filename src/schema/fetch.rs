// src/schema/fetch.rs

use anyhow::{Context, Result};
use futures::{stream::FuturesUnordered, StreamExt};
use regex::Regex;
use reqwest::Client;
use scraper::{Html, Selector};
use serde_json;
use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{fs as tokio_fs, io::AsyncWriteExt, time::sleep};
use tracing::{debug, error, info, instrument, trace, warn};
use url::Url;

use super::ctl::parse_ctl;
use super::types::{CtlSchema, MonthSchema};

async fn get_text_core(client: &Client, url: &Url) -> Result<String> {
    debug!("Fetching text from {}", url);
    Ok(client
        .get(url.clone())
        .send()
        .await
        .with_context(|| format!("GET {} failed", url))?
        .error_for_status()
        .with_context(|| format!("Non-success status {}", url))?
        .text()
        .await
        .with_context(|| format!("Reading text from {}", url))?)
}

async fn get_text_with_retry(
    client: &Client,
    url: &Url,
    max_retries: u32,
    initial_backoff_ms: u64,
) -> Result<String> {
    let mut attempts = 0;
    loop {
        match get_text_core(client, url).await {
            Ok(t) => return Ok(t),
            Err(e) if attempts < max_retries => {
                attempts += 1;
                let backoff = initial_backoff_ms * 2u64.pow(attempts - 1);
                warn!(%url, attempt = attempts, delay_ms = backoff, error = %e, "Retrying");
                sleep(Duration::from_millis(backoff)).await;
            }
            Err(e) => {
                error!(%url, error = %e, "Exhausted retries");
                return Err(e);
            }
        }
    }
}

async fn list_dirs(client: &Client, dir: &Url) -> Result<Vec<Url>> {
    debug!("Listing directories under {}", dir);
    let body = get_text_core(client, dir).await?;
    let doc = Html::parse_document(&body);
    let sel = Selector::parse("a[href]").unwrap();
    let mut dirs = Vec::new();
    for el in doc.select(&sel) {
        if let Some(href) = el.value().attr("href") {
            if href.ends_with('/') && !el.text().any(|t| t.contains("Parent Directory")) {
                if let Ok(full) = dir.join(href) {
                    trace!(url = %full, "Found subdirectory");
                    dirs.push(full);
                }
            }
        }
    }
    Ok(dirs)
}

async fn list_ctl_urls(client: &Client, ctl_dir: &Url) -> Result<Vec<Url>> {
    debug!("Listing CTL files under {}", ctl_dir);
    let body = get_text_core(client, ctl_dir).await?;
    let doc = Html::parse_document(&body);
    let sel = Selector::parse("a[href]").unwrap();
    let re = Regex::new(r"(?i)\.ctl$").unwrap();
    let mut urls = Vec::new();
    for el in doc.select(&sel) {
        if let Some(href) = el.value().attr("href") {
            if re.is_match(href) {
                if let Ok(full) = ctl_dir.join(href) {
                    trace!(url = %full, "Found CTL");
                    urls.push(full);
                }
            }
        }
    }
    Ok(urls)
}

fn month_code_from_url(u: &Url) -> Option<String> {
    let re = Regex::new(r"MMSDM_(\d{4})_(\d{2})/?$").ok()?;
    let caps = re.captures(u.path())?;
    Some(format!("{}{}", &caps[1], &caps[2]))
}

/// Fetch all unseen months, parse their CTLs, and write `<YYYYMM>.json`.
#[instrument(level = "info", skip(client, output_dir))]
pub async fn fetch_all<P: AsRef<Path>>(client: &Client, output_dir: P) -> Result<Vec<MonthSchema>> {
    let out_dir = output_dir.as_ref();
    fs::create_dir_all(out_dir).with_context(|| format!("creating {:?}", out_dir))?;

    // 1. Read existing `.json` files and collect their YYYYMM prefixes
    let done: HashSet<String> = fs::read_dir(out_dir)?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let fname = entry.file_name().to_string_lossy().to_string();
            // strip only if it ends with ".json"
            fname
                .strip_suffix(".json")
                // ensure the remaining part is exactly 6 chars (YYYYMM)
                .filter(|prefix| prefix.len() == 6 && prefix.chars().all(|c| c.is_ascii_digit()))
                .map(|prefix| prefix.to_string())
        })
        .collect();

    debug!(count = done.len(), "already-fetched months");

    // 2. Crawl archive dirs to build list of month URLs
    let base = Url::parse("https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/")?;
    let mut months = Vec::new();
    for year in list_dirs(client, &base).await? {
        months.extend(list_dirs(client, &year).await?);
    }
    // sort by YYYYMM
    months.sort_by_key(|u| month_code_from_url(u));
    // **reverse** so we process newest months first
    months.reverse();

    const MAX_CONCURRENCY: usize = 3;
    const MAX_RETRIES: u32 = 3;
    const BACKOFF_MS: u64 = 500;

    let mut tasks = FuturesUnordered::new();
    let mut results = Vec::new();

    for month_url in months {
        if let Some(code) = month_code_from_url(&month_url) {
            // 3. Skip if we've already fetched this month
            if done.contains(&code) {
                info!(month = %code, "skipping (already fetched)");
                continue;
            }
            let ctl_dir = month_url.join("MMSDM_Historical_Data_SQLLoader/CTL/")?;
            let client_c = client.clone();
            let out_c = out_dir.to_path_buf();

            tasks.push(process_month(
                client_c,
                code.clone(),
                ctl_dir,
                out_c,
                MAX_RETRIES,
                BACKOFF_MS,
            ));

            // throttle concurrency
            if tasks.len() >= MAX_CONCURRENCY {
                if let Some(res) = tasks.next().await {
                    if let Ok(Some(ms)) = res {
                        results.push(ms);
                    }
                }
            }
        }
    }

    // drain remaining tasks
    while let Some(res) = tasks.next().await {
        if let Ok(Some(ms)) = res {
            results.push(ms);
        }
    }

    Ok(results)
}

#[instrument(level = "info", skip(client, ctl_initial_backoff_ms))]
async fn process_month(
    client: Client,
    code: String,
    ctl_dir: Url,
    output_dir: PathBuf,
    ctl_max_retries: u32,
    ctl_initial_backoff_ms: u64,
) -> Result<Option<MonthSchema>> {
    let ctl_urls = list_ctl_urls(&client, &ctl_dir).await?;
    if ctl_urls.is_empty() {
        warn!(month = &code, "no CTLs");
        return Ok(None);
    }

    let mut schemas = Vec::new();
    for url in ctl_urls {
        let text = match get_text_with_retry(&client, &url, ctl_max_retries, ctl_initial_backoff_ms)
            .await
        {
            Ok(t) => t,
            Err(e) => {
                warn!(error = %e, url = %url, "skipping CTL");
                continue;
            }
        };

        match parse_ctl(&text) {
            Ok(mut cs) => {
                if cs.month != code {
                    warn!(ctl_month = &cs.month, dir_month = &code, "mismatch");
                    cs.month = code.clone();
                }
                schemas.push(cs);
            }
            Err(e) => warn!(error = %e, "parse_ctl failed"),
        }
    }

    if schemas.is_empty() {
        return Ok(None);
    }

    let ms = MonthSchema {
        month: code.clone(),
        schemas,
    };
    let path = output_dir.join(format!("{}.json", code));
    let json = serde_json::to_string_pretty(&ms)?;
    let mut f = tokio_fs::File::create(&path).await?;
    f.write_all(json.as_bytes()).await?;
    Ok(Some(ms))
}
