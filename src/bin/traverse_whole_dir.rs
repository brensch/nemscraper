// src/bin/traverse_reports.rs

use anyhow::Result;
use reqwest::blocking::Client;
use reqwest::header::CONTENT_TYPE;
use scraper::{Html, Selector};
use serde::Serialize;
use std::{collections::HashSet, fs::File, io::Write, time::Duration};

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Record {
    Directory { url: String, zip_names: Vec<String> },
    File { url: String, sample: String },
}

fn main() -> Result<()> {
    // Starting URL
    let start_url = "https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/2025/MMSDM_2025_01/MMSDM_Historical_Data_SQLLoader/".to_string();

    // HTTP client with timeout
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .user_agent("Mozilla/5.0 (compatible;)")
        .build()?;

    // Track visited URLs and queue for BFS
    let mut visited = HashSet::new();
    let mut queue = vec![start_url.clone()];

    // Precompile link selector
    let link_sel = Selector::parse("a").unwrap();

    // Collected records
    let mut records = Vec::new();

    // BFS crawl
    while let Some(url) = queue.pop() {
        if !url.starts_with(&start_url) {
            continue;
        }
        if !visited.insert(url.clone()) {
            continue;
        }
        eprintln!("Visiting: {}", url);

        // Fetch the URL
        let resp = client.get(&url).send()?;
        let ct = resp
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        // Directory detection: text/html and trailing slash
        if ct.starts_with("text/html") && url.ends_with('/') {
            // Parse directory page
            let body = resp.text()?;
            let document = Html::parse_document(&body);
            let mut zip_names = Vec::new();
            let mut subdirs = Vec::new();
            let mut html_pages = Vec::new();

            for a in document.select(&link_sel) {
                if let Some(href) = a.value().attr("href") {
                    let href = href.trim();
                    if href.starts_with('#') || href == "../" {
                        continue;
                    }
                    // Resolve full URL
                    let full = if href.starts_with("http") {
                        href.to_string()
                    } else {
                        let base = reqwest::Url::parse(&url)?;
                        base.join(href)?.to_string()
                    };

                    if href.ends_with('/') {
                        subdirs.push(full.clone());
                    } else if full.to_lowercase().ends_with(".zip") {
                        if zip_names.len() < 10 {
                            if let Some(name) = full.rsplit('/').next() {
                                zip_names.push(name.to_string());
                            }
                        }
                    } else if full.to_lowercase().ends_with(".htm")
                        || full.to_lowercase().ends_with(".html")
                    {
                        html_pages.push(full.clone());
                    }
                }
            }

            // Recurse into subdirs only if no zips
            if zip_names.is_empty() {
                queue.extend(subdirs);
            }
            // Always enqueue HTML files for sampling
            queue.extend(html_pages);

            // Record the directory
            records.push(Record::Directory {
                url: url.clone(),
                zip_names,
            });
        } else if ct.starts_with("text/html") {
            // HTML file (e.g. .htm or .html)
            let body = resp.text()?;
            // Sample entire file (it's typically small)
            let sample = if body.len() < 100 * 1024 {
                body.clone()
            } else {
                format!("{}...", &body[..1_000])
            };
            records.push(Record::File {
                url: url.clone(),
                sample,
            });
        } else if !url.to_lowercase().ends_with(".zip") {
            // Non-HTML, non-zip file: sample bytes
            let bytes = client.get(&url).send()?.bytes()?;
            let sample = if bytes.len() < 100 * 1024 {
                String::from_utf8_lossy(&bytes).to_string()
            } else {
                format!("{}...", String::from_utf8_lossy(&bytes[..1_000]))
            };
            records.push(Record::File {
                url: url.clone(),
                sample,
            });
        }
    }

    // Write all records to JSON file
    let mut file = File::create("full_index.json")?;
    let json = serde_json::to_string_pretty(&records)?;
    file.write_all(json.as_bytes())?;
    println!("Wrote {} records to full_index.json", records.len());

    Ok(())
}
