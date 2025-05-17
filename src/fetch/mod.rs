// src/fetch/mod.rs
// Add to Cargo.toml dependencies:
// reqwest = { version = "0.11", features = ["rustls-tls", "cookies"] }
// scraper = "0.14"
// url = "2.2"
// anyhow = "1.0"
// tokio = { version = "1", features = ["full"] }

use anyhow::Result;
use reqwest::Client;
use scraper::{Html, Selector};
use std::collections::BTreeMap;
use tokio::fs;
use tokio::task;
use url::Url;

/// Module for fetching ZIP file URLs from AEMO feeds
pub mod urls {
    use super::*;

    static CURRENT_FEED_URLS: &[&str] = &[
        "https://nemweb.com.au/Reports/Current/FPP/",
        "https://nemweb.com.au/Reports/Current/FPPDAILY/",
        "https://nemweb.com.au/Reports/Current/FPPRATES/",
        "https://nemweb.com.au/Reports/Current/FPPRUN/",
        "https://nemweb.com.au/Reports/Current/PD7Day/",
        "https://nemweb.com.au/Reports/Current/P5_Reports/",
    ];

    static ARCHIVE_FEED_URLS: &[&str] = &[
        "https://nemweb.com.au/Reports/Archive/FPPDAILY/",
        "https://nemweb.com.au/Reports/Archive/FPPRATES/",
        "https://nemweb.com.au/Reports/Archive/FPPRUN/",
        "https://nemweb.com.au/Reports/Archive/P5_Reports/",
    ];

    /// Fetch all ZIP URLs from the current feeds concurrently.
    pub async fn fetch_current_zip_urls(client: &Client) -> Result<BTreeMap<String, Vec<String>>> {
        fetch_zip_urls(client, CURRENT_FEED_URLS).await
    }

    /// Fetch all ZIP URLs from the archive feeds concurrently.
    pub async fn fetch_archive_zip_urls(client: &Client) -> Result<BTreeMap<String, Vec<String>>> {
        fetch_zip_urls(client, ARCHIVE_FEED_URLS).await
    }

    async fn fetch_zip_urls(
        client: &Client,
        feeds: &[&str],
    ) -> Result<BTreeMap<String, Vec<String>>> {
        // Parse a CSS selector for links ending with ".zip"
        let selector = Selector::parse(r#"a[href$=".zip"]"#)
            .expect("CSS selector for ZIP links should be valid");
        let mut handles = Vec::with_capacity(feeds.len());

        for &feed in feeds {
            let client = client.clone();
            let feed_url = feed.to_string();
            let selector = selector.clone();
            handles.push(task::spawn(async move {
                let base = Url::parse(&feed_url)?;
                let html = client
                    .get(&feed_url)
                    .send()
                    .await?
                    .error_for_status()?
                    .text()
                    .await?;
                let doc = Html::parse_document(&html);
                let links = doc
                    .select(&selector)
                    .filter_map(|e| e.value().attr("href"))
                    .filter_map(|href| base.join(href).ok())
                    .map(|u| u.to_string())
                    .collect::<Vec<_>>();
                Ok::<_, anyhow::Error>((feed_url, links))
            }));
        }

        let mut map = BTreeMap::new();
        for handle in handles {
            let (feed, links) = handle.await??;
            map.insert(feed, links);
        }

        Ok(map)
    }
}

/// Module for downloading a single ZIP to disk
pub mod zips {
    use super::*;
    use std::path::Path;

    /// Download the given ZIP URL and save it to the specified file path.
    pub async fn download_zip(client: &Client, url: &str, dest: impl AsRef<Path>) -> Result<()> {
        let resp = client.get(url).send().await?.error_for_status()?;
        let bytes = resp.bytes().await?;
        fs::write(dest, &bytes).await?;
        Ok(())
    }
}
