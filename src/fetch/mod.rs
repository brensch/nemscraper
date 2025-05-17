// src/fetch/mod.rs
// Add to Cargo.toml:
// reqwest = { version = "0.11", features = ["rustls-tls"] }
// scraper = "0.14"
// url = "2.2"
// anyhow = "1.0"

use anyhow::Result;
use reqwest::Client;
use scraper::{Html, Selector};
use std::collections::BTreeMap;
use tokio::fs;
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

    /// Fetch all ZIP URLs from the current feeds.
    pub async fn fetch_current_zip_urls(client: &Client) -> Result<BTreeMap<String, Vec<String>>> {
        fetch_zip_urls(client, CURRENT_FEED_URLS).await
    }

    /// Fetch all ZIP URLs from the archive feeds.
    pub async fn fetch_archive_zip_urls(client: &Client) -> Result<BTreeMap<String, Vec<String>>> {
        fetch_zip_urls(client, ARCHIVE_FEED_URLS).await
    }

    async fn fetch_zip_urls(
        client: &Client,
        feeds: &[&str],
    ) -> Result<BTreeMap<String, Vec<String>>> {
        let mut map = BTreeMap::new();
        let selector = Selector::parse(r#"a[href$=".zip"]"#).expect("selector should parse");

        for &feed in feeds {
            let base = Url::parse(feed)?;
            let html = client
                .get(feed)
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
                .collect();
            map.insert(feed.to_string(), links);
        }

        Ok(map)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use anyhow::{Context, Result};

        #[tokio::test]
        async fn test_fetch_all_feed_zips_returns_urls() -> Result<()> {
            // Build a real HTTP client
            let client = Client::builder()
                .cookie_store(true)
                .build()
                .context("building HTTP client for tests")?;

            // Call the function under test
            let result = fetch_current_zip_urls(&client).await?;

            // We should get a map containing each feed URL
            assert_eq!(result.len(), CURRENT_FEED_URLS.len());

            // And each entry should have at least zero or more URL strings
            for (feed, urls) in result {
                // Ensure the key matches our feed
                assert!(CURRENT_FEED_URLS.contains(&feed.as_str()));
                // Ensure each URL starts with the feed URL
                for url in urls {
                    assert!(url.starts_with(&feed));
                }
            }

            Ok(())
        }
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
