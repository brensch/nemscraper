// data/src/fetch/urls.rs
use anyhow::Result;
use reqwest::Client;
use scraper::{Html, Selector};
use std::collections::BTreeMap;
use tokio::task;
use url::Url;

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

async fn fetch_zip_urls(client: &Client, feeds: &[&str]) -> Result<BTreeMap<String, Vec<String>>> {
    let selector =
        Selector::parse(r#"a[href$=".zip"]"#).expect("Invalid CSS selector for .zip links");

    let mut handles = Vec::with_capacity(feeds.len());
    for &feed in feeds {
        let client = client.clone();
        let feed_url = feed.to_string();
        let sel = selector.clone();
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
                .select(&sel)
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
