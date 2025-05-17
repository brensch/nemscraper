// src/fetch.rs

use anyhow::{Context, Result};
use reqwest::Client;
use scraper::{Html, Selector};
use std::collections::BTreeMap;
use url::Url;

/// The two sets of URLs we want to scrape.
static DEFAULT_FEED_URLS: &[&str] = &[
    "https://nemweb.com.au/Reports/Current/FPP/",
    "https://nemweb.com.au/Reports/Current/FPPDAILY/",
    "https://nemweb.com.au/Reports/Current/FPPRATES/",
    "https://nemweb.com.au/Reports/Current/FPPRUN/",
    "https://nemweb.com.au/Reports/Current/PD7Day/",
    "https://nemweb.com.au/Reports/Current/P5_Reports/",
];

static DEFAULT_ARCHIVE_FEED_URLS: &[&str] = &[
    "https://nemweb.com.au/Reports/Archive/FPPDAILY/",
    "https://nemweb.com.au/Reports/Archive/FPPRATES/",
    "https://nemweb.com.au/Reports/Archive/FPPRUN/",
    "https://nemweb.com.au/Reports/Archive/P5_Reports/",
];

/// Fetches all `.zip` links from a single feed URL.
async fn fetch_zip_links(client: &Client, base: &Url) -> Result<Vec<Url>> {
    let html = client
        .get(base.clone())
        .send()
        .await
        .with_context(|| format!("GET {}", base))?
        .error_for_status()?
        .text()
        .await
        .with_context(|| format!("reading body from {}", base))?;

    let document = Html::parse_document(&html);
    let selector = Selector::parse(r#"a[href$=".zip"]"#).expect("selector should parse");

    Ok(document
        .select(&selector)
        .filter_map(|elem| elem.value().attr("href"))
        .filter_map(|href| base.join(href).ok())
        .collect::<Vec<_>>())
}

/// Generic helper: given a slice of directory URLs, fetch each
/// and build a map from that URL → list of `.zip` links.
async fn fetch_zips_for_urls(
    client: &Client,
    feeds: &[&str],
) -> Result<BTreeMap<String, Vec<String>>> {
    let mut out = BTreeMap::new();

    for &feed in feeds {
        let base = Url::parse(feed).with_context(|| format!("parsing feed URL {}", feed))?;

        let zips = fetch_zip_links(client, &base)
            .await
            .with_context(|| format!("fetching ZIP list from {}", feed))?;

        out.insert(
            feed.to_string(),
            zips.into_iter().map(|u| u.to_string()).collect(),
        );
    }

    Ok(out)
}

/// Public API: scrape the *current* feeds.
pub async fn fetch_all_feed_zips(client: &Client) -> Result<BTreeMap<String, Vec<String>>> {
    fetch_zips_for_urls(client, DEFAULT_FEED_URLS).await
}

/// Public API: scrape the *archive* feeds.
pub async fn fetch_all_archive_feed_zips(client: &Client) -> Result<BTreeMap<String, Vec<String>>> {
    fetch_zips_for_urls(client, DEFAULT_ARCHIVE_FEED_URLS).await
}
