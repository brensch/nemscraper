// src/main.rs

mod fetch;

use anyhow::{Context, Result}; // <— need Context here
use fetch::{fetch_all_archive_feed_zips, fetch_all_feed_zips};
use reqwest::Client;

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::builder()
        .cookie_store(true)
        .build()
        .context("building HTTP client")?;

    let current = fetch_all_feed_zips(&client).await?;
    let archive = fetch_all_archive_feed_zips(&client).await?;

    println!("number of feeds: {}", current.len());
    println!(
        "number of zips: {}",
        current.values().map(|v| v.len()).sum::<usize>()
    );

    println!("number of archives: {}", archive.len());
    println!(
        "number of archive zips: {}",
        archive.values().map(|v| v.len()).sum::<usize>()
    );

    Ok(())
}
