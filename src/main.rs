mod duck;
mod fetch;

use fetch::urls;
use fetch::zips;
use reqwest::Client;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = Client::new();
    let feeds = urls::fetch_current_zip_urls(&client).await?;
    let some_zip = &feeds["https://nemweb.com.au/Reports/Current/FPP/"][0];
    zips::download_zip(&client, some_zip, "latest_fpp.zip").await?;
    Ok(())
}
