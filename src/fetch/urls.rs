use anyhow::Context;
use anyhow::Result;
use futures::stream::{self, StreamExt};
use futures::TryStreamExt;
use reqwest::Client;
use scraper::{Html, Selector};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::time::sleep;
use url::Url;

static CURRENT_FEED_URLS: &[&str] = &[
    // "https://nemweb.com.au/Reports/Current/Ancillary_Services_Payments/", // - csv only
    // "https://nemweb.com.au/Reports/Current/Causer_Pays/", // - non cid format. unfortunate.
    // "https://nemweb.com.au/Reports/Current/Causer_Pays_Elements/", // - csv only, needed though for translations
    // "https://nemweb.com.au/Reports/Current/CDEII/", // element type seemingly
    // "https://nemweb.com.au/Reports/Current/IBEI/", // elements
    "https://nemweb.com.au/Reports/Current/Adjusted_Prices_Reports/",
    "https://nemweb.com.au/Reports/Current/Bidmove_Complete/",
    "https://nemweb.com.au/Reports/Current/Billing/",
    "https://nemweb.com.au/Reports/Current/Causer_Pays_Scada/",
    "https://nemweb.com.au/Reports/Current/CSC_CSP_Settlements/",
    "https://nemweb.com.au/Reports/Current/Daily_Reports/",
    "https://nemweb.com.au/Reports/Current/DAILYOCD/",
    "https://nemweb.com.au/Reports/Current/Directions_Reconciliation/",
    "https://nemweb.com.au/Reports/Current/Dispatch_IRSR/",
    "https://nemweb.com.au/Reports/Current/DISPATCH_NEGATIVE_RESIDUE/",
    "https://nemweb.com.au/Reports/Current/Dispatch_Reports/",
    "https://nemweb.com.au/Reports/Current/Dispatch_SCADA/",
    "https://nemweb.com.au/Reports/Current/DISPATCHFCST/",
    "https://nemweb.com.au/Reports/Current/DispatchIS_Reports/",
    "https://nemweb.com.au/Reports/Current/Dispatchprices_PRE_AP/",
    "https://nemweb.com.au/Reports/Current/FPP/",
    "https://nemweb.com.au/Reports/Current/FPPDAILY/",
    "https://nemweb.com.au/Reports/Current/FPPRATES/",
    "https://nemweb.com.au/Reports/Current/FPPRUN/",
    "https://nemweb.com.au/Reports/Current/HistDemand/",
    "https://nemweb.com.au/Reports/Current/Marginal_Loss_Factors/",
    "https://nemweb.com.au/Reports/Current/MCCDispatch/",
    "https://nemweb.com.au/Reports/Current/Medium_Term_PASA_Reports/",
    "https://nemweb.com.au/Reports/Current/Mktsusp_Pricing/",
    "https://nemweb.com.au/Reports/Current/MTPASA_DUIDAvailability/",
    "https://nemweb.com.au/Reports/Current/MTPASA_RegionAvailability/",
    "https://nemweb.com.au/Reports/Current/Network/",
    "https://nemweb.com.au/Reports/Current/Next_Day_Actual_Gen/",
    "https://nemweb.com.au/Reports/Current/NEXT_DAY_AVAIL_SUBMISS_CLUSTER/",
    "https://nemweb.com.au/Reports/Current/NEXT_DAY_AVAIL_SUBMISS_DAY/",
    "https://nemweb.com.au/Reports/Current/Next_Day_Dispatch/",
    "https://nemweb.com.au/Reports/Current/Next_Day_Intermittent_DS/",
    "https://nemweb.com.au/Reports/Current/Next_Day_Intermittent_Gen_Scada/",
    "https://nemweb.com.au/Reports/Current/NEXT_DAY_MCCDISPATCH/",
    "https://nemweb.com.au/Reports/Current/Next_Day_Offer_Energy_SPARSE/",
    "https://nemweb.com.au/Reports/Current/Next_Day_Offer_FCAS_SPARSE/",
    "https://nemweb.com.au/Reports/Current/Next_Day_PreDispatch/",
    "https://nemweb.com.au/Reports/Current/Next_Day_PreDispatchD/",
    "https://nemweb.com.au/Reports/Current/Next_Day_Trading/",
    "https://nemweb.com.au/Reports/Current/P5_Reports/",
    "https://nemweb.com.au/Reports/Current/P5MINFCST/",
    "https://nemweb.com.au/Reports/Current/PD7Day/",
    "https://nemweb.com.au/Reports/Current/PDPASA/",
    "https://nemweb.com.au/Reports/Current/Predispatch_IRSR/",
    "https://nemweb.com.au/Reports/Current/Predispatch_Reports/",
    "https://nemweb.com.au/Reports/Current/Predispatch_Sensitivities/",
    "https://nemweb.com.au/Reports/Current/PREDISPATCHFCST/",
    "https://nemweb.com.au/Reports/Current/PredispatchIS_Reports/",
    "https://nemweb.com.au/Reports/Current/Public_Prices/",
    "https://nemweb.com.au/Reports/Current/ROOFTOP_PV/",
    "https://nemweb.com.au/Reports/Current/Settlements/",
    "https://nemweb.com.au/Reports/Current/SEVENDAYOUTLOOK_FULL/",
    "https://nemweb.com.au/Reports/Current/SEVENDAYOUTLOOK_PEAK/",
    "https://nemweb.com.au/Reports/Current/Short_Term_PASA_Reports/",
    "https://nemweb.com.au/Reports/Current/Trading_Cumulative_Price/",
    "https://nemweb.com.au/Reports/Current/Trading_IRSR/",
    "https://nemweb.com.au/Reports/Current/TradingIS_Reports/",
    "https://nemweb.com.au/Reports/Current/Vwa_Fcas_Prices/",
    "https://nemweb.com.au/Reports/Current/WDR_CAPACITY_NO_SCADA/",
];

static ARCHIVE_FEED_URLS: &[&str] = &[
    "https://nemweb.com.au/Reports/Archive/FPPDAILY/",
    "https://nemweb.com.au/Reports/Archive/FPPRATES/",
    "https://nemweb.com.au/Reports/Archive/FPPRUN/",
    "https://nemweb.com.au/Reports/Archive/P5_Reports/",
];
const MAX_RETRIES: usize = 3;
const RETRY_DELAY: Duration = Duration::from_secs(1);

/// Fetch all ZIP URLs from the current feeds concurrently.
pub async fn fetch_current_zip_urls(client: &Client) -> Result<BTreeMap<String, Vec<String>>> {
    fetch_zip_urls(client, CURRENT_FEED_URLS).await
}

/// Fetch all ZIP URLs from the archive feeds concurrently.
pub async fn fetch_archive_zip_urls(client: &Client) -> Result<BTreeMap<String, Vec<String>>> {
    fetch_zip_urls(client, ARCHIVE_FEED_URLS).await
}

/// Helper that does the retry loop for a single feed URL, returning (feed_url, Vec<zip-links>).
async fn fetch_feed_links(
    client: Client,
    feed_url: String,
    selector: Selector,
) -> Result<(String, Vec<String>)> {
    let mut attempt = 0;

    loop {
        attempt += 1;
        let resp = client.get(&feed_url).send().await;

        match resp {
            Ok(resp) if resp.status().is_success() => {
                let html = resp
                    .text()
                    .await
                    .with_context(|| format!("reading body of {}", feed_url))?;

                let base = Url::parse(&feed_url)
                    .with_context(|| format!("parsing base URL {}", feed_url))?;

                let links = Html::parse_document(&html)
                    .select(&selector)
                    .filter_map(|elem| elem.value().attr("href"))
                    .filter_map(|href| base.join(href).ok())
                    .map(|url| url.to_string())
                    .collect::<Vec<String>>();

                return Ok((feed_url, links));
            }

            Err(_) if attempt < MAX_RETRIES => {
                sleep(RETRY_DELAY).await;
                continue;
            }

            Ok(resp) => {
                return Err(anyhow::anyhow!(
                    "HTTP error {} when fetching {}",
                    resp.status(),
                    feed_url
                ));
            }

            Err(e) => {
                return Err(e).context(format!(
                    "network error when fetching {} on attempt {}",
                    feed_url, attempt
                ));
            }
        }
    }
}

/// Run all feed‐fetch tasks with max 3 concurrent requests, then collect into a BTreeMap.
async fn fetch_zip_urls(client: &Client, feeds: &[&str]) -> Result<BTreeMap<String, Vec<String>>> {
    // Pre‐parse the selector once
    let selector =
        Selector::parse(r#"a[href$=".zip"]"#).expect("invalid CSS selector for .zip links");

    // Convert to owned strings first to avoid lifetime issues
    let owned_feeds: Vec<String> = feeds.iter().map(|s| s.to_string()).collect();

    // Create a stream of futures, but limit to 3 concurrent executions
    let results: Vec<(String, Vec<String>)> =
        stream::iter(owned_feeds.into_iter().map(|feed_url| {
            let client = client.clone();
            let selector = selector.clone();
            // Call our helper
            fetch_feed_links(client, feed_url, selector)
        }))
        .buffer_unordered(3) // Limit to 3 concurrent requests
        .try_collect()
        .await?;

    // Flatten into a BTreeMap
    let mut map = BTreeMap::new();
    for (url, links) in results {
        map.insert(url, links);
    }
    Ok(map)
}
