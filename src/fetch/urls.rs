use anyhow::Context;
use anyhow::Result;
use futures::stream::{self, StreamExt};
use futures::TryStreamExt;
use once_cell::sync::Lazy;
use reqwest::Client;
use scraper::{Html, Selector};
use std::collections::BTreeMap;
use std::thread;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task;
use tokio::time::interval;
use tokio::time::sleep;
use tokio::time::Interval;
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
/// Parse the CSS selector exactly one time, at program start.
/// We want `a[href$=".zip"]` to be reused on every fetch, so we don't re‐parse it each time.
static ZIP_SELECTOR: Lazy<Selector> = Lazy::new(|| {
    Selector::parse(r#"a[href$=".zip"]"#).expect("Invalid CSS selector for .zip links")
});

/// Maximum number of retries when fetching a feed page
const MAX_RETRIES: u32 = 3;
/// Delay between retries
const RETRY_DELAY: Duration = Duration::from_secs(5);

/// Helper that does the retry loop for a single feed URL, returning `Vec<zip‐links>`.
///
/// NOTE: we removed the `selector` parameter—inside this function we now refer to the
///       global `ZIP_SELECTOR`.
async fn fetch_feed_links(client: Client, feed_url: String) -> Result<Vec<String>> {
    let mut attempt = 0;

    loop {
        attempt += 1;
        let resp = client.get(&feed_url).send().await;

        match resp {
            // 1) We got an HTTP response; check if it's 2xx
            Ok(resp) if resp.status().is_success() => {
                // a) read the body as text
                let html = resp
                    .text()
                    .await
                    .with_context(|| format!("reading body of {}", feed_url))?;

                // b) parse feed_url into a Url so we can resolve relative <a href>
                let base = Url::parse(&feed_url)
                    .with_context(|| format!("parsing base URL {}", feed_url))?;

                // c) extract all <a href="…"> elements ending in `.zip`
                let links = Html::parse_document(&html)
                    .select(&*ZIP_SELECTOR) // use the global selector
                    .filter_map(|elem| elem.value().attr("href"))
                    .filter_map(|href| base.join(href).ok())
                    .map(|url| url.to_string())
                    .collect::<Vec<String>>();

                return Ok(links);
            }

            // 2) Transient network error, and we still have retries left
            Err(_) if attempt < MAX_RETRIES => {
                sleep(RETRY_DELAY).await;
                continue;
            }

            // 3) We got a non‐2xx status on the final attempt (or once attempt ≥ MAX_RETRIES)
            Ok(resp) => {
                return Err(anyhow::anyhow!(
                    "HTTP error {} when fetching {}",
                    resp.status(),
                    feed_url
                ));
            }

            // 4) A network error and we exhausted all retries
            Err(e) => {
                return Err(e).context(format!(
                    "Network error when fetching {} on attempt {}",
                    feed_url, attempt
                ));
            }
        }
    }
}

/// “Tick” every five minutes, but split that five‐minute window evenly across all feeds.
/// Each pass fetches exactly one feed’s ZIP links, then sends them down `url_tx`.
///
/// - `client`   : shared `reqwest::Client` for all requests
/// - `feeds`    : slice of feed URLs (e.g. `&["https://example.com/feed1", "https://.../feed2"]`)
/// - `url_tx`   : an `mpsc::UnboundedSender<String>` where each discovered ZIP link will be sent
///
/// This function never returns; it continuously loops, fetching one feed per tick.
pub fn spawn_fetch_zip_urls(
    client: Client,
    url_tx: mpsc::UnboundedSender<String>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Convert &str feeds → owned String vector
        let owned_feeds: Vec<String> = CURRENT_FEED_URLS.iter().map(|s| s.to_string()).collect();
        let num_feeds = owned_feeds.len() as u32;

        // Each feed should be fetched once per 5 minutes
        let frequency_seconds = 5 * 60; // 300
        let tick_secs = frequency_seconds / num_feeds; // e.g. 100s if 3 feeds
        let tick_duration = Duration::from_secs(tick_secs as u64);

        let mut ticker = tokio::time::interval(tick_duration);
        let mut current_feed: usize = 0;

        loop {
            // Wait until the next tick (asynchronously)
            ticker.tick().await;

            let feed_url = owned_feeds[current_feed].clone();

            // Call the async fetch with retries
            match fetch_feed_links(client.clone(), feed_url.clone()).await {
                Ok(links) => {
                    for zip_link in links {
                        let _ = url_tx.send(zip_link);
                    }
                }
                Err(e) => {
                    eprintln!("Failed to fetch links from {}: {}", feed_url, e);
                }
            }

            current_feed = (current_feed + 1) % owned_feeds.len();
        }
    })
}
