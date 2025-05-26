// data/src/fetch/urls.rs
use anyhow::Result;
use reqwest::Client;
use scraper::{Html, Selector};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::time::sleep;
use url::Url;

// static CURRENT_FEED_URLS: &[&str] = &[
//     "https://nemweb.com.au/Reports/Current/FPP/",
//     // "https://nemweb.com.au/Reports/Current/FPPDAILY/",
//     // "https://nemweb.com.au/Reports/Current/FPPRATES/",
//     // "https://nemweb.com.au/Reports/Current/FPPRUN/",
//     // "https://nemweb.com.au/Reports/Current/PD7Day/",
//     // "https://nemweb.com.au/Reports/Current/P5_Reports/",
// ];

static CURRENT_FEED_URLS: &[&str] = &[
    "https://nemweb.com.au/Reports/Current/Adjusted_Prices_Reports/",
    // "https://nemweb.com.au/Reports/Current/Ancillary_Services_Payments/", // - csv only
    "https://nemweb.com.au/Reports/Current/Bidmove_Complete/",
    "https://nemweb.com.au/Reports/Current/Billing/",
    "https://nemweb.com.au/Reports/Current/Causer_Pays/",
    // "https://nemweb.com.au/Reports/Current/Causer_Pays_Elements/", // - csv only, needed though for translations
    "https://nemweb.com.au/Reports/Current/Causer_Pays_Scada/",
    // "https://nemweb.com.au/Reports/Current/CDEII/", // element type seemingly
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
    // "https://nemweb.com.au/Reports/Current/IBEI/", // elements
    "https://nemweb.com.au/Reports/Current/Marginal_Loss_Factors/",
    "https://nemweb.com.au/Reports/Current/MCCDispatch/",
    "https://nemweb.com.au/Reports/Current/Medium_Term_PASA_Reports/",
    "https://nemweb.com.au/Reports/Current/Mktsusp_Pricing/",
    // done to here
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

/// Fetch all ZIP URLs from the current feeds concurrently.
pub async fn fetch_current_zip_urls(client: &Client) -> Result<BTreeMap<String, Vec<String>>> {
    fetch_zip_urls(client, CURRENT_FEED_URLS).await
}

/// Fetch all ZIP URLs from the archive feeds concurrently.
pub async fn fetch_archive_zip_urls(client: &Client) -> Result<BTreeMap<String, Vec<String>>> {
    fetch_zip_urls(client, ARCHIVE_FEED_URLS).await
}

const MAX_RETRIES: usize = 3;
const RETRY_DELAY: Duration = Duration::from_secs(1);

async fn fetch_zip_urls(client: &Client, feeds: &[&str]) -> Result<BTreeMap<String, Vec<String>>> {
    let selector =
        Selector::parse(r#"a[href$=".zip"]"#).expect("Invalid CSS selector for .zip links");

    let mut map = BTreeMap::new();

    for &feed in feeds {
        let feed_url = feed.to_string();
        let sel = selector.clone();
        let mut attempt = 0;

        // retry loop
        let links = loop {
            attempt += 1;

            // 1) fetch page
            let resp = client.get(&feed_url).send().await;
            match resp {
                Ok(resp) if resp.status().is_success() => {
                    // 2) get body text
                    match resp.text().await {
                        Ok(html) => {
                            // 3) parse links
                            let base = Url::parse(&feed_url)?;
                            let urls = Html::parse_document(&html)
                                .select(&sel)
                                .filter_map(|e| e.value().attr("href"))
                                .filter_map(|href| base.join(href).ok())
                                .map(|u| u.to_string())
                                .collect::<Vec<_>>();
                            break urls;
                        }
                        Err(_) if attempt < MAX_RETRIES => {
                            sleep(RETRY_DELAY).await;
                            continue;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
                Err(_) if attempt < MAX_RETRIES => {
                    sleep(RETRY_DELAY).await;
                    continue;
                }
                Ok(resp) => return Err(anyhow::anyhow!("HTTP error: {}", resp.status())),
                Err(e) => return Err(e.into()),
            }
        };

        map.insert(feed_url, links);
    }

    Ok(map)
}
