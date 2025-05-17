use anyhow::{bail, Result};
use regex::Regex;
use reqwest::blocking::Client;
use scraper::{Html, Selector};
use serde::Serialize;
use url::Url;

/// Fetches and parses all sub-directory URLs from a directory listing.
fn list_dirs(client: &Client, dir_url: &Url) -> Result<Vec<Url>> {
    let body = client.get(dir_url.clone()).send()?.text()?;
    let doc = Html::parse_document(&body);
    let sel = Selector::parse("a[href]").unwrap();

    let mut urls = Vec::new();
    for elem in doc.select(&sel) {
        let href = match elem.value().attr("href") {
            Some(h) => h,
            None => continue,
        };
        let text = elem.text().collect::<String>();

        if href.ends_with('/') && !text.contains("Parent Directory") {
            let full_url = dir_url.join(href)?;
            urls.push(full_url);
        }
    }
    Ok(urls)
}

/// Lists all `.ctl` URLs in a given directory.
fn list_ctl_urls(client: &Client, month_url: &Url) -> Result<Vec<Url>> {
    let body = client.get(month_url.clone()).send()?.text()?;
    let doc = Html::parse_document(&body);
    let sel = Selector::parse("a[href]").unwrap();
    let ctl_regex = Regex::new(r"(?i)\.ctl$").unwrap();

    let mut urls = Vec::new();
    for element in doc.select(&sel) {
        if let Some(href) = element.value().attr("href") {
            if ctl_regex.is_match(href) {
                let full = month_url.join(href)?;
                urls.push(full);
            }
        }
    }
    Ok(urls)
}

/// For each month directory, descend into `CTL/` and collect all `.ctl` URLs.
pub fn get_all_ctl_urls(client: &Client) -> Result<Vec<Url>> {
    let base = Url::parse("https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/")?;

    let years = list_dirs(client, &base)?;
    if years.is_empty() {
        bail!("No monthly directories found under {}", base);
    }

    let months = years
        .iter()
        .filter_map(|year_url| list_dirs(client, year_url).ok())
        .flatten();

    let ctl_urls = months
        .filter_map(|month_url| {
            let ctl_dir = month_url
                .join("MMSDM_Historical_Data_SQLLoader/CTL/")
                .ok()?;
            list_ctl_urls(client, &ctl_dir).ok()
        })
        .flatten()
        .collect();

    Ok(ctl_urls)
}

/// Represents a single column from the CTL file.
#[derive(Debug, PartialEq, Serialize)]
pub struct Column {
    pub name: String,
    pub ty: String,
    pub format: Option<String>,
}

/// The extracted schema for one CTL file
#[derive(Debug, PartialEq, Serialize)]
pub struct CtlSchema {
    pub table: String,
    pub month: String,        // e.g. "200907"
    pub columns: Vec<Column>, // in definition order
}

/// Parses a CTL file’s contents into a `CtlSchema`.
pub fn parse_ctl(contents: &str) -> Result<CtlSchema> {
    // 1) Capture YYYYMM by finding any 12-digit timestamp before .csv/.CSV
    let month_re = Regex::new(r"(?mi)^INFILE\s+\S*?(\d{6})\d{6}\.(?:csv)$")?;
    let month = month_re
        .captures(contents)
        .and_then(|cap| cap.get(1).map(|m| m.as_str().to_string()))
        .ok_or_else(|| anyhow::anyhow!("Failed to find month in INFILE"))?;

    // 2) Table name after APPEND INTO TABLE
    let table_re = Regex::new(r"(?mi)^APPEND INTO TABLE\s+(\w+)")?;
    let table = table_re
        .captures(contents)
        .and_then(|cap| cap.get(1).map(|m| m.as_str().to_string()))
        .ok_or_else(|| anyhow::anyhow!("Failed to find table name"))?;

    // 3) Grab the block inside TRAILING NULLCOLS(...)
    let cols_re = Regex::new(r"(?s)TRAILING NULLCOLS\s*\(\s*(.*?)\s*\)\s*(?:--|$)")?;
    let cols_block = cols_re
        .captures(contents)
        .and_then(|cap| cap.get(1).map(|m| m.as_str()))
        .ok_or_else(|| anyhow::anyhow!("Failed to find column list"))?;

    // 4) Split on commas, skip FILLER, record name/type/[format]
    let mut columns = Vec::new();
    for raw in cols_block.split(',') {
        let s = raw.trim();
        if s.is_empty() {
            continue;
        }
        let parts: Vec<&str> = s.split_whitespace().collect();
        if parts.len() < 2 {
            bail!("Unexpected column definition: {}", s);
        }
        let name = parts[0].to_string();
        let ty = parts[1].to_string();
        if ty.eq_ignore_ascii_case("FILLER") {
            continue;
        }
        let format = if parts.len() > 2 {
            Some(parts[2..].join(" "))
        } else {
            None
        };
        columns.push(Column { name, ty, format });
    }

    Ok(CtlSchema {
        table,
        month,
        columns,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// ensure our existing URL code still compiles & runs
    fn test_client() -> Client {
        Client::builder()
            .user_agent("test-client")
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap()
    }

    #[test]
    fn test_list_hrefs_from_known_year() {
        let client = test_client();
        let year_url =
            Url::parse("https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/2009/")
                .unwrap();
        let hrefs = list_dirs(&client, &year_url).unwrap();
        assert!(!hrefs.is_empty());
    }

    #[test]
    fn test_list_ctl_urls_from_known_month() {
        let client = test_client();
        let month_url = Url::parse(
            "https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/\
             2009/MMSDM_2009_07/MMSDM_Historical_Data_SQLLoader/CTL/",
        )
        .unwrap();
        let ctl_urls = list_ctl_urls(&client, &month_url).unwrap();
        assert!(ctl_urls.iter().any(|u| u.as_str().ends_with(".ctl")));
    }

    const EX1: &str = r#"-- ... example 1 ...
LOAD DATA
INFILE PUBLIC_DVD_SETFCASREGIONRECOVERY_200907010000.csv
APPEND INTO TABLE SETFCASREGIONRECOVERY
TRAILING NULLCOLS
(
  row_type FILLER,
  SETTLEMENTDATE DATE "yyyy/mm/dd hh24:mi:ss",
  VERSIONNO FLOAT EXTERNAL,
  BIDTYPE CHAR(10),
  LASTCHANGED DATE "yyyy/mm/dd hh24:mi:ss"
)
"#;

    const EX2: &str = r#"-- ... example 2 ...
LOAD DATA
INFILE PUBLIC_ARCHIVE#STPASA_CASESOLUTION#FILE01#202503010000.CSV
APPEND INTO TABLE STPASA_CASESOLUTION
TRAILING NULLCOLS
(
  row_type FILLER,
  RUN_DATETIME DATE "YYYY/MM/DD HH24:MI:SS",
  PASAVERSION CHAR(10)
)
"#;

    #[test]
    fn test_parse_example1() {
        let schema = parse_ctl(EX1).unwrap();
        assert_eq!(schema.month, "200907");
        assert_eq!(schema.table, "SETFCASREGIONRECOVERY");
        assert_eq!(
            schema
                .columns
                .iter()
                .map(|c| &c.name[..])
                .collect::<Vec<_>>(),
            &["SETTLEMENTDATE", "VERSIONNO", "BIDTYPE", "LASTCHANGED"]
        );
        assert_eq!(
            schema
                .columns
                .iter()
                .find(|c| c.name == "SETTLEMENTDATE")
                .unwrap()
                .format
                .as_deref(),
            Some("\"yyyy/mm/dd hh24:mi:ss\"")
        );
    }

    #[test]
    fn test_parse_example2() {
        let schema = parse_ctl(EX2).unwrap();
        assert_eq!(schema.month, "202503");
        assert_eq!(schema.table, "STPASA_CASESOLUTION");
        assert!(schema
            .columns
            .iter()
            .any(|c| c.name == "PASAVERSION" && c.ty == "CHAR(10)"));
        let run_dt_fmt = &schema
            .columns
            .iter()
            .find(|c| c.name == "RUN_DATETIME")
            .unwrap()
            .format;
        assert!(run_dt_fmt
            .as_ref()
            .unwrap()
            .contains("YYYY/MM/DD HH24:MI:SS"));
    }
}

fn main() -> Result<()> {
    // 1) Prepare HTTP client
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()?;

    // 2) Gather all CTL URLs
    let ctl_urls = get_all_ctl_urls(&client)?;
    if ctl_urls.is_empty() {
        println!("No CTL files found.");
        return Ok(());
    }

    // 3) For each URL: download, parse, and bucket by month
    use std::collections::HashMap;
    let mut by_month: HashMap<String, Vec<CtlSchema>> = HashMap::new();

    for url in ctl_urls {
        let text = client.get(url.clone()).send()?.text()?;
        let schema = parse_ctl(&text).unwrap_or_else(|e| panic!("Failed to parse {}: {}", url, e));
        by_month
            .entry(schema.month.clone())
            .or_default()
            .push(schema);
    }

    // 4) Ensure output directory exists
    std::fs::create_dir_all("schemas")?;

    // 5) Write one JSON file per month
    for (month, schemas) in by_month {
        let path = format!("schemas/{}.json", month);
        let file = std::fs::File::create(&path)?;
        serde_json::to_writer_pretty(file, &schemas)?;
        println!("Wrote {} schemas to {}", schemas.len(), path);
    }

    Ok(())
}
