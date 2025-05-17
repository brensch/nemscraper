//! extract_schemas.rs
//!
//! Downloads and parses NEM `.ctl` control-files month-by-month,
//! writing `schemas/YYYYMM.json` as soon as each month is finished.
//!
//! ── How it works ──
//!  • At start-up we list any `schemas/*.json` that already exist and
//!    skip those months entirely.
//!  • We walk years → months → ctl-files **sequentially** (no huge
//!    in-memory list).
//!  • As soon as all `.ctl`s for one month are parsed we write one
//!    pretty-printed JSON file to disk and proceed to the next month.

use anyhow::{bail, Context, Result};
use regex::Regex;
use reqwest::blocking::Client;
use scraper::{Html, Selector};
use serde::Serialize;
use std::{collections::HashSet, fs, path::Path};
use url::Url;

/* ────────────────────────── structures ───────────────────────── */

#[derive(Debug, PartialEq, Serialize)]
pub struct Column {
    pub name: String,
    pub ty: String,
    pub format: Option<String>,
}

#[derive(Debug, PartialEq, Serialize)]
pub struct CtlSchema {
    pub table: String,
    pub month: String,        // e.g. "200907"
    pub columns: Vec<Column>, // in definition order
}

/* ─────────────────────────── parsing ─────────────────────────── */

/// Parse a CTL file’s contents into a `CtlSchema`.
pub fn parse_ctl(contents: &str) -> Result<CtlSchema> {
    // Month: tolerate weird delimiters, ignore case on .csv
    let month_re = Regex::new(r"(?mi)INFILE[^\n]*?(\d{6})\D*\d{6}[^\n]*\.[cC][sS][vV]")?;
    let month = month_re
        .captures(contents)
        .and_then(|c| c.get(1).map(|m| m.as_str().to_owned()))
        .ok_or_else(|| anyhow::anyhow!("Failed to find month in INFILE"))?;

    let table_re = Regex::new(r"(?mi)^APPEND INTO TABLE\s+(\w+)")?;
    let table = table_re
        .captures(contents)
        .and_then(|c| c.get(1).map(|m| m.as_str().to_owned()))
        .ok_or_else(|| anyhow::anyhow!("Failed to find table name"))?;

    let cols_re = Regex::new(r"(?s)TRAILING NULLCOLS\s*\(\s*(.*?)\s*\)\s*(?:--|$)")?;
    let cols_block = cols_re
        .captures(contents)
        .and_then(|c| c.get(1).map(|m| m.as_str()))
        .ok_or_else(|| anyhow::anyhow!("Failed to find column list"))?;

    let mut columns = Vec::new();
    for raw in cols_block.split(',') {
        let s = raw.trim();
        if s.is_empty() {
            continue;
        }
        let mut parts = s.split_whitespace();
        let name = parts
            .next()
            .ok_or_else(|| anyhow::anyhow!("Bad column: {s}"))?
            .to_owned();
        let ty = parts
            .next()
            .ok_or_else(|| anyhow::anyhow!("Bad column: {s}"))?
            .to_owned();
        if ty.eq_ignore_ascii_case("FILLER") {
            continue;
        }
        let format = parts.next().map(|rest| {
            let mut s = rest.to_owned();
            for p in parts {
                s.push(' ');
                s.push_str(p);
            }
            s
        });
        columns.push(Column { name, ty, format });
    }

    Ok(CtlSchema {
        table,
        month,
        columns,
    })
}

/* ─────────────────────── directory helpers ───────────────────── */

fn list_dirs(client: &Client, dir: &Url) -> Result<Vec<Url>> {
    let body = client.get(dir.clone()).send()?.text()?;
    let doc = Html::parse_document(&body);
    let sel = Selector::parse("a[href]").unwrap();
    Ok(doc
        .select(&sel)
        .filter_map(|e| {
            let href = e.value().attr("href")?;
            if !href.ends_with('/') {
                return None;
            }
            let txt = e.text().collect::<String>();
            if txt.contains("Parent Directory") {
                return None;
            }
            dir.join(href).ok()
        })
        .collect())
}

fn list_ctl_urls(client: &Client, ctl_dir: &Url) -> Result<Vec<Url>> {
    let body = client.get(ctl_dir.clone()).send()?.text()?;
    let doc = Html::parse_document(&body);
    let sel = Selector::parse("a[href]").unwrap();
    let re = Regex::new(r"(?i)\.ctl$").unwrap();
    Ok(doc
        .select(&sel)
        .filter_map(|e| {
            let h = e.value().attr("href")?;
            if re.is_match(h) {
                ctl_dir.join(h).ok()
            } else {
                None
            }
        })
        .collect())
}

/// Extract `YYYYMM` from a month directory URL such as
/// `.../MMSDM_2009_07/`.
fn month_code_from_month_url(u: &Url) -> Option<String> {
    let re = Regex::new(r"MMSDM_(\d{4})_(\d{2})/?$").ok()?;
    let caps = re.captures(u.path())?;
    Some(format!("{}{}", &caps[1], &caps[2]))
}

/* ──────────────────────────── main ──────────────────────────── */

fn main() -> Result<()> {
    let output_dir = Path::new("schemas");
    fs::create_dir_all(output_dir)?;

    // 0) Record which months we already have
    let mut done: HashSet<String> = fs::read_dir(output_dir)?
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().into_string().ok())
        .filter_map(|name| {
            if name.ends_with(".json") && name.len() == 11 {
                Some(name[..6].to_owned()) // strip ".json"
            } else {
                None
            }
        })
        .collect();

    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()?;

    let base = Url::parse("https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/")?;
    let years = list_dirs(&client, &base).context("listing years")?;

    for year in years {
        let months = list_dirs(&client, &year).with_context(|| format!("months in {year}"))?;

        for month_url in months {
            let code = match month_code_from_month_url(&month_url) {
                Some(c) => c,
                None => continue,
            };

            if done.contains(&code) {
                println!("Skipping {code} (already done)");
                continue;
            }

            let ctl_dir = match month_url.join("MMSDM_Historical_Data_SQLLoader/CTL/") {
                Ok(u) => u,
                Err(e) => {
                    eprintln!("Bad ctl dir for {month_url}: {e}");
                    continue;
                }
            };

            println!("Processing month {code} …");
            let ctl_urls = match list_ctl_urls(&client, &ctl_dir) {
                Ok(v) if !v.is_empty() => v,
                Ok(_) => {
                    eprintln!("No CTLs in {ctl_dir}");
                    continue;
                }
                Err(e) => {
                    eprintln!("Failed listing CTLs for {ctl_dir}: {e}");
                    continue;
                }
            };

            let mut schemas = Vec::new();
            for url in ctl_urls {
                match client.get(url.clone()).send().and_then(|r| r.text()) {
                    Ok(text) => match parse_ctl(&text) {
                        Ok(s) => schemas.push(s),
                        Err(e) => eprintln!("Parse error {url}: {e}"),
                    },
                    Err(e) => eprintln!("Download error {url}: {e}"),
                }
            }

            if schemas.is_empty() {
                eprintln!("No valid schemas for {code}");
                continue;
            }

            // write immediately
            let file_path = output_dir.join(format!("{code}.json"));
            let file = fs::File::create(&file_path)?;
            serde_json::to_writer_pretty(file, &schemas)?;
            println!(
                "  → wrote {} schemas to {}",
                schemas.len(),
                file_path.display()
            );

            done.insert(code); // mark as done for rest of run
        }
    }

    Ok(())
}

/* ───────────────────────────── tests ───────────────────────────── */

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_month_code_extraction() {
        let u = Url::parse("https://example.com/2009/MMSDM_2009_07/").unwrap();
        assert_eq!(month_code_from_month_url(&u), Some("200907".into()));
    }

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

    const EX3: &str = r#"-- ************************************************************ 
-- Title:	PUBLIC_DVD_ANCILLARY_RECOVERY_SPLIT_200907.ctl 
-- ************************************************************  
...
INFILE PUBLIC_DVD_ANCILLARY_RECOVERY_SPLIT_200907010000.csv  
APPEND INTO TABLE ANCILLARY_RECOVERY_SPLIT  
WHEN (1:1) = 'D'  
FIELDS TERMINATED BY ','  
OPTIONALLY ENCLOSED BY '"'  
TRAILING NULLCOLS	
(
  row_type FILLER, 
  report_type FILLER, 
  report_subtype FILLER, 
  report_version FILLER, 
  EFFECTIVEDATE DATE "yyyy/mm/dd hh24:mi:ss", 
  VERSIONNO FLOAT EXTERNAL, 
  SERVICE CHAR(10), 
  PAYMENTTYPE CHAR(20), 
  CUSTOMER_PORTION FLOAT EXTERNAL, 
  LASTCHANGED DATE "yyyy/mm/dd hh24:mi:ss"
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

    #[test]
    fn test_parse_example3() {
        let schema = parse_ctl(EX3).expect("parse_ctl failed on EX3");
        assert_eq!(schema.month, "200907");
        assert_eq!(schema.table, "ANCILLARY_RECOVERY_SPLIT");
        assert!(schema.columns.iter().any(|c| c.name == "EFFECTIVEDATE"));
    }
}
