//! lib.rs  ‒ crate `schema` (Tokio async, single shared Client, no headers)
//
//! Call `schema::fetch_all(&client, "schemas").await?;`
//! with a `reqwest::Client` configured (headers, proxy, TLS, …) by the caller.

use anyhow::{Context, Result};
use futures::{stream::FuturesUnordered, StreamExt};
use regex::Regex;
use reqwest::Client;
use scraper::{Html, Selector};
use serde::Serialize;
use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};
use tokio::{fs as tokio_fs, io::AsyncWriteExt};
use url::Url;

/* ───────────────────────────── data types ───────────────────────────── */

#[derive(Debug, Serialize, PartialEq)]
pub struct Column {
    pub name: String,
    pub ty: String,
    pub format: Option<String>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct CtlSchema {
    pub table: String,
    pub month: String,
    pub columns: Vec<Column>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct MonthSchema {
    pub month: String,
    pub schemas: Vec<CtlSchema>,
}

/* ──────────────────────────── CTL parsing ──────────────────────────── */

pub fn parse_ctl(contents: &str) -> Result<CtlSchema> {
    let month = Regex::new(r"(?mi)INFILE[^\n]*?(\d{6})\D*\d{6}[^\n]*\.[cC][sS][vV]")?
        .captures(contents)
        .and_then(|c| c.get(1).map(|m| m.as_str().to_owned()))
        .ok_or_else(|| anyhow::anyhow!("month not found in INFILE"))?;

    let table = Regex::new(r"(?mi)^APPEND INTO TABLE\s+(\w+)")?
        .captures(contents)
        .and_then(|c| c.get(1).map(|m| m.as_str().to_owned()))
        .ok_or_else(|| anyhow::anyhow!("table name not found"))?;

    let cols_block = Regex::new(r"(?s)TRAILING NULLCOLS\s*\(\s*(.*?)\s*\)\s*(?:--|$)")?
        .captures(contents)
        .and_then(|c| c.get(1).map(|m| m.as_str()))
        .ok_or_else(|| anyhow::anyhow!("column block not found"))?;

    let mut columns = Vec::new();
    for raw in cols_block.split(',') {
        let s = raw.trim();
        if s.is_empty() {
            continue;
        }
        let mut parts = s.split_whitespace();
        let name = parts.next().context("bad column")?.to_owned();
        let ty = parts.next().context("bad column")?.to_owned();
        if ty.eq_ignore_ascii_case("FILLER") {
            continue;
        }
        let format = parts.next().map(|first| {
            let mut owned = first.to_owned();
            for p in parts {
                owned.push(' ');
                owned.push_str(p);
            }
            owned
        });
        columns.push(Column { name, ty, format });
    }

    Ok(CtlSchema {
        table,
        month,
        columns,
    })
}

/* ──────────────────── helpers (no header mutation) ──────────────────── */

async fn get_text(client: &Client, url: &Url) -> Result<String> {
    Ok(client
        .get(url.clone())
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?)
}

async fn list_dirs(client: &Client, dir: &Url) -> Result<Vec<Url>> {
    let body = get_text(client, dir).await?;
    let doc = Html::parse_document(&body);
    let sel = Selector::parse("a[href]").unwrap();
    Ok(doc
        .select(&sel)
        .filter_map(|e| {
            let href = e.value().attr("href")?;
            if !href.ends_with('/') {
                return None;
            }
            if e.text().any(|t| t.contains("Parent Directory")) {
                return None;
            }
            dir.join(href).ok()
        })
        .collect())
}

async fn list_ctl_urls(client: &Client, ctl_dir: &Url) -> Result<Vec<Url>> {
    let body = get_text(client, ctl_dir).await?;
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

fn month_code_from_url(u: &Url) -> Option<String> {
    let re = Regex::new(r"MMSDM_(\d{4})_(\d{2})/?$").ok()?;
    let caps = re.captures(u.path())?;
    Some(format!("{}{}", &caps[1], &caps[2]))
}

/* ───────────────────────── public API ───────────────────────── */

/// Process every unseen month (concurrency 4) into `schemas/<YYYYMM>.json`.
///
/// * The caller controls all headers & settings on `client`.
/// * Returns all `MonthSchema`s produced in this run.
pub async fn fetch_all<P: AsRef<Path>>(client: &Client, output_dir: P) -> Result<Vec<MonthSchema>> {
    let output_dir = output_dir.as_ref();
    fs::create_dir_all(output_dir)?;

    let done: HashSet<String> = fs::read_dir(output_dir)?
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().into_string().ok())
        .filter_map(|n| (n.ends_with(".json") && n.len() == 11).then(|| n[..6].to_owned()))
        .collect();

    let base = Url::parse("https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/")?;
    let mut month_urls = Vec::<Url>::new();
    for year in list_dirs(client, &base).await.context("listing years")? {
        month_urls.extend(
            list_dirs(client, &year)
                .await
                .with_context(|| format!("months in {year}"))?,
        );
    }

    const MAX_CONCURRENCY: usize = 4;
    let mut tasks = FuturesUnordered::new();
    let mut results = Vec::<MonthSchema>::new();

    for month_url in month_urls {
        let code = match month_code_from_url(&month_url) {
            Some(c) => c,
            None => continue,
        };
        if done.contains(&code) {
            println!("skip {code} (already on disk)");
            continue;
        }

        let ctl_dir = match month_url.join("MMSDM_Historical_Data_SQLLoader/CTL/") {
            Ok(u) => u,
            Err(_) => continue,
        };

        let client_clone = client.clone();
        let out_dir = PathBuf::from(output_dir);
        tasks.push(process_month(client_clone, code, ctl_dir, out_dir));

        if tasks.len() >= MAX_CONCURRENCY {
            if let Some(res) = tasks.next().await {
                if let Ok(Some(ms)) = res {
                    results.push(ms);
                }
            }
        }
    }

    while let Some(res) = tasks.next().await {
        if let Ok(Some(ms)) = res {
            results.push(ms);
        }
    }

    Ok(results)
}

/* ───────────────────── per-month worker ───────────────────── */

async fn process_month(
    client: Client,
    code: String,
    ctl_dir: Url,
    output_dir: PathBuf,
) -> Result<Option<MonthSchema>> {
    let ctl_urls = match list_ctl_urls(&client, &ctl_dir).await {
        Ok(v) if !v.is_empty() => v,
        _ => {
            eprintln!("No CTLs in {ctl_dir}");
            return Ok(None);
        }
    };

    let mut schemas = Vec::new();
    for url in ctl_urls {
        let text = match get_text(&client, &url).await {
            Ok(t) => t,
            Err(e) => {
                eprintln!("download {url}: {e}");
                continue;
            }
        };
        match parse_ctl(&text) {
            Ok(s) => schemas.push(s),
            Err(e) => eprintln!("parse {url}: {e}"),
        }
    }

    if schemas.is_empty() {
        eprintln!("month {code} produced no schemas");
        return Ok(None);
    }

    let path = output_dir.join(format!("{code}.json"));
    let mut f = tokio_fs::File::create(&path).await?;
    f.write_all(serde_json::to_string_pretty(&schemas)?.as_bytes())
        .await?;
    println!("wrote {code}.json ({} schemas)", schemas.len());

    Ok(Some(MonthSchema {
        month: code,
        schemas,
    }))
}

/* ───────────────────────────── tests ───────────────────────────── */
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Runtime;

    #[test]
    fn month_regex_variants() {
        let re = Regex::new(r"(?mi)INFILE[^\n]*?(\d{6})\D*\d{6}[^\n]*\.[cC][sS][vV]").unwrap();
        let cases = [
            ("INFILE F_201008010000.csv", "201008"),
            ("INFILE F_201008_010000.csv", "201008"),
            ("INFILE path/F_201008-010000.CSV", "201008"),
        ];
        for (s, want) in cases {
            assert_eq!(re.captures(s).unwrap()[1].to_string(), want);
        }
    }

    #[test]
    fn parse_minimal_ctl() {
        let ctl = r#"
LOAD DATA
INFILE FILE_202201010000.csv
APPEND INTO TABLE FOO
TRAILING NULLCOLS (foo FILLER, BAR DATE "yyyy/mm/dd")
"#;
        let s = parse_ctl(ctl).unwrap();
        assert_eq!(s.month, "202201");
        assert_eq!(s.table, "FOO");
        assert_eq!(s.columns.len(), 1);
    }

    // other parsing tests from previous versions can stay …
    #[test]
    #[ignore]
    fn integration_one_month() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let client = Client::builder().build().unwrap();
            let out = fetch_all(&client, tmp.path()).await.unwrap();
            assert!(!out.is_empty());
        });
    }
}
