use anyhow::{bail, Result};
use chrono::{DateTime, FixedOffset};
use regex::Regex;
use reqwest::blocking::Client;
use scraper::{Html, Selector};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    io::{Cursor, Read},
};
use url::Url;
use zip::ZipArchive;

#[derive(Debug, Serialize, Clone, PartialEq)]
struct FieldDef {
    name: String,
    data_type: String,
    is_nullable: bool,
    is_primary: bool,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
struct TableSchema {
    table: String,
    fields: Vec<FieldDef>,
}

struct Release {
    name: String,
    url: Url,
    published: DateTime<FixedOffset>,
}

struct Version {
    name: String,
    url: Url,
}

/// List `<a>` links from a directory index
fn list_links(client: &Client, dir: &Url) -> Result<Vec<(String, Url)>> {
    let body = client.get(dir.clone()).send()?.text()?;
    let doc = Html::parse_document(&body);
    let sel = Selector::parse("a").unwrap();
    let mut out = Vec::new();
    for a in doc.select(&sel) {
        if let Some(href) = a.value().attr("href") {
            let text = a.text().collect::<String>().trim().to_string();
            if let Ok(abs) = dir.join(href) {
                out.push((text, abs));
            }
        }
    }
    Ok(out)
}

/// Discover MMSDM releases by year directories
fn get_releases(client: &Client) -> Result<Vec<Release>> {
    let base = Url::parse("https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/")?;
    let mut rels = Vec::new();
    // First fetch year directories (e.g., "2025/")
    let years = list_links(client, &base)?
        .into_iter()
        .filter(|(text, url)| {
            url.as_str().ends_with('/') && text.chars().all(|c| c.is_ascii_digit())
        })
        .collect::<Vec<_>>();

    // Within each year, find subdirectories named "MMSDM_<year>_XX/"
    for (year, year_url) in years {
        for (text, url) in list_links(client, &year_url)? {
            if text.starts_with(&format!("MMSDM_{}", year)) && url.as_str().ends_with('/') {
                let head = client.head(url.clone()).send()?;
                let lm = head
                    .headers()
                    .get("last-modified")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("");
                let published = DateTime::parse_from_rfc2822(lm)?;
                rels.push(Release {
                    name: text,
                    url,
                    published,
                });
            }
        }
    }
    rels.sort_by_key(|r| r.published);
    Ok(rels)
}

/// Find version folders under a release
fn get_versions(client: &Client, release: &Release) -> Result<Vec<Version>> {
    let docs = release
        .url
        .join("MMSDM_Historical_Data_SQLLoader/DOCUMENTATION/MMS%20Data%20Model/")?;
    let mut vers = Vec::new();
    for (text, url) in list_links(client, &docs)? {
        if text.starts_with('v') && url.as_str().ends_with('/') {
            vers.push(Version { name: text, url });
        }
    }
    Ok(vers)
}

/// Identify the create ZIP within a version directory by inspecting links
fn find_zip(client: &Client, version: &Version) -> Result<Url> {
    for (_, url) in list_links(client, &version.url)? {
        let path = url.path().to_lowercase();
        if path.contains("mmsdm_create_") && path.ends_with(".zip") {
            return Ok(url);
        }
    }
    bail!("no create zip found in {}", version.url);
}

/// Download ZIP, extract Postgres DDL, parse schemas
fn download_and_parse_zip(client: &Client, zip_url: &Url) -> Result<Vec<TableSchema>> {
    let resp = client.get(zip_url.clone()).send()?;
    if !resp.status().is_success() {
        bail!("{} -> HTTP {}", zip_url, resp.status());
    }
    let bytes = resp.bytes()?;
    let mut zip = ZipArchive::new(Cursor::new(bytes))?;

    let names: Vec<String> = zip.file_names().map(|s| s.to_string()).collect();
    let target = names
        .into_iter()
        .find(|n| {
            n.to_lowercase().contains("postgresql") && n.ends_with("create_mms_data_model.sql")
        })
        .ok_or_else(|| anyhow::anyhow!("no Postgres DDL in {}", zip_url))?;

    let mut f = zip.by_name(&target)?;
    let mut sql = String::new();
    f.read_to_string(&mut sql)?;
    Ok(parse_sql(&sql))
}

/// Parse `CREATE TABLE` blocks to TableSchema
fn parse_sql(sql: &str) -> Vec<TableSchema> {
    let create_re = Regex::new(r"(?i)create\s+table\s+([A-Z0-9_]+)\s*\(").unwrap();
    let col_re =
        Regex::new(r"(?i)^\s*([A-Z0-9_]+)\s+([A-Z0-9\(\), ]+?)(?:\s+(NOT NULL|null))?[,)]")
            .unwrap();
    let pk_re = Regex::new(r"(?i)primary key\s*\(([^)]+)\)").unwrap();

    let mut pks = BTreeMap::new();
    for caps in pk_re.captures_iter(sql) {
        let cols = caps[1]
            .split(',')
            .map(|s| s.trim().to_string())
            .collect::<Vec<_>>();
        pks.insert(String::new(), cols);
    }

    let mut tables = Vec::new();
    for caps in create_re.captures_iter(sql) {
        let tbl = caps[1].to_string();
        if let Some(start) = caps.get(0).map(|m| m.start()) {
            if let Some(end) = sql[start..].find(");") {
                let block = &sql[start..start + end];
                let mut fields = Vec::new();
                for line in block.lines() {
                    if let Some(c) = col_re.captures(line) {
                        let name = c[1].to_string();
                        let dtype = c[2].to_string();
                        let nullable = c
                            .get(3)
                            .map_or(true, |m| m.as_str().eq_ignore_ascii_case("null"));
                        let is_primary =
                            pks.get(&String::new()).map_or(false, |v| v.contains(&name));
                        fields.push(FieldDef {
                            name,
                            data_type: dtype,
                            is_nullable: nullable,
                            is_primary,
                        });
                    }
                }
                tables.push(TableSchema { table: tbl, fields });
            }
        }
    }
    tables
}

fn main() -> Result<()> {
    let client = Client::builder()
        .user_agent("Mozilla/5.0 (compatible; mmsdm-debug)")
        .build()?;

    let releases = get_releases(&client)?;
    println!(
        "Releases: {:?}",
        releases.iter().map(|r| &r.name).collect::<Vec<_>>()
    );

    if let Some(first) = releases.first() {
        let versions = get_versions(&client, first)?;
        println!(
            "Versions in {}: {:?}",
            first.name,
            versions.iter().map(|v| &v.name).collect::<Vec<_>>()
        );

        if let Some(v0) = versions.first() {
            let zip_url = find_zip(&client, v0)?;
            println!("ZIP: {}", zip_url);
            let schemas = download_and_parse_zip(&client, &zip_url)?;
            println!("Parsed {} tables", schemas.len());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn real_client() -> Client {
        Client::builder()
            .user_agent("Mozilla/5.0 (compatible; mmsdm-test)")
            .build()
            .unwrap()
    }

    #[test]
    fn test_get_releases() {
        let client = real_client();
        let rels = get_releases(&client).unwrap();
        assert!(!rels.is_empty(), "Expected at least one release");
        // ensure names look like MMSDM_<year>_
        assert!(rels.iter().any(|r| r.name.starts_with("MMSDM_")));
    }

    #[test]
    fn test_get_versions() {
        let client = real_client();
        let first = get_releases(&client).unwrap().into_iter().next().unwrap();
        let vers = get_versions(&client, &first).unwrap();
        assert!(!vers.is_empty(), "Expected at least one version");
        assert!(vers[0].name.starts_with('v'));
    }

    #[test]
    fn test_find_zip_and_download() {
        let client = real_client();
        let first = get_releases(&client).unwrap().into_iter().next().unwrap();
        let vers = get_versions(&client, &first).unwrap();
        let zip_url = find_zip(&client, &vers[0]).unwrap();
        assert!(zip_url.as_str().to_lowercase().contains("create_"));
        let schemas = download_and_parse_zip(&client, &zip_url).unwrap();
        assert!(!schemas.is_empty(), "Expected to parse some tables");
    }
}
