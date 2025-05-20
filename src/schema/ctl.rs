// src/schema/ctl.rs

use anyhow::{Context, Result};
use regex::Regex;
use tracing::{debug, instrument, trace, warn};

use super::types::{Column, CtlSchema};

/// Parse a single CTL file’s contents into a `CtlSchema`.
#[instrument(level = "debug", skip(contents), fields(content_len = contents.len()))]
pub fn parse_ctl(contents: &str) -> Result<CtlSchema> {
    debug!("Starting CTL parsing");

    let month_in_ctl = Regex::new(r"(?mi)INFILE[^\n]*?(\d{6})\D*\d{6}[^\n]*\.[cC][sS][vV]")?
        .captures(contents)
        .and_then(|c| c.get(1).map(|m| m.as_str().to_owned()))
        .ok_or_else(|| {
            warn!("Month not found in INFILE during CTL parsing");
            anyhow::anyhow!("month not found in INFILE")
        })?;
    trace!(month = %month_in_ctl, "Parsed month from INFILE");

    let table = Regex::new(r"(?mi)^APPEND INTO TABLE\s+(\w+)")?
        .captures(contents)
        .and_then(|c| c.get(1).map(|m| m.as_str().to_owned()))
        .ok_or_else(|| {
            warn!("Table name not found during CTL parsing");
            anyhow::anyhow!("table name not found")
        })?;
    trace!(table = %table, "Parsed table name");

    let cols_block = Regex::new(r"(?s)TRAILING NULLCOLS\s*\(\s*(.*?)\s*\)\s*(?:--|$)")?
        .captures(contents)
        .and_then(|c| c.get(1).map(|m| m.as_str()))
        .ok_or_else(|| {
            warn!("Column block not found during CTL parsing");
            anyhow::anyhow!("column block not found")
        })?;
    trace!("Extracted columns block");

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
            trace!(name = %name, "Skipping FILLER column");
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
        trace!(name = %name, ty = %ty, format = ?format, "Parsed column");
        columns.push(Column { name, ty, format });
    }

    debug!(columns = columns.len(), "Finished CTL parsing");
    Ok(CtlSchema {
        table,
        month: month_in_ctl,
        columns,
    })
}
