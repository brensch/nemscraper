// src/schema/evolution.rs

use anyhow::{Context, Result};
use chrono::{Datelike, Utc};
use serde_json;
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
    sync::Arc,
};
use tracing::{debug, instrument, warn};

use super::types::{calculate_fields_hash, Column, MonthSchema, SchemaEvolution};

/// Read all `<YYYYMM>.json` in `input_dir` and produce `SchemaEvolution` entries.
#[instrument(level = "info", skip(input_dir), fields(input_path = %input_dir.as_ref().display()))]
pub fn extract_schema_evolutions<P: AsRef<Path>>(input_dir: P) -> Result<Vec<SchemaEvolution>> {
    debug!("Starting schema evolution extraction");
    let input = input_dir.as_ref();

    // 1. Gather & sort all month files
    let mut month_files: Vec<_> = fs::read_dir(input)
        .with_context(|| format!("reading {:?}", input))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.is_file()
                && p.extension().and_then(|s| s.to_str()) == Some("json")
                && p.file_stem()
                    .and_then(|s| s.to_str())
                    .map_or(false, |stem| {
                        stem.len() == 6 && stem.chars().all(|c| c.is_ascii_digit())
                    })
        })
        .collect();
    month_files.sort();

    let mut evolutions = Vec::new();
    let mut active: HashMap<String, (String, String, Vec<Column>)> = HashMap::new();
    let mut prev_month: Option<String> = None;

    for file in &month_files {
        let month_code = file.file_stem().unwrap().to_string_lossy().to_string();
        let data: MonthSchema = {
            let text = fs::read_to_string(file).with_context(|| format!("read {:?}", file))?;
            serde_json::from_str(&text).with_context(|| format!("parse {:?}", file))?
        };

        let mut seen = HashSet::new();
        for cs in data.schemas {
            seen.insert(cs.table.clone());
            let hash = calculate_fields_hash(&cs.columns);

            if let Some((active_hash, start, cols)) = active.get_mut(&cs.table) {
                if *active_hash != hash {
                    evolutions.push(SchemaEvolution {
                        table_name: cs.table.clone(),
                        fields_hash: active_hash.clone(),
                        start_month: start.clone(),
                        end_month: prev_month.clone().unwrap_or_else(|| start.clone()),
                        columns: cols.clone(),
                    });
                    *active_hash = hash;
                    *start = month_code.clone();
                    *cols = cs.columns.clone();
                }
            } else {
                active.insert(
                    cs.table.clone(),
                    (hash, month_code.clone(), cs.columns.clone()),
                );
            }
        }

        if let Some(prev) = &prev_month {
            let ended: Vec<_> = active
                .iter()
                .filter(|(t, _)| !seen.contains(*t))
                .map(|(t, (h, s, c))| (t.clone(), h.clone(), s.clone(), c.clone()))
                .collect();
            for (table, hash, start, cols) in ended {
                evolutions.push(SchemaEvolution {
                    table_name: table.clone(),
                    fields_hash: hash,
                    start_month: start,
                    end_month: prev.clone(),
                    columns: cols,
                });
                active.remove(&table);
            }
        }

        prev_month = Some(month_code);
    }

    // Final pass: extend every last evolution to today
    let now = Utc::now();
    let current = format!("{:04}{:02}", now.year(), now.month());
    let last_idxs: HashMap<_, _> = evolutions
        .iter()
        .enumerate()
        .map(|(i, e)| (e.table_name.clone(), i))
        .collect();
    for &i in last_idxs.values() {
        evolutions[i].end_month = current.clone();
    }

    evolutions.sort_by(|a, b| {
        a.table_name
            .cmp(&b.table_name)
            .then(a.start_month.cmp(&b.start_month))
            .then(a.end_month.cmp(&b.end_month))
    });

    Ok(evolutions)
}

/// Pick the one `SchemaEvolution` whose range covers `effective_month`.
pub fn find_schema_evolution(
    schema_lookup: &HashMap<String, Vec<Arc<SchemaEvolution>>>,
    table_name: &str,
    effective_month: &str,
) -> Option<Arc<SchemaEvolution>> {
    if let Some(list) = schema_lookup.get(table_name) {
        for evo in list {
            if effective_month >= evo.start_month.as_str()
                && effective_month <= evo.end_month.as_str()
            {
                return Some(evo.clone());
            }
        }
        warn!(
            table = table_name,
            month = effective_month,
            "no matching evolution; available ranges = {:?}",
            list.iter()
                .map(|e| format!("{}–{}", e.start_month, e.end_month))
                .collect::<Vec<_>>()
        );
    } else {
        warn!(table = table_name, "no evolutions for this table");
    }
    None
}
