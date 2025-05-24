use anyhow::{Context, Result};
use chrono::{Datelike, Utc};
use serde_json;
use std::io::Write;
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    path::Path,
    sync::Arc,
};
use tracing::{debug, instrument, warn};

use crate::schema::build_arrow_schema;

use super::types::{calculate_fields_hash, Column, MonthSchema, SchemaEvolution};

impl SchemaEvolution {
    /// Build a lookup map from table name -> list of Arc<SchemaEvolution>
    pub fn build_lookup(
        evolutions: Vec<SchemaEvolution>,
    ) -> HashMap<String, Vec<Arc<SchemaEvolution>>> {
        let mut lookup: HashMap<String, Vec<Arc<SchemaEvolution>>> = HashMap::new();
        for evo in evolutions {
            lookup
                .entry(evo.table_name.clone())
                .or_default()
                .push(Arc::new(evo));
        }

        // ensure each table's vector is sorted by start_month
        for evos in lookup.values_mut() {
            evos.sort_by(|a, b| a.start_month.cmp(&b.start_month));
        }

        lookup
    }

    /// Write this evolution to `./evolutions/<table>-<start>-<end>-<hash>.txt`
    pub fn print(&self) -> anyhow::Result<()> {
        // Ensure the output directory exists
        let dir = Path::new("evolutions");
        fs::create_dir_all(dir).with_context(|| format!("creating directory {:?}", dir))?;

        // Build filename
        let filename = format!(
            "{}-{}-{}-{}.txt",
            self.table_name, self.start_month, self.end_month, self.fields_hash
        );
        let path = dir.join(filename);

        // Open the file for writing
        let mut file = File::create(&path).with_context(|| format!("creating file {:?}", path))?;

        // Write header line
        writeln!(
            file,
            "{}: [{}–{}] ({} columns, hash: {})",
            self.table_name,
            self.start_month,
            self.end_month,
            self.columns.len(),
            self.fields_hash
        )?;

        // Write each column
        for col in &self.columns {
            writeln!(
                file,
                "  - {}: {} ({})",
                col.name,
                col.ty,
                col.format.as_deref().unwrap_or("N/A")
            )?;
        }

        Ok(())
    }
}

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
                    let arrow_schema = build_arrow_schema(&cols);
                    evolutions.push(SchemaEvolution {
                        table_name: cs.table.clone(),
                        fields_hash: active_hash.clone(),
                        start_month: start.clone(),
                        end_month: prev_month.clone().unwrap_or_else(|| start.clone()),
                        columns: cols.clone(),
                        arrow_schema,
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
                let arrow_schema = build_arrow_schema(&cols);
                evolutions.push(SchemaEvolution {
                    table_name: table.clone(),
                    fields_hash: hash,
                    start_month: start,
                    end_month: prev.clone(),
                    columns: cols,
                    arrow_schema,
                });
                active.remove(&table);
            }
        }

        prev_month = Some(month_code);
    }

    // flush any schemas that never changed and never disappeared
    if let Some(prev) = &prev_month {
        for (table, (hash, start, cols)) in active.drain() {
            let arrow_schema = build_arrow_schema(&cols);
            evolutions.push(SchemaEvolution {
                table_name: table,
                fields_hash: hash,
                start_month: start,
                end_month: prev.clone(), // last seen month
                columns: cols,
                arrow_schema,
            });
        }
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

    // ─── sort by table_name then start_month ─────────────────────────────────
    evolutions.sort_by(|a, b| {
        a.table_name
            .cmp(&b.table_name)
            .then(a.start_month.cmp(&b.start_month))
    });

    // ─── bridge any gaps where the hash is identical ────────────────────
    let mut bridged: Vec<SchemaEvolution> = Vec::with_capacity(evolutions.len());
    for evo in evolutions.into_iter() {
        if let Some(prev) = bridged.last_mut() {
            if prev.table_name == evo.table_name && prev.fields_hash == evo.fields_hash {
                // same schema, just extend the end_month to cover any gap
                if evo.end_month > prev.end_month {
                    prev.end_month = evo.end_month.clone();
                }
                continue; // skip pushing a new segment
            }
        }
        bridged.push(evo);
    }

    Ok(bridged)
}

/// Pick the one `SchemaEvolution` whose range covers `effective_month`.
pub fn find_schema_evolution(
    schema_lookup: &HashMap<String, Vec<Arc<SchemaEvolution>>>,
    table_name: &str,
    effective_month: &str,
) -> Option<Arc<SchemaEvolution>> {
    match schema_lookup.get(table_name) {
        Some(list) => {
            // list is sorted by start_month
            if let Some(evo) = list.iter().find(|e| {
                e.start_month.as_str() <= effective_month && e.end_month.as_str() >= effective_month
            }) {
                return Some(evo.clone());
            }
            warn!(
                table = table_name,
                month = effective_month,
                "no matching evolution; available ranges = {:?}",
                list.iter()
                    .map(|e| format!("{}–{}", e.start_month, e.end_month))
                    .collect::<Vec<_>>()
            );
            None
        }
        None => {
            warn!(table = table_name, "no evolutions for this table");
            None
        }
    }
}
