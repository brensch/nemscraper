use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::Path;
use tracing_subscriber::{fmt, EnvFilter};

// Assuming your library crate is accessible as `nemscraper`
// If your crate structure is different (e.g., just `crate`), adjust this.
// If schema is a module in your lib.rs, and your crate is named `nemscraper`:
use nemscraper::schema::{self, Column, SchemaEvolution};

use prettytable::{format, Cell, Row, Table};

#[derive(Debug)]
struct TableDisplayStats {
    table_name: String,
    versions: usize,
    first_seen: String,
    last_seen: String,
    min_cols: usize,
    max_cols: usize,
    current_cols: usize, // Columns in the most recent version of this table
}

fn main() -> Result<()> {
    // Initialize tracing subscriber
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr) // Log to stderr
        .init();

    tracing::info!("Starting schema statistics generation.");

    let schemas_input_dir = Path::new("schemas");
    if !schemas_input_dir.exists() || !schemas_input_dir.is_dir() {
        tracing::error!(
            path = %schemas_input_dir.display(),
            "Schema directory not found. Please run the schema fetching process first."
        );
        return Err(anyhow::anyhow!("Schema directory 'schemas' not found."));
    }

    let evolutions = schema::extract_schema_evolutions(schemas_input_dir)
        .context("Failed to extract schema evolutions")?;

    if evolutions.is_empty() {
        tracing::info!(
            "No schema evolutions found in the '{}' directory.",
            schemas_input_dir.display()
        );
        println!("No schema evolutions found. The 'schemas' directory might be empty or files might not contain valid evolution data.");
        return Ok(());
    }

    tracing::info!("Processing {} schema evolution records.", evolutions.len());

    // Aggregate stats by table name
    let mut aggregated_stats: HashMap<String, TableDisplayStats> = HashMap::new();

    // The evolutions are already sorted by table_name, then start_month from extract_schema_evolutions
    for evo in evolutions {
        let num_columns = evo.columns.len();
        let entry = aggregated_stats
            .entry(evo.table_name.clone())
            .or_insert_with(|| TableDisplayStats {
                table_name: evo.table_name.clone(),
                versions: 0,
                first_seen: evo.start_month.clone(), // Will be the earliest due to sorting
                last_seen: evo.end_month.clone(),    // Will be updated
                min_cols: num_columns,
                max_cols: num_columns,
                current_cols: num_columns, // Will be updated by the last evolution for this table
            });

        entry.versions += 1;
        // Update last_seen if this evolution's end_month is later
        // (Considering evolutions for the same table are sorted by start_month,
        // the last one processed will have the latest end_month for that table's continuous presence)
        if evo.end_month > entry.last_seen {
            // String comparison works for YYYYMM
            entry.last_seen = evo.end_month.clone();
        }
        entry.min_cols = entry.min_cols.min(num_columns);
        entry.max_cols = entry.max_cols.max(num_columns);
        // The last evolution for a table (due to sorting) will set the current_cols
        entry.current_cols = num_columns;
    }

    // Convert HashMap to Vec and sort by table name for display
    let mut display_data: Vec<TableDisplayStats> = aggregated_stats.into_values().collect();
    display_data.sort_by(|a, b| a.table_name.cmp(&b.table_name));

    // Create the table for display
    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_BOX_CHARS); // Using a format with box characters

    table.add_row(Row::new(vec![
        Cell::new("Table Name").style_spec("bFg"), // Bold, Green foreground
        Cell::new("Versions").style_spec("bFg"),
        Cell::new("First Seen (YYYYMM)").style_spec("bFg"),
        Cell::new("Last Seen (YYYYMM)").style_spec("bFg"),
        Cell::new("Min Cols").style_spec("bFg"),
        Cell::new("Max Cols").style_spec("bFg"),
        Cell::new("Current Cols").style_spec("bFg"),
    ]));

    if display_data.is_empty() {
        println!(
            "No tables found after processing evolutions. This might indicate an issue or no data."
        );
    } else {
        for stats in display_data {
            table.add_row(Row::new(vec![
                Cell::new(&stats.table_name),
                Cell::new(&stats.versions.to_string()).style_spec("r"), // Right aligned
                Cell::new(&stats.first_seen),
                Cell::new(&stats.last_seen),
                Cell::new(&stats.min_cols.to_string()).style_spec("r"),
                Cell::new(&stats.max_cols.to_string()).style_spec("r"),
                Cell::new(&stats.current_cols.to_string()).style_spec("r"),
            ]));
        }
        // Print the table to stdout
        println!("\n--- Schema Statistics ---");
        table.printstd();
    }

    tracing::info!("Schema statistics generation finished.");
    Ok(())
}
