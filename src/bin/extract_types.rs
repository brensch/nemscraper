use serde::Deserialize;
use serde_json;
use std::collections::HashSet;
use std::env;
use std::fs;

#[derive(Debug, Deserialize)]
struct Column {
    ty: String,
    format: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TableSchema {
    columns: Vec<Column>,
}

fn main() -> anyhow::Result<()> {
    // Expect a single argument: path to the JSON file
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <schema_file.json>", args[0]);
        std::process::exit(1);
    }
    let path = &args[1];

    // Read and parse JSON
    let data = fs::read_to_string(path)?;
    let schemas: Vec<TableSchema> = serde_json::from_str(&data)?;

    // Collect unique (ty, format) pairs
    let mut set: HashSet<(String, Option<String>)> = HashSet::new();
    for table in schemas {
        for col in table.columns {
            set.insert((col.ty, col.format));
        }
    }

    // Print unique combinations
    let mut combos: Vec<_> = set.into_iter().collect();
    combos.sort();
    println!("ty,format");
    for (ty, fmt) in combos {
        match fmt {
            Some(f) => println!("{},{}", ty, f),
            None => println!("{},<none>", ty),
        }
    }

    Ok(())
}
