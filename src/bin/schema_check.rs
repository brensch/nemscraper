use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::ColumnDescriptor;
use regex::Regex;
use serde::Serialize;
use serde_yaml;
use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

/// A minimal representation of a single column in a Parquet schema
#[derive(Serialize)]
struct ColumnInfo {
    name: String,
    physical_type: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1) Read the directory from the first CLI argument
    let dir = env::args()
        .nth(1)
        .expect("Usage: cargo run -- <PARQUET_DIR_PATH>");
    let parquet_dir = Path::new(&dir);
    if !parquet_dir.is_dir() {
        panic!("{} is not a directory.", dir);
    }

    // 2) Prepare a regex that captures "everything up to the underscore before the timestamp"
    //    We assume filenames look like: <prefix>_<TIMESTAMP>_... .parquet
    //    where <TIMESTAMP> is at least 8 digits (e.g. 20250403 or 202506022020, etc.).
    //
    //    This regex captures the prefix (including its trailing underscore) into group 1.
    let re = Regex::new(r"^(.+_\d)_.*\.parquet$").unwrap();

    // 3) Iterate every file in that directory, collect unique table-prefixes → schema
    let mut seen_schemas: HashMap<String, Vec<ColumnInfo>> = HashMap::new();

    for entry in fs::read_dir(parquet_dir)? {
        let entry = entry?;
        let path: PathBuf = entry.path();
        if path.is_dir() {
            continue;
        }

        // Only consider files ending in ".parquet"
        if let Some(ext) = path.extension() {
            if ext == "parquet" {
                let filename = path.file_name().unwrap().to_string_lossy().into_owned();

                // Try to extract the “table name” prefix
                if let Some(caps) = re.captures(&filename) {
                    let table_prefix = caps.get(1).unwrap().as_str().to_string();
                    // If we have not yet seen this prefix, open one file to extract schema
                    if !seen_schemas.contains_key(&table_prefix) {
                        let schema = extract_parquet_schema(&path)?;
                        seen_schemas.insert(table_prefix.clone(), schema);
                    }
                }
            }
        }
    }

    // 4) Emit everything into YAML
    //    The top‐level is a mapping: table_prefix -> [ ColumnInfo, ColumnInfo, ... ]
    let mut out = File::create("schemas.yaml")?;
    // serde_yaml will produce something like:
    //
    // TRADING_UNIT_SOLUTION_2_PUBLIC_NEXT_DAY_TRADING_:
    //   - name: "col1"
    //     physical_type: "INT64"
    //   - name: "col2"
    //     physical_type: "BYTE_ARRAY"
    // BID_BIDDAYOFFER_D_3_PUBLIC_BIDMOVE_COMPLETE_:
    //   - name: "..."
    //     physical_type: "..."
    //
    // etc.
    let yaml_string = serde_yaml::to_string(&seen_schemas)?;
    out.write_all(yaml_string.as_bytes())?;

    println!(
        "→ Successfully wrote schemas.yaml ({} tables)",
        seen_schemas.len()
    );
    Ok(())
}

/// Given a path to one Parquet file, open it and return a Vec<ColumnInfo>
/// describing each column’s name + physical type.
fn extract_parquet_schema(path: &Path) -> Result<Vec<ColumnInfo>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata().file_metadata();
    let schema_descr = metadata.schema_descr();

    // schema_descr.fields() returns a slice of ColumnDescriptor
    let mut columns = Vec::with_capacity(schema_descr.num_columns());
    for col_desc in schema_descr.columns() {
        let name = col_desc.name().to_string();
        let physical_type = format!("{:?}", col_desc.physical_type());
        columns.push(ColumnInfo {
            name,
            physical_type,
        });
    }

    Ok(columns)
}
