use parquet::file::metadata::{ColumnChunkMetaData, RowGroupMetaData};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::Type;
use std::{env, fs::File, path::Path, process::exit};

fn main() {
    // Expect exactly one CLI argument: path to a Parquet file.
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <PARQUET_FILE>", args[0]);
        exit(1);
    }
    let file_path = &args[1];
    if let Err(e) = inspect_parquet(Path::new(file_path)) {
        eprintln!("Error: {}", e);
        exit(1);
    }
}

/// Open the Parquet file, read its metadata, and print schema + row‐group details.
fn inspect_parquet(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    // 1) Open the file and build a SerializedFileReader.
    let file = File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    let parquet_meta = reader.metadata(); // ParquetMetaData :contentReference[oaicite:2]{index=2}
    let file_meta = parquet_meta.file_metadata(); // FileMetaData

    // 2) File‐level metadata
    //    - 'num_row_groups' moved to ParquetMetaData
    //    - 'total_byte_size' is no longer on FileMetaData, so use OS file size instead.
    let num_row_groups = parquet_meta.num_row_groups(); // total RG count :contentReference[oaicite:3]{index=3}
    let file_size_disk = std::fs::metadata(path)?.len();

    println!("=== Parquet File: {} ===", path.display());
    println!(
        "Created by:           {}",
        file_meta.created_by().unwrap_or("<unknown>")
    );
    println!("Parquet version:      {}", file_meta.version());
    println!("Total rows:           {}", file_meta.num_rows());
    println!("Number of row groups: {}", num_row_groups);
    println!("File‐size on disk:    {} bytes", file_size_disk);
    println!();

    // 3) Full schema (nested)
    println!("=== Schema ===");
    print_schema(file_meta.schema_descr().root_schema(), 0);
    println!();

    // 4) Flattened column descriptors
    println!("=== Columns ===");
    for col_desc in file_meta.schema_descr().columns() {
        let name = col_desc.name();
        let phys = format!("{:?}", col_desc.physical_type());
        let logical = col_desc
            .logical_type()
            .as_ref()
            .map_or("<none>".to_string(), |lt| format!("{:?}", lt));
        println!(
            "- {:<30} | Physical: {:<8} | Logical: {}",
            name, phys, logical
        );
    }
    println!();

    // 5) Per‐row‐group details
    for rg_idx in 0..parquet_meta.num_row_groups() {
        let rg_md = parquet_meta.row_group(rg_idx);
        print_row_group(rg_idx, rg_md);
    }

    Ok(())
}

/// Recursively print a Parquet schema (`Type`), indenting by `level`.
fn print_schema(node: &Type, level: usize) {
    let indent = "  ".repeat(level);
    match node {
        Type::PrimitiveType {
            basic_info,
            physical_type,
            ..
        } => {
            let name = basic_info.name();
            let phys = format!("{:?}", physical_type);
            // In v55.1.0, logical_type is accessed via basic_info.logical_type()
            let logical = basic_info
                .logical_type()
                .as_ref()
                .map_or(String::new(), |lt| format!(", {:?}", lt));
            println!("{}- {}: {}{}", indent, name, phys, logical);
        }
        Type::GroupType {
            basic_info, fields, ..
        } => {
            let name = basic_info.name();
            println!("{}+ {} (group)", indent, name);
            for field in fields {
                print_schema(field.as_ref(), level + 1);
            }
        }
    }
}

/// Print details for a single row group, including each column chunk’s metadata.
fn print_row_group(idx: usize, rg_md: &RowGroupMetaData) {
    println!("--- Row Group {} ---", idx);
    println!("  Rows in this RG:        {}", rg_md.num_rows());
    println!(
        "  Uncompressed size (RG): {} bytes",
        rg_md.total_byte_size()
    ); // from RowGroupMetaData :contentReference[oaicite:4]{index=4}
    println!("  Columns in this RG:");

    for col_idx in 0..rg_md.num_columns() {
        let col_md = rg_md.column(col_idx);
        print_column_chunk(col_idx, col_md);
    }
    println!();
}

/// Print a single column chunk’s metadata, including compression, encodings, sizes, and stats.
fn print_column_chunk(col_idx: usize, col_md: &ColumnChunkMetaData) {
    let descr = col_md.column_descr();
    let col_name = descr.name();
    let phys = format!("{:?}", descr.physical_type());
    let logical = descr
        .logical_type()
        .as_ref()
        .map_or("<none>".into(), |lt| format!("{:?}", lt));

    let num_values = col_md.num_values();
    let compression = format!("{:?}", col_md.compression());
    let encodings: Vec<String> = col_md
        .encodings()
        .iter()
        .map(|e| format!("{:?}", e))
        .collect();

    println!("  > Column {} (\"{}\")", col_idx, col_name);
    println!("    • Physical type:   {}", phys);
    println!("    • Logical type:    {}", logical);
    println!("    • Compression:     {}", compression);
    println!("    • Encodings:       {}", encodings.join(", "));
    println!("    • Number of values: {}", num_values);
    println!(
        "    • Compressed size:   {} bytes",
        col_md.compressed_size()
    );
    println!(
        "    • Uncompressed size: {} bytes",
        col_md.uncompressed_size()
    );

    // In v55.1.0, use null_count_opt(), min_bytes_opt(), max_bytes_opt(), distinct_count_opt()
    if let Some(stats) = col_md.statistics() {
        println!("    • Statistics:");
        if let Some(null_count) = stats.null_count_opt() {
            println!("        └─ null count: {}", null_count);
        }
        if let Some(min_bytes) = stats.min_bytes_opt() {
            if descr.physical_type() == parquet::basic::Type::BYTE_ARRAY {
                // Attempt to display UTF8 columns as strings
                match std::str::from_utf8(min_bytes) {
                    Ok(s) => println!("        └─ min: \"{}\"", s),
                    Err(_) => println!("        └─ min (bytes): {:?}", min_bytes),
                }
            } else {
                println!("        └─ min (bytes): {:?}", min_bytes);
            }
        }
        if let Some(max_bytes) = stats.max_bytes_opt() {
            if descr.physical_type() == parquet::basic::Type::BYTE_ARRAY {
                match std::str::from_utf8(max_bytes) {
                    Ok(s) => println!("        └─ max: \"{}\"", s),
                    Err(_) => println!("        └─ max (bytes): {:?}", max_bytes),
                }
            } else {
                println!("        └─ max (bytes): {:?}", max_bytes);
            }
        }
        if let Some(distinct_count) = stats.distinct_count_opt() {
            println!("        └─ distinct count: {}", distinct_count);
        }
    } else {
        println!("    • Statistics: <none>");
    }
}
