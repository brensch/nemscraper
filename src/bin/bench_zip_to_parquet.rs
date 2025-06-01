use std::env;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::Result;
use nemscraper::process::split::split_zip_to_parquet;
use tracing::Level;
use tracing_subscriber::{self, EnvFilter};

fn print_usage_and_exit(program: &str) -> ! {
    eprintln!("Usage: {} <input-zip-or-csv-path> <output-dir>", program);
    std::process::exit(1);
}

fn main() -> Result<()> {
    // Initialize a basic tracing subscriber so that the `#[instrument]` on split_zip_to_parquet logs work.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(Level::DEBUG.into()))
        .init();

    let mut args = env::args();
    let program = args.next().unwrap_or_else(|| "splitter".into());

    // Expect exactly two positional arguments: input path and output directory.
    let (input_path, out_dir) = match (args.next(), args.next()) {
        (Some(i), Some(o)) => (PathBuf::from(i), PathBuf::from(o)),
        _ => print_usage_and_exit(&program),
    };

    // Time the call to split_zip_to_parquet
    let start = Instant::now();
    split_zip_to_parquet(&input_path, &out_dir)?;
    let elapsed = start.elapsed();

    println!(
        "Finished splitting `{}` to `{}` in {:.3}s",
        input_path.display(),
        out_dir.display(),
        elapsed.as_secs_f64()
    );

    Ok(())
}
