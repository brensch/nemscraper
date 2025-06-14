use anyhow::Result;
use chrono::Local;
use nemscraper::process::split::stream_zip_to_parquet;
use pprof::ProfilerGuard;
use std::{
    env,
    fs::{self, File},
    io::Write,
    path::PathBuf,
    time::Instant,
};
use tracing::Level;
use tracing_subscriber::{fmt, EnvFilter};

fn print_usage_and_exit(program: &str) -> ! {
    eprintln!("Usage: {} <input-zip-url> <output-dir>", program);
    std::process::exit(1);
}

#[tokio::main]
async fn main() -> Result<()> {
    // Start profiler at 100Hz
    let guard = ProfilerGuard::new(100)?;

    // Init tracing
    fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(Level::DEBUG.into()))
        .init();

    let mut args = env::args();
    let prog = args.next().unwrap_or_else(|| "bench_zip_to_parquet".into());
    let (input, out_dir) = match (args.next(), args.next()) {
        (Some(i), Some(o)) => (i, PathBuf::from(o)),
        _ => print_usage_and_exit(&prog),
    };

    // Time the run
    let start = Instant::now();
    let res = stream_zip_to_parquet(&input, &out_dir).await;
    let elapsed = start.elapsed().as_secs_f64();

    match res {
        Ok(metrics) => {
            println!(
                "✅ Processed {} rows → {} bytes in {:.3}s",
                metrics.rows, metrics.bytes, elapsed
            );
        }
        Err(e) => {
            eprintln!("❌ Error: {:?}", e);
            std::process::exit(1);
        }
    }

    // Prepare flames dir and timestamped filenames
    let now = Local::now().format("%Y%m%d_%H%M%S").to_string();
    let flames_dir = PathBuf::from("flames");
    fs::create_dir_all(&flames_dir)?;

    // Emit SVG
    if let Ok(report) = guard.report().build() {
        let svg_path = flames_dir.join(format!("flamegraph_{}.svg", now));
        let mut svg_file = File::create(&svg_path)?;
        report.flamegraph(&mut svg_file)?;
        println!("Wrote flamegraph SVG to {}", svg_path.display());

        // Dump raw protobuf for text conversion
        #[cfg(feature = "_protobuf")]
        {
            let profile = report.pprof()?;
            let mut buf = Vec::new();
            profile.encode(&mut buf)?;
            let pb_path = flames_dir.join(format!("profile_{}.pb", now));
            let mut pb_file = File::create(&pb_path)?;
            pb_file.write_all(&buf)?;
            println!("Wrote raw profile to {}", pb_path.display());
        }
    }

    Ok(())
}
