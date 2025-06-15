use anyhow::Result;
use chrono::Local;
use nemscraper::process::split::stream_zip_to_parquet_gcs;
use pprof::ProfilerGuard;
use serde::{Deserialize, Serialize};
use std::{
    env,
    fs::{self, File},
    io::Write,
    path::PathBuf,
    time::Instant,
};
use tracing::{info, warn, Level};
use tracing_subscriber::{fmt, EnvFilter};
use warp::{reject::Rejection, reply::Reply, Filter};

#[derive(Deserialize)]
struct ProcessRequest {
    zip_url: String,
    gcs_bucket: String,
    gcs_prefix: Option<String>, // Optional prefix for organized storage
}

#[derive(Serialize)]
struct ProcessResponse {
    success: bool,
    message: String,
    rows_processed: u64,
    bytes_written: u64,
    processing_time_seconds: f64,
    gcs_objects_created: Vec<String>,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
    details: Option<String>,
}

async fn health_check() -> Result<impl Reply, Rejection> {
    Ok(warp::reply::json(&serde_json::json!({
        "status": "healthy",
        "service": "zip-to-parquet-processor"
    })))
}

async fn process_zip(req: ProcessRequest) -> Result<impl Reply, Rejection> {
    let start = Instant::now();

    info!(
        "Processing request: zip_url={}, gcs_bucket={}, prefix={:?}",
        req.zip_url, req.gcs_bucket, req.gcs_prefix
    );

    // Optional profiling for development
    let enable_profiling = env::var("ENABLE_PROFILING").unwrap_or_default() == "true";
    let _guard = if enable_profiling {
        Some(
            ProfilerGuard::new(100)
                .map_err(|e| {
                    warn!("Failed to start profiler: {}", e);
                    e
                })
                .ok(),
        )
    } else {
        None
    };

    match stream_zip_to_parquet_gcs(&req.zip_url, &req.gcs_bucket, req.gcs_prefix.as_deref()).await
    {
        Ok((metrics, gcs_objects)) => {
            let elapsed = start.elapsed().as_secs_f64();

            info!(
                "✅ Successfully processed {} rows → {} bytes in {:.3}s, created {} GCS objects",
                metrics.rows,
                metrics.bytes,
                elapsed,
                gcs_objects.len()
            );

            // Write flamegraph if profiling enabled
            if enable_profiling {
                if let Some(Some(guard)) = _guard {
                    write_flamegraph(guard).await.unwrap_or_else(|e| {
                        warn!("Failed to write flamegraph: {}", e);
                    });
                }
            }

            Ok(warp::reply::json(&ProcessResponse {
                success: true,
                message: "ZIP processed successfully".to_string(),
                rows_processed: metrics.rows,
                bytes_written: metrics.bytes,
                processing_time_seconds: elapsed,
                gcs_objects_created: gcs_objects,
            }))
        }
        Err(e) => {
            let elapsed = start.elapsed().as_secs_f64();
            warn!("❌ Error processing ZIP after {:.3}s: {:?}", elapsed, e);

            Ok(warp::reply::json(&ErrorResponse {
                error: "Processing failed".to_string(),
                details: Some(format!("{:?}", e)),
            }))
        }
    }
}

async fn write_flamegraph(guard: ProfilerGuard<'_>) -> Result<()> {
    let now = Local::now().format("%Y%m%d_%H%M%S").to_string();
    let flames_dir = PathBuf::from("flames");
    fs::create_dir_all(&flames_dir)?;

    if let Ok(report) = guard.report().build() {
        let svg_path = flames_dir.join(format!("flamegraph_{}.svg", now));
        let mut svg_file = File::create(&svg_path)?;
        report.flamegraph(&mut svg_file)?;
        info!("Wrote flamegraph SVG to {}", svg_path.display());
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    let log_level = env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string());
    fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive(log_level.parse().unwrap_or(Level::INFO.into())),
        )
        .init();

    info!("Starting ZIP to Parquet processor service");

    // Health check endpoint
    let health = warp::path("health").and(warp::get()).and_then(health_check);

    // Main processing endpoint
    let process = warp::path("process")
        .and(warp::post())
        .and(warp::body::json())
        .and_then(process_zip);

    // Combine routes
    let routes = health.or(process);

    // Get port from environment or default to 8080 (Cloud Run default)
    let port: u16 = env::var("PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse()
        .unwrap_or(8080);

    info!("Server starting on port {}", port);
    info!("Health check: http://localhost:{}/health", port);
    info!("Process endpoint: POST http://localhost:{}/process", port);

    warp::serve(routes).run(([0, 0, 0, 0], port)).await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_check() {
        let result = health_check().await;
        assert!(result.is_ok());
    }

    // Example test for local development
    #[tokio::test]
    #[ignore] // Remove this to run the test
    async fn test_process_local() {
        let req = ProcessRequest {
            zip_url: "https://example.com/test.zip".to_string(),
            gcs_bucket: "test-bucket".to_string(),
            gcs_prefix: Some("test/".to_string()),
        };

        // This would fail without proper GCS setup, but shows the structure
        let result = process_zip(req).await;
        // Add appropriate assertions based on your test setup
    }
}
