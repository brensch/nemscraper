// data/src/process/split.rs
use anyhow::Result;
use num_cpus;
use rayon::prelude::*;
use std::fs;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, instrument};
use zip::ZipArchive;

use crate::process::chunk::chunk_and_write_segment;

#[instrument(level = "info", skip(zip_path, out_dir), fields(zip = %zip_path.as_ref().display()))]
pub fn split_zip_to_parquet<P: AsRef<Path>, Q: AsRef<Path>>(zip_path: P, out_dir: Q) -> Result<()> {
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_cpus::get())
        .build_global()
        .ok();

    let start = Instant::now();
    info!("starting split_zip_to_parquet");

    let zip_path = zip_path.as_ref().to_path_buf();
    let out_dir = out_dir.as_ref().to_path_buf();
    fs::create_dir_all(&out_dir)?;

    let file = File::open(&zip_path)?;
    let mut archive = ZipArchive::new(file)?;

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        let name = entry.name().to_string();
        if !name.to_lowercase().ends_with(".csv") {
            continue;
        }

        let mut buf = Vec::new();
        entry.read_to_end(&mut buf)?;
        let text = Arc::new(String::from_utf8_lossy(&buf).to_string());

        let segments: Vec<(String, usize, usize, usize)> = {
            let mut segs = Vec::new();
            let mut pos = 0;
            let mut header_start = 0;
            let mut data_start = 0;
            let mut current_schema = None;
            let mut seen_footer = false;

            for chunk in text.split_inclusive('\n') {
                if chunk.starts_with("C,") {
                    if seen_footer {
                        break;
                    }
                    seen_footer = true;
                    pos += chunk.len();
                    continue;
                }
                if chunk.starts_with("I,") {
                    if let Some(schema_id) = current_schema.take() {
                        segs.push((schema_id, header_start, data_start, pos));
                    }
                    header_start = pos;
                    let parts: Vec<&str> = chunk.trim_end_matches('\n').split(',').collect();
                    current_schema = Some(format!("{}_{}", parts[1], parts[2]));
                    data_start = pos + chunk.len();
                }
                pos += chunk.len();
            }
            if let Some(schema_id) = current_schema {
                segs.push((schema_id, header_start, data_start, pos));
            }
            segs
        };

        segments
            .into_par_iter()
            .for_each(|(schema_id, hs, ds, es)| {
                let header = &text[hs..ds];
                let data = &text[ds..es];
                chunk_and_write_segment(&name, &schema_id, header, data, &out_dir);
            });
    }

    info!("completed in {:?}", start.elapsed());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::{env, fs, io::Write};
    use tempfile::NamedTempFile;
    use tracing_subscriber::{EnvFilter, FmtSubscriber};
    use zip::write::{ExtendedFileOptions, FileOptions};
    use zip::CompressionMethod;

    #[test]
    fn test_split_zip_to_parquet() -> Result<()> {
        let subscriber = FmtSubscriber::builder()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| EnvFilter::new("info,data::process=trace")),
            )
            .with_test_writer()
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);

        // prepare sample ZIP…
        let zip_path = if let Ok(path) = env::var("ZIP_PATH") {
            PathBuf::from(path)
        } else {
            let sample = r#"C,SETP.WORLD,FPP_DCF,AEMO,PUBLIC,\"2024/12/14\",...\nI,FPP,...\nD,FPP,...\nI,FPP,...\nD,FPP,...\"#;
            let mut buf = Vec::new();
            {
                let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
                let options = FileOptions::<ExtendedFileOptions>::default()
                    .compression_method(CompressionMethod::Stored);
                zip.start_file("aemo.csv", options)?;
                zip.write_all(sample.as_bytes())?;
                zip.finish()?;
            }
            let tmp = NamedTempFile::new()?;
            tmp.reopen()?.write_all(&buf)?;
            tmp.path().to_path_buf()
        };

        let out_dir = PathBuf::from("tests/output");
        if !out_dir.exists() {
            fs::create_dir_all(&out_dir)?;
        }

        split_zip_to_parquet(&zip_path, &out_dir)?;
        let files: Vec<_> = fs::read_dir(&out_dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("parquet"))
            .collect();
        assert!(!files.is_empty(), "no parquet files produced");
        Ok(())
    }
}
