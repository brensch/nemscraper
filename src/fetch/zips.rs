use anyhow::{Context, Result};
use reqwest::Client;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tokio::time::sleep;
use tokio_stream::StreamExt;
use url::Url;

/// The result of a download: where it ended up on disk, and how many bytes were written.
pub struct DownloadResult {
    pub path: PathBuf,
    pub size: u64,
}

/// Download the given ZIP URL and save it under `dest_dir` using the original filename.
/// Streams directly to disk without loading the entire file into memory.
/// Retries up to 3 times with exponential backoff on failure.
/// Returns the final path *and* the total size in bytes.
pub async fn download_zip(
    client: &Client,
    url_str: &str,
    dest_dir: impl AsRef<Path>,
) -> Result<DownloadResult> {
    let dest_dir = dest_dir.as_ref();
    let url = Url::parse(url_str)?;

    let filename = url
        .path_segments()
        .and_then(|mut segments| segments.next_back())
        .filter(|name| !name.is_empty())
        .unwrap_or("download.zip");

    let dest_path = dest_dir.join(filename);
    if let Some(parent) = dest_path.parent() {
        fs::create_dir_all(parent).await?;
    }

    let mut attempt = 0;
    const MAX_RETRIES: usize = 3;

    loop {
        attempt += 1;

        match download_attempt(client, &url, &dest_path).await {
            Ok(()) => {
                // after the file is fully written, read its metadata to get the size
                let meta = fs::metadata(&dest_path)
                    .await
                    .with_context(|| format!("reading metadata for {:?}", dest_path))?;
                return Ok(DownloadResult {
                    path: dest_path,
                    size: meta.len(),
                });
            }
            Err(e) if attempt >= MAX_RETRIES => {
                let _ = fs::remove_file(&dest_path).await;
                return Err(e.context(format!(
                    "Failed to download {} after {} attempts",
                    url_str, MAX_RETRIES
                )));
            }
            Err(e) => {
                if let Some(req_err) = e.downcast_ref::<reqwest::Error>() {
                    if req_err.status() == Some(reqwest::StatusCode::NOT_FOUND) {
                        return Err(e.context(format!("got 404, bailing: {}", url_str)));
                    }
                }
                let _ = fs::remove_file(&dest_path).await;
                let delay = Duration::from_secs(1 << (attempt - 1));
                tracing::warn!(
                    "Download attempt {}/{} failed for {}: {}. Retrying in {:?}…",
                    attempt,
                    MAX_RETRIES,
                    url_str,
                    e,
                    delay
                );
                sleep(delay).await;
            }
        }
    }
}

async fn download_attempt(
    client: &Client,
    url: &Url,
    dest_path: &Path,
) -> Result<(), anyhow::Error> {
    let resp = client.get(url.as_str()).send().await?.error_for_status()?;
    let mut file = File::create(dest_path).await?;
    let mut stream = resp.bytes_stream();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk).await?;
    }

    file.flush().await?;
    Ok(())
}
