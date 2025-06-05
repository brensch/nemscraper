use anyhow::Result;
use reqwest::Client;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::time::sleep;
use tokio_stream::StreamExt;
use url::Url;

/// Download the given ZIP URL and save it under `dest_dir` using the original filename.
/// Streams directly to disk without loading the entire file into memory.
/// Retries up to 3 times with exponential backoff on failure.
/// Returns the full path of the saved file.
pub async fn download_zip(
    client: &Client,
    url_str: &str,
    dest_dir: impl AsRef<Path>,
) -> Result<PathBuf> {
    let dest_dir = dest_dir.as_ref();
    let url = Url::parse(url_str)?;

    let filename = url
        .path_segments()
        .and_then(|mut segments| segments.next_back())
        .filter(|name| !name.is_empty())
        .unwrap_or("download.zip");

    let dest_path = dest_dir.join(filename);

    if let Some(parent) = dest_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let mut attempt = 0;
    const MAX_RETRIES: usize = 3;

    loop {
        attempt += 1;

        match download_attempt(client, &url, &dest_path).await {
            Ok(()) => {
                return Ok(dest_path);
            }
            Err(e) if attempt >= MAX_RETRIES => {
                // Clean up any partial file on final failure
                let _ = tokio::fs::remove_file(&dest_path).await;
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
                // Clean up any partial file before retry
                let _ = tokio::fs::remove_file(&dest_path).await;

                // Exponential backoff: 1s, 2s, 4s
                let delay = Duration::from_secs(1 << (attempt - 1));
                tracing::warn!(
                    "Download attempt {}/{} failed for {}: {}. Retrying in {:?}...",
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

/// Single download attempt - downloads the URL and streams to the given path.
async fn download_attempt(
    client: &Client,
    url: &Url,
    dest_path: &Path,
) -> Result<(), anyhow::Error> {
    let resp = client.get(url.as_str()).send().await?.error_for_status()?;

    // Create the output file
    let mut file = File::create(dest_path).await?;

    // Stream the response body directly to the file
    let mut stream = resp.bytes_stream();
    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result?;
        file.write_all(&chunk).await?;
    }

    // Ensure all data is written to disk
    file.flush().await?;

    Ok(())
}
