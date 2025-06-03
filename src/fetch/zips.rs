use anyhow::Result;
use reqwest::Client;
use std::path::{Path, PathBuf};
use tokio::fs;
use url::Url;

/// Download the given ZIP URL and save it under `dest_dir` using the original filename.
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
        .and_then(|segments| segments.last())
        .filter(|name| !name.is_empty())
        .unwrap_or("download.zip");
    let dest_path = dest_dir.join(filename);

    if let Some(parent) = dest_path.parent() {
        fs::create_dir_all(parent).await?;
    }

    let resp = client.get(url.as_str()).send().await?.error_for_status()?;
    let bytes = resp.bytes().await?;
    fs::write(&dest_path, &bytes).await?;

    Ok(dest_path)
}
