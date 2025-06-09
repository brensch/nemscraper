// This program demonstrates concurrent URL downloading in Rust.
//
// To run this, you'll need to add the following to your `Cargo.toml`:
//
// [dependencies]
// tokio = { version = "1", features = ["full"] }
// tokio-stream = "0.1"
// reqwest = "0.11" // A popular HTTP client, though we only simulate downloads here.
// futures = "0.3"

use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

/// Simulates downloading a single URL, now with a worker ID for logging.
/// In a real application, this function would use an HTTP client like `reqwest`
/// to fetch the content from the URL.
async fn download_url(url: &str, worker_id: usize) {
    println!("[Worker {}] -> Starting download: {}", worker_id, url);
    // Simulate the time it takes to download the content.
    tokio::time::sleep(Duration::from_secs(2)).await;
    println!("[Worker {}] <- Finished download: {}", worker_id, url);
}

#[tokio::main]
async fn main() {
    const CONCURRENT_LIMIT: usize = 3;

    // A list of URLs to be "downloaded".
    let urls_to_process = vec![
        "https://example.com/file1.zip",
        "https://example.com/file2.zip",
        "https://example.com/file3.zip",
        "https://example.com/file4.zip",
        "https://example.com/file5.zip",
        "https://example.com/file6.zip",
        "https://example.com/file7.zip",
        "https://example.com/file8.zip",
        "https://example.com/file9.zip",
        "https://example.com/file10.zip",
    ];

    // --- The URL Queue ---
    // We use an unbounded channel for the list of URLs to process.
    let (url_tx, mut url_rx) = mpsc::unbounded_channel();

    // --- The Concurrency Limiter / Worker Pool ---
    // Instead of a semaphore, we use a bounded channel as a pool of worker IDs.
    // A task must acquire an ID from this channel before it can run.
    // The channel's capacity limits the concurrency.
    let (worker_id_tx, mut worker_id_rx) = mpsc::channel(CONCURRENT_LIMIT);

    // Pre-fill the pool with worker IDs.
    for i in 1..=CONCURRENT_LIMIT {
        // We can unwrap here because the channel has capacity and we know it's not closed.
        worker_id_tx.send(i).await.unwrap();
    }

    // --- The Producer ---
    // Spawn a separate task to act as the producer. This task will loop through
    // our list of URLs and send them into the channel.
    let producer_task = tokio::spawn(async move {
        for url in urls_to_process {
            if let Err(e) = url_tx.send(url.to_string()) {
                eprintln!("Failed to send URL to queue: {}", e);
                break;
            }
        }
        // The sender (url_tx) is dropped here, which will cause the url_rx.recv()
        // loop in the consumer to eventually end.
    });

    println!(
        "Starting URL processing... Concurrency limit: {}",
        CONCURRENT_LIMIT
    );

    // --- The Consumer ---
    // The consumer pulls a URL from the queue, waits for a free worker ID,
    // and then spawns a task to perform the download.
    let consumer_task = tokio::spawn(async move {
        let mut download_tasks = JoinSet::new();

        while let Some(url) = url_rx.recv().await {
            // Wait until a worker ID is available from the pool.
            // This blocks if all workers are currently busy.
            let worker_id = worker_id_rx.recv().await.unwrap();

            // We need a clone of the sender to move into the download task
            // so it can return its ID to the pool.
            let worker_id_tx_clone = worker_id_tx.clone();

            // Spawn the actual download task.
            download_tasks.spawn(async move {
                download_url(&url, worker_id).await;
                // Return the worker ID to the pool so another task can use it.
                worker_id_tx_clone.send(worker_id).await.unwrap();
            });
        }

        // Wait for all the spawned download tasks to complete.
        while let Some(res) = download_tasks.join_next().await {
            if let Err(e) = res {
                eprintln!("A download task failed: {}", e);
            }
        }
    });

    // Wait for both the producer and consumer tasks to complete.
    if let Err(e) = tokio::try_join!(producer_task, consumer_task) {
        eprintln!("A task failed to complete: {}", e);
    }

    println!("\nAll URLs have been processed.");
}
