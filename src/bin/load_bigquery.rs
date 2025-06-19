// //! src/bin/load_bigquery.rs

// use anyhow::{Context, Result};
// use futures::future::join_all;
// use google_cloud_bigquery::client::{Client as BqClient, ClientConfig as BqConfig};
// use google_cloud_bigquery::http::job::query::QueryRequest;
// use google_cloud_bigquery::query::row::Row;
// use google_cloud_storage::client::{Client as StorageClient, ClientConfig as StorageConfig};
// use google_cloud_storage::http::objects::list::ListObjectsRequest; // ← correct
// use rustls::crypto::aws_lc_rs::default_provider;

// #[tokio::main]
// async fn main() -> Result<()> {
//     let _ = default_provider().install_default();

//     let project =
//         std::env::var("GOOGLE_CLOUD_PROJECT").context("Please set env var GOOGLE_CLOUD_PROJECT")?;
//     let dataset = "aemo_analytics";
//     let bucket = "aemo_data";

//     // GCS client
//     let storage_cfg = StorageConfig::default()
//         .with_auth()
//         .await
//         .context("authenticating GCS client")?;
//     let storage_client = StorageClient::new(storage_cfg);

//     // list “folders” under gs://bucket/ with delimiter = "/"
//     let list_req = ListObjectsRequest {
//         bucket: bucket.to_string(),
//         delimiter: Some("/".to_string()),
//         ..Default::default()
//     };
//     let resp = storage_client
//         .list_objects(&list_req)
//         .await
//         .context("listing bucket prefixes")?;
//     let prefixes = resp.prefixes.unwrap_or_default();

//     if prefixes.is_empty() {
//         println!("⚠️  No top-level folders under gs://{}/", bucket);
//         return Ok(());
//     }

//     // BigQuery client
//     let (bq_cfg, project_opt) = BqConfig::new_with_auth()
//         .await
//         .context("authenticating BigQuery client")?;
//     let project_id =
//         project_opt.ok_or_else(|| anyhow::anyhow!("no project_id from credentials"))?;
//     let bq_client = BqClient::new(bq_cfg).await?;

//     // spawn all DDLs in parallel
//     let mut tasks = Vec::with_capacity(prefixes.len());
//     for raw_prefix in prefixes {
//         let raw = raw_prefix.trim_end_matches('/');
//         let table_id = format!("ext_{}", raw.replace('-', "_").to_lowercase());
//         let uri_prefix = format!("gs://{}/{}/", bucket, raw);
//         let gcs_uri = format!("{}*", uri_prefix);

//         let ddl = format!(
//             r#"
// CREATE OR REPLACE EXTERNAL TABLE `{project}.{dataset}.{table_id}`
// WITH PARTITION COLUMNS (
//   date DATE
// )
// OPTIONS (
//   format = 'PARQUET',
//   uris = ['{gcs_uri}'],
//   hive_partition_uri_prefix = '{uri_prefix}',
//   require_hive_partition_filter = FALSE
// );
// "#,
//             project = project,
//             dataset = dataset,
//             table_id = table_id,
//             gcs_uri = gcs_uri,
//             uri_prefix = uri_prefix,
//         );

//         let client = bq_client.clone();
//         let proj = project_id.clone();
//         let dataset = dataset.to_string();
//         let table_ref = table_id.clone();

//         tasks.push(tokio::spawn(async move {
//             println!("▶ Creating/updating: {}.{}", dataset, table_ref);
//             let req = QueryRequest {
//                 query: ddl,
//                 use_legacy_sql: false,
//                 ..Default::default()
//             };
//             let mut rows = client.query::<Row>(&proj, req).await?;
//             while let Some(_) = rows.next().await? { /* drain */ }
//             Ok::<_, anyhow::Error>(())
//         }));
//     }

//     let results = join_all(tasks).await;
//     let success = results
//         .into_iter()
//         .filter(|r| matches!(r, Ok(Ok(()))))
//         .count();
//     println!("✅ {} external tables created/updated.", success);

//     Ok(())
// }

fn main() {
    // This is a placeholder for the main function.
    // The actual implementation would go here.
    println!("This is a placeholder for the load_bigquery.rs script.");
}
