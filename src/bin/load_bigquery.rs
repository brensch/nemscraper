use anyhow::Result;
use futures::future::join_all;
use google_cloud_bigquery::client::{Client as BigqueryClient, ClientConfig as BigqueryConfig};
use google_cloud_bigquery::http::dataset::{Dataset as BqDataset, DatasetReference};
use google_cloud_bigquery::http::job::{
    Job, JobConfiguration, JobConfigurationQuery, JobReference, JobType,
};
use google_cloud_storage::client::{Client as StorageClient, ClientConfig as StorageConfig};
use google_cloud_storage::http::objects::list::ListObjectsRequest;

const GCP_PROJECT_ID: &str = "selfforecasting";
const BQ_DATASET_ID: &str = "aemo_analytics";
const GCS_BUCKET_NAME: &str = "aemo_data";
const GCS_FOLDER_PREFIX: &str = "";
const BQ_LOCATION: &str = "US";

#[tokio::main]
async fn main() -> Result<()> {
    // 0) bootstrap TLS for rustls
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    // 1) BigQuery + GCS clients
    let (bq_config, _) = BigqueryConfig::new_with_auth().await?;
    let bq_client = BigqueryClient::new(bq_config).await?;
    let storage_cfg = StorageConfig::default().with_auth().await?;
    let storage_client = StorageClient::new(storage_cfg);

    // 2) ensure dataset
    ensure_dataset_exists(&bq_client).await?;

    // 3) list folders
    let folders = get_top_level_folders(&storage_client).await?;

    // 4) build all CREATE EXTERNAL TABLE statements
    let mut ext_sqls = Vec::with_capacity(folders.len());
    for folder in folders.into_iter().filter(|f| !f.is_empty()) {
        let table_name = folder.to_lowercase().replace("---", "_");
        let ext_id = format!("{}.{}.ext_{}", GCP_PROJECT_ID, BQ_DATASET_ID, table_name);
        let gcs_uri = format!("gs://{}/{}/*", GCS_BUCKET_NAME, folder);
        let hive_pref = format!("gs://{}/{}", GCS_BUCKET_NAME, folder);

        let sql = format!(
            r#"
CREATE OR REPLACE EXTERNAL TABLE `{ext_id}`
WITH PARTITION COLUMNS (
  date DATE
)
OPTIONS (
  format = 'PARQUET',
  uris = ['{gcs_uri}'],
  hive_partition_uri_prefix = '{hive_pref}',
  require_hive_partition_filter = FALSE
)"#,
            ext_id = ext_id,
            gcs_uri = gcs_uri,
            hive_pref = hive_pref,
        );
        ext_sqls.push(sql);
    }

    // 5) fire them all off in parallel
    let jobs = ext_sqls.into_iter().map(|q| run_bq_query(&bq_client, q));
    let results = join_all(jobs).await;
    for res in results {
        if let Err(err) = res {
            eprintln!("BigQuery DDL failed: {:?}", err);
        }
    }

    println!("✔︎ all external tables creation jobs dispatched");
    Ok(())
}

async fn ensure_dataset_exists(bq_client: &BigqueryClient) -> Result<()> {
    if bq_client
        .dataset()
        .get(GCP_PROJECT_ID, BQ_DATASET_ID)
        .await
        .is_ok()
    {
        return Ok(());
    }
    let mut meta = BqDataset::default();
    meta.dataset_reference = DatasetReference {
        project_id: GCP_PROJECT_ID.to_string(),
        dataset_id: BQ_DATASET_ID.to_string(),
    };
    meta.location = BQ_LOCATION.to_string();
    bq_client.dataset().create(&meta).await?;
    Ok(())
}

async fn get_top_level_folders(storage: &StorageClient) -> Result<Vec<String>> {
    let resp = storage
        .list_objects(&ListObjectsRequest {
            bucket: GCS_BUCKET_NAME.to_string(),
            prefix: Some(GCS_FOLDER_PREFIX.to_string()),
            delimiter: Some("/".to_string()),
            ..Default::default()
        })
        .await?;
    Ok(resp
        .prefixes
        .unwrap_or_default()
        .into_iter()
        .map(|p| {
            p.replace(GCS_FOLDER_PREFIX, "")
                .trim_end_matches('/')
                .to_string()
        })
        .filter(|s| !s.is_empty())
        .collect())
}

async fn run_bq_query(bq: &BigqueryClient, query: String) -> Result<()> {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let job_ref = JobReference {
        project_id: GCP_PROJECT_ID.to_string(),
        job_id: format!("ddl_{}", ts),
        location: Some(BQ_LOCATION.to_string()),
    };
    let job = Job {
        job_reference: job_ref,
        configuration: JobConfiguration {
            job: JobType::Query(JobConfigurationQuery {
                query,
                use_legacy_sql: Some(false),
                ..Default::default()
            }),
            ..Default::default()
        },
        ..Default::default()
    };
    let res = bq.job().create(&job).await?;
    if res.status.error_result.is_some() || res.status.errors.is_some() {
        anyhow::bail!("BigQuery reported errors: {:?}", res.status);
    }
    println!("✅  Job {} submitted", res.job_reference.job_id);
    Ok(())
}
