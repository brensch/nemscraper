use anyhow::Result;
use google_cloud_bigquery::client::{Client as BigqueryClient, ClientConfig as BigqueryConfig};
use google_cloud_bigquery::http::dataset::{Dataset as BqDataset, DatasetReference};
use google_cloud_bigquery::http::job::{
    Job, JobConfiguration, JobConfigurationQuery, JobReference, JobType,
};
use google_cloud_storage::client::{Client as StorageClient, ClientConfig as StorageConfig};
use google_cloud_storage::http::objects::list::ListObjectsRequest;

// =========================================================================
// Configuration
// =========================================================================
const GCP_PROJECT_ID: &str = "selfforecasting";
const BQ_DATASET_ID: &str = "aemo_analytics";
const GCS_BUCKET_NAME: &str = "aemo_data";
const GCS_FOLDER_PREFIX: &str = "";
const BQ_LOCATION: &str = "US";
// =========================================================================

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize crypto provider for rustls
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    // --- BigQuery client ---
    let (bq_config, _project_id) = BigqueryConfig::new_with_auth().await?;
    let bq_client = BigqueryClient::new(bq_config).await?;

    // --- GCS client ---
    let storage_config = StorageConfig::default().with_auth().await?;
    let storage_client = StorageClient::new(storage_config);

    // Step 1: Ensure the target BigQuery dataset exists.
    ensure_dataset_exists(&bq_client).await?;

    // Step 2: List the top-level "folders" in the GCS bucket.
    let gcs_folders = get_top_level_folders(&storage_client).await?;

    // Step 3: Iterate through each folder and create the corresponding tables.
    for folder_name in &gcs_folders {
        if let Err(e) = create_bigquery_tables_for_folder(&bq_client, folder_name).await {
            eprintln!("Failed to process folder '{}': {:?}", folder_name, e);
        }
    }

    println!("\n--- Rust automation script finished. ---");
    Ok(())
}

async fn ensure_dataset_exists(bq_client: &BigqueryClient) -> Result<()> {
    println!("Ensuring dataset '{}' exists...", BQ_DATASET_ID);

    if bq_client
        .dataset()
        .get(GCP_PROJECT_ID, BQ_DATASET_ID)
        .await
        .is_ok()
    {
        println!("Dataset '{}' already exists.", BQ_DATASET_ID);
        return Ok(());
    }

    println!("Dataset not found. Creating...");
    // Build the Dataset metadata
    let mut metadata = BqDataset::default();
    // Fix 1: Remove Some() wrapper - dataset_reference expects DatasetReference directly
    metadata.dataset_reference = DatasetReference {
        project_id: GCP_PROJECT_ID.to_string(),
        dataset_id: BQ_DATASET_ID.to_string(),
    };
    // Fix 2: Remove Some() wrapper - location expects String directly
    metadata.location = BQ_LOCATION.to_string();

    bq_client.dataset().create(&metadata).await?;
    println!("Successfully created dataset '{}'", BQ_DATASET_ID);
    Ok(())
}

async fn get_top_level_folders(storage_client: &StorageClient) -> Result<Vec<String>> {
    println!(
        "\nListing top-level folders in gs://{}/{}",
        GCS_BUCKET_NAME, GCS_FOLDER_PREFIX
    );

    let req = ListObjectsRequest {
        bucket: GCS_BUCKET_NAME.to_string(),
        prefix: Some(GCS_FOLDER_PREFIX.to_string()),
        delimiter: Some("/".to_string()),
        ..Default::default()
    };
    let resp = storage_client.list_objects(&req).await?;

    // Fix 3: Handle the prefixes field properly
    let cleaned = resp
        .prefixes
        .unwrap_or_default() // Handle Option<Vec<String>> if that's the case
        .into_iter()
        .map(|f| {
            // f is now String, so replace method should work
            f.replace(GCS_FOLDER_PREFIX, "")
                .trim_end_matches('/')
                .to_string()
        })
        .filter(|f| !f.is_empty()) // Filter out empty strings
        .collect();

    println!("Found folders: {:?}", cleaned);
    Ok(cleaned)
}

fn generate_bq_table_name(gcs_folder_name: &str) -> String {
    gcs_folder_name.to_lowercase().replace("---", "_")
}

async fn run_bq_query(bq_client: &BigqueryClient, query: String) -> Result<()> {
    println!("Executing query: {}", query.trim());

    // Generate a unique job ID
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();

    // Create a job for the DDL statement
    let job = Job {
        job_reference: JobReference {
            project_id: GCP_PROJECT_ID.to_string(),
            job_id: format!("ddl_job_{}", timestamp),
            location: Some(BQ_LOCATION.to_string()),
        },
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

    // Create and run the job
    let result = bq_client.job().create(&job).await?;

    if result.status.errors.is_some() || result.status.error_result.is_some() {
        if let Some(error) = result.status.error_result {
            return Err(anyhow::anyhow!("BigQuery job failed: {:?}", error));
        }
        if let Some(errors) = result.status.errors {
            return Err(anyhow::anyhow!("BigQuery job had errors: {:?}", errors));
        }
    }

    println!(
        "Query completed successfully. Job ID: {}",
        result.job_reference.job_id
    );
    Ok(())
}

async fn create_bigquery_tables_for_folder(
    bq_client: &BigqueryClient,
    folder_name: &str,
) -> Result<()> {
    if folder_name.is_empty() {
        return Ok(());
    }

    let table_base = generate_bq_table_name(folder_name);
    let ext_id = format!("{}.{}.ext_{}", GCP_PROJECT_ID, BQ_DATASET_ID, table_base);
    let native_id = format!("{}.{}.{}", GCP_PROJECT_ID, BQ_DATASET_ID, table_base);
    let gcs_uri = format!("gs://{}/{}/*", GCS_BUCKET_NAME, folder_name);
    let hive_prefix = format!("gs://{}/{}", GCS_BUCKET_NAME, folder_name);

    println!("\n--- Processing folder: {} ---", folder_name);
    println!("Creating external table: {}", ext_id);
    let external_sql = format!(
        r#"
CREATE OR REPLACE EXTERNAL TABLE `{}`
WITH PARTITION COLUMNS (
  date DATE
)
OPTIONS (
  format = 'PARQUET',
  uris = ['{}'],
  hive_partition_uri_prefix = '{}',
  require_hive_partition_filter = false
);
"#,
        ext_id, gcs_uri, hive_prefix
    );
    run_bq_query(bq_client, external_sql).await?;
    println!("SUCCESS: Created external table {}", ext_id);

    println!("Creating native table: {}", native_id);
    let native_sql = format!(
        r#"
CREATE OR REPLACE TABLE `{}` PARTITION BY date
AS SELECT * FROM `{}`;"#,
        native_id, ext_id
    );
    run_bq_query(bq_client, native_sql).await?;
    println!("SUCCESS: Loaded data into native table {}", native_id);

    Ok(())
}
