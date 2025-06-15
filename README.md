# nemscraper
scrape all files from the nem archives and compress them to a more usable parquet format.

# Usage
## Remove all processed files to reprocess
```bash
find assets/history -type f -name '*Proc*' -delete
```

## 

# Benchmarking Compression Algorithms

Note using brotli 5 for compression, it performed the best

Group      | Algorithm    | Level | CompressedSize  | TimeMs    
-----------------------------------------------------------------
smallest   | BROTLI       | 5     | 2301            | 0         
smallest   | BROTLI       | 7     | 2301            | 4         
smallest   | ZSTD         | 1     | 2365            | 0         
smallest   | ZSTD         | 10    | 2365            | 16        
smallest   | ZSTD         | 15    | 2365            | 63        
largest    | BROTLI       | 5     | 551840901       | 50088     
largest    | BROTLI       | 7     | 549416093       | 72331     
largest    | ZSTD         | 1     | 742352382       | 23886     
largest    | ZSTD         | 10    | 582382671       | 67637     
largest    | ZSTD         | 15    | 581370235       | 286124    


## provision gcp client for viewing

```
# 1. Make sure you’re operating on the right project
gcloud config set project selfforecasting

# 2. Enable the BigQuery API (if not already)
gcloud services enable bigquery.googleapis.com

# 3. Create a dedicated service account for Grafana
gcloud iam service-accounts create grafana-bq \
  --display-name="Grafana BigQuery Client"

# 4. Grant it permissions to run queries and read data
gcloud projects add-iam-policy-binding selfforecasting \
  --member="serviceAccount:grafana-bq@selfforecasting.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding selfforecasting \
  --member="serviceAccount:grafana-bq@selfforecasting.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"

# note this is only for aemo_data
gcloud storage buckets add-iam-policy-binding gs://aemo_data \
  --member="serviceAccount:grafana-bq@selfforecasting.iam.gserviceaccount.com" \
  --role="roles/storage.objectViewer"

# 5. Create a JSON key for that service account
gcloud iam service-accounts keys create grafana-bq-key.json \
  --iam-account=grafana-bq@selfforecasting.iam.gserviceaccount.com
```