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


## Deploy services

Note we build locally since cloud build is slow af

```bash
gcloud artifacts repositories create api-repo \
  --repository-format=docker --location=us-central1 \
  --description="API Processor images" --project=selfforecasting

gcloud iam service-accounts create api-processor-sa \
  --display-name "API Processor Service Account"

gcloud projects add-iam-policy-binding selfforecasting \
  --member="serviceAccount:api-processor-sa@selfforecasting.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

# 2) Configure Docker auth (one time per machine)
gcloud auth configure-docker us-central1-docker.pkg.dev

# 3) Build & push **in one go**—no extra tag step
docker build -f Dockerfile.processor \
  -t us-central1-docker.pkg.dev/selfforecasting/api-repo/api-processor:latest \
  .

docker push us-central1-docker.pkg.dev/selfforecasting/api-repo/api-processor:latest

# 4) Deploy to Cloud Run
gcloud run deploy api-processor \
  --image us-central1-docker.pkg.dev/selfforecasting/api-repo/api-processor:latest \
  --platform managed --region us-central1 \
  --service-account api-processor-sa@selfforecasting.iam.gserviceaccount.com \
  --allow-unauthenticated \
  --set-env-vars LOG_LEVEL=info \
  --memory 512Mi

```

Test

```bash
curl -X POST https://api-processor-313346895426.us-central1.run.app/process \
  -H "Content-Type: application/json" \
  -d '{"zip_url": "https://nemweb.com.au/Reports/Current/FPPDAILY/PUBLIC_NEXT_DAY_FPPMW_20250424_0000000460581581.zip", "gcs_bucket": "aemo_data"}'
```


curl -X POST http://0.0.0.0:8080/process \
  -H "Content-Type: application/json" \
  -d '{"zip_url": "https://nemweb.com.au/Reports/Current/FPPDAILY/PUBLIC_NEXT_DAY_FPPMW_20250424_0000000460581581.zip", "gcs_bucket": "aemo_data"}'