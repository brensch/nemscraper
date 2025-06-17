# docker compose secrets

This repo needs this service account:

```bash
PROJECT_ID=selfforecasting

# Create the SA
gcloud iam service-accounts create uploader-sa \
  --display-name="File Uploader Service Account"

# Grant Storage Object Admin (to write to your bucket)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:uploader-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

# Grant BigQuery User (to run queries/jobs)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:uploader-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.user"

# Generate a key if you need it elsewhere
gcloud iam service-accounts keys create ./secrets/uploader-sa.json \
  --iam-account=uploader-sa@$PROJECT_ID.iam.gserviceaccount.com
```

