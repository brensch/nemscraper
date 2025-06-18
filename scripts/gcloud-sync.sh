#!/bin/bash

set -e

echo "gcloud-sync: Starting continuous sync monitoring..."

# Authenticate with service account
echo "Authenticating with service account..."
gcloud auth activate-service-account --key-file=/secrets/gcs.json

# Verify bucket access
echo "Testing bucket access..."
if ! gcloud storage ls gs://aemo_data/ > /dev/null 2>&1; then
    echo "ERROR: Cannot access bucket gs://aemo_data/"
    echo "Please ensure the service account has Storage Admin role"
    exit 1
fi

echo "Bucket access verified. Starting sync loop..."

# Sync loop - runs every 30 seconds
while true; do
    echo "$(date): Starting sync..."
    
    # Sync parquet files to GCS, excluding temporary files
    gcloud storage rsync /assets/compacted/ gs://aemo_data/ \
        --recursive \
        --checksums-only \
        --delete-unmatched-destination-objects \
        --continue-on-error \
        --exclude=".*\.tmp$" 
    
    if [ $? -eq 0 ]; then
        echo "$(date): Sync completed successfully"
    else
        echo "$(date): Sync failed, will retry in 30s..."
    fi
    
    echo "$(date): Sleeping 30s..."
    sleep 30
done