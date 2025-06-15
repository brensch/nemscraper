#!/bin/bash

# Local testing script - test with user credentials first

echo "Testing with local filesystem first..."
cargo run --bin local-processor \
  "https://nemweb.com.au/Reports/Current/FPPDAILY/PUBLIC_NEXT_DAY_FPPMW_20250421_0000000460163846.zip" \
  "./output"

echo ""
echo "Creating test bucket with user credentials..."

# Create a unique bucket name
BUCKET_NAME="selfforecasting-test-$(date +%s)"
echo "Creating bucket: $BUCKET_NAME"
gsutil mb gs://$BUCKET_NAME

echo ""
echo "Testing GCS with user credentials (no service account)..."

# Unset service account to use user credentials
unset GOOGLE_APPLICATION_CREDENTIALS

# Test GCS upload with user credentials
cargo run --bin test-gcs $BUCKET_NAME

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ User credentials work! Testing Cloud Run service..."
    
    # Set up environment variables for user credentials
    export LOG_LEVEL="debug"
    export ENABLE_PROFILING="false"
    export PORT="8080"
    
    # Start the service with user credentials
    cargo run --bin cloud-run-service &
    SERVICE_PID=$!
    
    # Wait for service to start
    sleep 5
    
    # Test the service
    echo "Testing with bucket: $BUCKET_NAME"
    cargo run --bin test-client -- \
      http://localhost:8080 \
      "https://nemweb.com.au/Reports/Current/FPPDAILY/PUBLIC_NEXT_DAY_FPPMW_20250421_0000000460163846.zip" \
      $BUCKET_NAME \
      "processed/"
    
    # Clean up
    kill $SERVICE_PID 2>/dev/null
else
    echo "❌ Even user credentials don't work - there might be a deeper billing issue"
fi