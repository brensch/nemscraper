// src/bin/test_gcs.rs
// Enhanced test to verify GCS credentials and bucket access with detailed logging

use anyhow::Result;
use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use std::process::Command;

#[tokio::main]
async fn main() -> Result<()> {
    println!("🔍 Testing GCS credentials and bucket access with detailed logging...");

    // Check environment
    match std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
        Ok(path) => println!("✅ GOOGLE_APPLICATION_CREDENTIALS set to: {}", path),
        Err(_) => println!("ℹ️  GOOGLE_APPLICATION_CREDENTIALS not set, using default credentials"),
    }

    // Check what gcloud thinks about current auth
    println!("\n🔍 Checking gcloud auth status:");
    if let Ok(output) = Command::new("gcloud")
        .args(&["auth", "list", "--format", "json"])
        .output()
    {
        if let Ok(auth_info) = String::from_utf8(output.stdout) {
            println!("gcloud auth list: {}", auth_info);
        }
    }

    // Check current project
    if let Ok(output) = Command::new("gcloud")
        .args(&["config", "get-value", "project"])
        .output()
    {
        if let Ok(project) = String::from_utf8(output.stdout) {
            println!("gcloud current project: {}", project.trim());
        }
    }

    // Check ADC location
    let adc_path = std::env::var("HOME").unwrap_or_default()
        + "/.config/gcloud/application_default_credentials.json";
    if std::path::Path::new(&adc_path).exists() {
        println!(
            "✅ Application Default Credentials file exists at: {}",
            adc_path
        );

        // Try to read project from ADC file
        if let Ok(adc_content) = std::fs::read_to_string(&adc_path) {
            if let Ok(adc_json) = serde_json::from_str::<serde_json::Value>(&adc_content) {
                if let Some(quota_project) = adc_json.get("quota_project_id") {
                    println!("ADC quota_project_id: {}", quota_project);
                }
                if let Some(project_id) = adc_json.get("project_id") {
                    println!("ADC project_id: {}", project_id);
                }
                if let Some(client_id) = adc_json.get("client_id") {
                    println!(
                        "ADC client_id (first 20 chars): {}",
                        client_id
                            .as_str()
                            .unwrap_or("")
                            .chars()
                            .take(20)
                            .collect::<String>()
                    );
                }
            }
        }
    } else {
        println!(
            "❌ Application Default Credentials file NOT found at: {}",
            adc_path
        );
        println!("💡 Run: gcloud auth application-default login");
    }

    println!("\n🔧 Initializing GCS client...");

    // Try to initialize client with detailed error info
    let config = match ClientConfig::default().with_auth().await {
        Ok(config) => {
            println!("✅ Successfully initialized GCS client config");
            config
        }
        Err(e) => {
            println!("❌ Failed to initialize GCS client: {:?}", e);
            return Err(e.into());
        }
    };

    let client = Client::new(config);
    println!("✅ GCS client created successfully");

    // Test uploading a small file
    let test_bucket = std::env::args().nth(1).unwrap_or_else(|| {
        println!("Usage: cargo run --bin test-gcs <bucket-name>");
        println!("Using default bucket name: test");
        "test".to_string()
    });

    println!("\n🧪 Testing upload to bucket: {}", test_bucket);

    let test_data = b"Hello, GCS! This is a test file.";
    let object_name = format!("test-{}.txt", chrono::Utc::now().timestamp());

    let upload_type = UploadType::Simple(Media::new(object_name.clone()));
    let request = UploadObjectRequest {
        bucket: test_bucket.clone(),
        ..Default::default()
    };

    println!("📤 Attempting to upload object: {}", object_name);

    match client
        .upload_object(&request, test_data.to_vec(), &upload_type)
        .await
    {
        Ok(_) => {
            println!("✅ Successfully uploaded test file: {}", object_name);
            println!("🎉 GCS credentials and bucket access test passed!");
        }
        Err(e) => {
            println!("❌ Failed to upload test file: {:?}", e);
            println!("\n💡 Debug information:");
            println!("   - Bucket name: {}", test_bucket);
            println!("   - Object name: {}", object_name);
            println!("   - Error details: {:#?}", e);

            println!("\n💡 Try:");
            println!("   1. Creating bucket: gsutil mb gs://{}", test_bucket);
            println!("   2. Using a different bucket name");
            println!("   3. Checking: gcloud config get-value project");
            println!("   4. Checking: gcloud auth list");
            println!("   5. Re-running: gcloud auth application-default login");

            return Err(e.into());
        }
    }

    Ok(())
}
