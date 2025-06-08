# Create Cloud Storage bucket for function source
resource "google_storage_bucket" "function_bucket" {
  name          = "${var.project_id}-gcf-source-${random_string.bucket_suffix.result}"
  location      = var.region
  force_destroy = true
  
  uniform_bucket_level_access = true
}

# Random string for bucket name uniqueness
resource "random_string" "bucket_suffix" {
  length  = 8
  special = false
  upper   = false
}

# Zip the function source code
data "archive_file" "function_source" {
  type        = "zip"
  output_path = "/tmp/function-source.zip"
  
  source {
    content  = file("${path.module}/../cloud_functions/identity_match/main.py")
    filename = "main.py"
  }
  
  source {
    content  = file("${path.module}/../cloud_functions/identity_match/requirements.txt")
    filename = "requirements.txt"
  }
}

# Upload function source to bucket
resource "google_storage_bucket_object" "function_zip" {
  name   = "identity-match-${var.environment}-${data.archive_file.function_source.output_base64sha256}.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.function_source.output_path
}

# Create 2nd Gen Cloud Function
resource "google_cloudfunctions2_function" "identity_match" {
  name        = "identity-match-${var.environment}"
  location    = var.region
  description = "Process GA4 export events for identity resolution"

  build_config {
    runtime     = "python311" 
    entry_point = "identity_match"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = google_storage_bucket_object.function_zip.name
      }
    }
  }

  service_config {
    max_instance_count    = 100
    min_instance_count    = 0
    available_memory      = "512M"
    timeout_seconds       = 540
    service_account_email = var.service_account_email
    
    environment_variables = {
      PROJECT_ID = var.project_id
      DATASET_ID = google_bigquery_dataset.identity_resolution.dataset_id
      REGION     = var.region
    }
  }

  event_trigger {
    event_type            = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic          = google_pubsub_topic.ga4_export.id
    service_account_email = var.service_account_email
    retry_policy          = "RETRY_POLICY_RETRY"
  }
}

# Output the function URI
output "function_uri" {
  value = google_cloudfunctions2_function.identity_match.service_config[0].uri
}