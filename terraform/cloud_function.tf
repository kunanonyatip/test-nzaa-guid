# Create Cloud Storage bucket for function source
resource "google_storage_bucket" "function_bucket" {
  name     = "${var.project_id}-gcf-source"
  location = var.region
}

# Zip the function source code
data "archive_file" "function_source" {
  type        = "zip"
  source_dir  = "${path.module}/../cloud_functions/identity_match"
  output_path = "/tmp/function-source.zip"
}

# Upload function source to bucket
resource "google_storage_bucket_object" "function_zip" {
  name   = "identity-match-${data.archive_file.function_source.output_md5}.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.function_source.output_path
}

# Create Pub/Sub topic
resource "google_pubsub_topic" "ga4_export" {
  name = "ga4-export-identity-resolution"
}

# Create Cloud Function
resource "google_cloudfunctions_function" "identity_match" {
  name                  = "identity-match-${var.environment}"
  runtime               = "python39"
  available_memory_mb   = 512
  timeout               = 540
  entry_point          = "identity_match"
  service_account_email = var.service_account_email
  
  source_archive_bucket = google_storage_bucket.function_bucket.name
  source_archive_object = google_storage_bucket_object.function_zip.name
  
  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = google_pubsub_topic.ga4_export.id
  }
  
  environment_variables = {
    PROJECT_ID    = var.project_id
    DATASET_ID    = google_bigquery_dataset.identity_resolution.dataset_id
    GA4_PROJECT   = var.ga4_project_id  
    GA4_DATASET   = var.ga4_dataset     
  }
}