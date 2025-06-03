# Create Cloud Storage bucket for function source
resource "google_storage_bucket" "function_bucket" {
  name     = "${var.project_id}-gcf-source"
  location = var.region
}

# Zip the function source code
data "archive_file" "function_source" {
  type        = "zip"
  output_path = "/tmp/identity-match-function-${var.environment}-${formatdate("YYYYMMDD-hhmmss", timestamp())}.zip"
  
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
  name   = "identity-match/${var.environment}/function-${data.archive_file.function_source.output_base64sha256}.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.function_source.output_path
  
  metadata = {
    environment = var.environment
    version     = data.archive_file.function_source.output_base64sha256
  }
}

# Create Cloud Function
resource "google_cloudfunctions_function" "identity_match" {
  name                  = "identity-match-${var.environment}"
  runtime               = "python41"
  available_memory_mb   = 512
  timeout               = 540
  entry_point          = "identity_match"
  service_account_email = var.service_account_email
  
  source_archive_bucket = google_storage_bucket.function_bucket.name
  source_archive_object = google_storage_bucket_object.function_zip.name
  
  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = google_pubsub_topic.ga4_export.id
    failure_policy {
      retry = false
    }
  }
}