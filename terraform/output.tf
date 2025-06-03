output "dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.identity_resolution.dataset_id
}

output "cloud_function_name" {
  description = "Cloud Function name"
  value       = google_cloudfunctions_function.identity_match.name
}

output "pubsub_topic" {
  description = "Pub/Sub topic name"
  value       = google_pubsub_topic.ga4_export.name
}

output "log_sink_writer_identity" {
  description = "Log sink writer identity (needs Pub/Sub publisher permission)"
  value       = google_logging_project_sink.ga4_export_sink.writer_identity
}