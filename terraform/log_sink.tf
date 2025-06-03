# Locals for filter construction
locals {
  common_filter = [
    "resource.type=\"bigquery_resource\"",
    "protoPayload.methodName=\"jobservice.jobcompleted\"",
    "protoPayload.serviceData.jobCompletedEvent.eventName=\"load_job_completed\""
  ]
  
  ga4_dataset_filter = [
    "protoPayload.serviceData.jobCompletedEvent.job.jobConfiguration.load.destinationTable.datasetId=\"${var.ga4_dataset}\""
  ]
}

# Pub/Sub topic for GA4 export notifications
resource "google_pubsub_topic" "ga4_export" {
  name  = "ga4-export-identity-resolution-${var.environment}"
  count = length(var.bigquery_export.datasets.ga4) > 0 ? 1 : 0
}

# Log sink to capture GA4 BigQuery export completions
resource "google_logging_project_sink" "ga4_export_sink" {
  name    = "ga4-identity-resolution-sink-${var.environment}"
  project = var.ga4_project_id
  
  destination = "pubsub.googleapis.com/projects/${var.project_id}/topics/${google_pubsub_topic.ga4_export[0].name}"
  
  filter = join(" AND ", concat(local.common_filter, local.ga4_dataset_filter, [
    "protoPayload.authenticationInfo.principalEmail=\"firebase-measurement@system.gserviceaccount.com\"",
    "NOT protoPayload.serviceData.jobCompletedEvent.job.jobConfiguration.load.destinationTable.tableId:\"events_intraday_\""
  ]))
  
  count = length(var.bigquery_export.datasets.ga4) > 0 ? 1 : 0
  unique_writer_identity = true
}

# Grant log sink permission to publish to topic
resource "google_pubsub_topic_iam_member" "log_sink_publisher" {
  topic  = google_pubsub_topic.ga4_export[0].name
  role   = "roles/pubsub.publisher"
  member = google_logging_project_sink.ga4_export_sink[0].writer_identity
  count  = length(var.bigquery_export.datasets.ga4) > 0 ? 1 : 0
}

resource "google_pubsub_subscription" "function_trigger" {
  name  = "ga4-export-identity-resolution-${var.environment}-sub"
  topic = google_pubsub_topic.ga4_export[0].name
  count = length(var.bigquery_export.datasets.ga4) > 0 ? 1 : 0
}