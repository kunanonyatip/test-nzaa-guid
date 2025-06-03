# Create Pub/Sub topic
resource "google_pubsub_topic" "ga4_export" {
  name = "ga4-export-identity-resolution-${var.environment}"
}

# COMMENT OUT THE LOG SINK - We'll create it manually
# Create log sink in GA4 project
#resource "google_logging_project_sink" "ga4_export_sink" {
#  name    = "ga4-identity-resolution-sink-${var.environment}"
#  project = var.ga4_project_id
#  destination = "pubsub.googleapis.com/projects/${var.project_id}/topics/${google_pubsub_topic.ga4_export.name}"
  
#  filter = <<-EOT
#    resource.type="bigquery_resource"
#    protoPayload.methodName="jobservice.jobcompleted"
#    protoPayload.serviceData.jobCompletedEvent.eventName="load_job_completed"
#    protoPayload.serviceData.jobCompletedEvent.job.jobConfiguration.load.destinationTable.datasetId="${var.ga4_dataset}"
#   protoPayload.authenticationInfo.principalEmail="firebase-measurement@system.gserviceaccount.com"
#    NOT protoPayload.serviceData.jobCompletedEvent.job.jobConfiguration.load.destinationTable.tableId=~"events_intraday_.*"
#  EOT
#  unique_writer_identity = true
#}

# Grant publish permission to log sink writer
#resource "google_pubsub_topic_iam_member" "log_sink_publisher" {
#  topic  = google_pubsub_topic.ga4_export.name
#  role   = "roles/pubsub.publisher"
#  member = google_logging_project_sink.ga4_export_sink.writer_identity
#}