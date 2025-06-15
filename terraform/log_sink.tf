# Create Pub/Sub topic
resource "google_pubsub_topic" "ga4_export" {
  name = "ga4-export-identity-resolution-${var.environment}"
}