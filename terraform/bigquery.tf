# Get the current user's email
data "google_client_openid_userinfo" "me" {}

# Create BigQuery dataset
resource "google_bigquery_dataset" "identity_resolution" {
  dataset_id                  = "identity_resolution_${var.environment}"
  location                    = var.region
  delete_contents_on_destroy  = false
  
  access {
    role          = "OWNER"
    user_by_email = var.service_account_email
  }
   # Current user access (whoever runs Terraform)
    access {
    role          = "OWNER"
    user_by_email = data.google_client_openid_userinfo.me.email
  }
}

# Create identity_match table
resource "google_bigquery_table" "identity_match" {
  dataset_id          = google_bigquery_dataset.identity_resolution.dataset_id
  table_id            = "identity_match"
  deletion_protection = false
  
  schema = file("${path.module}/../bigquery/schemas/identity_match.json")
}

# Create alternate_identity_match table
resource "google_bigquery_table" "alternate_identity_match" {
  dataset_id          = google_bigquery_dataset.identity_resolution.dataset_id
  table_id            = "alternate_identity_match"
  deletion_protection = false
  
  schema = file("${path.module}/../bigquery/schemas/alternate_identity_match.json")
}

# Create stored procedure
resource "google_bigquery_routine" "update_identity_match" {
  dataset_id   = google_bigquery_dataset.identity_resolution.dataset_id
  routine_id   = "update_identity_match"
  routine_type = "PROCEDURE"
  language     = "SQL"
  
  arguments {
    name      = "latest_update_date"
    data_type = jsonencode({typeKind = "STRING"})
  }
  
  definition_body = templatefile("${path.module}/../bigquery/procedures/update_identity_match.sql", {
    project_id       = var.project_id
    dataset_id       = google_bigquery_dataset.identity_resolution.dataset_id
    ga4_project      = var.ga4_project_id
    ga4_dataset      = var.ga4_dataset
  })
}

