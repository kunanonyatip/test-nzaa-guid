terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Store state in GCS bucket
terraform {
  backend "gcs" {
    bucket = "em-identity-graph-terraform-state"
    prefix = "identity-resolution"
  }
}

# Get current user for permissions
data "google_client_openid_userinfo" "me" {}

# Local values for common tags
locals {
  common_labels = {
    project     = "identity-resolution"
    environment = var.environment
    managed_by  = "terraform"
    owner       = "nzaa"
  }
  
  # Dataset ID based on environment
  dataset_id = "identity_resolution_${var.environment}"

  # Function name based on environment
  function_name = "identity-match-${var.environment}"
  
  # Topic name based on environment  
  topic_name = "ga4-export-identity-resolution-${var.environment}"
  
  # Log sink name
  log_sink_name = "ga4-identity-resolution-sink-${var.environment}"
}
