terraform {
  required_version = ">= 1.0"
  
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
  
  # Backend configuration
  backend "gcs" {
    bucket = "em-identity-graph-terraform-state"
    prefix = "identity-resolution"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "random" {}
provider "archive" {}

# Get current user for permissions
data "google_client_openid_userinfo" "me" {}

# Local values
locals {
  common_labels = {
    project     = "identity-resolution"
    environment = var.environment
    managed_by  = "terraform"
    owner       = "nzaa"
  }
  
  dataset_id    = "identity_resolution_${var.environment}"
  function_name = "identity-match-${var.environment}"
  topic_name    = "ga4-export-identity-resolution-${var.environment}"
  log_sink_name = "ga4-identity-resolution-sink-${var.environment}"
}