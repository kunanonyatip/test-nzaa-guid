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