variable "project_id" {
  description = "Identity Graph GCP project ID"
  type        = string
  default     = "em-identity-graph"
}

variable "ga4_project_id" {
  description = "GCP project ID containing GA4 data"
  type        = string
  default     = "em-sandbox-455802"
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Environment (staging or prod)"
  type        = string
  validation {
    condition     = contains(["staging", "prod"], var.environment)
    error_message = "Environment must be either 'staging' or 'prod'."
  }
}

variable "ga4_dataset" {
  description = "GA4 BigQuery dataset in client project"
  type        = string
}

variable "service_account_email" {
  description = "Service account email"
  type        = string
}