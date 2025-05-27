variable "project_id" {
  description = "Identity Graph GCP project ID"
  type        = string
}

variable "ga4_project_id" {
  description = "GCP project ID containing GA4 data"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Environment (staging or prod)"
  type        = string
}

variable "ga4_dataset" {
  description = "GA4 BigQuery dataset in  project"
  type        = string
}

variable "service_account_email" {
  description = "Service account email"
  type        = string
}