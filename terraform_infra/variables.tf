variable "project_id" {
  description = "The GCP project ID"
  type        = string
  default     = "gcp-data-engineer-awoy"
  sensitive   = true
}

variable "region" {
  description = "The GCP region"
  type        = string
  default     = "asia-southeast1"
}

variable "zone" {
  description = "The GCP zone"
  type        = string
  default     = "asia-southeast1-a"
}




