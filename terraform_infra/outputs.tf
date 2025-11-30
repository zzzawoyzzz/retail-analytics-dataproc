output "raw_data_bucket" {
  description = "Name of the raw data bucket (existing bucket)"
  value       = google_storage_bucket.raw_data.name
}

output "processed_data_bucket" {
  description = "Name of the processed data bucket"
  value       = google_storage_bucket.processed_data.name
}

# output "dataproc_jobs_bucket" {
#   description = "Name of the Dataproc jobs bucket"
#   value       = google_storage_bucket.dataproc_jobs.name
# }

output "bucket_urls" {
  description = "GCS URLs for all buckets"
  value = {
    raw_data  = "gs://${google_storage_bucket.raw_data.name}"
    processed = "gs://${google_storage_bucket.processed_data.name}"
  }
}

