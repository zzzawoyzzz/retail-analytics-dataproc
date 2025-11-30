resource "google_storage_bucket" "raw_data" {
  name                        = local.config.buckets.raw
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
  storage_class               = "STANDARD"

  autoclass {
    enabled = true
  }

  versioning {
    enabled = false
  }

  depends_on = [google_project_service.enabled_apis]
}


resource "google_storage_bucket" "processed_data" {
  name                        = local.config.buckets.processed
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
  storage_class               = "STANDARD"

  autoclass {
    enabled = true
  }
}

resource "google_storage_bucket" "staging" {
  name                        = local.config.buckets.temp
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
  storage_class               = "STANDARD"

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket" "dataproc_jobs" {
  name                        = local.config.buckets.jobs
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
  storage_class               = "STANDARD"
}


resource "google_storage_bucket_object" "main_spark_job" {

  name         = "main_spark.py"
  bucket       = google_storage_bucket.dataproc_jobs.name
  source       = "${path.module}/../pyspark_jobs/main_spark.py"
  content_type = "text/x-python"
}

resource "google_storage_bucket_object" "utils_file" {
  name         = "utils.py"
  bucket       = google_storage_bucket.dataproc_jobs.name
  source       = "${path.module}/../pyspark_jobs/utils.py"
  content_type = "text/x-python"
}

resource "google_storage_bucket_object" "config_file" {
  name         = "config.yaml"
  bucket       = google_storage_bucket.dataproc_jobs.name
  source       = "${path.module}/../configs/config.yaml"
  content_type = "application/yaml"
}

resource "google_storage_bucket_object" "raw_data_files" {
  for_each = fileset("${path.module}/../data/", "*.csv")

  name         = each.value
  bucket       = google_storage_bucket.raw_data.name
  source       = "${path.module}/../data/${each.value}"
  content_type = "text/csv"
}
