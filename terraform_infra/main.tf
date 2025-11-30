locals {
  config = yamldecode(file("${path.module}/../configs/config.yaml"))
  services = [
    "dataproc.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "iam.googleapis.com",
    "cloudresourcemanager.googleapis.com"
  ]
}

resource "google_project_service" "enabled_apis" {
  for_each = toset(local.services)
  project  = var.project_id
  service  = each.key

  disable_on_destroy = false
}



provider "google" {
  project = local.config.project_id
  region  = local.config.region
  zone    = local.config.zone
}

provider "google-beta" {
  project = local.config.project_id
  region  = local.config.region
  zone    = local.config.zone
}

resource "google_storage_bucket" "raw_data" {
  name                        = local.config.buckets.raw
  location                    = local.config.region
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
  location                    = local.config.region
  force_destroy               = true
  uniform_bucket_level_access = true
  storage_class               = "STANDARD"

  autoclass {
    enabled = true
  }
}

resource "google_storage_bucket" "staging" {
  name                        = local.config.buckets.temp
  location                    = local.config.region
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


resource "random_id" "dataproc_batch" {
  byte_length = 2
}

resource "google_dataproc_batch" "delta_customer_ingest" {
  provider = google-beta
  batch_id = "batch-delta-demo-${random_id.dataproc_batch.hex}"
  location = local.config.region
  labels   = { workload = "delta-demo" }


  environment_config {
    execution_config {
      service_account = google_service_account.dataproc_sa.email
    }
  }

  pyspark_batch {
    main_python_file_uri = "gs://${google_storage_bucket.dataproc_jobs.name}/main_spark.py"
    python_file_uris     = ["gs://${google_storage_bucket.dataproc_jobs.name}/utils.py"]
    args = [
      "--project_id", local.config.project_id,
      "-i", "gs://${local.config.buckets.raw}/${local.config.paths.input_file}",
      "-o", "gs://${local.config.buckets.processed}/online_retail_II",
      "--config_file", "gs://${google_storage_bucket.dataproc_jobs.name}/config.yaml"
    ]
  }

  runtime_config {
    version = "2.3"
    properties = {
      "spark.jars.packages"                    = "io.delta:delta-spark_2.12:3.2.0"
      "spark.sql.extensions"                   = "io.delta.sql.DeltaSparkSessionExtension"
      "spark.sql.catalog.spark_catalog"        = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      "dataproc.batch.pyspark.python_packages" = "delta-spark==3.2.0,PyYAML"
      "spark.executor.memory"                  = "16g" # for dev test
    }


  }


  depends_on = [
    google_project_service.enabled_apis,
    google_storage_bucket_object.main_spark_job,
    google_project_iam_member.dataproc_worker,
    google_storage_bucket_object.config_file,
    google_project_iam_member.storage_admin,
    google_project_iam_member.bigquery_editor,
    google_project_iam_member.bigquery_job_user
  ]
  timeouts {
    create = "15m"
  }
}

resource "google_storage_bucket" "dataproc_jobs" {
  name                        = local.config.buckets.jobs
  location                    = local.config.region
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
