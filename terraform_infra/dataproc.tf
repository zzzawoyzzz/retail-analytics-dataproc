resource "random_id" "dataproc_batch" {
  byte_length = 2
}

resource "google_dataproc_batch" "delta_customer_ingest" {
  provider = google-beta
  batch_id = "batch-delta-demo-${random_id.dataproc_batch.hex}"
  location = var.region
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
