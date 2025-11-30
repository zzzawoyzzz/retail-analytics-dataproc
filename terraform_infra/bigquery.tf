resource "google_bigquery_dataset" "staging_bronze" {
  dataset_id                 = local.config.datasets.bronze
  friendly_name              = "Staging Bronze Layer"
  description                = "detail of raw data as bronze layer"
  location                   = local.config.region
  delete_contents_on_destroy = true

  labels = {
    project_name = "dataproc-to-bigquery-${var.project_id}"
    environment  = "dev"
    layer        = "bronze"
    owner        = "data_engineer"
  }

}



resource "google_bigquery_dataset" "analytics_datawarehouse" {
  dataset_id                 = local.config.datasets.datawarehouse
  friendly_name              = "Analytics Data Warehouse"
  description                = "Data warehouse layer with dimensional and factmodels"
  location                   = local.config.region
  delete_contents_on_destroy = true

  labels = {
    project_name = "dataproc-to-bigquery-${var.project_id}"
    environment  = "dev"
    layer        = "datawarehouse"
    owner        = "data_engineer"
  }
}

resource "google_bigquery_dataset" "analytics_datamart" {
  dataset_id                 = local.config.datasets.datamart
  friendly_name              = "Analytics Datamart"
  description                = "Datamart layer with aggregated analytics"
  location                   = local.config.region
  delete_contents_on_destroy = true

  labels = {
    project_name = "dataproc-to-bigquery-${var.project_id}"
    environment  = "dev"
    layer        = "datamart"
    owner        = "data_engineer"
  }
}
