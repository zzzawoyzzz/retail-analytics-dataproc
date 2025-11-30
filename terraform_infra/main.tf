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
