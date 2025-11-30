provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

resource "google_project_service" "enabled_apis" {
  for_each = toset(local.services)
  project  = var.project_id
  service  = each.key

  disable_on_destroy = true
}
