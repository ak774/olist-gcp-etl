resource "google_service_account" "cloud_run" {
  project      = var.project_id
  account_id   = "olist-cloud-run-sa"
  display_name = "Olist Cloud Run Ingestion Service Account"

  description = "Runtime identity for the Olist incremental ingestion Cloud Run Job"
}

resource "google_service_account" "composer" {
  project      = var.project_id
  account_id   = "olist-composer-sa"
  display_name = "olist-composer-sa"

  description = "Olist Cloud Composer Service Account"
}

resource "google_service_account" "dataform_execution" {
  project      = var.project_id
  account_id   = "dataform-execution-sa"
  display_name = "Dataform Execution Service Account"
}