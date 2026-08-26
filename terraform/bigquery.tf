resource "google_bigquery_dataset" "metadata" {
  dataset_id = "olist_metadata"
  project    = var.project_id
  location   = var.region

  delete_contents_on_destroy = false
}

resource "google_bigquery_dataset" "staging" {
  dataset_id = "olist_staging"
  project    = var.project_id
  location   = var.region

  delete_contents_on_destroy = false
}

resource "google_bigquery_dataset" "silver" {
  dataset_id = "olist_silver"
  project    = var.project_id
  location   = var.region

  delete_contents_on_destroy = false
}

resource "google_bigquery_dataset" "gold" {
  dataset_id = "olist_gold"
  project    = var.project_id
  location   = var.region

  delete_contents_on_destroy = false
}

resource "google_bigquery_dataset" "assertions" {
  dataset_id = "olist_assertions"
  project    = var.project_id
  location   = var.region

  delete_contents_on_destroy = false
}

resource "google_bigquery_dataset" "dataform_assertions" {
  dataset_id = "dataform_assertions"
  project    = var.project_id
  location   = var.region

  delete_contents_on_destroy = false
}