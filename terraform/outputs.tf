output "bronze_bucket_name" {
  description = "Name of the Bronze GCS bucket"

  value = google_storage_bucket.bronze.name
}

output "composer_environment_name" {
  description = "Cloud Composer environment name"

  value = google_composer_environment.olist_composer.name
}

output "composer_dag_gcs_prefix" {
  description = "GCS path where Composer DAGs are stored"

  value = google_composer_environment.olist_composer.config[0].dag_gcs_prefix
}

output "cloud_run_job_name" {
  description = "Cloud Run ingestion job name"

  value = google_cloud_run_v2_job.olist_ingestion.name
}

output "metadata_dataset" {
  description = "BigQuery metadata dataset"

  value = google_bigquery_dataset.metadata.dataset_id
}

output "silver_dataset" {
  description = "BigQuery Silver dataset"

  value = google_bigquery_dataset.silver.dataset_id
}

output "gold_dataset" {
  description = "BigQuery Gold dataset"

  value = google_bigquery_dataset.gold.dataset_id
}