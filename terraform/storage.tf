resource "google_storage_bucket" "bronze" {
  name     = var.bronze_bucket_name
  project  = var.project_id
  location = "ASIA-SOUTH1"

  storage_class = "STANDARD"

  force_destroy = false

  uniform_bucket_level_access = true

  public_access_prevention = "enforced"

  lifecycle {
    ignore_changes = [
      encryption
    ]
  }

  soft_delete_policy {
    retention_duration_seconds = 604800
  }

  hierarchical_namespace {
    enabled = false
  }
}