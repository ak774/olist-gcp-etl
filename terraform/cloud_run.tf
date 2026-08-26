resource "google_cloud_run_v2_job" "olist_ingestion" {
  name     = "olist-ingestion-job"
  location = var.region
  project  = var.project_id

  client         = "gcloud"
  client_version = "580.0.0"


  template {
    task_count = 1

    template {
      service_account = google_service_account.cloud_run.email
      timeout         = "1800s"
      max_retries     = 1

      containers {
        image = "asia-south1-docker.pkg.dev/olist-batch-etl/olist-repo/olist-ingestion:v8"

        env {
          name  = "PROJECT_ROOT"
          value = "/app"
        }

        resources {
          limits = {
            cpu    = "1000m"
            memory = "512Mi"
          }
        }

        volume_mounts {
          name       = "kaggle-api-token-guf-maf"
          mount_path = "/secrets/kaggle"
        }
      }

      volumes {
        name = "kaggle-api-token-vop-lir"

        secret {
          secret = "kaggle-api-token"

          items {
            version = "latest"
            path    = "kaggle.json"
          }
        }
      }

      volumes {
        name = "kaggle-api-token-guf-maf"

        secret {
          secret = "kaggle-api-token"

          items {
            version = "latest"
            path    = "credentials.json"
          }
        }
      }
    }

  }
}