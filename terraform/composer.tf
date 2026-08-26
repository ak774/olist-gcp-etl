
resource "google_composer_environment" "olist_composer" {
  name    = "olist-composer-dev"
  project = var.project_id
  region  = var.region

  config {
    environment_size = "ENVIRONMENT_SIZE_SMALL"

    node_config {
      service_account = google_service_account.composer.email
    }

    software_config {
      image_version = "composer-3-airflow-2.11.1-build.17"

      env_variables = {
        RESTART_TRIGGER = "1"
      }

      web_server_plugins_mode = "ENABLED"
    }

    data_retention_config {
      airflow_metadata_retention_config {
        retention_days = 60
        retention_mode = "RETENTION_MODE_ENABLED"
      }
    }

    workloads_config {
      scheduler {
        count      = 1
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
      }

      web_server {
        cpu        = 1
        memory_gb  = 4
        storage_gb = 1
      }

      worker {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 10
        min_count  = 1
        max_count  = 3
      }

      triggerer {
        count     = 1
        cpu       = 1
        memory_gb = 2

      }

      dag_processor {
        count      = 1
        cpu        = 1
        memory_gb  = 4
        storage_gb = 1
      }
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

