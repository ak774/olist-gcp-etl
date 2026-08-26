resource "google_bigquery_table" "ingestion_manifest" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.metadata.dataset_id
  table_id   = "ingestion_manifest"

  schema = jsonencode([
    {
      name = "source_name"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "source_file"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "file_generation"
      type = "STRING"
    },
    {
      name = "file_md5_hash"
      type = "STRING"
    },
    {
      name = "file_size"
      type = "INTEGER"
    },
    {
      name = "ingestion_date"
      type = "DATE"
      mode = "REQUIRED"
    },
    {
      name = "processed_at"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    },
    {
      name = "bronze_gcs_path"
      type = "STRING"
    },
    {
      name = "quarantine_gcs_path"
      type = "STRING"
    },
    {
      name = "total_rows"
      type = "INTEGER"
    },
    {
      name = "valid_rows"
      type = "INTEGER"
    },
    {
      name = "invalid_rows"
      type = "INTEGER"
    },
    {
      name = "status"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "error_message"
      type = "STRING"
    },
    {
      name = "pipeline_run_id"
      type = "STRING"
    }
  ])

  time_partitioning {
    type  = "DAY"
    field = "ingestion_date"
  }

  clustering = [
    "source_name",
    "source_file"
  ]
}

resource "google_bigquery_table" "pipeline_run_audit" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.metadata.dataset_id
  table_id   = "pipeline_run_audit"

  schema = jsonencode([
    {
      name = "pipeline_run_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "dag_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "task_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "phase"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "status"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "start_time"
      type = "TIMESTAMP"
    },
    {
      name = "end_time"
      type = "TIMESTAMP"
    },
    {
      name = "duration_seconds"
      type = "FLOAT"
    },
    {
      name = "error_message"
      type = "STRING"
    },
    {
      name = "created_at"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    },
    {
      name = "map_index"
      type = "INTEGER"
    }
  ])

  time_partitioning {
    type  = "DAY"
    field = "created_at"
  }

  clustering = [
    "dag_id",
    "pipeline_run_id",
    "status"
  ]
}