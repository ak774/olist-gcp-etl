

locals {
  project_iam_roles = {
    cloud_run_artifactregistry_reader = {
      role   = "roles/artifactregistry.reader"
      member = "serviceAccount:${google_service_account.cloud_run.email}"
    }

    cloud_run_bigquery_data_editor = {
      role   = "roles/bigquery.dataEditor"
      member = "serviceAccount:${google_service_account.cloud_run.email}"
    }

    cloud_run_bigquery_job_user = {
      role   = "roles/bigquery.jobUser"
      member = "serviceAccount:${google_service_account.cloud_run.email}"
    }

    cloud_run_secret_accessor = {
      role   = "roles/secretmanager.secretAccessor"
      member = "serviceAccount:${google_service_account.cloud_run.email}"
    }


    composer_bigquery_data_editor = {
      role   = "roles/bigquery.dataEditor"
      member = "serviceAccount:${google_service_account.composer.email}"
    }

    composer_bigquery_data_viewer = {
      role   = "roles/bigquery.dataViewer"
      member = "serviceAccount:${google_service_account.composer.email}"
    }

    composer_bigquery_job_user = {
      role   = "roles/bigquery.jobUser"
      member = "serviceAccount:${google_service_account.composer.email}"
    }

    composer_admin = {
      role   = "roles/composer.admin"
      member = "serviceAccount:${google_service_account.composer.email}"
    }

    composer_worker = {
      role   = "roles/composer.worker"
      member = "serviceAccount:${google_service_account.composer.email}"
    }

    composer_dataform_editor = {
      role   = "roles/dataform.editor"
      member = "serviceAccount:${google_service_account.composer.email}"
    }

    composer_run_developer = {
      role   = "roles/run.developer"
      member = "serviceAccount:${google_service_account.composer.email}"
    }


    dataform_bigquery_data_editor = {
      role   = "roles/bigquery.dataEditor"
      member = "serviceAccount:${google_service_account.dataform_execution.email}"
    }

    dataform_bigquery_data_viewer = {
      role   = "roles/bigquery.dataViewer"
      member = "serviceAccount:${google_service_account.dataform_execution.email}"
    }

    dataform_bigquery_job_user = {
      role   = "roles/bigquery.jobUser"
      member = "serviceAccount:${google_service_account.dataform_execution.email}"
    }

    dataform_service_account_token_creator = {
      role   = "roles/iam.serviceAccountTokenCreator"
      member = "serviceAccount:${google_service_account.dataform_execution.email}"
    }

    dataform_secret_accessor = {
      role   = "roles/secretmanager.secretAccessor"
      member = "serviceAccount:${google_service_account.dataform_execution.email}"
    }

  }
}

resource "google_project_iam_member" "custom_service_accounts" {
  for_each = local.project_iam_roles

  project = var.project_id
  role    = each.value.role
  member  = each.value.member
}



resource "google_service_account_iam_member" "composer_act_as_dataform_execution" {
  service_account_id = google_service_account.dataform_execution.name

  role = "roles/iam.serviceAccountUser"

  member = "serviceAccount:${google_service_account.composer.email}"
}


resource "google_service_account_iam_member" "dataform_agent_act_as_dataform_execution" {
  service_account_id = google_service_account.dataform_execution.name

  role = "roles/iam.serviceAccountUser"

  member = "serviceAccount:service-326893325807@gcp-sa-dataform.iam.gserviceaccount.com"
}


resource "google_service_account_iam_member" "dataform_agent_token_creator" {
  service_account_id = google_service_account.dataform_execution.name

  role = "roles/iam.serviceAccountTokenCreator"

  member = "serviceAccount:service-326893325807@gcp-sa-dataform.iam.gserviceaccount.com"
}



