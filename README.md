# Olist GCP Batch ETL Pipeline

Production-style batch data pipeline built on Google Cloud Platform using Infrastructure as Code, containerized ingestion, Cloud Composer orchestration, BigQuery, and Dataform.

## Architecture

```text
                Kaggle / Source Data
                        │
                        ▼
                Cloud Run Job
                 Data Ingestion
                        │
                        ▼
                 GCS Bronze Layer
                        │
                        ▼
               BigQuery Staging
                        │
                        ▼
                 Dataform Silver
                        │
                        ▼
                  Dataform Gold
                        │
                        ▼
               Analytics Data Marts
```

Cloud Composer orchestrates the pipeline and coordinates ingestion and downstream transformations.

## GCP Services

* Cloud Storage
* BigQuery
* Cloud Run Jobs
* Cloud Composer
* Dataform
* Artifact Registry
* Secret Manager
* IAM
* Terraform

## Data Architecture

### Bronze

Raw source files are stored in Google Cloud Storage.

### Staging

Raw structured data is loaded into BigQuery staging tables.

### Silver

Data is cleaned, standardized, validated, and transformed.

### Gold

Business-ready dimensional models are created.

### Data Marts

Analytics-ready tables provide customer, seller, sales, payment, and review insights.

## Data Quality

The pipeline includes:

* Schema validation
* Row-level validation
* Invalid record handling
* Quarantine paths
* Ingestion manifest tracking
* Pipeline audit logging
* Dataform assertions
* Reconciliation checks

## Infrastructure

Terraform manages:

* GCS buckets
* BigQuery datasets
* BigQuery metadata tables
* Service accounts
* IAM bindings
* Cloud Run ingestion job
* Cloud Composer environment

Terraform state is stored remotely in Google Cloud Storage.

## Project Structure

```text
terraform/       Infrastructure as Code
ingestion/       Containerized ingestion application
orchestration/   Cloud Composer DAGs
dataform/        Transformation models
docs/            Architecture and documentation
scripts/         Deployment and validation scripts
```

## Deployment

### Terraform

```powershell
cd terraform
terraform init
terraform validate
terraform plan
terraform apply
```

### Verify Infrastructure

```powershell
terraform plan
```

Expected result:

```text
No changes. Your infrastructure matches the configuration.
```

## Security

Secrets and credentials are not committed to the repository.

The project uses Google Cloud IAM and Secret Manager for runtime authentication and secret access.
