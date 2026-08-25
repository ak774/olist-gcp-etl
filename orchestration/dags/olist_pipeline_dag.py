from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunExecuteJobOperator,
)

from google.cloud import bigquery

PROJECT_ID = "olist-batch-etl"
REGION = "asia-south1"
CLOUD_RUN_JOB = "olist-ingestion-job"



MANIFEST_DATASET = "olist_metadata"

MANIFEST_TABLE = "ingestion_manifest"

STAGING_DATASET = "olist_staging"

LOCATION = "asia-south1"


STAGING_TABLES = {
    "orders": "orders_staging",
    "customers": "customers_staging",
    "geolocation": "geolocation_staging",
    "order_items": "order_items_staging",
    "payments": "payments_staging",
    "reviews": "reviews_staging",
    "products": "products_staging",
    "sellers": "sellers_staging",
    "product_category_name_translation":
        "category_translation_staging",
}

BRONZE_BUCKET = "olist-data-lake-dev"

with DAG(
    dag_id="olist_incremental_pipeline",
    description=(
        "Triggers incremental Olist ingestion "
        "using a Cloud Run Job"
    ),
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    tags=[
        "olist",
        "gcp",
        "cloud-run",
        "incremental",
    ],
) as dag:

    

    run_ingestion = CloudRunExecuteJobOperator(
    task_id="run_cloud_run_ingestion",

    project_id=PROJECT_ID,

    region=REGION,

    job_name=CLOUD_RUN_JOB,

    overrides={
        "container_overrides": [
            {
                "env": [
                    {
                        "name": "PIPELINE_RUN_ID",
                        "value": "{{ dag_run.run_id }}",
                    }
                ]
            }
        ]
    },

    deferrable=False,
    )

    



    @task
    def get_successful_datasets(**context):

        pipeline_run_id = context["dag_run"].run_id

        print(
            "Reading ingestion manifest for "
            f"pipeline_run_id={pipeline_run_id}"
        )

        client = bigquery.Client(
            project=PROJECT_ID
        )

        query = f"""
        SELECT
            source_name,
            bronze_gcs_path,
            processed_at
        FROM
            `{PROJECT_ID}.{MANIFEST_DATASET}.{MANIFEST_TABLE}`
        WHERE
            pipeline_run_id = @pipeline_run_id
            AND status = 'SUCCESS'
            AND bronze_gcs_path IS NOT NULL
        ORDER BY
            source_name
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "pipeline_run_id",
                    "STRING",
                    pipeline_run_id,
                )
            ]
        )

        query_job = client.query(
            query,
            job_config=job_config,
            location=LOCATION,
        )

        results = query_job.result()

        datasets = []

        for row in results:

            dataset = {
                "source_name": row.source_name,
                "bronze_gcs_path": row.bronze_gcs_path,
            }

            datasets.append(dataset)

            print(
                f"Manifest SUCCESS: "
                f"{row.source_name} → "
                f"{row.bronze_gcs_path}"
            )

        print(
            f"Total datasets requiring staging load: "
            f"{len(datasets)}"
        )

        return datasets

    @task
    def load_dataset_to_staging(dataset):

        source_name = dataset[
            "source_name"
        ]

        bronze_gcs_path = dataset[
            "bronze_gcs_path"
        ]

        # ---------------------------------------------
        # Convert relative object path to full GCS URI
        # ---------------------------------------------
        if bronze_gcs_path.startswith("gs://"):

            bronze_gcs_uri = bronze_gcs_path

        else:

            bronze_gcs_uri = (
                f"gs://{BRONZE_BUCKET}/"
                f"{bronze_gcs_path.lstrip('/')}"
            )

        print(
            f"Bronze GCS URI: {bronze_gcs_uri}"
        )

        # ---------------------------------------------
        # Validate staging table mapping
        # ---------------------------------------------
        if source_name not in STAGING_TABLES:

            raise ValueError(
                f"No staging table mapping found "
                f"for source: {source_name}"
            )

        staging_table_name = (
            STAGING_TABLES[source_name]
        )

        table_id = (
            f"{PROJECT_ID}."
            f"{STAGING_DATASET}."
            f"{staging_table_name}"
        )

        print("Starting staging load")

        print(
            f"Source dataset: {source_name}"
        )

        print(
            f"Destination: {table_id}"
        )

        client = bigquery.Client(
            project=PROJECT_ID
        )

        # Verify staging table exists
        client.get_table(
            table_id
        )

        job_config = bigquery.LoadJobConfig(

            source_format=(
                bigquery.SourceFormat.CSV
            ),

            skip_leading_rows=1,

            autodetect=False,

            write_disposition=(
                bigquery.WriteDisposition
                .WRITE_TRUNCATE
            ),
        )

        # ---------------------------------------------
        # Load Bronze GCS file into BigQuery staging
        # ---------------------------------------------
        load_job = client.load_table_from_uri(

            bronze_gcs_uri,

            table_id,

            job_config=job_config,

            location=LOCATION,
        )

        print(
            f"BigQuery load job started: "
            f"{load_job.job_id}"
        )

        load_job.result()

        destination_table = client.get_table(
            table_id
        )

        rows_loaded = (
            destination_table.num_rows
        )

        print(
            f"SUCCESS: {source_name} loaded"
        )

        print(
            f"Rows loaded: {rows_loaded}"
        )

        return {
            "source_name": source_name,
            "staging_table": table_id,
            "bronze_gcs_path": bronze_gcs_uri,
            "rows_loaded": rows_loaded,
            "load_job_id": load_job.job_id,
        }

    successful_datasets = (
        get_successful_datasets()
    )

    staging_loads = (
        load_dataset_to_staging.expand(
            dataset=successful_datasets
        )
    )

    run_ingestion >> successful_datasets

    successful_datasets >> staging_loads