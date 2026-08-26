from datetime import datetime,timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.utils import timezone

from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunExecuteJobOperator,
)

from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateWorkflowInvocationOperator,
)

from airflow.utils.email import send_email

from google.cloud import bigquery




PROJECT_ID = "olist-batch-etl"

REGION = "asia-south1"

CLOUD_RUN_JOB = "olist-ingestion-job"

MANIFEST_DATASET = "olist_metadata"

MANIFEST_TABLE = "ingestion_manifest"

STAGING_DATASET = "olist_staging"

LOCATION = "asia-south1"

# -------------------------------------------------
# Dataform configuration
# -------------------------------------------------

DATAFORM_REGION = "asia-south1"

DATAFORM_REPOSITORY = "olist-dataform"

PHASE_8_WORKFLOW = "olist-phase-8-silver"

PHASE_9_WORKFLOW = "olist-phase-9-gold"

PHASE_10_WORKFLOW = "olist-phase-10-quality"

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


def write_audit_record(
    pipeline_run_id,
    dag_id,
    task_id,
    map_index,
    phase,
    status,
    start_time=None,
    end_time=None,
    duration_seconds=None,
    error_message=None,
):

    print("=" * 60)
    print("WRITE_AUDIT_RECORD CALLED")
    print(f"STATUS: {status}")
    print(f"TASK ID: {task_id}")
    print(f"MAP INDEX: {map_index}")
    print("=" * 60)

    client = bigquery.Client(
        project=PROJECT_ID
    )

    audit_table_id = (
        f"{PROJECT_ID}."
        f"{MANIFEST_DATASET}."
        f"pipeline_run_audit"
    )

    row = {
        "pipeline_run_id": pipeline_run_id,
        "dag_id": dag_id,
        "task_id": task_id,
        "map_index": map_index,
        "phase": phase,
        "status": status,

        "start_time": (
            start_time.isoformat()
            if start_time is not None
            else None
        ),

        "end_time": (
            end_time.isoformat()
            if end_time is not None
            else None
        ),

        "duration_seconds": (
            float(duration_seconds)
            if duration_seconds is not None
            else None
        ),

        "error_message": (
            str(error_message)
            if error_message is not None
            else None
        ),

        "created_at": timezone.utcnow().isoformat(),
    }

    print(f"TARGET TABLE: {audit_table_id}")
    print(f"AUDIT ROW: {row}")

    errors = client.insert_rows_json(
        audit_table_id,
        [row],
    )

    if errors:

        print(
            f"BIGQUERY INSERT ERRORS: {errors}"
        )

        raise RuntimeError(
            f"Audit logging failed: {errors}"
        )

    print(
        "AUDIT RECORD WRITTEN SUCCESSFULLY"
    )


def task_failure_callback(context):

    task_instance = context["task_instance"]
    dag_run = context["dag_run"]

    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    map_index = task_instance.map_index

    pipeline_run_id = dag_run.run_id

    exception = context.get("exception")

    start_time = task_instance.start_date
    end_time = timezone.utcnow()

    duration_seconds = None

    if start_time and end_time:

        duration_seconds = (
            end_time - start_time
        ).total_seconds()

    print("=" * 60)
    print("PIPELINE TASK FAILED")
    print(f"DAG ID: {dag_id}")
    print(f"TASK ID: {task_id}")
    print(f"MAP INDEX: {map_index}")
    print(f"RUN ID: {pipeline_run_id}")
    print(f"ERROR: {exception}")
    print("=" * 60)

    phase_mapping = {

        "run_cloud_run_ingestion":
            "ingestion",

        "get_successful_datasets":
            "staging",

        "load_dataset_to_staging":
            "staging",

        "run_phase_8_silver":
            "phase_8",

        "run_phase_9_gold":
            "phase_9",

        "run_phase_10_quality":
            "phase_10",
    }

    phase = phase_mapping.get(
        task_id,
        "unknown",
    )

    try:

        write_audit_record(

            pipeline_run_id=pipeline_run_id,

            dag_id=dag_id,

            task_id=task_id,

            map_index=map_index,

            phase=phase,

            status="FAILED",

            start_time=start_time,

            end_time=end_time,

            duration_seconds=duration_seconds,

            error_message=str(exception),
        )

    except Exception as audit_exception:

        # Never hide the original pipeline failure.
        print(
            "WARNING: Failed to write "
            "FAILED audit record"
        )

        print(
            f"Audit error: {audit_exception}"
        )



def task_success_callback(context):

    task_instance = context["task_instance"]
    dag_run = context["dag_run"]

    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    map_index = task_instance.map_index

    pipeline_run_id = dag_run.run_id

    start_time = task_instance.start_date
    end_time = timezone.utcnow()

    duration_seconds = None

    if start_time and end_time:

        duration_seconds = (
            end_time - start_time
        ).total_seconds()

    print("=" * 60)
    print("PIPELINE TASK SUCCEEDED")
    print(f"DAG ID: {dag_id}")
    print(f"TASK ID: {task_id}")
    print(f"MAP INDEX: {map_index}")
    print(f"RUN ID: {pipeline_run_id}")
    print("=" * 60)

    phase_mapping = {

        "run_cloud_run_ingestion":
            "ingestion",

        "get_successful_datasets":
            "staging",

        "load_dataset_to_staging":
            "staging",

        "run_phase_8_silver":
            "phase_8",

        "run_phase_9_gold":
            "phase_9",

        "run_phase_10_quality":
            "phase_10",
    }

    phase = phase_mapping.get(
        task_id,
        "unknown",
    )

    try:

        write_audit_record(

            pipeline_run_id=pipeline_run_id,

            dag_id=dag_id,

            task_id=task_id,

            map_index=map_index,

            phase=phase,

            status="SUCCESS",

            start_time=start_time,

            end_time=end_time,

            duration_seconds=duration_seconds,

            error_message=None,
        )

    except Exception as audit_exception:

        # A monitoring failure must not change
        # a successful pipeline task to FAILED.
        print(
            "WARNING: Failed to write "
            "SUCCESS audit record"
        )

        print(
            f"Audit error: {audit_exception}"
        )





default_args = {

    "owner": "airflow",

    "retries": 2,

    "retry_delay": timedelta(
        minutes=5
    ),

    "on_failure_callback":
        task_failure_callback,

    "on_success_callback":
        task_success_callback,
}




with DAG(
    dag_id="olist_incremental_pipeline",

    description=(
        "Triggers incremental Olist ingestion "
        "and executes staging, Silver, Gold, "
        "and data quality workflows"
    ),

    start_date=datetime(2026, 8, 1),

    schedule=None,

    catchup=False,

    default_args=default_args,

    tags=[
        "olist",
        "gcp",
        "cloud-run",
        "bigquery",
        "dataform",
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


    # -------------------------------------------------
    # Phase 8: Silver transformations
    # -------------------------------------------------

    run_phase_8_silver = (
        DataformCreateWorkflowInvocationOperator(
            task_id="run_phase_8_silver",

            project_id=PROJECT_ID,

            region=DATAFORM_REGION,

            repository_id=DATAFORM_REPOSITORY,

            workflow_invocation={
                "workflow_config": (
                    f"projects/{PROJECT_ID}/"
                    f"locations/{DATAFORM_REGION}/"
                    f"repositories/{DATAFORM_REPOSITORY}/"
                    f"workflowConfigs/{PHASE_8_WORKFLOW}"
                )
            },

            asynchronous=False,
        )
    )


    # -------------------------------------------------
    # Phase 9: Gold transformations
    # -------------------------------------------------

    run_phase_9_gold = (
        DataformCreateWorkflowInvocationOperator(
            task_id="run_phase_9_gold",

            project_id=PROJECT_ID,

            region=DATAFORM_REGION,

            repository_id=DATAFORM_REPOSITORY,

            workflow_invocation={
                "workflow_config": (
                    f"projects/{PROJECT_ID}/"
                    f"locations/{DATAFORM_REGION}/"
                    f"repositories/{DATAFORM_REPOSITORY}/"
                    f"workflowConfigs/{PHASE_9_WORKFLOW}"
                )
            },

            asynchronous=False,
        )
    )


    # -------------------------------------------------
    # Phase 10: Quality and reconciliation
    # -------------------------------------------------

    run_phase_10_quality = (
        DataformCreateWorkflowInvocationOperator(
            task_id="run_phase_10_quality",

            project_id=PROJECT_ID,

            region=DATAFORM_REGION,

            repository_id=DATAFORM_REPOSITORY,

            workflow_invocation={
                "workflow_config": (
                    f"projects/{PROJECT_ID}/"
                    f"locations/{DATAFORM_REGION}/"
                    f"repositories/{DATAFORM_REPOSITORY}/"
                    f"workflowConfigs/{PHASE_10_WORKFLOW}"
                )
            },

            asynchronous=False,
        )
    )


    # -------------------------------------------------
    # Pipeline dependencies
    # -------------------------------------------------

    run_ingestion >> successful_datasets

    successful_datasets >> staging_loads

    staging_loads >> run_phase_8_silver

    run_phase_8_silver >> run_phase_9_gold

    run_phase_9_gold >> run_phase_10_quality