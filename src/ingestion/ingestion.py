from pathlib import Path
from datetime import datetime,timezone
from kaggle import api
import shutil
import yaml
import zipfile
import hashlib
import csv
import sys
import os
import json
from google.cloud import storage
from google.cloud import bigquery
from kaggle.api.kaggle_api_extended import KaggleApi

PROJECT_ROOT = Path(
    os.getenv(
        "PROJECT_ROOT",
        Path(__file__).resolve().parents[2]
    )
)

CONFIG_FILE = PROJECT_ROOT / "config" / "sources.yaml"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
SCHEMA_CONFIG = PROJECT_ROOT / "config" /"schemas"

def get_project_id(config):

    return config["project"]["id"]


def get_file_metadata(file_path):
    """
    Return metadata used to determine whether
    this exact file version was already processed.
    """

    md5_hash = hashlib.md5()

    with open(file_path, "rb") as file:

        for chunk in iter(
            lambda: file.read(1024 * 1024),
            b""
        ):
            md5_hash.update(chunk)

    return {
        "source_file": file_path.name,
        "file_md5_hash": md5_hash.hexdigest(),
        "file_size": file_path.stat().st_size,
    }

def get_manifest_table_id(config):
    """
    Build the fully qualified BigQuery manifest table ID.
    """

    project_id = config["project"]["id"]

    dataset = config["metadata"]["dataset"]

    table = config["metadata"]["table"]

    return (
        f"{project_id}."
        f"{dataset}."
        f"{table}"
    )

def is_already_processed(
    config,
    source_name,
    file_metadata
):
    """
    Check whether this exact version of a source file
    was successfully processed before.
    """

    project_id = get_project_id(config)

    client = bigquery.Client(
        project=project_id
    )   
   

    table_id = get_manifest_table_id(
        config
    )

    query = f"""
        SELECT 1
        FROM `{table_id}`
        WHERE source_name = @source_name
          AND source_file = @source_file
          AND file_md5_hash = @file_md5_hash
          AND status = 'SUCCESS'
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "source_name",
                "STRING",
                source_name,
            ),
            bigquery.ScalarQueryParameter(
                "source_file",
                "STRING",
                file_metadata["source_file"],
            ),
            bigquery.ScalarQueryParameter(
                "file_md5_hash",
                "STRING",
                file_metadata["file_md5_hash"],
            ),
        ]
    )

    results =  client.query(
        query,
        job_config=job_config,
        location="asia-south1"
    ).result()

    return results.total_rows > 0


def load_config():
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config

def load_schema(schema_file):
    schema_path = SCHEMA_CONFIG/schema_file

    if not schema_path.exists():
        raise FileNotFoundError(
            f"Schema file not found : {schema_path}"
        )

    with open(schema_path,"r",encoding="utf-8") as file:
        return yaml.safe_load(file)

def validate_value_type(value, data_type):
    """
    Validate one CSV value against the configured schema type.

    Empty values are handled separately by nullable validation.
    """

    if value is None or str(value).strip() == "":
        return True

    value = str(value).strip()

    if data_type == "STRING":
        return True

    elif data_type == "TIMESTAMP":
        try:
            datetime.fromisoformat(
                value.replace("Z", "+00:00")
            )
            return True
        except ValueError:
            return False

    elif data_type == "INTEGER":
        try:
            numeric_value = float(value)

            return numeric_value.is_integer()

        except (ValueError, TypeError):
            return False

    elif data_type == "NUMERIC":
        try:
            float(value)
            return True

        except (ValueError, TypeError):
            return False

    else:
        raise ValueError(
            f"Unsupported data type: {data_type}"
        )

def validate_row(row, schema):
    """
    Validate one CSV row.

    Returns a list of validation errors.
    """

    errors = []

    for column in schema["columns"]:

        name = column["name"]
        data_type = column["type"]
        nullable = column.get("nullable", True)

        value = row.get(name)

        # ---------------------------------------------
        # NULL validation
        # ---------------------------------------------

        is_empty = (
            value is None
            or str(value).strip() == ""
        )

        if not nullable and is_empty:

            errors.append(
                f"{name}: NULL value not allowed"
            )

            # No need to run type validation
            continue

        # ---------------------------------------------
        # Data type validation
        # ---------------------------------------------

        if not is_empty:

            is_valid_type = validate_value_type(
                value,
                data_type
            )

            if not is_valid_type:

                errors.append(
                    f"{name}: invalid {data_type} value "
                    f"'{value}'"
                )

    return errors


def validate_csv_stream(
    input_file,
    schema,
    valid_file,
    invalid_file
):
    """
    Stream a CSV file row by row.

    Valid rows are written immediately to valid_file.
    Invalid rows are written immediately to invalid_file.

    Returns processing statistics.
    """

    total_rows = 0
    valid_rows = 0
    invalid_rows = 0

    expected_columns = [
        column["name"]
        for column in schema["columns"]
    ]

    with open(
    input_file,
    mode="r",
    encoding="utf-8-sig",
    newline=""
    ) as source:

        reader = csv.DictReader(source)

        if reader.fieldnames is None:

            raise ValueError(
                f"CSV file has no header: {input_file}"
            )

        actual_columns = [
            column.strip()
            for column in reader.fieldnames
        ]

        # ---------------------------------------------
        # File-level schema validation
        # ---------------------------------------------

        missing_columns = [
            column
            for column in expected_columns
            if column not in actual_columns
        ]

        unexpected_columns = [
            column
            for column in actual_columns
            if column not in expected_columns
        ]

        if missing_columns or unexpected_columns:

            if missing_columns:

                print(
                    f"Missing columns: {missing_columns}"
                )

            if unexpected_columns:

                print(
                    f"Unexpected columns: {unexpected_columns}"
                )

            raise ValueError(
                f"Schema validation failed for "
                f"{input_file.name}"
            )

        # ---------------------------------------------
        # Output file writers
        # ---------------------------------------------

        invalid_fieldnames = (
            actual_columns
            + [
                "validation_errors",
                "source_row_number"
            ]
        )

        with open(
            valid_file,
            mode="w",
            encoding="utf-8",
            newline=""
        ) as valid_output, open(
            invalid_file,
            mode="w",
            encoding="utf-8",
            newline=""
        ) as invalid_output:

            valid_writer = csv.DictWriter(
                valid_output,
                fieldnames=actual_columns,
                quoting=csv.QUOTE_ALL,
                quotechar='"',
                doublequote=True,
                lineterminator="\n"
            )

            invalid_writer = csv.DictWriter(
                invalid_output,
                fieldnames=invalid_fieldnames,
                quoting=csv.QUOTE_ALL,
                quotechar='"',
                doublequote=True,
                lineterminator="\n",
                extrasaction="ignore"
            )

            valid_writer.writeheader()
            invalid_writer.writeheader()

            # -----------------------------------------
            # Streaming row processing
            # -----------------------------------------

            for row_number, row in enumerate(
                reader,
                start=2
            ):

                total_rows += 1

                # Normalize column names and values
                cleaned_row = {}

                for key, value in row.items():

                    if key is None:
                        continue

                    cleaned_key = key.strip()

                    cleaned_value = (
                        value.strip()
                        if isinstance(value, str)
                        else value
                    )

                    cleaned_row[cleaned_key] = cleaned_value

                # -------------------------------------
                # Validate current row
                # -------------------------------------

                row_errors = validate_row(
                    cleaned_row,
                    schema
                )

                # -------------------------------------
                # Valid row
                # -------------------------------------

                if not row_errors:

                    valid_writer.writerow(
                        cleaned_row
                    )

                    valid_rows += 1

                # -------------------------------------
                # Invalid row
                # -------------------------------------

                else:

                    rejected_row = dict(
                        cleaned_row
                    )

                    rejected_row[
                        "validation_errors"
                    ] = " | ".join(row_errors)

                    rejected_row[
                        "source_row_number"
                    ] = row_number

                    invalid_writer.writerow(
                        rejected_row
                    )

                    invalid_rows += 1

    print(f"Total rows: {total_rows:,}")
    print(f"Valid rows: {valid_rows:,}")
    print(f"Invalid rows: {invalid_rows:,}")

    return {
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows,
    }

def upload_quarantine_to_gcs(
    config,
    dataset_config,
    invalid_file,
    invalid_row_count
):

    if invalid_row_count == 0:

        print(
            "No invalid rows. "
            "Quarantine upload skipped."
        )

        if invalid_file.exists():
            invalid_file.unlink()

        return None

    bucket_name = config["storage"]["bucket"]

    quarantine_prefix = (
        config["storage"]["quarantine_prefix"]
    )

    folder = dataset_config["gcs_folder"]

    ingestion_date = (
        datetime.now(timezone.utc)
        .strftime("%Y-%m-%d")
    )

    destination = (
        f"{quarantine_prefix}/"
        f"{folder}/"
        f"ingestion_date={ingestion_date}/"
        f"invalid_rows.csv"
    )

    project_id = get_project_id(config)

    client = storage.Client(
    project=project_id
    )

    bucket = client.bucket(bucket_name)

    blob = bucket.blob(destination)

    print(
        f"Uploading quarantine file to "
        f"gs://{bucket_name}/{destination}"
    )

    blob.upload_from_filename(
        str(invalid_file),
        content_type="text/csv",
        timeout=600
    )

    print(
        f"Quarantine upload successful: "
        f"gs://{bucket_name}/{destination}"
    )

    return destination


def cleanup_temporary_files(*file_paths):

    for file_path in file_paths:

        if file_path is None:
            continue

        try:

            path = Path(file_path)

            if path.exists() and path.is_file():

                path.unlink()

                print(
                    f"Deleted temporary file: "
                    f"{path.name}"
                )

        except Exception as error:

            print(
                f"WARNING: Failed to delete "
                f"{file_path}: {error}"
            )


def process_dataset(
    config,
    dataset_name,
    dataset_config
):

    pipeline_run_id = os.getenv(
    "PIPELINE_RUN_ID",
    "local_manual_run"
    )

    print(f"\n{'=' * 60}")
    print(f"Processing dataset: {dataset_name}")
    print(f"{'=' * 60}")

    # -------------------------------------------------
    # Initialize temporary file references
    # -------------------------------------------------

    local_file = None
    valid_file = None
    invalid_file = None
    file_metadata = None

    try:

        # -------------------------------------------------
        # 1. Download source
        # -------------------------------------------------

        local_file = download_dataset(
            config,
            dataset_config
        )

        # -------------------------------------------------
        # 2. Get source version metadata
        # -------------------------------------------------

        file_metadata = get_file_metadata(
            local_file
        )

        print(
            f"File: "
            f"{file_metadata['source_file']}"
        )

        print(
            f"Size: "
            f"{file_metadata['file_size']:,} bytes"
        )

        print(
            f"MD5: "
            f"{file_metadata['file_md5_hash']}"
        )

        # -------------------------------------------------
        # 3. Incremental check
        # -------------------------------------------------

        if is_already_processed(
            config=config,
            source_name=dataset_name,
            file_metadata=file_metadata,
        ):

            print(
                f"SKIPPED: {dataset_name} "
                f"has already been successfully processed."
            )

            return {
                "dataset_name": dataset_name,
                "status": "SKIPPED",
            }

        print(
            "New or changed file detected. "
            "Starting ingestion..."
        )

        # -------------------------------------------------
        # 4. Load schema
        # -------------------------------------------------

        schema_file = dataset_config[
            "schema_file"
        ]

        schema = load_schema(
            schema_file
        )

        # -------------------------------------------------
        # 5. Prepare temporary output files
        # -------------------------------------------------

        valid_file = (
            RAW_DIR /
            f"{dataset_name}_valid.csv"
        )

        invalid_file = (
            RAW_DIR /
            f"{dataset_name}_invalid.csv"
        )

        # Remove leftover files from an earlier failed run
        cleanup_temporary_files(
            valid_file,
            invalid_file,
        )

        # -------------------------------------------------
        # 6. Stream and validate
        # -------------------------------------------------

        validation_result = (
            validate_csv_stream(
                input_file=local_file,
                schema=schema,
                valid_file=valid_file,
                invalid_file=invalid_file,
            )
        )

        if validation_result["valid_rows"] == 0:

            raise ValueError(
                f"No valid rows found for "
                f"{dataset_name}. "
                f"Bronze upload aborted."
            )

        # -------------------------------------------------
        # 7. Upload invalid rows
        # -------------------------------------------------

        quarantine_destination = (
            upload_quarantine_to_gcs(
                config=config,
                dataset_config=dataset_config,
                invalid_file=invalid_file,
                invalid_row_count=validation_result[
                    "invalid_rows"
                ],
            )
        )

        # -------------------------------------------------
        # 8. Upload valid rows
        # -------------------------------------------------

        bronze_destination = (
            upload_to_gcs(
                config,
                dataset_config,
                valid_file,
            )
        )

        # -------------------------------------------------
        # 9. Record SUCCESS
        # -------------------------------------------------

        record_manifest_entry(
            config=config,
            source_name=dataset_name,
            file_metadata=file_metadata,
            status="SUCCESS",
            bronze_gcs_path=bronze_destination,
            quarantine_gcs_path=quarantine_destination,
            validation_result=validation_result,
            pipeline_run_id=pipeline_run_id,
        )

        print(
            f"SUCCESS: {dataset_name}"
        )

        return {
            "dataset_name": dataset_name,
            "status": "SUCCESS",
            "validation_result": validation_result,
        }

    except Exception as error:

        print(
            f"FAILED: {dataset_name}"
        )

        print(
            f"Error: {str(error)}"
        )

        # -------------------------------------------------
        # Record FAILED attempt
        # -------------------------------------------------

        if file_metadata is not None:

            try:

                record_manifest_entry(
                    config=config,
                    source_name=dataset_name,
                    file_metadata=file_metadata,
                    status="FAILED",
                    error_message=str(error),
                    pipeline_run_id=pipeline_run_id,
                )

            except Exception as manifest_error:

                print(
                    "WARNING: Could not record "
                    f"failure in manifest: "
                    f"{manifest_error}"
                )

        raise

    finally:

        # -------------------------------------------------
        # 10. Clean up Cloud Run temporary files
        # -------------------------------------------------

        print(
            f"\nCleaning up temporary files "
            f"for {dataset_name}..."
        )

        cleanup_temporary_files(
            local_file,
            valid_file,
            invalid_file,
        )

def record_manifest_entry(
    config,
    source_name,
    file_metadata,
    status,
    bronze_gcs_path=None,
    quarantine_gcs_path=None,
    validation_result=None,
    error_message=None,
    pipeline_run_id=None,
):
    """
    Record the result of an ingestion attempt
    in the BigQuery ingestion manifest.
    """

    project_id = get_project_id(
        config
    )

    client = bigquery.Client(
        project=project_id
    )

    table_id = get_manifest_table_id(
        config
    )

    now = datetime.now(
        timezone.utc
    )

    row = {
        "source_name": source_name,
        "source_file": file_metadata[
            "source_file"
        ],
        "file_generation": None,
        "file_md5_hash": file_metadata[
            "file_md5_hash"
        ],
        "file_size": file_metadata[
            "file_size"
        ],
        "ingestion_date": now.date().isoformat(),
        "processed_at": now.isoformat(),

        # ---------------------------------------------
        # Current Composer / pipeline execution ID
        # ---------------------------------------------
        "pipeline_run_id": pipeline_run_id,

        "bronze_gcs_path": bronze_gcs_path,
        "quarantine_gcs_path": quarantine_gcs_path,

        "total_rows": (
            validation_result["total_rows"]
            if validation_result
            else None
        ),

        "valid_rows": (
            validation_result["valid_rows"]
            if validation_result
            else None
        ),

        "invalid_rows": (
            validation_result["invalid_rows"]
            if validation_result
            else None
        ),

        "status": status,
        "error_message": error_message,
    }

    errors = client.insert_rows_json(
        table_id,
        [row]
    )

    if errors:

        raise RuntimeError(
            f"Failed to insert manifest entry: "
            f"{errors}"
        )

    print(
        f"Manifest updated: "
        f"{source_name} → {status} "
        f"| pipeline_run_id={pipeline_run_id}"
    )

def get_kaggle_api():
    # -------------------------------------------------
    # Cloud Run secret mount config path
    # -------------------------------------------------
    secret_credentials_path = Path("/secrets/kaggle/credentials.json")

    # -------------------------------------------------
    # Case A: Running in Cloud Run (Secret Mount Exists)
    # -------------------------------------------------
        # -------------------------------------------------
    # Case A: Running in Cloud Run (Secret Mount Exists)
    # -------------------------------------------------
    if secret_credentials_path.exists():
        print("Kaggle credentials found in Cloud Run secret mount.")
        try:
            # 1. Read the secret JSON payload directly as text strings
            with open(secret_credentials_path, "r") as file:
                credentials = json.load(file)
            
            # 2. Extract username safely
            os.environ["KAGGLE_USERNAME"] = credentials["username"]
            
            # 3. CRITICAL FIX: Fall back to 'access_token' if 'key' does not exist
            if "key" in credentials:
                os.environ["KAGGLE_KEY"] = credentials["key"]
            elif "access_token" in credentials:
                os.environ["KAGGLE_KEY"] = credentials["access_token"]
            else:
                raise KeyError("Neither 'key' nor 'access_token' was found in the JSON file.")
            
            print("Directly injected Kaggle credentials into system variables.")
        except Exception as file_error:
            raise RuntimeError(f"Failed parsing credentials JSON layout: {file_error}")


    # -------------------------------------------------
    # Case B: Local Docker / Local Dev Machine Workflow
    # -------------------------------------------------
    else:
        local_kaggle_dir = Path.home() / ".kaggle"
        if local_kaggle_dir.exists():
            os.environ["KAGGLE_CONFIG_DIR"] = str(local_kaggle_dir)
            print("Using local standard Kaggle credentials path environment.")
        else:
            raise FileNotFoundError(
                "Kaggle credentials were not found. "
                "Expected either Cloud Run secret credentials or local ~/.kaggle credentials."
            )

    # -------------------------------------------------
    # Instantiate & Authenticate Client
    # -------------------------------------------------
    try:
        # Initializing here reads KAGGLE_USERNAME and KAGGLE_KEY directly, 
        # completely bypassing read-only file system dependencies!
        api = KaggleApi()
        api.authenticate()
        print("Kaggle authentication successful.")
        return api
        
    except Exception as error:
        raise RuntimeError(f"Kaggle authentication failed: {error}")


    
def download_dataset(config, dataset_config):

    RAW_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    dataset = config["source"]["dataset"]

    filename = dataset_config["filename"]

    print(
        f"\nDownloading {filename} from Kaggle..."
    )

    api = get_kaggle_api()
    api.authenticate()

    api.dataset_download_file(
        dataset,
        file_name=filename,
        path=str(RAW_DIR),
        force=True,
        quiet=False
    )

   
    zip_file = RAW_DIR / f"{filename}.zip"
    csv_file = RAW_DIR / filename

    # -----------------------------------------------------
    # Case 1: Kaggle returned ZIP
    # -----------------------------------------------------

    if zip_file.exists():

        print(f"Downloaded ZIP: {zip_file}")

        with zipfile.ZipFile(
            zip_file,
            "r"
        ) as zip_ref:

            zip_ref.extractall(RAW_DIR)

        if not csv_file.exists():

            raise FileNotFoundError(
                f"CSV not found after extracting "
                f"{zip_file}"
            )

        zip_file.unlink()

        print(
            f"Extracted: {csv_file}"
        )

    # -----------------------------------------------------
    # Case 2: Kaggle returned CSV directly
    # -----------------------------------------------------

    elif csv_file.exists():

        print(
            f"CSV downloaded directly: "
            f"{csv_file}"
        )

    # -----------------------------------------------------
    # Case 3: Neither exists
    # -----------------------------------------------------

    else:

        print("Files currently present in data/raw:")

        for file in RAW_DIR.iterdir():
            print(f"  - {file.name}")

        raise FileNotFoundError(
            f"Neither ZIP nor CSV was found for "
            f"{filename}"
        )

    return csv_file


def upload_to_gcs(
    config,
    dataset_config,
    local_file
):

    bucket_name = config["storage"]["bucket"]

    bronze_prefix = (
        config["storage"]["bronze_prefix"]
    )

    folder = dataset_config["gcs_folder"]

    ingestion_date = (
        datetime.now(timezone.utc)
        .strftime("%Y-%m-%d")
    )

    destination = (
        f"{bronze_prefix}/"
        f"{folder}/"
        f"ingestion_date={ingestion_date}/"
        f"{local_file.name}"
    )

    print(
        f"Uploading to "
        f"gs://{bucket_name}/{destination}"
    )

    project_id = get_project_id(config)

    client = storage.Client(
        project=project_id
    )

    bucket = client.bucket(bucket_name)

    blob = bucket.blob(destination)

    # Increase the upload timeout for large files
    blob.upload_from_filename(
        str(local_file),
        timeout=600
    )

    print(
        "Upload successful."
    )

    return destination

def main():

    print("Starting Olist incremental ingestion...")

    config = load_config()

    datasets = config["files"]

    results = []
    failures = []

    for dataset_name, dataset_config in datasets.items():

        try:

            result = process_dataset(
                config,
                dataset_name,
                dataset_config,
            )

            results.append(result)

        except Exception as error:

            failures.append({
                "dataset_name": dataset_name,
                "error": str(error),
            })

            print(
                f"\nDataset failed: {dataset_name}"
            )

            print(
                f"Error: {error}"
            )

    # =====================================================
    # RUN SUMMARY
    # =====================================================

    print("\n" + "=" * 60)
    print("INGESTION RUN SUMMARY")
    print("=" * 60)

    for result in results:

        print(
            f"{result['dataset_name']}: "
            f"{result['status']}"
        )

    successful = sum(
        1
        for result in results
        if result["status"] == "SUCCESS"
    )

    skipped = sum(
        1
        for result in results
        if result["status"] == "SKIPPED"
    )

    print(
        f"\nSuccessfully processed: {successful}"
    )

    print(
        f"Skipped unchanged: {skipped}"
    )

    # =====================================================
    # CLOUD RUN JOB STATUS
    # =====================================================

    if failures:

        print("\nFAILED DATASETS:")

        for failure in failures:

            print(
                f"{failure['dataset_name']}: "
                f"{failure['error']}"
            )

        print(
            f"\nIngestion completed with "
            f"{len(failures)} failure(s)."
        )

        sys.exit(1)

    print(
        "\nIngestion completed successfully."
    )

    sys.exit(0)




if __name__ == "__main__":
    main()