import os
import logging
from datetime import timedelta
import pendulum

from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET = os.getenv("GCP_GCS_BUCKET")

parquet_file = "yellow_tripdata_2023-01.parquet"
parquet_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{parquet_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/home/datatalks_jan/Data_Eden/3_airflow_demo/airflow_docker")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'demo_dataset') 


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    try:
            # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
            # (Ref: https://github.com/googleapis/python-storage/issues/74)
            storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
            storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

            client = storage.Client()
            bucket = client.bucket(bucket)

            blob = bucket.blob(object_name) 
            blob.upload_from_filename(local_file) 

            print("upload sucess!")

    except Exception:
        print("upload unsucess!")
        raise

default_args = {
    "owner": "airflow",
    "start_date": pendulum.today("UTC").subtract(days=1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule=timedelta(days=1),
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['zhe-data-2-gcs'],
) as dag:

    download_parquet_task = BashOperator(
        task_id="download_parquet_task",
        bash_command=f"curl -sSL {parquet_url} > {path_to_local_home}/{parquet_file}"
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/{parquet_file}"],
            },
        },
    )

    download_parquet_task >> local_to_gcs_task >> bigquery_external_table_task