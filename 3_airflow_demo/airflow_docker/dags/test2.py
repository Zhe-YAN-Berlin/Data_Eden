import os
import logging
from datetime import timedelta
from datetime import datetime
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

print(BUCKET)

parquet_file = "yellow_tripdata_2023-01.parquet"
parquet_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{parquet_file}"
print(parquet_url)
path_to_local_home = os.getenv("AIRFLOW_HOME", "/home/datatalks_jan/Data_Eden/3_airflow_demo/airflow_docker")
print(path_to_local_home)
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", 'demo_dataset') 

default_args = {
    "owner": "airflow",
    "start_date": datetime.now(),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['zhe-data-2-gcs'],
) as dag:

    download_parquet_task = BashOperator(
        task_id="download_parquet_task",
        bash_command=f"curl -sSL {parquet_url} > {path_to_local_home}/{parquet_file}"
    )
    
    download_parquet_task