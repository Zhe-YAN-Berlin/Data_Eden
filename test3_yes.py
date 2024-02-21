import os
import logging
from datetime import timedelta
import pendulum
import requests
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
path_to_local_home = os.getenv("AIRFLOW_HOME", "/home/datatalks_jan/Data_Eden/3_airflow_demo/airflow_docker")
local_file_path = os.path.join(path_to_local_home, parquet_file)

response = requests.get(parquet_url)
if response.status_code == 200:
    with open(local_file_path, 'wb') as f:
        f.write(response.content)
    print(f"File downloaded to: {local_file_path}")
else:
    print("Failed to download file.")