import os
import logging
from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
#BUCKET = "my-zhe-414813"

print(str(BUCKET))
'''
parquet_file = "yellow_tripdata_2023-01.parquet"
parquet_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{parquet_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/home/datatalks_jan/Data_Eden/3_airflow_demo/airflow_docker")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all') 

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
'''