import gdown
import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator, Mount
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from google.cloud import storage

#remember pip install apache-airflow-providers-docker
credentials_location = '/home/datatalks_jan/.google/credentials/google_credentials.json'

def download_and_unzip_data():
    source_url = 'https://drive.google.com/uc?id=1vcb_HBWsOSKW4XxhLfRpGlLzBLwHlGWJ'
    output_path = '/home/datatalks_jan/Data_Eden/8_pySpark_pilot/source_data/multi_source_data'
    output_file = os.path.join(output_path, 'multi_source_demo.zip') 
    gdown.download(source_url, output_file, quiet=False)
    os.system(f'unzip -o {output_file} -d {output_path}')

def data_outbound_GCS(ti):
    df_str = ti.xcom_pull(task_ids='task_2_spark')
    with open("/home/datatalks_jan/Data_Eden/8_pySpark_pilot/source_data/output/file.txt", "w") as file:
        file.write(df_str[0])
    local_file_path = '/home/datatalks_jan/Data_Eden/8_pySpark_pilot/source_data/output/file.txt'
    gcs_bucket_name = 'my-zhe-414813'
    gcs_file_path = 'conrad_output/file.txt'
    client = storage.Client()
    bucket = client.get_bucket(gcs_bucket_name)
    blob = bucket.blob(gcs_file_path)
    blob.upload_from_filename(local_file_path)

default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG('conrad_coding_test', default_args=default_args, schedule_interval='@weekly') as dag:

    task_1_ingestion = PythonOperator(
        task_id='task_1_ingestion',
        python_callable=download_and_unzip_data,
        dag=dag
    )

    task_2_spark = DockerOperator(
        task_id='task_2_spark',
        image = 'sparkhome:latest',
        command='python /opt/spark/etl_task_2.py',
        docker_url='unix://var/run/docker.sock',
        xcom_all = True,
        mounts=[Mount(source='/home/datatalks_jan/Data_Eden/8_pySpark_pilot/source_data/multi_source_data',target='/opt/spark/mount_data',type='bind')],
        dag=dag
    )

    task_3_data_output = PythonOperator(
        task_id='task_3_data_output',
        python_callable=data_outbound_GCS,
        provide_context=True,
        dag=dag
    )
task_1_ingestion >> task_2_spark >> task_3_data_output