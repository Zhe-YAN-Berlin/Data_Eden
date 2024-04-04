import gdown
import os
from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

#remember pip install apache-airflow-providers-docker

credentials_location = '/home/datatalks_jan/.google/credentials/google_credentials.json'

"""
def download_and_unzip_data():
    source_url = 'https://drive.google.com/uc?id=1vcb_HBWsOSKW4XxhLfRpGlLzBLwHlGWJ'
    output_path = 'source_data/multi_source_data'
    output_file = os.path.join(output_path, 'multi_source_demo.zip') 
    gdown.download(source_url, output_file, quiet=False)
    os.system(f'unzip -o {output_file} -d {output_path}')
"""

def data_outbound_GCS():
    import sparkETL
    df_final = sparkETL.df_final
    df_final.write.csv("gs://my-zhe-414813/conrad_output", mode="overwrite", header=True, inferSchema=True)

default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('conrad_coding', default_args=default_args, schedule_interval='@weekly') as dag:

    task_2_spark = DockerOperator(
        task_id='task_2_spark',
        image = 'cb004f06511d:latest',
        command='spark-submit /opt/spark/sparkETL.py',
        dag=dag
    )

    task_3_data_output = PythonOperator(
        task_id='task_3_data_output',
        python_callable=data_outbound_GCS
    )

task_2_spark >> task_3_data_output