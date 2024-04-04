import gdown
import os
from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

#remember pip install apache-airflow-providers-docker
credentials_location = '/home/datatalks_jan/.google/credentials/google_credentials.json'

def data_outbound_GCS(ti):
    df_final = ti.xcom_pull(task_ids='task_2_spark')
    #df_final.write.csv("gs://my-zhe-414813/conrad_output", mode="overwrite", header=True, inferSchema=True)
    output_path = '/home/datatalks_jan/Data_Eden/8_pySpark_pilot/source_data/output/df_final.txt'
    df_final.write.text(output_path, mode="overwrite")

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

    task_2_spark = DockerOperator(
        task_id='task_2_spark',
        image = 'sparkhome:latest',
        command='python /opt/spark/etl_task_2.py',
        docker_url='unix://var/run/docker.sock',
        xcom_all = True,
        mounts = [
            Mount(source='/home/datatalks_jan/Data_Eden/8_pySpark_pilot/source_data/multi_source_data', target='/opt/spark/mount_data')
        ],
        dag=dag
    )

    task_3_data_output = PythonOperator(
        task_id='task_3_data_output',
        python_callable=data_outbound_GCS,
        provide_context=True,
        dag=dag
    )
task_2_spark >> task_3_data_output