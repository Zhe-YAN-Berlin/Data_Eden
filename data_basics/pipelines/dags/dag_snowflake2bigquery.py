from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.dates import days_ago
from airflow.operators.empyty import EmptyOperator
from airflow.hooks.base import BaseHook

class ProcessOperator(BaseOperator):
    # define main block
    define __init__(self, query_name, query_data, *args, **kwargs):

    # define airflow skip func 

    # define execute func

    # spark session definition 

    # read snowflake data from connection 

    # write data into parquet file 

    # upload the parquet file to gcs
    
    # append & write the data into BigQuery table 

    # release resources & stop spark session

default_args = {
    'owner':'airflow',
    'start_date': days_ago(2),
    'schedule_interval': '0 3 * * *',
    'catchup': False,
    'retries': 1,
}

dag = DAG(
    task_id = 'ebay_data_mapping_snowflake_to_bigquery',
    default_args=default_args,
)

start = EmptyOperator(task_id='start')

end = EmptyOperator(task_id='end')

for query_name, query_data in queries.queries.items():
    data_mapping = ProcessOperator(
        task_id = f"process_{query_name}"
        query_name = query_name,
        query_data = query_data,
        dag=dag,
        pool='snowflake_pool'
    )
    start >> data_mapping >> end