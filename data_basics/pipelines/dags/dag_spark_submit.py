from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.providers.kubernetes.operators.kubernetes_pod import KubernetesPodOperator 
from airflow.models import variables as var
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator  

### variables 
ENV = var.get("ENV")
NAME_SPACE = var.get("airflow")
IMAGE = 
SERVICE_ACCOUNT = 
K8S_MASTER = 
HDFS_DEFAULT_FS = 

INPUT_BASE = 
OUTPUT_BASE =
HIVE_DB = 
HIVE_TABLE =

DBT_IMAGE = 
###


default_vars = {
    'owner':'airflow',
    'start_date': days_ago(1),
    'catchup': False,
    'retries' : 1,
}

with DAG(
    dag_id = 'dbt_run_dag',
    default_args = default_vars,
    schedule_interval = '0 3 * * *',
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    spark_submit_args = [
        '--master',
        '--name',
        '--conf',f"spark.kubernetes.namespace={NAME_SPACE}",
        '--conf',f"spark.kubernetes.authenticate.driver.serviceAccountName={SERVICE_ACCOUNT}",
        '--conf',f"spark.kubernetes.container.image={IMAGE}",
        '--conf',f"spark.hadoop.fs.defaultFS={HDFS_DEFAULT_FS}",
        '--conf',f"spark.hadoop.hive.metastore.uris={K8S_MASTER}",
        '--conf',"spark.executor.instances=3",
        '--conf',"spark.executor.memory=2g",
        '--conf',"spark.executor.cores=1",
        '--conf',"spark.driver.memory=2g",
        '--conf',"spark.driver.cores=1", 
        'local:///opt/jobs/spark_submit_job.py',
        INPUT_BASE,
        OUTPUT_BASE,
        HIVE_DB,
        HIVE_TABLE,
        'party_key', 'part_val'
    ]

    spark_batch = KubernetesPodOperator(
        task_id = 'spark_submit_job', 
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=spark_submit_args,
        get_logs=True,
        is_delete_operator_pod=True,
        dag=dag,
    )

    dbt_run = KubernetesPodOperator(
        task_id = 'dbt_run_job',
        name_space = NAME_SPACE,
        image = DBT_IMAGE,
        cmds = ['sh', '-c'],
        arguments = [f'dbt run --models ebay_fraud_detection_features --target {ENV}'],
        get_logs = True,
        dag=dag,
    )

    start >> spark_batch >> dbt_run >> end
