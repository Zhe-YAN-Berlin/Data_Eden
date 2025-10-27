from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.providers.kubernetes.operators.kubernestes_pod import KubernetesPodOperator
from utils.dbt_builder import DBTBuilder

# optional : slack notifier 
default_args = {
    'owner':'airflow',
    'start_date': days_ago(1),
    'catchup': False,
    'retries' : 1,
}

dag = DAG(
    task_id = 'dbt_run_dag',
    default_args = default_args,
    schedule_interval = '0 3 * * *',
)

models = "ebay_fraud_detection_features"

dbt_config = DBTBuilder(
    models = models,
).get_settings()

ENV = dbt_config.dbt_target

if ENV == 'development':
    dbt_run = BashOperator(
        task_id=f"dbt_run_models_{models}",
        bash_command = dbt_config.dbt_command,
        dag=dag,
    )
else:
    dbt_run = KubernetesPodOperator(
        task_id = f"dbt_run_models_{models}",
        name_space = dbt_config.namespace,
        image = dbt_config.image,
        cmds = ['sh', '-c'],
        arguments = [dbt_config.dbt_command],
        env_vars = dbt_config.env_vars,
        get_logs = True,
        dag=dag,
    )

dbt_run