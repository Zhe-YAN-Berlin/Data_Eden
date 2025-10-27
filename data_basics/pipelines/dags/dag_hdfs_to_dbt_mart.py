from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.empty import EmptyOperator

default_args = {
    'owner': 'airflow',
    'retries':1,
}

with DAG(
    dag_id = 'hdfs_to_dbt_mart',
    default_args=default_args,
    schedule_interval = '0 3 * * *',
    start_date = days_ago(1),
    catchup = False,
) as dag:
    
    spark_transform = KubernetesPodOperator(
        task_id='spark_transform',
        name= 'spark_job'
        namespace='airflow',
        image='your-spark-ecr-image-uri:latest',
        cmds = ["/opt/spark/bin/spark-submit"],
        arguments=[
            "--master", "k8s://https://<k8s-api-server>",
            "deploy-mode", "cluster",
            "image", "your-spark-ecr-image-uri:latest",
            "--conf", "spark.executor.instances=3",
            "--conf", "spark.executor.memory=4g",
            "--conf", "spark.executor.cores=2",
            "--conf", "spark.driver.memory=2g",
            # Path to the Spark job script
            "/app/jobs/load_hdfs_to_hive.py",
            "--execution_date", "{{ ds }}",
        ],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    dbt_run = KubernetesPodOperator(
        task_id='dbt_run',
        name='dbt_job',
        namespace='airflow',
        image='your-dbt-ecr-image-uri:latest',
        cmds = ["dbt"],
        arguments=["run", "--models", "dbt_test_models", "--profiles-dir", "/app/dbt_profiles"],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    start >> spark_transform >> dbt_run >> end

