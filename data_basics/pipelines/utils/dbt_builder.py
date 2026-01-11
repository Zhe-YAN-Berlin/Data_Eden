from airflow.hooks.base import BaseHook
from airflow.models import Variable 

class DBTBuilder:
    def __init__(self, models:str):
        self.namespace = "airflow",
        self.image = "ECR URL",
        self.dbt_target = (
            Variable.get("ENV") if Variable.exists("ENV") 
            else "development"
        )
        if self.dbt_target == 'development':
            self.dbt_command = f"""
                cd /opt/airflow/dbt_project/ && \
                dbt seed --profiles-dir . --target dev && \
                dbt run --profiles-dir . --target dev --models {models} 
            """
        else:
            self.dbt_command = f"""
                cd /dbt_project/ && \
                dbt seed --profiles-dir . --target {self.dbt_target} && \
                dbt run --profiles-dir . --target {self.dbt_target} --models {models} 
            """
    def get_settings(self):
        return self