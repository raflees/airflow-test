from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from scripts.clever_main_pipeline import upload_to_postgres

class DAGFactory:
    def __init__(self, dag: DAG):
        self.dag = dag

    def create_upload_tasks(self, task_id: str, file_name: list) -> PythonOperator:
        file_without_extension = file_name.split('.')[0]
        upload_to_postgres_task = PythonOperator(
            task_id=f"upload_to_postgres_{file_without_extension}",
            python_callable=upload_to_postgres,
            dag=self.dag,
            execution_timeout=timedelta(seconds=30),
            op_kwargs={"file_name": file_name}
        )
        return upload_to_postgres_task
    
    def create_transform_task(self, task_id: str) -> BashOperator:
        transform_task = BashOperator(
            task_id=task_id,
            dag=self.dag,
            bash_command="""
                cd $AIRFLOW_HOME/dags/clever_transform &&
                dbt deps &&
                dbt run --selector main --target prod"""
        )
        return transform_task
