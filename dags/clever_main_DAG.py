from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from scripts.dag_factory import DAGFactory

default_args = {
    "owner": "rafael.lucena",
    "start_date": datetime(2024, 10, 1),
}

datasets = [
    'fmcsa_complaints.csv',
    'fmcsa_safer_data.csv',
    'fmcsa_company_snapshot.csv',
    'fmcsa_companies.csv',
    'customer_reviews_google.csv',
    'company_profiles_google_maps.csv'
]

with DAG("clever_main_DAG", default_args=default_args, catchup=False, schedule_interval='20 0 * * *', max_active_runs=1) as dag:
    start_task = EmptyOperator(task_id='Start', dag=dag)
    finish_task = EmptyOperator(task_id='Finish', dag=dag)

    factory = DAGFactory(dag)
    transform_task = factory.create_transform_task("clever_transform")

    for file_name in datasets:
        file_without_extension = file_name.split('.')[0]
        upload_task = factory.create_upload_tasks(
            task_id=f"upload_to_postgres_{file_without_extension}",
            file_name=file_name)
        start_task.set_downstream(upload_task)    
        upload_task.set_downstream(transform_task)
    
    transform_task.set_downstream(finish_task)
