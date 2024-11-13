from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from scripts.vader_sentiment_analyst import VaderSentimentAnalyst

default_args = {
    "owner": "rafael.lucena",
    "start_date": datetime(2024, 10, 1),
}

with DAG("clever_review_sentiment_DAG", default_args=default_args, catchup=False, schedule_interval='30 0 * * *', max_active_runs=1) as dag:
    start_task = EmptyOperator(task_id='Start', dag=dag)
    finish_task = EmptyOperator(task_id='Finish', dag=dag)

    review_sentiment_task = PythonOperator(
        task_id=f"review_sentiment",
        python_callable=VaderSentimentAnalyst().sentiment_analysis,
        dag=dag,
        execution_timeout=timedelta(seconds=30),
    )

    start_task.set_downstream(review_sentiment_task)
    review_sentiment_task.set_downstream(finish_task)
