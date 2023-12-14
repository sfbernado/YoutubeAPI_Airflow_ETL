from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from youtube_etl import run_youtube_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'youtube_dag',
    default_args=default_args,
    description='A simple youtube DAG',
)

run_etl = PythonOperator(
    task_id='complete_youtube_etl',
    python_callable=run_youtube_etl,
    dag=dag,
)

run_etl