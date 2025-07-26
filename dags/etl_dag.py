from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

def get_command():
    return "echo 'ETL pipeline would run here' && echo 'Success: ETL completed'"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='Run ETL pipeline using CLI',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 7, 26),
    catchup=False,
)

run_etl = BashOperator(
    task_id='run_etl_cli',
    bash_command=get_command(),
    dag=dag,
) 