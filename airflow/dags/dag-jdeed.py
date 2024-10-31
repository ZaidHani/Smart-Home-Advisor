from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Jermy',
    'retries': 5,
    'retries_delay': timedelta(minutes = 2)
}

with DAG(
    dag_id = 'test-dag',
    default_args = default_args,
    start_date = datetime(2024, 10, 25),
    schedule_interval = '@daily'
) as dag:
    task = BashOperator(
        task_id = 'tt',
        bash_command = 'python3 --version'
    )

    task