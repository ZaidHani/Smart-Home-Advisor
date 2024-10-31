from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="my_dag_name",
    start_date=datetime.datetime(2024, 9, 20),
    schedule="@daily"
    ) as dag:
    pass