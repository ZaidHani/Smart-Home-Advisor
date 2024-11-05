from ml_pipeline import prop_model, land_model
from airflow.operators.python import PythonOperator
from airflow import DAG
import datetime
from datetime import timedelta

# Define the default arguments for the DAG
default_args = {
    'owner': 'Al-Jermy',  # Replace with your name
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.datetime(2024, 11, 4),  # Adjust to your desired start date
}

with DAG(
    dag_id='weekly_machine_learning',
    schedule_interval='@weekly',
    default_args=default_args) as dag:

    prop_model_task = PythonOperator(task_id='protperty_model', python_callable=prop_model.main)
    land_model_task = PythonOperator(task_id='lands_model', python_callable=land_model.main)


prop_model_task >> land_model_task