from airflow import DAG
from airflow.operators.python import PythonOperator
from ml_pipline.prop_model import extract_data, build_model
from datetime import datetime, timedelta
import sys 
import os
import mlflow
import mlflow.sklearn  # For tracking the final model

# Set MLflow tracking URI to save runs to a specific path
mlflow.set_tracking_uri("file:////opt/airflow/mlruns")


def extract_data_task():
    db_url = 'postgresql://postgres:mdkn@host.docker.internal:5432/houses'
    return extract_data(db_url)
    


def build_model_task():
    data = extract_data_task()
    model_save_path = '/opt/airflow/models/prop-model'
    log_save_path = '/opt/airflow/log_save_path/model_training_log.log'
    build_model(data, target_column='price', save_path=model_save_path, log_path=log_save_path)



default_args = {
    'owner':'Al-Jermy',
    'retries':1,
    'retries_delay':timedelta(minutes = 2)
}

with DAG(
    dag_id = 'property_ml_pipeline_v05',
    default_args = default_args,
    start_date = datetime(2024, 11, 10),
    schedule_interval = '@daily',
    catchup = False
) as dag:

    extract_data_operator = PythonOperator(
        task_id = 'extract_data_task',
        python_callable = extract_data_task
    )

    build_model_operator = PythonOperator(
        task_id = 'build_model_task',
        python_callable =  build_model_task
    )

    extract_data_operator >> build_model_operator