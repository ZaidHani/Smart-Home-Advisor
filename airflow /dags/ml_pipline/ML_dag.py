from airflow import DAG
from airflow.operators.python import PythonOperator
from ml_pipline.prop_model import extracting_prop_data, building_prop_model
from ml_pipline.land_model import extracting_land_data, building_land_model
from datetime import datetime, timedelta
import sys 
import os
import mlflow
import mlflow.sklearn  # For tracking the final model

# Set MLflow tracking URI to save runs to a specific path
mlflow.set_tracking_uri("file:////opt/airflow/mlruns")

db_url = 'postgresql://postgres:mdkn@host.docker.internal:5432/houses'

def log_extract_data_start():
    print('Starting extracting data...')

def extract_prop_data_task():
    return extracting_prop_data(db_url)

def extract_land_data_task():
    return extracting_land_data(db_url)

def log_extracting_success():
    print('Extracting data completed successfully ✔')

def log_training_model_start():
    print('Starting model training...')

def build_prop_model_task():
    data = extract_prop_data_task()
    model_save_path = '/opt/airflow/models/prop-model'
    log_save_path = '/opt/airflow/log_save_path/model_training_log.log'
    building_prop_model(data, target_column='price', save_path=model_save_path, log_path=log_save_path)

def build_land_model_task():
    data = extract_land_data_task()
    model_save_path = '/opt/airflow/models/land-model'
    log_save_path = '/opt/airflow/log_save_path/model_training_log.log'
    building_land_model(data, target_column='price', save_path=model_save_path, log_path=log_save_path)

def log_training_success():
    print('Model trainingcompleted successfuly ✔')

default_args = {
    'owner':'Al-Jermy',
    'retries':5,
    'retries_delay':timedelta(minutes = 5)
}

with DAG(
    dag_id = 'weekly_machine_learning_pipline',
    start_date=datetime(2024, 11, 11),
    schedule_interval = "@weekly",
    default_args = default_args
) as dag:

    log_extract_data_start_task = PythonOperator(
        task_id = 'log_extracting_data_start',
        python_callable = log_extract_data_start
    )

    extract_prop_data_operator = PythonOperator(
        task_id = 'extract_prop_data_task',
        python_callable = extract_prop_data_task
    )

    extract_land_data_operator = PythonOperator(
        task_id = 'extract_land_data_task',
        python_callable = extract_land_data_task
    )

    log_extracting_success_task = PythonOperator(
        task_id = 'log_extracting_success',
        python_callable = log_extracting_success
    )

    log_training_model_start_task = PythonOperator(
        task_id = 'log_training_model_start',
        python_callable = log_training_model_start
    )

    build_prop_model_operator = PythonOperator(
        task_id = 'build_prop_model_task',
        python_callable =  build_prop_model_task
    )

    build_land_model_operator = PythonOperator(
        task_id = 'build_land_model_task',
        python_callable =  build_land_model_task
    )

    log_training_success_task = PythonOperator(
        task_id = 'log_training_success',
        python_callable = log_training_success
    )


    log_extract_data_start_task >> [extract_prop_data_operator, extract_land_data_operator] >> log_extracting_success_task >> log_training_model_start_task >> [build_prop_model_operator, build_land_model_operator] >> log_training_success_task