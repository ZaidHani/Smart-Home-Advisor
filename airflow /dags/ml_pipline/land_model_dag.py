from airflow import DAG
from airflow.operators.python import PythonOperator
from ml_pipline.land_model import extracting_land_data, building_land_model
from datetime import datetime, timedelta
import sys 
import os
import mlflow
import mlflow.sklearn  # For tracking the final model

# Set MLflow tracking URI to save runs to a specific path
mlflow.set_tracking_uri("file:////opt/airflow/mlruns")


def log_extracting_land_data_start():
    print('Starting extracting data...')


def extracting_land_data_task():
    db_url = 'postgresql://postgres:mdkn@host.docker.internal:5432/houses'
    return extracting_land_data(db_url)
    
def log_extracting_success():
    print('Extracting data completed successfully ✔')

def log_training_model_start():
    print('Starting model training...')

def building_land_model_task():
    data = extracting_land_data_task()
    model_save_path = '/opt/airflow/models/land-model'
    log_save_path = '/opt/airflow/log_save_path/model_training_log.log'
    building_land_model(data, target_column='price', save_path=model_save_path, log_path=log_save_path)

def log_training_success():
    print('Model trainingcompleted successfuly ✔')


default_args = {
    'owner':'Al-Jermy',
    'retries':1,
    'retries_delay':timedelta(minutes = 2)
}

with DAG(
    dag_id = 'land_ml_pipeline',
    default_args = default_args,
    start_date = datetime(2024, 11, 11),
    schedule_interval = '@weekly'
) as dag:

    log_extracting_land_data_start_task = PythonOperator(
        task_id = 'log_extracting_data_start',
        python_callable = log_extracting_land_data_start
    )

    extracting_land_data_operator = PythonOperator(
        task_id = 'extracting_land_data_task',
        python_callable = extracting_land_data_task
    )

    log_extracting_success_task = PythonOperator(
        task_id = 'log_extracting_success',
        python_callable = log_extracting_success
    )

    log_training_model_start_task = PythonOperator(
        task_id = 'log_training_model_start',
        python_callable = log_training_model_start
    )

    building_land_model_operator = PythonOperator(
        task_id = 'building_land_model_task',
        python_callable =  building_land_model_task
    )

    log_training_success_task = PythonOperator(
        task_id = 'log_training_success',
        python_callable = log_training_success
    )

    log_extracting_land_data_start_task >> extracting_land_data_operator >> log_extracting_success_task >> log_training_model_start_task >> building_land_model_operator >> log_training_success_task