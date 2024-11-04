from data_pipeline import etl
from data_pipeline import scraping
from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
from datetime import timedelta

# Define the default arguments for the DAG
default_args = {
    'owner': 'Zaid',  # Replace with your name
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.datetime(2024, 11, 4),  # Adjust to your desired start date
}

with DAG(
    dag_id='daily_data_pipeline',
    schedule_interval='@daily',
    default_args=default_args) as dag:

    web_scraping = PythonOperator(task_id='scrape', python_callable=scraping.main)
    extract_transform_load = PythonOperator(task_id='etl', python_callable=etl.main)


web_scraping >> extract_transform_load