from data_pipeline import etl
from data_pipeline import scraping
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
import datetime
from datetime import timedelta

# Define the default arguments for the DAG
default_args = {
    'owner': 'Zaid',  # Replace with your name
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Log messages for different steps of the data pipeline

def log_scraping_start():
    """Logs a message to indicate the start of the web scraping process."""
    print("Starting web scraping...")

def log_scraping_success():
    """Logs a message to indicate the successful completion of the web scraping process."""
    print("Web scraping completed successfully ✔")

def log_etl_start():
    """Logs a message to indicate the start of the ETL process."""
    print("Starting ETL process...")

def log_etl_success():
    """Logs a message to indicate the successful completion of the ETL process."""
    print("ETL process completed successfully ✔")

# Define the DAG
with DAG(
    dag_id='daily_data_pipeline_v03',
    start_date=datetime.datetime(2024, 11, 7),  # Adjust to your desired start date
    schedule_interval='1 0 * * *',  # Set to run daily at 12:01 AM
    default_args=default_args,
    catchup=False  # Optional: prevents the DAG from running for missed intervals
) as dag:

    # Tasks for logging the start and completion of web scraping
    scraping_start_log_task = PythonOperator(
        task_id='log_scraping_start', 
        python_callable=log_scraping_start
    )

    perform_scraping_task = PythonOperator(
        task_id='perform_scraping', 
        python_callable=scraping.main  # Replace with the actual scraping function in `scraping`
    )

    scraping_success_log_task = PythonOperator(
        task_id='log_scraping_success', 
        python_callable=log_scraping_success
    )

    # Tasks for logging the start and completion of ETL
    etl_start_log_task = PythonOperator(
        task_id='log_etl_start', 
        python_callable=log_etl_start
    )

    perform_etl_task = PythonOperator(
        task_id='perform_etl', 
        python_callable=etl.main  # Replace with the actual ETL function in `etl`
    )

    etl_success_log_task = PythonOperator(
        task_id='log_etl_success', 
        python_callable=log_etl_success
    )

    # Define the task execution order
    scraping_start_log_task >> perform_scraping_task >> scraping_success_log_task
    scraping_success_log_task >> etl_start_log_task >> perform_etl_task >> etl_success_log_task
