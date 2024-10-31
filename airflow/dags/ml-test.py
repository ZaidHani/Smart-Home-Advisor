from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from ml_pipeline.prop_model import extract_data, build_model


# Define the default arguments for the DAG
default_args = {
    'owner': 'Al-Jermy',  # Replace with your name
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 10, 25),  # Adjust to your desired start date
}

# Initialize the DAG
with DAG(dag_id='ml_pipeline',
         default_args=default_args,
         description='Weekly machine learning pipeline for OpenSooq house prices',
         schedule_interval='@daily',  # Runs every week
         catchup=False) as dag:

    # Task 1: Extract data from the PostgreSQL database
    def extract_data_task():
        db_url = 'postgresql://postgres:anon@localhost:5432/houses'  # Replace with your actual DB URL
        return extract_data(db_url)

    extract_data_operator = PythonOperator(
        task_id='extract_data_task',
        python_callable=extract_data_task,
    )

    # Task 2: Train and save the machine learning model
    def build_model_task(**context):
        data = context['task_instance'].xcom_pull(task_ids='extract_data_task')
        model_save_path = r'C:\Users\MANAL\GP2\src\ML\saved_model'  # Adjust the path if necessary
        build_model(data, target_column='price', save_path=model_save_path)

    build_model_operator = PythonOperator(
        task_id='build_model_task',
        python_callable=build_model_task,
        provide_context=True,
    )

    # Define task dependencies
    extract_data_operator >> build_model_operator