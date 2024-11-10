import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2
import os
import logging
from pycaret.regression import *
import mlflow
import mlflow.sklearn  # For tracking the final model

# Set MLflow tracking URI to save runs to a specific path
mlflow.set_tracking_uri("file:///D:/gproject/airflow/models/mlflow_runs")

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_data(db_url: str) -> pd.DataFrame:
    """
    Extracts data from a PostgreSQL database using the provided database URL.
    """
    logging.info('Connecting to the database...')
    try:
        # Create a SQLAlchemy engine using the database URL
        engine = create_engine(db_url)
        # SQL query to extract data
        query = """
            SELECT long, lat, city, neighborhood, area, subcategory, facade,
                   bedrooms, bathrooms, furnished, floor,
                   building_age, price
            FROM fact_listing FL
            LEFT JOIN dim_location DL ON DL.location_id = FL.location_id
            LEFT JOIN dim_property DP ON DP.property_id = FL.property_id
            LEFT JOIN dim_property_details DPD ON DPD.details_id = DP.details_id
            WHERE subcategory != 'Lands for Sale'
        """
        # Extract data into a pandas DataFrame
        with engine.connect() as connection:
            data = pd.read_sql_query(query, con=connection)
        logging.info('Data extracted successfully.')
        return data
    except Exception as e:
        logging.error(f"Error extracting data: {e}")
        raise

def build_model(data: pd.DataFrame, target_column: str, save_path: str, log_path: str):
    """
    Sets up the PyCaret environment, trains a regression model, logs metrics, and saves the model.
    """
    logging.basicConfig(level=logging.INFO, handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler()
    ])
    
    logging.info('Setting up PyCaret environment...')
    
    try:
        # Set or create an experiment in MLflow
        experiment_name = 'Data without land'
        if not mlflow.get_experiment_by_name(experiment_name):
            mlflow.create_experiment(experiment_name)
        mlflow.set_experiment(experiment_name)

        # Start MLflow run
        with mlflow.start_run():
            # Initialize PyCaret regression setup
            s = setup(data, target=target_column, normalize=True, log_experiment=True,
                      experiment_name=experiment_name, session_id=123)

            # Train, tune, and finalize the model
            model = compare_models()
            model = tune_model(model)
            model = finalize_model(model)
            
            logging.info(f'Model training completed. Best model: {model}')
            
            # Save the trained model
            save_model_path = os.path.abspath(save_path)
            save_model(model, save_model_path)
            logging.info(f'Model saved at {save_model_path}')

            # Log model and metrics to MLflow
            mlflow.sklearn.log_model(model, artifact_path='model')
            metrics = pull()  # Retrieve model metrics
            mlflow.log_metric("MAE", metrics["MAE"].iloc[0])
            mlflow.log_metric("MSE", metrics["MSE"].iloc[0])
            mlflow.log_metric("RMSE", metrics["RMSE"].iloc[0])
            mlflow.log_metric("R2", metrics["R2"].iloc[0])

        return model

    except Exception as e:
        logging.error(f"Error in building or saving model: {e}")
        mlflow.end_run(status='FAILED')
        raise

def main():
    """
    Main function to extract data, train model, and save/load the model.
    """
    # Define the PostgreSQL database connection URL
    db_url = 'postgresql://postgres:mdkn@localhost:5432/houses'
    
    # Extract data from the database
    data = extract_data(db_url)
    print("Data preview:")
    print(data.head())
    
    # Paths for saving the model and logs
    model_save_path = 'D:/gproject/airflow/models/Prop-model'
    log_save_path = 'D:/gproject/airflow/models/logs/model_training.log'
    
    # Train the model and save it
    model = build_model(data, target_column='price', save_path=model_save_path, log_path=log_save_path)

    # Verify model loading
    try:
        loaded_model = load_model(model_save_path)
        logging.info(f'Loaded model: {loaded_model}')
    except Exception as e:
        logging.error(f"Error loading model: {e}")

# Run the script when executed directly
if __name__ == '__main__':
    main()
