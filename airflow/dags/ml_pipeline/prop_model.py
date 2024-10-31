import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2
import os
import logging
from pycaret.regression import *
import mlflow
import mlflow.sklearn  # For tracking the final model

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_data(db_url: str) -> pd.DataFrame:
    """
    Extracts data from a PostgreSQL database using the provided database URL.

    Args:
        db_url (str): The connection string for the PostgreSQL database.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the extracted data.
    """
    logging.info('Connecting to the database...')
    
    try:
        # Create a SQLAlchemy engine using the database URL
        engine = create_engine(db_url)
        
        # Open a connection to the database and execute the SQL query
        with engine.connect() as connection:
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
            logging.info('Executing query...')
            
            # Read the query result into a pandas DataFrame
            data = pd.read_sql_query(query, con=connection)
        
        logging.info('Data extracted successfully.')
        return data

    except Exception as e:
        # Log any errors that occur during the data extraction process
        logging.error(f"Error extracting data: {e}")
        raise

def build_model(data: pd.DataFrame, target_column: str, save_path: str):
    """
    Sets up the PyCaret environment, trains a regression model, logs metrics, and saves the model.

    Args:
        data (pd.DataFrame): The input dataset for model training.
        target_column (str): The column in the dataset to predict (i.e., the target variable).
        save_path (str): The file path to save the trained model.

    Returns:
        model: The trained machine learning model.
    """
    logging.info('Setting up PyCaret environment...')
    
    try:
        
        # Initialize MLflow tracking
        mlflow.start_run()

        # Initialize the PyCaret regression setup with the provided dataset and target column
        s = setup(data, target=target_column, normalize=True, log_experiment=True, experiment_name='Data without land',
                  session_id=123)

        # Compare multiple models and select the best one
        model = compare_models()

        # Tune the best model
        model = tune_model(model)

        # Finalize the tuned model
        model = finalize_model(model)
        
        logging.info(f'Model training completed. Best model: {model}')
        
        # Generate the absolute file path for saving the model
        save_path = os.path.abspath(save_path)
        
        # Save the trained model to the specified path
        save_model(model, save_path)
        
        logging.info(f'Model saved at {save_path}')

        # Log the model to MLflow using sklearn since PyCaret's model is compatible
        mlflow.sklearn.log_model(model, artifact_path='model')

        # Retrieve and log performance metrics
        metrics = pull()  # Pull the metrics from the latest model training/tuning

        # Log specific metrics to MLflow
        mlflow.log_metric("MAE", metrics["MAE"].iloc[0])
        mlflow.log_metric("MSE", metrics["MSE"].iloc[0])
        mlflow.log_metric("RMSE", metrics["RMSE"].iloc[0])
        mlflow.log_metric("R2", metrics["R2"].iloc[0])
        
        mlflow.end_run()

        return model

    except Exception as e:
        # Log any errors that occur during model building or saving
        logging.error(f"Error in building or saving model: {e}")
        mlflow.end_run(status='FAILED')
        raise

def main():
    """
    The main function that orchestrates the data extraction, model training, 
    and saving/loading of the machine learning model.
    """
    # Define the PostgreSQL database connection URL (can be made configurable)
    db_url = 'postgresql://postgres:anon@localhost:5432/houses'
    
    # Extract data from the database
    data = extract_data(db_url)
    
    # Print the first few rows of the data
    print("Data preview:")
    print(data.head())
    
    # Define the file path for saving the trained model
    model_save_path = r'C:\Users\MANAL\GP2\src\ML\saved_model'
    
    # Build the regression model using the extracted data and save it
    model = build_model(data, target_column='price', save_path=model_save_path)

    # Load and display the saved model for verification
    try:
        loaded_model = load_model(model_save_path)
        logging.info(f'Loaded model: {loaded_model}')
    
    except Exception as e:
        # Log any errors that occur during model loading
        logging.error(f"Error loading model: {e}")

# Run the script when executed directly
if __name__ == '__main__':
    main()