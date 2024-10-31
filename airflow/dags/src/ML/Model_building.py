import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2
import os
import logging
from pycaret.regression import *

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
                       payment_method, bedrooms, bathrooms, furnished, floor, 
                       building_age, price
                FROM fact_listing FL
                LEFT JOIN dim_location DL ON DL.location_id = FL.location_id
                LEFT JOIN dim_property DP ON DP.property_id = FL.property_id
                LEFT JOIN dim_property_details DPD ON DPD.details_id = DP.details_id
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
    Sets up the PyCaret environment, trains a regression model, and saves it.

    Args:
        data (pd.DataFrame): The input dataset for model training.
        target_column (str): The column in the dataset to predict (i.e., the target variable).
        save_path (str): The file path to save the trained model.

    Returns:
        model: The trained machine learning model.
    """
    logging.info('Setting up PyCaret environment...')
    
    try:
        # Initialize the PyCaret regression setup with the provided dataset and target column
        s = setup(data, target=target_column, session_id=123, verbose=False)
        
        # Compare multiple models and select the best one
        model = compare_models()
        
        logging.info(f'Model training completed. Best model: {model}')
        
        # Generate the absolute file path for saving the model
        save_path = os.path.abspath(save_path)
        
        # Save the trained model to the specified path
        save_model(model, save_path)
        
        logging.info(f'Model saved at {save_path}')
        return model

    except Exception as e:
        # Log any errors that occur during model building or saving
        logging.error(f"Error in building or saving model: {e}")
        raise

def main():
    """
    The main function that orchestrates the data extraction, model training, 
    and saving/loading of the machine learning model.
    """
    # Define the PostgreSQL database connection URL (can be made configurable)
    user = 'postgres'
    password = 'anon'
    host = 'localhost'
    port = '5432'
    database = 'houses'
    db_url = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    #db_url = 'postgresql://postgres:mdkn@localhost:5432/houses'
    
    # Extract data from the database
    data = extract_data(db_url)
    
    # Print the first few rows of the data
    print("Data preview:")
    print(data.head())
    
    # Define the file path for saving the trained model
    model_save_path = 'my-model'
    
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
