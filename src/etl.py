import pandas as pd
import os
import time
import glob
import numpy as np
from sqlalchemy import create_engine

import time

def func_time(func):
    """
    Decorator that measures and prints the execution time of the given function.

    Args:
        func (callable): The function to be wrapped by the decorator.

    Returns:
        callable: A wrapper function that measures the execution time of the original function.
        
    The wrapper function:
        - Records the start time before calling the original function.
        - Calls the original function and stores its result.
        - Records the end time after the function call.
        - Prints the time taken to execute the original function.
        - Returns the result of the original function.
    
    Example:
        >>> @func_time
        ... def example_function():
        ...     time.sleep(2)
        ...
        >>> example_function()
        Time taken for example_function: 2.0001 seconds

    """
    def wrapper(*args, **kwargs):
        start = time.time()  # Start time
        result = func(*args, **kwargs)  # Call the original function
        end = time.time()  # End time
        print(f"Time taken for {func.__name__}: {end - start:.4f} seconds")
        return result
    return wrapper

@func_time
def extract_data(products_directory:str, sellers_csv_file_path:str) -> tuple:
    """
    Extracts data from all CSV files in the product directory and a sellers CSV file,
    and returns them as two separate pandas DataFrames.

    Parameters:
    ----------
    products_directory : str
        Path to the directory containing the product CSV files.

    sellers_csv_file_path : str
        Path to the sellers CSV file in the data directory.

    Returns:
    -------
    tuple
        A tuple containing two pandas DataFrames:
        - combined_product_df: Combined DataFrame of all CSV files in the product directory.
        - sellers_df: DataFrame of the sellers CSV file.
    """
    all_files = glob.glob(os.path.join(products_directory , "*.csv"))
    li = []
    for filename in all_files:
        df = pd.read_csv(filename)
        li.append(df)
    products = pd.concat(li, axis=0, ignore_index=True)
    sellers = pd.read_csv(sellers_csv_file_path)
    return products, sellers

@func_time
def transform_data(products_df:pd.DataFrame, sellers_df:pd.DataFrame):
    df = products_df.merge(sellers_df, on='owner_link', how='left')
    return 'Hello'

@func_time
def load_data(transformed_data) -> str:
    # building a star schema and loading csv files.
    return "Data Was Loaded Succesfully!"

def main():
    # Extract
    products = r'.\data\products'
    sellers = r'.\data\sellers.csv'
    products_data, sellers_data = extract_data(products, sellers)
    # Transform
    transformed_data = transform_data(products_data, sellers_data)
    # Load
    load_data(transformed_data)
    
    
if __name__ == '_main__':
    main()