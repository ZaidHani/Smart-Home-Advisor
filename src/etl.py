import pandas as pd
import os
import glob
import numpy as np
from sqlalchemy import create_engine

def extract_data(data_dir:str) -> pd.DataFrame:
    """
    Extracts and combines all CSV files in the specified directory into a single DataFrame.

    This function searches for all files with a .csv extension within the given directory, reads each 
    file into a pandas DataFrame, and then concatenates all DataFrames into a single DataFrame.

    Parameters:
    data_dir (str): The directory path where the CSV files are located.

    Returns:
    pd.DataFrame: A DataFrame containing the combined data from all CSV files in the specified directory.

    Example:
    >>> combined_df = extract_data('/path/to/csv/files')
    >>> display(combined_df.head())
    """
    all_files = glob.glob(os.path.join(data_dir , "*.csv"))
    li = []
    for filename in all_files:
        df = pd.read_csv(filename)
        li.append(df)
    frame = pd.concat(li, axis=0, ignore_index=True)
    return frame


def transform_data(extracted_data:pd.DataFrame):
    # transform csv files
    pass
def load_data(transformed_data) -> str:
    # building a star schema and loading csv files.
    return "Data Was Loaded Succesfully!"

def main():
    data_directory = r'.\data\products'
    # E
    extracted_data = extract_data(data_directory)
    # T
    transformed_data = transform_data(extracted_data)
    # L
    load_data(transformed_data)

if __name__ == '_main__':
    main()