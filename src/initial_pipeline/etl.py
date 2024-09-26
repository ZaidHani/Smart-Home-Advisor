import pandas as pd
import os
import time
import glob
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import datetime
from sklearn.preprocessing import LabelEncoder


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
def extract_data(products_directory:str) -> pd.DataFrame:
    """
    Extracts data from all CSV files in the product directory and a sellers CSV file,
    and returns them as two separate pandas DataFrames.

    Parameters:
    ----------
    products_directory : str
        Path to the directory containing the product CSV files.

    Returns:
    -------
    pd.DataFrame
        - combined_product_df: Combined DataFrame of all CSV files in the product directory.
    """
    all_files = glob.glob(os.path.join(products_directory , "*.csv"))
    li = []
    for filename in all_files:
        df = pd.read_csv(filename)
        li.append(df)
    products = pd.concat(li, axis=0, ignore_index=True)
    return products

@func_time
def cleanining(products_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the given dataframe by performing the following operations:
    
    1. Removes columns that contain only null values.
    2. Replaces any remaining null values with appropriate values 
       (such as default placeholders or mean/median for numerical data).

    Parameters:
    ----------
    products_df : pd.DataFrame
        A pandas DataFrame containing product data that needs to be cleaned.

    Returns:
    -------
    pd.DataFrame
        A cleaned products DataFrame ready to be normalized.
    """
    ####################################################################
    products_df.drop_duplicates('id', inplace = True)

    products_df.dropna(subset='id', inplace = True)
    products_df['id'] = products_df['id'].astype('int64').astype('str')

    products_df['title'].fillna('There is no title', inplace = True)

    products_df = products_df[(products_df['Country'] == 'Jordan') | (products_df['Country'].isnull() == True)]

    products_df['images'].fillna('No image', inplace= True)

    products_df['description'].fillna('There is no description', inplace = True)

    products_df.dropna(subset = ['owner'], inplace = True)

    products_df['reviews'].fillna(0, inplace = True)

    products_df['google_maps_locatoin_link'].fillna('Unavailable Link', inplace = True)

    avg_coords = products_df.groupby(['City', 'Neighborhood'])[['long', 'lat']].mean().reset_index()
    # Merge the average coordinates back to the original DataFrame
    products_df = pd.merge(products_df, avg_coords, on=['City', 'Neighborhood'], suffixes=('', '_avg'), how='left')
    # Impute missing longitude and latitude with the computed average
    products_df['long'].fillna(products_df['long_avg'], inplace=True)
    products_df['lat'].fillna(products_df['lat_avg'], inplace=True)
    # Drop the temporary columns
    products_df.drop(columns=['long_avg', 'lat_avg'], inplace=True)
    products_df.dropna(subset=['long','lat'], inplace=True)

    # products_df.drop(['Real Estate Type', 'Country', 'Reference ID', 'Category', 'Property Status', 'Lister Type', 'Additional Amenities'], axis=1, inplace=True)

    products_df.drop(['Real Estate Type', 'Country', 'Reference ID', 'Category', 'Property Status', 'Lister Type'], axis=1, inplace=True, errors='ignore')

    products_df.dropna(subset = 'price', inplace = True) 
    products_df['price'] = products_df['price'].str.replace('\D+', '', regex=True)
    products_df['price'] = products_df['price'].astype('int')


    # products_df['predicted'] = False

    products_df['area'] = products_df['Land Area'].fillna(products_df['Surface Area'])
    products_df['area'] = products_df['area'].str.replace('\D+', '', regex=True)
    products_df.dropna(subset=['area'], inplace=True)
    products_df.drop(['Land Area', 'Surface Area'], axis=1, inplace=True)
    products_df['area'] = products_df['area'].astype('int')
    products_df = products_df[(products_df['area'] >= 75) & (products_df['area'] <= 1000000)] ###

    products_df['Facade'].fillna('Not mentioned', inplace=True)
    products_df['Property Mortgaged?'].fillna('Not mentioned', inplace=True)
    products_df['Payment Method'].fillna('Not mentioned', inplace=True)
    products_df['Nearby'].fillna('Not mentioned', inplace=True)
    products_df['Main Amenities'].fillna('Not mentioned', inplace=True)
    # products_df['Additional Amenities'].fillna('Not mentioned', inplace=True)

    products_df['Zoned for'].fillna('Not a land', inplace=True)

    columns_to_fill = ['Bedrooms', 'Bathrooms', 'Furnished?', 'Floor', 'Building Age', 'Number of Floors']
    for col in columns_to_fill:
        products_df[col] = np.where(products_df[col].isna() & (products_df['Zoned for'] == 'Not a land'), 'Not mentioned', products_df[col])

    products_df['Bedrooms'].fillna('Not a building', inplace=True)
    products_df['Bathrooms'].fillna('Not a building', inplace=True)
    products_df['Furnished?'].fillna('Not a building', inplace=True)
    products_df['Floor'].fillna('Not a building', inplace=True)
    products_df['Building Age'].fillna('Not a building', inplace=True)
    products_df['Number of Floors'].fillna('Not a building', inplace=True)

    products_df['Facade'].fillna('Not mentioned', inplace=True)

    df_without_land = products_df[products_df['Subcategory'] != 'Lands for Sale']
    df_without_land.reset_index(drop = True, inplace = True)

    land_df = products_df[products_df['Subcategory'] == 'Lands for Sale']
    land_df.reset_index(drop = True, inplace = True)


    df_without_land = df_without_land[(df_without_land['price'] >= 10000) & (df_without_land['price'] <= 1.750000e+05)]

    land_df = land_df[(land_df['price']>=5000) & (land_df['price']<=1000000)]

    products_df = pd.concat([df_without_land, land_df])


    products_df.rename(str.lower, axis='columns', inplace=True)
    products_df.columns = products_df.columns.str.replace(' ', '_')
    products_df.columns = products_df.columns.str.replace('?', '')

    ####################################################################


    # products_df.drop(['Real Estate Type', 'Country', 'Reference ID', 'Category', 'Property Status', 'Lister Type'], axis=1, inplace=True)
    # products_df.dropna(subset=['id', 'owner'], inplace=True)
    # products_df.drop_duplicates(inplace=True)

    # products_df['images'] = products_df['images'].fillna('No Images')

    # # Dealing With Coordinates
    # # Group by city and neighbourhood, and compute the average longitude and latitude
    # avg_coords = products_df.groupby(['City', 'Neighborhood'])[['long', 'lat']].mean().reset_index()
    # # Merge the average coordinates back to the original DataFrame
    # products_df = pd.merge(products_df, avg_coords, on=['City', 'Neighborhood'], suffixes=('', '_avg'), how='left')
    # # Impute missing longitude and latitude with the computed average
    # products_df['long'].fillna(products_df['long_avg'], inplace=True)
    # products_df['lat'].fillna(products_df['lat_avg'], inplace=True)
    # # Drop the temporary columns
    # products_df.drop(columns=['long_avg', 'lat_avg'], inplace=True)
    # products_df.dropna(subset=['long','lat'], inplace=True)

    # products_df['google_maps_locatoin_link'] = products_df['google_maps_locatoin_link'].fillna('Unavailable Link')


    # products_df.dropna(subset = 'price', inplace = True) 
    # products_df['price'] = products_df['price'].str.replace('\D+', '', regex=True)
    # products_df['price'] = products_df['price'].astype('int')

    ###df_without_land = products_df[products_df['Subcategory'] != 'Lands for Sale']  #########################
    ###df_without_land.reset_index(drop = True, inplace = True) #####################


    # products_df = products_df[products_df['price'] >= 10000] ##########
    # products_df = products_df[products_df['price'] <= 1.750000e+05 ] ##########################

    
    # products_df['predicted'] = False

    # products_df['area'] = products_df['Land Area'].fillna(products_df['Surface Area'])
    # products_df['area'] = products_df['area'].str.replace('\D+', '', regex=True)
    # products_df.dropna(subset=['area'], inplace=True)
    # products_df.drop(['Land Area', 'Surface Area'], axis=1, inplace=True)
    # products_df['area'] = products_df['area'].astype('int')

    # products_df['Zoned for'].fillna('Not a land', inplace=True)
    # products_df['Facade'].fillna('Not mentioned', inplace=True)
    # products_df['Property Mortgaged?'].fillna('Not mentioned', inplace=True)
    # products_df['Payment Method'].fillna('Not mentioned', inplace=True)
    # products_df['Nearby'].fillna('Not mentioned', inplace=True)
    # products_df['Main Amenities'].fillna('Not mentioned', inplace=True)
    ###################################products_df['Additional Amenities'].fillna('Not mentioned', inplace=True)

    # columns_to_fill = ['Bedrooms', 'Bathrooms', 'Furnished?', 'Floor', 'Building Age', 'Number of Floors']
    # for col in columns_to_fill:
    #     products_df[col] = np.where(products_df[col].isna() & (products_df['Zoned for'] == 'Not a land'), 'Not mentioned', products_df[col])

    # products_df['Bedrooms'].fillna('Not a building', inplace=True)
    # products_df['Bathrooms'].fillna('Not a building', inplace=True)
    # products_df['Furnished?'].fillna('Not a building', inplace=True)
    # products_df['Floor'].fillna('Not a building', inplace=True)
    # products_df['Building Age'].fillna('Not a building', inplace=True)
    # products_df['Number of Floors'].fillna('Not a building', inplace=True)
    # rewrite the line above, make it so when there is a 'Floor', the 'Number of Floors' is one floor, 
    # if you don't understand write this code in a notebook and just look at it
    # dim_property_details[['Floor', 'Number of Floors']].drop_duplicates()
    # and also edit it so when there is more than one floor in 'Number of Floors' the column 'Floor' has the number of floors
    
    # Renaming Columns
    # products_df.rename(str.lower, axis='columns', inplace=True)
    # products_df.columns = products_df.columns.str.replace(' ', '_')
    # products_df.columns = products_df.columns.str.replace('?', '')
    

    return products_df

@func_time
def normalization(products_df:pd.DataFrame) -> tuple:
    """
    Normalizes the given dataframes to prepare them for efficient storage in a data warehouse.
    
    The function restructures the data into smaller, related tables that follow database 
    normalization principles. This helps reduce redundancy and ensures optimized querying 
    in the data warehouse. The data is split into fact and dimension tables, where appropriate.

    Parameters:
    ----------
    products_df : pd.DataFrame
        A pandas DataFrame containing product data to be normalized.
        
    sellers_df : pd.DataFrame
        A pandas DataFrame containing seller data to be normalized.

    Returns:
    -------
    tuple of pd.DataFrame
        A tuple containing the normalized tables, ready for loading into the data warehouse.
        For example:
        (dim_property_details, dim_property, dim_amenities, dim_location, dim_date, fact_listing)."""
    # Sub Dimensional Table
    dim_property_details = products_df[['zoned_for', 'facade', 'property_mortgaged', 'payment_method', 'subcategory', 'bedrooms', 'bathrooms', 
                                'furnished', 'floor', 'building_age', 'number_of_floors']]
    dim_property_details = dim_property_details.drop_duplicates()
    dim_property_details['details_id'] = range(1, len(dim_property_details)+1)
    dim_property_details = dim_property_details[['details_id', 'zoned_for', 'facade', 'property_mortgaged', 'payment_method', 'subcategory', 'bedrooms', 
                                             'bathrooms', 'furnished', 'floor', 'building_age', 'number_of_floors']]
    # Dimension Tables
    dim_property = products_df[['title', 'link', 'images', 'description', 'area', 'owner', 'owner_link', 'nearby',
                            'zoned_for', 'facade', 'property_mortgaged', 'payment_method', 'subcategory', 'bedrooms', 'bathrooms', 
                            'furnished', 'floor', 'building_age', 'number_of_floors']]
    dim_property = dim_property.drop_duplicates()
    dim_property['property_id'] = range(1, len(dim_property)+1)
    dim_property = dim_property.merge(dim_property_details, 
                                    on=['zoned_for', 'facade', 'property_mortgaged', 'payment_method', 'subcategory', 'bedrooms', 'bathrooms', 
                                        'furnished', 'floor', 'building_age', 'number_of_floors'],
                                    how='left')
    dim_property = dim_property[['property_id', 'details_id', 'title', 'link', 'images', 'description', 'area', 'owner', 'owner_link', 'nearby']]
    
    dim_amenities = products_df[['main_amenities', 'additional_amenities']]
    dim_amenities = dim_amenities.drop_duplicates()
    dim_amenities['amenities_id'] = range(1, len(dim_amenities)+1)
    dim_amenities = dim_amenities[['amenities_id', 'main_amenities', 'additional_amenities']]
    
    dim_location = products_df[['google_maps_locatoin_link', 'long', 'lat', 'city', 'neighborhood']]
    dim_location = dim_location.drop_duplicates()
    dim_location['location_id'] = range(1, len(dim_location)+1)
    dim_location = dim_location[['location_id', 'google_maps_locatoin_link', 'long', 'lat', 'city', 'neighborhood']]
    
    # delete that it is only for testing
    products_df['timestamp'] = datetime.datetime.now()

    dim_date = pd.DataFrame(products_df['timestamp'])
    dim_date['year'] = dim_date['timestamp'].dt.year
    dim_date['month'] = dim_date['timestamp'].dt.month
    dim_date['day'] = dim_date['timestamp'].dt.day
    dim_date['hour'] = dim_date['timestamp'].dt.hour
    dim_date['minute'] = dim_date['timestamp'].dt.minute
    dim_date['second'] = dim_date['timestamp'].dt.second
    dim_date = dim_date.drop_duplicates()
    dim_date['date_id'] = range(1, len(dim_date)+1)
    dim_date = dim_date[['date_id', 'timestamp', 'year', 'month', 'day', 'hour', 'minute', 'second']]
    
    # Fact Table
    fact_listing = products_df.merge(dim_property, 
                                 on=['title', 'link', 'images', 'description', 'area', 'owner', 'owner_link', 'nearby'],
                                 how='left')
    fact_listing = fact_listing.merge(dim_amenities, 
                                    on=['main_amenities', 'additional_amenities'],
                                    how='left')
    fact_listing = fact_listing.merge(dim_location, 
                                    on=['google_maps_locatoin_link', 'long', 'lat', 'city', 'neighborhood'],
                                    how='left')
    fact_listing = fact_listing.merge(dim_date, 
                                    on=['timestamp'],
                                    how='left')
    fact_listing = fact_listing[['id', 'property_id', 'location_id', 'amenities_id', 'date_id', 'price']]

    fact_listing.drop_duplicates('id', inplace=True)
    
    return dim_property_details, dim_property, dim_amenities, dim_location, dim_date, fact_listing
    

@func_time
def load_data(dim_property_details, dim_property, dim_amenities, dim_location, dim_date, fact_listing) -> str:
    """
    Loads transformed and normalized data into a PostgreSQL data warehouse.

    This function performs the following steps:
    1. Creates a star schema consisting of fact and dimension tables from the provided data.
    2. Loads the data from CSV files into the corresponding tables in the PostgreSQL data warehouse.

    Parameters:
    - dim_property_details: Data for the property details dimension table.
    - dim_property: Data for the property dimension table.
    - dim_amenities: Data for the amenities dimension table.
    - dim_location: Data for the location dimension table.
    - dim_date: Data for the date dimension table.
    - fact_listing: Data for the fact table, containing the core business metrics.

    Returns:
    - str: A message indicating the status of the data loading process.
    """
    user = 'postgres'
    password = 'mdkn'
    host = 'localhost'
    port = '5432'
    database = 'houses'
    connection_str = f'postgresql://{user}:{password}@{host}:{port}/{database}'

    # SQLAlchemy engine
    engine = create_engine(connection_str)
    
    Session = sessionmaker(bind=engine)
    session = Session()

    session.execute(text('TRUNCATE TABLE dim_property_details CASCADE'))
    session.execute(text('TRUNCATE TABLE dim_property CASCADE'))
    session.execute(text('TRUNCATE TABLE dim_amenities CASCADE'))
    session.execute(text('TRUNCATE TABLE dim_location CASCADE'))
    session.execute(text('TRUNCATE TABLE dim_date CASCADE'))
    session.execute(text('TRUNCATE TABLE fact_listing CASCADE'))

    session.commit()
    session.close()
    
    dim_property_details.to_sql('dim_property_details', con=engine, if_exists='append', index=False)
    dim_property.to_sql('dim_property', con=engine, if_exists='append', index=False)
    dim_amenities.to_sql('dim_amenities', con=engine, if_exists='append', index=False)
    dim_location.to_sql('dim_location', con=engine, if_exists='append', index=False)
    dim_date.to_sql('dim_date', con=engine, if_exists='append', index=False)
    fact_listing.to_sql('fact_listing', con=engine, if_exists='append', index=False)
    
    return "Data Was Loaded Successfully!"

def main():
    # Extract
    products = 'data/initial/products'
    products_df = extract_data(products)
    # Transform
    products_df = cleanining(products_df)
    dim_property_details, dim_property, dim_amenities, dim_location, dim_date, fact_listing = normalization(products_df)
    # Load
    load_data(dim_property_details, dim_property, dim_amenities, dim_location, dim_date, fact_listing)
    
if __name__ == '__main__':
    main()