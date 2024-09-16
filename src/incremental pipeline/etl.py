import pandas as pd
import numpy as np
import datetime
from sqlalchemy import exc, create_engine
import psycopg2


def extract_data(products_path:str) -> pd.DataFrame:
    """
    Reads a CSV file from the specified file path and returns it as a pandas DataFrame.

    Args:
        products_path (str): The file path to the CSV containing product data.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the data from the CSV file.
    """
    return pd.read_csv(f'{products_path}')


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
    products_df.drop(['Real Estate Type', 'Country', 'Reference ID', 'Category', 'Property Status', 'Lister Type'], 
                     axis=1, inplace=True,errors='ignore')
    products_df.dropna(subset=['id', 'owner'], inplace=True)
    products_df.drop_duplicates(inplace=True)

    products_df['images'] = products_df['images'].fillna('No Images')

    # Dealing With Coordinates
    # Group by city and neighbourhood, and compute the average longitude and latitude
    avg_coords = products_df.groupby(['City', 'Neighborhood'])[['long', 'lat']].mean().reset_index()
    # Merge the average coordinates back to the original DataFrame
    products_df = pd.merge(products_df, avg_coords, on=['City', 'Neighborhood'], suffixes=('', '_avg'), how='left')
    # Impute missing longitude and latitude with the computed average
    products_df['long'].fillna(products_df['long_avg'], inplace=True)
    products_df['lat'].fillna(products_df['lat_avg'], inplace=True)
    # Drop the temporary columns
    products_df.drop(columns=['long_avg', 'lat_avg'], inplace=True)
    products_df.dropna(subset=['long','lat'], inplace=True)

    products_df['google_maps_locatoin_link'] = products_df['google_maps_locatoin_link'].fillna('Unavailable Link')

    products_df['price'] = products_df['price'].str.replace('\D+', '', regex=True)
    products_df['price'].fillna(0,inplace=True)
    products_df['price'] = products_df['price'].astype('float')
    
    products_df['predicted'] = False

    products_df['area'] = products_df['Land Area'].fillna(products_df['Surface Area'])
    products_df['area'] = products_df['area'].str.replace('\D+', '', regex=True)
    products_df.dropna(subset=['area'], inplace=True)
    products_df.drop(['Land Area', 'Surface Area'], axis=1, inplace=True)
    products_df['area'] = products_df['area'].astype('int')

    products_df['Zoned for'].fillna('Not a land', inplace=True)
    products_df['Facade'].fillna('Unknown', inplace=True)
    products_df['Property Mortgaged?'].fillna('Unknown', inplace=True)
    products_df['Payment Method'].fillna('Unknown', inplace=True)
    products_df['Nearby'].fillna('Unknown', inplace=True)
    products_df['Main Amenities'].fillna('Unknown', inplace=True)
    products_df['Additional Amenities'].fillna('Unknown', inplace=True)

    columns_to_fill = ['Bedrooms', 'Bathrooms', 'Furnished?', 'Floor', 'Building Age', 'Number of Floors']
    for col in columns_to_fill:
        products_df[col] = np.where(products_df[col].isna() & (products_df['Zoned for'] == 'Not a land'), 'Unknown', products_df[col])

    products_df['Bedrooms'].fillna('Not a building', inplace=True)
    products_df['Bathrooms'].fillna('Not a building', inplace=True)
    products_df['Furnished?'].fillna('Not a building', inplace=True)
    products_df['Floor'].fillna('Not a building', inplace=True)
    products_df['Building Age'].fillna('Not a building', inplace=True)
    products_df['Number of Floors'].fillna('Not a building', inplace=True) #try to fill the unkonw with appropriate value later when you have time, there are also full floors which is replacable by 1 floor
    # rewrite the line above, make it so when there is a 'Floor', the 'Number of Floors' is one floor, 
    # if you don't understand write this code in a notebook and just look at it
    # dim_property_details[['Floor', 'Number of Floors']].drop_duplicates()
    # and also edit it so when there is more than one floor in 'Number of Floors' the column 'Floor' has the number of floors
    
    # Renaming Columns
    products_df.rename(str.lower, axis='columns', inplace=True)
    products_df.columns = products_df.columns.str.replace(' ', '_')
    products_df.columns = products_df.columns.str.replace('?', '')

    return products_df


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
    dim_property_details = dim_property_details[['details_id', 'zoned_for', 'facade', 'property_mortgaged', 'payment_method', 'subcategory', 'bedrooms', 
                                             'bathrooms', 'furnished', 'floor', 'building_age', 'number_of_floors']]
    # Dimension Tables
    dim_property = products_df[['title', 'link', 'images', 'description', 'area', 'owner', 'owner_link', 'nearby',
                            'zoned_for', 'facade', 'property_mortgaged', 'payment_method', 'subcategory', 'bedrooms', 'bathrooms', 
                            'furnished', 'floor', 'building_age', 'number_of_floors']]
    dim_property = dim_property.drop_duplicates()
    dim_property = dim_property.merge(dim_property_details, 
                                    on=['zoned_for', 'facade', 'property_mortgaged', 'payment_method', 'subcategory', 'bedrooms', 'bathrooms', 
                                        'furnished', 'floor', 'building_age', 'number_of_floors'],
                                    how='left')
    dim_property = dim_property[['property_id', 'details_id', 'title', 'link', 'images', 'description', 'area', 'owner', 'owner_link', 'nearby']]
    
    dim_amenities = products_df[['main_amenities', 'additional_amenities']]
    dim_amenities = dim_amenities.drop_duplicates()
    dim_amenities = dim_amenities[['amenities_id', 'main_amenities', 'additional_amenities']]
    
    dim_location = products_df[['google_maps_locatoin_link', 'long', 'lat', 'city', 'neighborhood']]
    dim_location = dim_location.drop_duplicates()
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
    fact_listing = fact_listing[['id', 'property_id', 'location_id', 'amenities_id', 'date_id', 'price', 'predicted']]
    
    return dim_property_details, dim_property, dim_amenities, dim_location, dim_date, fact_listing


class DatabaseConnection:
    def __init__(self, user, password, host, port, database):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database

    def connect_to_database(self):
        conn = psycopg2.connect(
            database=self.database, 
            user=self.user, 
            password=self.password, 
            host=self.host, 
            port=self.port)
        cursor = conn.cursor()
        return cursor, conn

    def close_database(self, cursor, conn):
        cursor.close()
        conn.close()

    def make_sql_engine(self):
        engine = f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'
        return engine

def load_dimention_table(dataframe:pd.DataFrame, table:str, table_primary_key:str, df_columns:list[str]):
    connection = DatabaseConnection(user='postgres', password='anon', host='localhost', port='5432', database='houses')
    cursor, conn = connection.connect_to_database()
    engine = connection.make_sql_engine()
    try:
        cursor.execute(f'SELECT * FROM {table};')
        result = cursor.fetchall()
        result = [element[1:] for element in result]
        x = []
        for i in range(len(dataframe)):
            tuple_to_add = tuple(dataframe.loc[i])
            if tuple_to_add in result:
                continue
            else:
                x.append(tuple_to_add)
        dataframe = pd.DataFrame(x, columns=df_columns)
        if len(x) > 0:
            dataframe.to_sql(name=table, con=engine, index=False, if_exists='append')
        else:
            print('No New Data For This Dimension')
    except exc.IntegrityError:
        print('Duplicate Index')
        cursor.execute(f'SELECT MAX({table_primary_key}) FROM {table};')
        last_id = cursor.fetchone()
        cursor.execute(f'SELECT nextval(pg_get_serial_sequence({table}, {table_primary_key}));')
        next_id = cursor.fetchone()
        if next_id[0] - last_id[0] < 3:
            cursor.execute(f'''
            SELECT 
                setval(pg_get_serial_sequence({table}, {table_primary_key}), 
                (SELECT MAX({table_primary_key}) FROM {table}) + 1);''')
        load_dimention_table(dataframe)
    connection.close_database(cursor, conn)
    
def load_fact_table(fact_table):
    connection = DatabaseConnection()
    engine = connection.make_sql_engine()
    fact_table.to_sql('fact_listing', con=engine, if_exists='append', index=False)

    
    
def main():
    # Extract
    products = r'.\data\incremental\products.csv'
    products_data = extract_data(products)
    # Transform
    cleaned_data = cleanining(products_data)
    dim_property_details, dim_property, dim_amenities, dim_location, dim_date, fact_listing = normalization(cleaned_data)
    # Loading Tables
    # Load The Sub Dimensional Table 'dim_property_details'
    dim_property_details_columns = ['zoned_for', 'facade', 'property_mortgaged', 'payment_method', 'subcategory', 
                                    'bedrooms', 'bathrooms', 'furnished', 'floor', 'building_age', 'number_of_floors']
    load_dimention_table(
        data_frame=dim_property_details, 
        table='dim_property_details', 
        table_primary_key='details_id', 
        df_columns=dim_property_details_columns)
    
    # Loadint All The Dimension Tables
    dim_property_columns = ['details_id', 'title', 'link', 'images', 'description', 'area', 'owner', 'owner_link', 'nearby']
    load_dimention_table(
        data_frame=dim_property, 
        table='dim_property', 
        table_primary_key='property_id', 
        df_columns=dim_property_columns)
    
    dim_amenities_columns = ['main_amenities', 'additional_amenities']
    load_dimention_table(
        data_frame=dim_amenities, 
        table='dim_amenities', 
        table_primary_key='amenities_id', 
        df_columns=dim_amenities_columns)

    dim_location_columns = ['google_maps_locatoin_link', 'long', 'lat', 'city', 'neighborhood']
    load_dimention_table(
        data_frame=dim_location, 
        table='dim_location', 
        table_primary_key='location_id', 
        df_columns=dim_location_columns)

    dim_date_columns = ['timestamp', 'year', 'month', 'day', 'hour', 'minute', 'second']
    load_dimention_table(
        data_frame=dim_date, 
        table='dim_date', 
        table_primary_key='date_id', 
        df_columns=dim_date_columns)
    
    # Load the Fact Table
    load_fact_table(fact_listing)
    
if __name__ == '__main__':
    main()