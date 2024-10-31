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
    products_df.drop_duplicates(subset=['id'], inplace=True)


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
    products_df['timestamp'] = pd.to_datetime(products_df['timestamp'])

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

    def make_sql_engine(self):
        engine = f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'
        return engine

class Load:
    def __init__(self, df:pd.DataFrame, table:str, id:str):
        self.df = df
        self.table = table
        self.id = id

    
    def normalize_dim_table(self):
        self.df = self.df.drop_duplicates()
        self.df = self.df.reset_index()
        self.df = self.df.drop(columns=self.df.columns[0], axis=1)

    
    def normalize_fact_table(self, list_of_tables, tables_columns, final_schema, engine):
        self.df = self.df.drop_duplicates()
        for i in range(len(list_of_tables)):
            table_to_merge = pd.read_sql(f'SELECT * FROM {list_of_tables[i]}', con=engine)
            self.df = self.df.merge(table_to_merge,
                         on=tables_columns[i],
                         how='left')
        self.df = self.df[final_schema]
        if self.df.columns[0]=='id':
            print(len(self.df['id']))
            fact_listing_id = pd.read_sql('SELECT id FROM fact_listing;', con=engine)
            for j in self.df['id']:
                if j in list(fact_listing_id['id']):
                    self.df = self.df[self.df['id'] != j]
        self.df.reset_index(inplace=True, drop=True)


    
    def load_table(self, cursor, engine):
        try:
            cursor.execute(f'SELECT * FROM {self.table};')
            result = cursor.fetchall()
            result = [i[1:] for i in result]
            x = []
            for i in range(len(self.df)):
                tuple_to_add = tuple(self.df.loc[i])
                if tuple_to_add in result:
                    continue
                else:
                    x.append(tuple_to_add)
            self.df = pd.DataFrame(x, columns=list(self.df.columns))
            if len(x) > 0:
                self.df.to_sql(name=self.table, con=engine, index=False, if_exists='append')
            else:
                print('No New Data For This Dimension')
        except exc.IntegrityError:
            print('Duplicate Index')
            cursor.execute(f'SELECT MAX({self.id}) FROM {self.table};')
            last_id = cursor.fetchone()
            cursor.execute(f"SELECT nextval(pg_get_serial_sequence('{self.table}', '{self.id}'));")
            next_id = cursor.fetchone()
            # Sometimes the next id will be None idk why so i added this
            if next_id[0] - last_id[0] < 3:
                cursor.execute(f'''
                SELECT 
                    setval(pg_get_serial_sequence('{self.table}', '{self.id}'), 
                    (SELECT MAX({self.id}) FROM {self.table}) + 1);''')
            self.df.to_sql(name=self.table, con=engine, index=False, if_exists='append') # or load_table() again idk
    
def main():
    # Extract
    products = r'.\data\incremental\products.csv'
    products_data = extract_data(products)
    
    # Transform
    cleaned_data = cleanining(products_data)
    
    # Loading & Normalizing Tables
    c = DatabaseConnection(user='postgres', password='anon', host='localhost', port='5432', database='houses')
    cursor, conn = c.connect_to_database()
    engine = c.make_sql_engine()
    
    # Starting with sub dimensional tables and dimention tables with no children
    dim_property_details = cleaned_data[['zoned_for', 'facade', 'property_mortgaged', 'payment_method', 'subcategory', 
                                    'bedrooms', 'bathrooms', 'furnished', 'floor', 'building_age', 'number_of_floors']]
    l = Load(df=dim_property_details, table='dim_property_details', id='details_id')
    l.normalize_dim_table()
    l.load_table(cursor,engine)

    dim_amenities = cleaned_data[['main_amenities', 'additional_amenities']]
    l = Load(df=dim_amenities, table='dim_amenities', id='amenities_id')
    l.normalize_dim_table()
    l.load_table(cursor,engine)

    dim_location = cleaned_data[['google_maps_locatoin_link', 'long', 'lat', 'city', 'neighborhood']]
    l = Load(df=dim_location, table='dim_location', id='location_id')
    l.normalize_dim_table()
    l.load_table(cursor,engine)

    dim_date = pd.DataFrame(cleaned_data['timestamp'])
    dim_date['year'] = dim_date['timestamp'].dt.year
    dim_date['month'] = dim_date['timestamp'].dt.month
    dim_date['day'] = dim_date['timestamp'].dt.day
    dim_date['hour'] = dim_date['timestamp'].dt.hour
    dim_date['minute'] = dim_date['timestamp'].dt.minute
    dim_date['second'] = dim_date['timestamp'].dt.second
    dim_date = dim_date.drop_duplicates()
    dim_date = dim_date[['timestamp', 'year', 'month', 'day', 'hour', 'minute', 'second']]
    l = Load(df=dim_date, table='dim_date', id='date_id')
    l.normalize_dim_table()
    l.load_table(cursor,engine)
    
    # Loading The Fact Table and the Super Dimention Table
    dim_property = cleaned_data[['title', 'link', 'images', 'description', 'area', 'owner', 'owner_link', 'nearby',
                                'zoned_for', 'facade', 'property_mortgaged', 'payment_method', 'subcategory', 'bedrooms', 'bathrooms', 
                                'furnished', 'floor', 'building_age', 'number_of_floors']]
    list_of_tables = ['dim_property_details']
    tables_columns = [list(dim_property_details.columns)]
    final_schema = ['details_id', 'title', 'link', 'images', 'description', 'area', 'owner', 'owner_link', 'nearby']

    l = Load(df=dim_property, id='property_id', table='dim_property')
    l.normalize_fact_table(list_of_tables=list_of_tables, tables_columns=tables_columns, final_schema=final_schema, engine=engine)
    l.load_table(cursor, engine)
    
    dim_property = l.df
    
    fact_listing = cleaned_data
    list_of_tables = ['dim_property', 'dim_location', 'dim_amenities', 'dim_date']
    dim_property = dim_property.drop('details_id', axis=1)

    tables_columns = [list(dim_property.columns), list(dim_location.columns), list(dim_amenities.columns), ['timestamp']]
    final_schema = ['id', 'property_id', 'location_id', 'amenities_id', 'date_id', 'price', 'predicted']

    l = Load(df=fact_listing, id='id', table='fact_listing')
    l.normalize_fact_table(list_of_tables=list_of_tables, tables_columns=tables_columns, final_schema=final_schema, engine=engine)
    
    l.load_table(cursor, engine)
    # Closing The Database Connection
    cursor.close()
    conn.close()
if __name__ == '__main__':
    main()