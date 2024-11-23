import psycopg2

# Setting Up the Database:
conn = psycopg2.connect("user='postgres' host='localhost' password='anon' port='5432'")
cursor = conn.cursor()
conn.autocommit = True

cursor.execute('DROP DATABASE IF EXISTS houses;')
cursor.execute('CREATE DATABASE houses;')

cursor.close()
conn.close()

# Creating the Tables
conn = psycopg2.connect("dbname='houses' user='postgres' host='localhost' password='anon' port='5432'")
cursor = conn.cursor()

cursor.execute('''
    DROP TABLE IF EXISTS dim_property_details;
    DROP TABLE IF EXISTS dim_property;
    DROP TABLE IF EXISTS dim_date;
    DROP TABLE IF EXISTS dim_location;
    DROP TABLE IF EXISTS fact_listing;
    --begin-sql 
    CREATE TABLE dim_property_details(
        details_id SERIAL PRIMARY KEY,
        zoned_for VARCHAR(50),
        facade VARCHAR(50),
        property_mortgaged VARCHAR(50),
        payment_method VARCHAR(50),
        subcategory VARCHAR(50),
        bedrooms VARCHAR(50),
        bathrooms VARCHAR(50),
        furnished VARCHAR(50),
        floor VARCHAR(50),
        building_age VARCHAR(50),
        number_of_floors VARCHAR(50)); --end-sql
        
        --begin-sql 
    CREATE TABLE dim_property(
        property_id SERIAL PRIMARY KEY,
        details_id INT,
        title VARCHAR(255),
        link VARCHAR(255),
        images TEXT,
        description TEXT,
        area INT,
        owner VARCHAR(255),
        owner_link VARCHAR(255),
        nearby TEXT,
        CONSTRAINT fk_property_details 
            FOREIGN KEY(details_id)
                REFERENCES dim_property_details(details_id)); --end-sql
        
        --begin-sql 
    CREATE TABLE dim_location(
        location_id SERIAL PRIMARY KEY,
        google_maps_locatoin_link VARCHAR(255),
        long FLOAT,
        lat FLOAT,
        city VARCHAR(255),
        neighborhood VARCHAR(255)); --end-sql
        
        --begin-sql 
    CREATE TABLE dim_date(
        date_id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP,
        year INT,
        month INT,
        day INT,
        hour INT,        
        minute INT,
        second INT); --end-sql
        
        --begin-sql 
    CREATE TABLE fact_listing(
        id INT PRIMARY KEY,
        property_id INT,
        location_id INT,
        date_id INT,
        price FLOAT,        
        CONSTRAINT fk_property
            FOREIGN KEY(property_id)
                REFERENCES dim_property(property_id),
        CONSTRAINT fk_location
            FOREIGN KEY(location_id)
                REFERENCES dim_location(location_id),
        CONSTRAINT fk_date
            FOREIGN KEY(date_id)
                REFERENCES dim_date(date_id)); --end-sql
        ''')
conn.commit()
cursor.close()
conn.close()
