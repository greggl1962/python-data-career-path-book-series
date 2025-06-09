# book1_etl_local.py
#
# This script is the capstone project for "Data Analysis and Engineering for Beginners."
# It performs a complete ETL process:
# 1. Extracts data from a public API (JSONPlaceholder).
# 2. Transforms the data using Pandas (cleans data, derives new columns).
# 3. Loads the clean data into a local PostgreSQL database.

import requests
import pandas as pd
from sqlalchemy import create_engine
import logging

# --- Configuration ---
# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# API endpoint for user data
API_URL = "https://jsonplaceholder.typicode.com/users"

# PostgreSQL Database connection details
# IMPORTANT: Replace with your actual database credentials
DB_USER = "your_db_user"
DB_PASSWORD = "your_db_password"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "your_db_name"
TABLE_NAME = "users"

# Create the database connection string and engine
DATABASE_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URI)


def extract_data():
    """Extracts data from the JSONPlaceholder API."""
    logging.info(f"Extracting data from {API_URL}...")
    try:
        response = requests.get(API_URL)
        # Raise an exception if the request was unsuccessful
        response.raise_for_status()
        data = response.json()
        logging.info("Data extraction successful.")
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"Error during data extraction: {e}")
        return None

def transform_data(data):
    """Transforms the raw user data using Pandas."""
    if data is None:
        logging.warning("No data to transform.")
        return None

    logging.info("Transforming data...")
    # Convert the list of dictionaries to a Pandas DataFrame
    df = pd.DataFrame(data)

    # 1. Select and rename columns for clarity
    df_transformed = df[['id', 'name', 'username', 'email', 'phone', 'website']]
    df_transformed = df_transformed.rename(columns={
        'id': 'user_id',
        'name': 'full_name'
    })

    # 2. Handle missing data (though this sample data is clean)
    # df_transformed.fillna({'phone': 'N/A'}, inplace=True)

    # 3. Derive a new column: extract the domain from the email
    df_transformed['email_domain'] = df_transformed['email'].apply(lambda x: x.split('@')[1])

    # 4. Standardize text data (e.g., convert username to lowercase)
    df_transformed['username'] = df_transformed['username'].str.lower()
    
    logging.info("Data transformation complete.")
    df_transformed.info()
    return df_transformed

def load_data(df):
    """Loads the transformed DataFrame into the PostgreSQL database."""
    if df is None:
        logging.warning("No data to load.")
        return

    logging.info(f"Loading data into PostgreSQL table: {TABLE_NAME}...")
    try:
        # Use pandas.to_sql to load the data
        # 'if_exists='replace'' will drop the table if it exists and create a new one.
        # Use 'append' if you want to add data to an existing table.
        df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
        logging.info("Data loaded successfully.")
    except Exception as e:
        logging.error(f"Error during data loading: {e}")

def main():
    """Main function to run the ETL pipeline."""
    logging.info("Starting ETL pipeline...")
    
    # Extract
    raw_data = extract_data()
    
    # Transform
    transformed_data = transform_data(raw_data)
    
    # Load
    load_data(transformed_data)
    
    logging.info("ETL pipeline finished successfully.")

if __name__ == "__main__":
    main()
