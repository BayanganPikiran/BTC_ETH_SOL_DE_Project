"""
loading.py

This script is responsible for loading cryptocurrency data from CSV files into a PostgreSQL database.
It establishes a database connection, reads data from specified CSV files, and loads this data into
the corresponding database tables. The script is configured to run in different environments by
using environment variables.

Usage:
    Execute the script directly from the command line. Ensure that the .env file contains the correct
    database credentials and CSV file paths.
"""

import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os
import logging
from typing import Optional

load_dotenv()  # Load environment variables from .env
print(f"Debug: PG_HOST={os.getenv('PG_HOST')}, PG_PORT={os.getenv('PG_PORT')}")

# Database configuration
DB_HOST = os.getenv('PG_HOST')
DB_PORT = os.getenv('PG_PORT')
DB_USER = os.getenv('PG_USER')
DB_PASSWORD = os.getenv('PG_USER_PASSWORD')
DB_NAME = os.getenv('PG_DB_NAME')
# DB_NAME = 'crypto_etl'

# CSV file paths
BTC_CSV_PATH = os.getenv('BTC_CSV_PATH')
ETH_CSV_PATH = os.getenv('ETH_CSV_PATH')
SOL_CSV_PATH = os.getenv('SOL_CSV_PATH')

# Constants for logging
DEBUG_MODE = os.getenv('DEBUG_MODE', 'False').lower() == 'true'
LOG_LEVEL = logging.DEBUG if DEBUG_MODE else logging.INFO
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
LOG_FILE = 'loading_daily.log'

# Feature flag
DRY_RUN = os.getenv('DRY_RUN', 'False').lower() == 'true'



# Setup logging
logging.basicConfig(filename=LOG_FILE, level=LOG_LEVEL, format=LOG_FORMAT)


def create_db_connection() -> Optional[psycopg2.extensions.connection]:
    """
    Create and return a database connection.
    The connection details are obtained from environment variables.

    Returns:
        Optional[psycopg2.extensions.connection]: Database connection object if successful, None otherwise.
    """
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME,
            port=DB_PORT
        )
        logging.info("Database connection successfully established.")
        return connection
    except psycopg2.Error as e:
        logging.error(f"Error connecting to the PostgreSQL Database: {e}")
        return None


def load_daily_csv_to_db(csv_file_path: str, table_name: str, db_connection: psycopg2.extensions.connection) -> None:
    """
    Load data from a CSV file into a database table.
    """
    logging.info(f"Attempting to load data from {csv_file_path} into {table_name}")

    # Check if the CSV file exists
    if not os.path.exists(csv_file_path):
        logging.error(f"CSV file not found: {csv_file_path}")
        return

    try:
        # Read CSV file using pandas
        logging.info(f"Reading data from CSV file: {csv_file_path}")
        data = pd.read_csv(csv_file_path)
        logging.info(f"Successfully read {len(data)} rows from {csv_file_path}")

        # Convert DataFrame to list of tuples for SQL insertion
        data_tuples = [tuple(row) for row in data.to_numpy()]

        # SQL query for inserting data
        columns = ', '.join(data.columns.tolist())
        placeholders = ', '.join(['%s'] * len(data.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        logging.info(f"Prepared SQL query for inserting data into {table_name}")

        # Create a cursor object using the connection
        with db_connection.cursor() as cursor:
            # Execute the SQL command with the data
            logging.info(f"Executing SQL query to insert data into {table_name}")
            cursor.executemany(insert_query, data_tuples)
            logging.info(f"SQL query executed successfully")

            # Check if dry run is enabled
            if DRY_RUN:
                db_connection.rollback()  # Roll back transaction in dry run mode
                logging.info(f"Dry run: Data from {csv_file_path} not committed to {table_name}.")
            else:
                db_connection.commit()
                logging.info(f"Data from {csv_file_path} loaded into {table_name} successfully.")

    except Exception as e:
        db_connection.rollback()
        logging.error(f"Error occurred while loading data from {csv_file_path}: {e}")


if __name__ == "__main__":

    logging.info("Starting script execution...")

    # Check for dry run mode
    if DRY_RUN:
        logging.info("Running in DRY RUN mode. No changes will be committed to the database.")

    # Create a database connection
    db_connection = create_db_connection()
    if db_connection:
        try:
            # Load data from CSV files into database tables
            # In dry run mode, these operations will not commit any changes to the database
            load_daily_csv_to_db(BTC_CSV_PATH, 'BTC_Daily', db_connection)
            load_daily_csv_to_db(ETH_CSV_PATH, 'ETH_Daily', db_connection)
            load_daily_csv_to_db(SOL_CSV_PATH, 'SOL_Daily', db_connection)
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
        finally:
            # In dry run mode, the connection is rolled back and closed without committing
            if DRY_RUN:
                db_connection.rollback()
            logging.info("Database connection closed.")
    else:
        logging.error("Failed to establish a database connection.")

    logging.info("Script execution completed.")
