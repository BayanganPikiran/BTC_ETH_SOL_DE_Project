"""
Data Loader for Hourly Trade Records of BTC, ETH, and SOL

This script is designed to load hourly trading data for cryptocurrencies (Bitcoin, Ethereum, Solana) into a PostgreSQL
database. It reads data from CSV files, validates the file paths, and inserts the data into specified database tables.

The script utilizes environment variables for database configuration and CSV file paths. It supports a dry run feature
for testing without committing data to the database.

Dependencies:
- psycopg2: For PostgreSQL database connection.
- pandas: For reading and processing CSV files.
- python-dotenv: For loading environment variables from a .env file.
- os: For file path and environment variable operations.
- logging: For logging information and errors.

Usage:
Set the required environment variables and run the script. The script reads the specified CSV files and loads the data into the database tables.

Author: Andre La Flamme
Date: January 11, 2024
"""


import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os
import logging
from typing import Optional

# Load environment variables from .env
load_dotenv()

# Database configuration
DB_HOST = os.getenv('PG_HOST')
DB_PORT = os.getenv('PG_PORT')
DB_USER = os.getenv('PG_USER')
DB_PASSWORD = os.getenv('PG_USER_PASSWORD')
DB_NAME = os.getenv('PG_DB_NAME')

# CSV file paths for hourly data
BTC_HOURLY_CSV_PATH = os.getenv('BTC_HOURLY_CSV_PATH')
ETH_HOURLY_CSV_PATH = os.getenv('ETH_HOURLY_CSV_PATH')
SOL_HOURLY_CSV_PATH = os.getenv('SOL_HOURLY_CSV_PATH')

# Feature flag
DRY_RUN = os.getenv('DRY_RUN', 'False').lower() == 'true'


def setup_logging() -> None:
     """
    Set up logging configuration.

    This function configures logging to output messages both to the console and a log file.
    It ensures that if the logging configuration is already set, it does not get overwritten.

    Logging Level: INFO
    Log Format: Timestamp, Log Level, and Message
    Log File: hourly_loading_script.log (appended mode)

    Returns:
    None
    """
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            filename="hourly_loading_script.log",
            filemode="a"
        )
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        console.setFormatter(formatter)
        logging.getLogger("").addHandler(console)


def create_db_connection():
    """
    Create and return a database connection using psycopg2.

    This function attempts to establish a connection to a PostgreSQL database using
    credentials obtained from environment variables. It logs the status of the connection attempt.

    Returns:
        psycopg2.extensions.connection: A database connection object if successful, None otherwise.
    """
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME,
            port=int(DB_PORT)
        )
        logging.info("Database connection successfully established.")
        return connection
    except psycopg2.Error as e:
        logging.error(f"Error connecting to the PostgreSQL Database: {e}")
        return None


def read_csv_data(csv_file_path: str) -> Optional[pd.DataFrame]:
    """
     Reads data from a CSV file into a pandas DataFrame.

     Args:
         csv_file_path (str): The file path of the CSV file to be read.

     Returns:
         Optional[pd.DataFrame]: DataFrame containing the data read from the CSV file,
         or None if an error occurs during file reading.

     This function logs the status of the file reading process and any errors encountered.
     """
    try:
        logging.info(f"Reading data from {csv_file_path}")
        data = pd.read_csv(csv_file_path)
        logging.info(f"Successfully read {len(data)} rows from {csv_file_path}")
        return data
    except Exception as e:
        logging.error(f"Error reading the CSV file {csv_file_path}: {e}")
        return None


def load_csv_to_db_hourly(csv_file_path: str, table_name: str, db_connection: psycopg2.extensions.connection) -> None:
    """
    Loads data from a CSV file into a PostgreSQL database table.

    This function reads cryptocurrency trading data from a specified CSV file and loads it into a given table.
    It handles data conversion and database insertion, with support for a dry run mode where changes are not committed.

    Args:
        csv_file_path (str): Path to the CSV file containing the hourly data.
        table_name (str): Name of the database table where data will be inserted.
        db_connection (psycopg2.extensions.connection): Active database connection.

    Returns:
    None

    The function logs the progress and any errors encountered during the loading process.
    """

    # Validate CSV File Path
    if not os.path.exists(csv_file_path):
        logging.error(f"CSV file not found: {csv_file_path}")
        return

    # Read CSV File into DataFrame using the new read_csv_data function
    data = read_csv_data(csv_file_path)
    if data is None or data.empty:
        logging.error(f"Error reading or empty data in the CSV file {csv_file_path}")
        return

    try:
        # Prepare SQL Insert Query
        columns = ', '.join(data.columns.tolist())
        placeholders = ', '.join(['%s'] * len(data.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Convert DataFrame to Tuples
        data_tuples = [tuple(row) for row in data.to_numpy()]

        # Database Insertion
        with db_connection.cursor() as cursor:
            cursor.executemany(insert_query, data_tuples)
            if DRY_RUN:
                db_connection.rollback()
                logging.info(f"Dry run: Data from {csv_file_path} not committed to {table_name}.")
            else:
                db_connection.commit()
                logging.info(f"Data from {csv_file_path} loaded into {table_name} successfully.")

    except Exception as e:
        if db_connection:
            db_connection.rollback()
        logging.error(f"An unexpected error occurred in load_csv_to_db_hourly: {e}")


if __name__ == '__main__':
    setup_logging()
    db_connection = create_db_connection()

    if db_connection is None:
        logging.error("Failed to establish database connection. Exiting script.")
    else:
        crypto_csv_paths = {
            'BTC': BTC_HOURLY_CSV_PATH,
            'ETH': ETH_HOURLY_CSV_PATH,
            'SOL': SOL_HOURLY_CSV_PATH
        }

        for coin_symbol, csv_path in crypto_csv_paths.items():
            # Include data reading and validation logic here (if separate functions are created)
            load_csv_to_db_hourly(csv_path, f"{coin_symbol}_hourly_table", db_connection)

        # Close the database connection
        db_connection.close()
        logging.info("Database connection closed.")
