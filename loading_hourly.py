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
    Placeholder for future docstring.
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
    Placeholder for future docstring.
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


def load_csv_to_db_hourly(csv_file_path: str, table_name: str, db_connection: psycopg2.extensions.connection) -> None:
    """
    Placeholder for future docstring.
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
