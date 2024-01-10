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


def load_csv_to_db_hourly(csv_file_path: str, table_name: str, db_connection: psycopg2.extensions.connection) -> None:
    try:
        # Validate CSV File Path
        if not os.path.exists(csv_file_path):
            logging.error(f"CSV file not found: {csv_file_path}")
            return

        # Read CSV File into DataFrame
        try:
            data = pd.read_csv(csv_file_path)
        except Exception as e:
            logging.error(f"Error reading the CSV file {csv_file_path}: {e}")
            return

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


