import pymysql
import pandas as pd
from dotenv import load_dotenv
import os
import logging
from typing import Optional

load_dotenv()  # Load environment variables from .env

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
BTC_CSV_PATH = os.getenv('BTC_CSV_PATH')
ETH_CSV_PATH = os.getenv('ETH_CSV_PATH')
SOL_CSV_PATH = os.getenv('SOL_CSV_PATH')



def create_db_connection() -> Optional[pymysql.connections.Connection]:
    """
    Create and return a database connection.

    Returns:
        pymysql.connections.Connection: Database connection object if successful, None otherwise.
    """
    try:
        connection = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            db=DB_NAME,
            port=int(DB_PORT)
        )
        logging.info("Database connection successfully established.")
        return connection
    except pymysql.MySQLError as e:
        logging.error(f"Error connecting to the MariaDB Database: {e}")
        return None


def load_csv_to_db(csv_file_path: str, table_name: str, db_connection: pymysql.connections.Connection) -> None:
    """
    Load data from a CSV file into a database table.

    Args:
        csv_file_path (str): Path to the CSV file.
        table_name (str): Name of the table to load data into.
        db_connection (pymysql.connections.Connection): Active database connection.
    """
    try:
        if not os.path.exists(csv_file_path):
            logging.error(f"CSV file not found: {csv_file_path}")
            return

        data = pd.read_csv(csv_file_path)
        data_tuples = [tuple(row) for row in data.to_numpy()]
        columns = ', '.join(data.columns.tolist())
        placeholders = ', '.join(['%s'] * len(data.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        with db_connection.cursor() as cursor:
            cursor.executemany(insert_query, data_tuples)
            db_connection.commit()
            logging.info(f"Data from {csv_file_path} loaded into {table_name} successfully.")

    except (pymysql.MySQLError, pd.errors.ParserError) as e:
        db_connection.rollback()
        logging.error(f"Error occurred while loading data: {e}")


def setup_logging() -> None:
    """
    Set up logging configuration.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        filename="loading_script.log",
        filemode="a"
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console.setFormatter(formatter)
    logging.getLogger("").addHandler(console)


if __name__ == "__main__":
    setup_logging()
    logging.info("Starting script execution...")

    with create_db_connection() as db_connection:
        if db_connection:
            try:
                load_csv_to_db('btc_data.csv', 'Bitcoin_records', db_connection)
                load_csv_to_db('eth_data.csv', 'Ethereum_records', db_connection)
                load_csv_to_db('sol_data.csv', 'Solana_records', db_connection)
            except Exception as e:
                logging.error(f"An unexpected error occurred: {e}")

    logging.info("Script execution completed.")
