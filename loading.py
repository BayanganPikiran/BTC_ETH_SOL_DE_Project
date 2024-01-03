import pymysql
import pandas as pd
from dotenv import load_dotenv
import os
import logging

load_dotenv()  # This loads the environment variables from .env

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

# Load environment variables
load_dotenv()


def create_db_connection():
    try:
        connection = pymysql.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            db=os.getenv('DB_NAME'),
            port=int(os.getenv('DB_PORT'))
        )
        print("Database connection successfully established.")
        return connection
    except pymysql.MySQLError as e:
        print(f"Error connecting to the MariaDB Database: {e}")
        return None


def load_csv_to_db(csv_file_path, table_name, db_connection):
    try:
        # Read CSV file using pandas
        data = pd.read_csv(csv_file_path)

        # Convert DataFrame to list of tuples for SQL insertion
        data_tuples = [tuple(row) for row in data.to_numpy()]

        # SQL query for inserting data
        # The placeholders (%s) will be replaced by the actual data in `executemany` method
        # Adjust the number of %s placeholders to match the number of columns in your table
        columns = ', '.join(data.columns.tolist())
        placeholders = ', '.join(['%s'] * len(data.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Create a cursor object using the connection
        with db_connection.cursor() as cursor:
            # Execute the SQL command with the data
            cursor.executemany(insert_query, data_tuples)
            db_connection.commit()
            print(f"Data from {csv_file_path} loaded into {table_name} successfully.")

    except Exception as e:
        db_connection.rollback()
        print(f"Error occurred: {e}")


def load_csv_to_db(csv_file_path, table_name, db_connection):
    try:
        # Read CSV file using pandas
        data = pd.read_csv(csv_file_path)

        # Convert DataFrame to list of tuples for SQL insertion
        data_tuples = [tuple(row) for row in data.to_numpy()]

        # SQL query for inserting data
        columns = ', '.join(data.columns.tolist())
        placeholders = ', '.join(['%s'] * len(data.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Create a cursor object using the connection
        with db_connection.cursor() as cursor:
            # Execute the SQL command with the data
            cursor.executemany(insert_query, data_tuples)
            db_connection.commit()
            print(f"Data from {csv_file_path} loaded into {table_name} successfully.")

    except Exception as e:
        db_connection.rollback()
        print(f"Error occurred: {e}")
def load_csv_to_db(csv_file_path, table_name, db_connection):
    try:
        # Read CSV file using pandas
        data = pd.read_csv(csv_file_path)

        # Convert DataFrame to list of tuples for SQL insertion
        data_tuples = [tuple(row) for row in data.to_numpy()]

        # SQL query for inserting data
        columns = ', '.join(data.columns.tolist())
        placeholders = ', '.join(['%s'] * len(data.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Create a cursor object using the connection
        with db_connection.cursor() as cursor:
            # Execute the SQL command with the data
            cursor.executemany(insert_query, data_tuples)
            db_connection.commit()
            print(f"Data from {csv_file_path} loaded into {table_name} successfully.")

    except Exception as e:
        db_connection.rollback()
        print(f"Error occurred: {e}")


    except Exception as e:
        db_connection.rollback()
        print(f"Error occurred: {e}")


# Setup logging
def setup_logging():
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

    db_connection = create_db_connection()
    if db_connection:
        try:
            load_csv_to_db('btc_data.csv', 'Bitcoin_records', db_connection)
            load_csv_to_db('eth_data.csv', 'Ethereum_records', db_connection)
            load_csv_to_db('sol_data.csv', 'Solana_records', db_connection)
        except Exception as e:
            logging.error(f"An error occurred: {e}")
        finally:
            db_connection.close()
            logging.info("Database connection closed.")

    logging.info("Script execution completed.")

