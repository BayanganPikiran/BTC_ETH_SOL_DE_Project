import pymysql
import pandas as pd
from dotenv import load_dotenv
import os


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



# # Example usage
# db_connection = create_db_connection()
# if db_connection is not None:
#     # Proceed with operations using db_connection
#     db_connection.close()
